package controller

// Issue #188: tatara-operator must recognize that tatara itself posted the last
// comment under an issue/PR/MR and skip scheduling agents in a constant loop.
// These tests pin the scan-time bot-last-word gate (botIsLastCommenter /
// botHadLastWord) and its wiring into issueScan and mrScan.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

func blwCmt(author string, ts int64) scm.IssueComment {
	return scm.IssueComment{Author: author, CreatedAt: time.Unix(ts, 0)}
}

// lastWordReader is a per-number SCM reader that distinguishes issue comments
// from PR/MR comments and implements scm.PRCommentLister, so a PR candidate is
// routed to ListPRComments and an issue candidate to ListIssueComments.
type lastWordReader struct {
	fakeReader
	issueComments map[int][]scm.IssueComment
	prComments    map[int][]scm.IssueComment
	issueErr      error
	prErr         error
	issueCalls    int
	prCalls       int
}

func (r *lastWordReader) ListIssueComments(_ context.Context, _, _ string, number int) ([]scm.IssueComment, error) {
	r.issueCalls++
	return r.issueComments[number], r.issueErr
}

func (r *lastWordReader) ListPRComments(_ context.Context, _, _ string, number int) ([]scm.IssueComment, error) {
	r.prCalls++
	return r.prComments[number], r.prErr
}

func TestBotIsLastCommenter(t *testing.T) {
	cases := []struct {
		name     string
		comments []scm.IssueComment
		bot      string
		want     bool
	}{
		{"no comments", nil, "bot", false},
		{"bot last (ordered)", []scm.IssueComment{blwCmt("alice", 1), blwCmt("bot", 2)}, "bot", true},
		{"human last (ordered)", []scm.IssueComment{blwCmt("bot", 1), blwCmt("alice", 2)}, "bot", false},
		{"bot last regardless of slice order", []scm.IssueComment{blwCmt("bot", 9), blwCmt("alice", 3)}, "bot", true},
		{"human last regardless of slice order", []scm.IssueComment{blwCmt("alice", 9), blwCmt("bot", 3)}, "bot", false},
		{"single bot comment", []scm.IssueComment{blwCmt("bot", 5)}, "bot", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := botIsLastCommenter(tc.comments, tc.bot); got != tc.want {
				t.Fatalf("botIsLastCommenter = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBotHadLastWord(t *testing.T) {
	ctx := context.Background()
	const bot = "tatara-bot"

	t.Run("issue: bot last -> true via ListIssueComments", func(t *testing.T) {
		r := &lastWordReader{issueComments: map[int][]scm.IssueComment{5: {blwCmt("alice", 1), blwCmt(bot, 2)}}}
		c := candidate{repo: "o/r", number: 5}
		if !botHadLastWord(ctx, r, c, bot) {
			t.Fatal("want true")
		}
		if r.issueCalls != 1 || r.prCalls != 0 {
			t.Fatalf("issue candidate must use ListIssueComments only (issueCalls=%d prCalls=%d)", r.issueCalls, r.prCalls)
		}
	})

	t.Run("issue: human last -> false", func(t *testing.T) {
		r := &lastWordReader{issueComments: map[int][]scm.IssueComment{5: {blwCmt(bot, 1), blwCmt("alice", 2)}}}
		if botHadLastWord(ctx, r, candidate{repo: "o/r", number: 5}, bot) {
			t.Fatal("want false")
		}
	})

	t.Run("PR: routed to ListPRComments, not the issue timeline", func(t *testing.T) {
		// PR timeline says bot is last; issue timeline says a human is last. The
		// gate must read the PR/MR timeline (proving GitLab MR notes are honored).
		r := &lastWordReader{
			prComments:    map[int][]scm.IssueComment{9: {blwCmt("alice", 1), blwCmt(bot, 2)}},
			issueComments: map[int][]scm.IssueComment{9: {blwCmt(bot, 1), blwCmt("alice", 2)}},
		}
		c := candidate{repo: "o/r", number: 9, isPR: true}
		if !botHadLastWord(ctx, r, c, bot) {
			t.Fatal("want true (bot last on the PR/MR timeline)")
		}
		if r.prCalls != 1 || r.issueCalls != 0 {
			t.Fatalf("PR candidate must use ListPRComments only (prCalls=%d issueCalls=%d)", r.prCalls, r.issueCalls)
		}
	})

	t.Run("PR: reader without PRCommentLister falls back to ListIssueComments", func(t *testing.T) {
		// Plain fakeReader does not implement scm.PRCommentLister (GitHub semantics:
		// a PR comment is an issue comment), so the gate falls back gracefully.
		r := &fakeReader{comments: []scm.IssueComment{blwCmt("alice", 1), blwCmt(bot, 2)}}
		c := candidate{repo: "o/r", number: 9, isPR: true}
		if !botHadLastWord(ctx, r, c, bot) {
			t.Fatal("want true via ListIssueComments fallback")
		}
	})

	t.Run("empty botLogin -> false, no SCM read", func(t *testing.T) {
		r := &lastWordReader{issueComments: map[int][]scm.IssueComment{5: {blwCmt("anyone", 2)}}}
		if botHadLastWord(ctx, r, candidate{repo: "o/r", number: 5}, "") {
			t.Fatal("want false")
		}
		if r.issueCalls != 0 {
			t.Fatalf("empty botLogin must short-circuit before any SCM read, got issueCalls=%d", r.issueCalls)
		}
	})

	t.Run("nil reader -> false", func(t *testing.T) {
		if botHadLastWord(ctx, nil, candidate{repo: "o/r", number: 5}, bot) {
			t.Fatal("want false")
		}
	})

	t.Run("unsplittable repo -> false", func(t *testing.T) {
		r := &lastWordReader{issueComments: map[int][]scm.IssueComment{5: {blwCmt(bot, 2)}}}
		if botHadLastWord(ctx, r, candidate{repo: "norepo", number: 5}, bot) {
			t.Fatal("want false")
		}
	})

	t.Run("read error -> false (fail-open)", func(t *testing.T) {
		r := &lastWordReader{issueErr: errors.New("boom")}
		if botHadLastWord(ctx, r, candidate{repo: "o/r", number: 5}, bot) {
			t.Fatal("want false on read error (fail-open, matches humanCommentAfter)")
		}
	})
}

// TestIssueScan_SkipsWhenBotHadLastWord verifies issueScan does not spawn a fresh
// agent for a dormant issue whose most recent comment is the bot's, and DOES spawn
// one once a human replies. This is the GitLab-issue half of issue #188 (the loop
// is provider-agnostic; the gate sits in the shared scan path).
func TestIssueScan_SkipsWhenBotHadLastWord(t *testing.T) {
	const bot = "tatara-bot"

	t.Run("bot last word -> no QE, metric fires", func(t *testing.T) {
		const projName = "blw-issue-skip"
		cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 5}}
		proj, repoObj := seedScanProject(t, projName, cron)
		reader := &lastWordReader{
			fakeReader:    fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 5, Author: "human", UpdatedAt: time.Unix(100, 0)}}},
			issueComments: map[int][]scm.IssueComment{5: {blwCmt("human", 1), blwCmt(bot, 2)}},
		}
		r := newScanReconciler(reader)
		reg := prometheus.NewRegistry()
		r.Metrics = obs.NewOperatorMetrics(reg)

		_, _ = r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{*repoObj}, nil, cron.IssueScan)

		if qes := listScanQEs(t, projName); len(qes) != 0 {
			t.Fatalf("want 0 QEs (bot had last word), got %d", len(qes))
		}
		cnt := counterValue(t, reg, "tatara_scan_items_total",
			map[string]string{"activity": "issueScan", "outcome": "skipped_bot_last_word"})
		require.GreaterOrEqual(t, cnt, float64(1), "skipped_bot_last_word must fire")
	})

	t.Run("human last word -> QE created", func(t *testing.T) {
		const projName = "blw-issue-create"
		cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 5}}
		proj, repoObj := seedScanProject(t, projName, cron)
		reader := &lastWordReader{
			fakeReader:    fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 6, Author: "human", UpdatedAt: time.Unix(100, 0)}}},
			issueComments: map[int][]scm.IssueComment{6: {blwCmt(bot, 1), blwCmt("human", 2)}},
		}
		r := newScanReconciler(reader)
		r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

		_, _ = r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{*repoObj}, nil, cron.IssueScan)

		qes := listScanQEs(t, projName)
		if len(qes) != 1 {
			t.Fatalf("want 1 QE (human replied), got %d", len(qes))
		}
	})
}

// TestMRScan_BotPR_SkipsWhenBotHadLastWord verifies that a bot-authored PR whose
// most recent comment is the bot's (e.g. an MRCI park comment) is NOT re-created
// every cron cycle, and IS picked up again once a human replies. This is the
// GitHub-PR half of issue #188.
func TestMRScan_BotPR_SkipsWhenBotHadLastWord(t *testing.T) {
	const bot = "tatara-bot"

	t.Run("bot last word -> no QE, metric fires", func(t *testing.T) {
		const projName = "blw-mr-skip"
		cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 5}}
		proj, repoObj := seedScanProject(t, projName, cron)
		reader := &lastWordReader{
			fakeReader: fakeReader{prs: []scm.PRRef{
				{Repo: "o/r", Number: 9, Author: bot, HeadSHA: "abc", UpdatedAt: time.Unix(100, 0)},
			}},
			prComments: map[int][]scm.IssueComment{9: {blwCmt("human", 1), blwCmt(bot, 2)}},
		}
		r := newScanReconciler(reader)
		reg := prometheus.NewRegistry()
		r.Metrics = obs.NewOperatorMetrics(reg)

		r.mrScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{*repoObj}, nil, cron.MRScan)

		if qes := listScanQEs(t, projName); len(qes) != 0 {
			t.Fatalf("want 0 QEs (bot had last word on the PR), got %d", len(qes))
		}
		cnt := counterValue(t, reg, "tatara_scan_items_total",
			map[string]string{"activity": "mrScan", "outcome": "skipped_bot_last_word"})
		require.GreaterOrEqual(t, cnt, float64(1), "skipped_bot_last_word must fire")
	})

	t.Run("human last word -> QE created", func(t *testing.T) {
		const projName = "blw-mr-create"
		cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 5}}
		proj, repoObj := seedScanProject(t, projName, cron)
		reader := &lastWordReader{
			fakeReader: fakeReader{prs: []scm.PRRef{
				{Repo: "o/r", Number: 11, Author: bot, HeadSHA: "abc", UpdatedAt: time.Unix(100, 0)},
			}},
			prComments: map[int][]scm.IssueComment{11: {blwCmt(bot, 1), blwCmt("human", 2)}},
		}
		r := newScanReconciler(reader)
		r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

		r.mrScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{*repoObj}, nil, cron.MRScan)

		qes := listScanQEs(t, projName)
		if len(qes) != 1 {
			t.Fatalf("want 1 QE (human replied), got %d", len(qes))
		}
		if src := qes[0].Spec.Payload.Source; src == nil || src.Number != 11 {
			t.Fatalf("want QE for PR #11, got %+v", qes[0].Spec.Payload.Source)
		}
	})
}

// compile-time guard: the embedded-reader helper satisfies both interfaces.
var (
	_ scm.SCMReader       = (*lastWordReader)(nil)
	_ scm.PRCommentLister = (*lastWordReader)(nil)
)
