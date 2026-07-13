package controller

import (
	"testing"
	"time"

	"github.com/szymonrychu/tatara-operator/internal/scm"
)

func tc(author string, minsAgo int) scm.IssueComment {
	return scm.IssueComment{Author: author, CreatedAt: time.Unix(1_700_000_000, 0).Add(time.Duration(minsAgo) * time.Minute)}
}

func TestBotHasLastWordAmong(t *testing.T) {
	const bot = "tatara-bot"
	approvers := []string{"maintainer"}
	tests := []struct {
		name     string
		comments []scm.IssueComment
		breakers []string
		want     bool
	}{
		{"no comments", nil, approvers, false},
		{"bot never spoke", []scm.IssueComment{tc("maintainer", 1)}, approvers, false},
		{"bot last, silent", []scm.IssueComment{tc("maintainer", 1), tc(bot, 2)}, approvers, true},
		{"approver after bot, open", []scm.IssueComment{tc(bot, 1), tc("maintainer", 2)}, approvers, false},
		{"third party after bot ignored, silent", []scm.IssueComment{tc(bot, 1), tc("random", 2)}, approvers, true},
		{"third party after bot with empty breakers, open", []scm.IssueComment{tc(bot, 1), tc("random", 2)}, nil, false},
		{"unordered slice, bot newest", []scm.IssueComment{tc(bot, 5), tc("maintainer", 2), tc("random", 4)}, approvers, true},
		{"empty author skipped", []scm.IssueComment{tc(bot, 1), tc("", 2)}, approvers, true},
		{"same-timestamp breaker favours posting", []scm.IssueComment{{Author: bot, CreatedAt: time.Unix(1_700_000_000, 0)}, {Author: "maintainer", CreatedAt: time.Unix(1_700_000_000, 0)}}, approvers, false},
		{"zero timestamp breaker ignored, bot silent", []scm.IssueComment{tc(bot, 1), {Author: "maintainer"}}, approvers, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := botHasLastWordAmong(tt.comments, bot, tt.breakers); got != tt.want {
				t.Fatalf("botHasLastWordAmong = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNormalizeCommentBody_StripsVolatileTokens(t *testing.T) {
	a := normalizeCommentBody("Turn 3: done at 2026-07-11T10:00:00Z, see above")
	b := normalizeCommentBody("Turn 9: done at 2026-07-11T18:22:01Z, see above")
	if a != b {
		t.Fatalf("normalized forms should match after stripping turn/timestamp: %q vs %q", a, b)
	}
}

func TestDuplicateRecentBotComment_MatchesNormalizedBody(t *testing.T) {
	comments := []scm.IssueComment{{Author: "tatara-bot", Body: "Done.  Opened PR: foo"}}
	if !duplicateRecentBotComment(comments, "tatara-bot", "done. opened pr: foo") {
		t.Fatal("want duplicate match on normalized body")
	}
	if duplicateRecentBotComment(comments, "tatara-bot", "a completely different body") {
		t.Fatal("distinct body must not match")
	}
}

func TestPermitComment(t *testing.T) {
	const bot = "tatara-bot"
	botLast := []scm.IssueComment{tc("human", 5), tc(bot, 10)}
	humanLast := []scm.IssueComment{tc(bot, 5), tc("human", 10)}
	tests := []struct {
		name       string
		kind       string
		comments   []scm.IssueComment
		botLogin   string
		wantPermit bool
		wantReason string
	}{
		{"non-refine refused under bot last word", "implement", botLast, bot, false, "bot_last_word"},
		{"clarify cannot answer its own comment", "clarify", botLast, bot, false, "bot_last_word"},
		{"review refused under bot last word", "review", botLast, bot, false, "bot_last_word"},
		{"refine may answer bot last word", "refine", botLast, bot, true, ""},
		{"incident carve-out always permits", "incident", botLast, bot, true, ""},
		{"non-refine permitted when human has last word", "implement", humanLast, bot, true, ""},
		{"refine permitted when human has last word", "refine", humanLast, bot, true, ""},
		{"fail open on empty bot login", "implement", botLast, "", true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			permit, reason := PermitComment(tt.kind, tt.comments, tt.botLogin, nil)
			if permit != tt.wantPermit || reason != tt.wantReason {
				t.Fatalf("PermitComment(%q)=(%v,%q), want (%v,%q)", tt.kind, permit, reason, tt.wantPermit, tt.wantReason)
			}
		})
	}
}
