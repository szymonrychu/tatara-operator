package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

func TestPrInReactionScope(t *testing.T) {
	proj := &tatarav1alpha1.Project{Spec: tatarav1alpha1.ProjectSpec{
		TriggerLabel: "tatara",
		Scm: &tatarav1alpha1.ScmSpec{
			PRReactionScope:  "labeledOrMentioned",
			BotLogin:         "szymonrychu-bot",
			MaintainerLogins: []string{"szymonrychu"},
		},
	}}
	cases := []struct {
		name string
		c    candidate
		want bool
	}{
		{"trusted author bypasses scope", candidate{author: "szymonrychu"}, true},
		{"third party unlabeled skipped", candidate{author: "x"}, false},
		{"third party labeled passes", candidate{author: "x", labels: []string{"tatara"}}, true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := prInReactionScope(proj, nil, tt.c, "szymonrychu-bot"); got != tt.want {
				t.Errorf("got %v want %v", got, tt.want)
			}
		})
	}
}

// seedScanProjectWithScope creates a project+repo pair with the given
// prReactionScope and trigger label.
func seedScanProjectWithScope(t *testing.T, name, scope, triggerLabel string) (*tatarav1alpha1.Project, *tatarav1alpha1.Repository) {
	t.Helper()
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 10}}
	proj, repo := seedScanProject(t, name, cron)
	proj.Spec.TriggerLabel = triggerLabel
	if scope != "" {
		proj.Spec.Scm.PRReactionScope = scope
	}
	return proj, repo
}

// TestMRScan_LabeledOrMentioned_SkipsUnlabeledHumanMR: under
// prReactionScope=labeledOrMentioned an unlabeled, un-mentioned human MR must
// not create a review Task (the !1090 pattern).
func TestMRScan_LabeledOrMentioned_SkipsUnlabeledHumanMR(t *testing.T) {
	proj, repo := seedScanProjectWithScope(t, "scope-skip", "labeledOrMentioned", "tatara/review")
	repos := []tatarav1alpha1.Repository{*repo}
	reader := &fakeReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 1, Author: "human", HeadSHA: "sha1",
			UpdatedAt: time.Unix(100, 0)}, // no label, no @mention
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	cron := tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 10}
	r.mrScan(context.Background(), proj, reader, repos, nil, cron)

	qes := listScanQEs(t, "scope-skip")
	for _, qe := range qes {
		if qe.Spec.Kind == "review" {
			t.Fatalf("unlabeled/un-mentioned MR must NOT get a review Task under labeledOrMentioned scope; got QE kind=%q", qe.Spec.Kind)
		}
	}
}

// TestMRScan_LabeledOrMentioned_ReviewsLabeledMR: trigger label present -> Task created.
func TestMRScan_LabeledOrMentioned_ReviewsLabeledMR(t *testing.T) {
	proj, repo := seedScanProjectWithScope(t, "scope-label", "labeledOrMentioned", "tatara/review")
	repos := []tatarav1alpha1.Repository{*repo}
	reader := &fakeReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 2, Author: "human", HeadSHA: "sha2",
			Labels: []string{"tatara/review"}, UpdatedAt: time.Unix(100, 0)},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	cron := tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 10}
	r.mrScan(context.Background(), proj, reader, repos, nil, cron)

	qes := listScanQEs(t, "scope-label")
	reviewFound := false
	for _, qe := range qes {
		if qe.Spec.Kind == "review" {
			reviewFound = true
		}
	}
	if !reviewFound {
		t.Fatal("labeled MR must create a review Task under labeledOrMentioned scope")
	}
}

// TestMRScan_LabeledOrMentioned_ReviewsMentionedMR: body @-mentions bot -> Task created.
func TestMRScan_LabeledOrMentioned_ReviewsMentionedMR(t *testing.T) {
	proj, repo := seedScanProjectWithScope(t, "scope-mention", "labeledOrMentioned", "tatara/review")
	repos := []tatarav1alpha1.Repository{*repo}
	reader := &fakeReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 3, Author: "human", HeadSHA: "sha3",
			Body: "please @tatara-bot review this", UpdatedAt: time.Unix(100, 0)},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	cron := tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 10}
	r.mrScan(context.Background(), proj, reader, repos, nil, cron)

	qes := listScanQEs(t, "scope-mention")
	reviewFound := false
	for _, qe := range qes {
		if qe.Spec.Kind == "review" {
			reviewFound = true
		}
	}
	if !reviewFound {
		t.Fatal("@-mentioned MR must create a review Task under labeledOrMentioned scope")
	}
}

// TestMRScan_OutOfScopeMR_NoBacklog: a single out-of-scope human PR (unlabeled,
// un-mentioned, scope=labeledOrMentioned) must not freeze the mrScan stamp.
// Before the fix, created(0) < len(eligible)(1) returned backlog=true even
// though the only "work" was a terminal scope skip - causing LastMRScan to
// stall and mrScan to re-fire ~18x its schedule (skipped_scope storm).
func TestMRScan_OutOfScopeMR_NoBacklog(t *testing.T) {
	proj, repo := seedScanProjectWithScope(t, "scope-nobacklog", "labeledOrMentioned", "tatara/review")
	repos := []tatarav1alpha1.Repository{*repo}
	reader := &fakeReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 10, Author: "human", HeadSHA: "sha10",
			UpdatedAt: time.Unix(100, 0)}, // no label, no @mention -> skipped_scope
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	cron := tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 10}
	// out-of-scope-only cycle must not freeze the mrScan stamp (backlog == false)
	backlog := r.mrScan(context.Background(), proj, reader, repos, nil, cron)
	if backlog {
		t.Fatal("out-of-scope-only mrScan cycle must return backlog=false; got true (stamp will freeze)")
	}
}

// TestMRScan_DefaultScope_ReviewsUnlabeledMR: no scope set -> unlabeled human MR
// is reviewed (existing behavior preserved).
func TestMRScan_DefaultScope_ReviewsUnlabeledMR(t *testing.T) {
	proj, repo := seedScanProjectWithScope(t, "scope-default", "", "tatara/review")
	repos := []tatarav1alpha1.Repository{*repo}
	reader := &fakeReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 4, Author: "human", HeadSHA: "sha4",
			UpdatedAt: time.Unix(100, 0)}, // no label, no @mention
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	cron := tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 10}
	r.mrScan(context.Background(), proj, reader, repos, nil, cron)

	qes := listScanQEs(t, "scope-default")
	reviewFound := false
	for _, qe := range qes {
		if qe.Spec.Kind == "review" {
			reviewFound = true
		}
	}
	if !reviewFound {
		t.Fatal("unlabeled MR must still create a review Task when prReactionScope is empty (default behavior)")
	}
}

// TestIssueScan_BotLastWord_NoBacklog: when the only issueScan candidate is
// blocked by the bot-last-word gate (a terminal skip), the returned backlog must
// be false. Before the fix, the old created<len(eligible) heuristic would have
// returned backlog=true here (1 eligible, 0 created), freezing LastIssueScan.
func TestIssueScan_BotLastWord_NoBacklog(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 10}}
	proj, repo := seedScanProject(t, "issuescan-blw-nobacklog", cron)
	repos := []tatarav1alpha1.Repository{*repo}
	// Bot (tatara-bot) authored the most recent comment -> botHadLastWord fires -> skipped_bot_last_word.
	reader := &fakeReader{
		issues: []scm.IssueRef{
			{Repo: "o/r", Number: 1, Author: "human", UpdatedAt: time.Unix(100, 0)},
		},
		comments: []scm.IssueComment{
			{Author: "tatara-bot", CreatedAt: time.Unix(200, 0)},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	backlog, _ := r.issueScan(context.Background(), proj, reader, repos, nil, cron.IssueScan)
	if backlog {
		t.Fatal("bot-last-word-only issueScan cycle must return backlog=false; got true (stamp will freeze)")
	}
}
