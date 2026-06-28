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
