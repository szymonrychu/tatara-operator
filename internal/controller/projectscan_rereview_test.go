package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// mkReviewTask builds a terminal review Task for a human MR/PR, carrying a
// role:reviewed ledger entry with the given head SHA (as the cron backstop or
// the creation-time seed populates it). This is the real shape of a completed
// review Task, distinct from a bot-opened role:openedPR entry.
func mkReviewTask(repo string, number int, headSHA, provider string) tatarav1alpha1.Task {
	sep := "#"
	if provider == "gitlab" {
		sep = "!"
	}
	tk := tatarav1alpha1.Task{}
	tk.Spec.Source = &tatarav1alpha1.TaskSource{
		Provider: provider,
		IssueRef: fmt.Sprintf("%s%s%d", repo, sep, number),
		Number:   number,
		IsPR:     true,
	}
	tk.Status.Phase = "Succeeded"
	tk.Status.WorkItems = []tatarav1alpha1.WorkItemRef{{
		Provider: provider,
		Repo:     repo,
		Number:   number,
		Kind:     tatarav1alpha1.WorkItemPR,
		Role:     tatarav1alpha1.RoleReviewed,
		State:    tatarav1alpha1.WIOpen,
		HeadSHA:  headSHA,
	}}
	return tk
}

// TestHeadSHAForTask_FromReviewedEntry: a review Task's head SHA lives on the
// role:reviewed ledger entry, not role:openedPR. headSHAForTask must read it,
// otherwise same-head re-review dedup silently fails and the bot re-reviews the
// same MR every scan cycle (the !1090 token-burn loop).
func TestHeadSHAForTask_FromReviewedEntry(t *testing.T) {
	tk := mkReviewTask("o/r", 1090, "sha-head", "gitlab")
	if got := headSHAForTask(&tk); got != "sha-head" {
		t.Fatalf("headSHAForTask on review task = %q, want sha-head", got)
	}
}

// TestIsDeduped_ReviewTask_SameHead_Deduped: a terminal review Task at the
// candidate's current head must suppress a re-review.
func TestIsDeduped_ReviewTask_SameHead_Deduped(t *testing.T) {
	existing := []tatarav1alpha1.Task{mkReviewTask("o/r", 1090, "sha-head", "gitlab")}
	managed := managedPhaseLabels(nil)
	c := candidate{repo: "o/r", number: 1090, headSHA: "sha-head", isPR: true}
	if !isDeduped(c, existing, managed, nil) {
		t.Fatal("terminal review task at same head must dedup (no re-review)")
	}
}

// TestIsDeduped_ReviewTask_HeadChanged_Eligible: a new head commit must release
// the dedup so the bot re-reviews the new code.
func TestIsDeduped_ReviewTask_HeadChanged_Eligible(t *testing.T) {
	existing := []tatarav1alpha1.Task{mkReviewTask("o/r", 1090, "sha-old", "gitlab")}
	managed := managedPhaseLabels(nil)
	c := candidate{repo: "o/r", number: 1090, headSHA: "sha-new", isPR: true}
	if isDeduped(c, existing, managed, nil) {
		t.Fatal("review task at a changed head must be eligible for re-review")
	}
}

// TestSeedLedgerFromSpec_ReviewTask_RecordsHeadSHA: the review Task's head SHA
// must be seeded onto the role:reviewed entry at creation (from Spec.Source),
// so the next scan dedups WITHOUT waiting for the cron backstop to backfill it.
func TestSeedLedgerFromSpec_ReviewTask_RecordsHeadSHA(t *testing.T) {
	tk := tatarav1alpha1.Task{}
	tk.Spec.Source = &tatarav1alpha1.TaskSource{
		Provider: "gitlab",
		IssueRef: "o/r!1090",
		Number:   1090,
		IsPR:     true,
		HeadSHA:  "sha-head",
	}
	seedLedgerFromSpec(&tk)
	if got := headSHAForTask(&tk); got != "sha-head" {
		t.Fatalf("seeded review ledger head = %q, want sha-head", got)
	}
}

// TestMRScan_GitLab_UnlabeledMR_LabeledOrMentioned_NotReviewed reproduces the
// !1090 incident: a GitLab project with prReactionScope=labeledOrMentioned and
// an unlabeled, un-mentioned human MR must NOT spawn a review Task.
func TestMRScan_GitLab_UnlabeledMR_LabeledOrMentioned_NotReviewed(t *testing.T) {
	proj, repo := seedScanProjectWithScope(t, "gl-1090", "labeledOrMentioned", "tatara/review")
	proj.Spec.Scm.Provider = "gitlab"
	repos := []tatarav1alpha1.Repository{*repo}
	reader := &fakeReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 1090, Author: "szymonrychu", HeadSHA: "sha-head",
			UpdatedAt: time.Unix(100, 0)}, // unlabeled, no @mention
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	cron := tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 10}
	r.mrScan(context.Background(), proj, reader, repos, nil, cron)

	for _, qe := range listScanQEs(t, "gl-1090") {
		if qe.Spec.Kind == "review" {
			t.Fatalf("unlabeled GitLab MR under labeledOrMentioned must NOT be reviewed; got QE kind=%q", qe.Spec.Kind)
		}
	}
}

// TestMRScan_AlreadyReviewedSameHead_NotRereviewed: an in-scope human MR that
// the bot already reviewed at its CURRENT head must NOT be re-reviewed.
func TestMRScan_AlreadyReviewedSameHead_NotRereviewed(t *testing.T) {
	proj, repo := seedScanProjectWithScope(t, "rr-same", "", "tatara/review")
	repos := []tatarav1alpha1.Repository{*repo}
	reader := &fakeReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 7, Author: "human", HeadSHA: "sha-head", UpdatedAt: time.Unix(100, 0)},
	}}
	prior := mkReviewTask("o/r", 7, "sha-head", "github")
	prior.CreationTimestamp = metav1.Now()
	existing := []tatarav1alpha1.Task{prior}

	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	cron := tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 10}
	r.mrScan(context.Background(), proj, reader, repos, existing, cron)

	for _, qe := range listScanQEs(t, "rr-same") {
		if qe.Spec.Kind == "review" {
			t.Fatal("MR already reviewed at current head must NOT be re-reviewed")
		}
	}
}

// TestMRScan_HeadChanged_Rereviewed: once the MR head advances, the bot must
// review the new code.
func TestMRScan_HeadChanged_Rereviewed(t *testing.T) {
	proj, repo := seedScanProjectWithScope(t, "rr-changed", "", "tatara/review")
	repos := []tatarav1alpha1.Repository{*repo}
	reader := &fakeReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 7, Author: "human", HeadSHA: "sha-new", UpdatedAt: time.Unix(200, 0)},
	}}
	prior := mkReviewTask("o/r", 7, "sha-old", "github")
	prior.CreationTimestamp = metav1.Now()
	existing := []tatarav1alpha1.Task{prior}

	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	cron := tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 10}
	r.mrScan(context.Background(), proj, reader, repos, existing, cron)

	reviewFound := false
	for _, qe := range listScanQEs(t, "rr-changed") {
		if qe.Spec.Kind == "review" {
			reviewFound = true
		}
	}
	if !reviewFound {
		t.Fatal("MR with a changed head must be re-reviewed")
	}
}
