package controller

// Refine is genuinely periodic grooming, not demand-driven work, so it keeps a
// cron - its OWN, under scm.cron.refine.schedule, decoupled from brainstorm.
// This file replaces the refine pre-scan barrier tests that used to pin
// refine to the brainstorm cron tick (see projectscan_refine_test.go's
// surviving TestRefine_OnePerProjectPerCycle for the one barrier-era test
// that still applies unchanged).

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// seedRefineCronProject creates a Project with refine due every minute on its
// own schedule, and brainstorm enabled but WITHOUT a schedule (the shipped end
// state: brainstorm is demand-driven and has no cron of its own). Copied from
// seedRefineProject (projectscan_refine_test.go) with the cron block changed
// to match.
func seedRefineCronProject(t *testing.T, name string) *tatarav1alpha1.Project {
	t.Helper()
	ctx := context.Background()
	mkSecret(t, name+"-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	cron := &tatarav1alpha1.ScmCron{
		IssueScan:  tatarav1alpha1.CronActivity{Schedule: "* * * * *"},
		Brainstorm: tatarav1alpha1.BrainstormActivity{Enabled: true},
		Refine:     tatarav1alpha1.RefineActivity{ClosedLookbackDays: 30, Schedule: "* * * * *"},
	}
	proj := &tatarav1alpha1.Project{}
	proj.Name = name
	proj.Namespace = testNS
	proj.Spec.ScmSecretRef = name + "-scm"
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{
		Provider: "github", Owner: "o", BotLogin: "bot", Cron: cron,
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project %s: %v", name, err)
	}
	// Stamp LastIssueScan and LastRefine two minutes in the past so the
	// every-minute schedules are immediately due on the first reconcile.
	past := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	proj.Status.LastIssueScan = &past
	proj.Status.LastRefine = &past
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("stamp scan status: %v", err)
	}
	mkScanRepo(t, name, name+"-repo", "https://github.com/o/r.git")
	return proj
}

// Refine must run on its own schedule, independent of brainstorm.
func TestRefineCron_DueTickCreatesARefineTask(t *testing.T) {
	proj := seedRefineCronProject(t, "refine-cron-1")
	reader := &fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 1, Title: "open issue"}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	if _, _, _, _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}
	if got := len(listRefineQEs(t, proj.Name)); got != 1 {
		t.Fatalf("want 1 refine QueuedEvent on a due refine tick, got %d", got)
	}
}

// The load-bearing regression guard for this whole change: refine must keep
// running when brainstorm has NO schedule at all, which is the shipped
// configuration. Before the re-homing, refine's only trigger was
// activityDue(proj, "brainstorm"), so this case created nothing.
func TestRefineCron_RunsWithNoBrainstormSchedule(t *testing.T) {
	proj := seedRefineCronProject(t, "refine-cron-2")
	if proj.Spec.Scm.Cron.Brainstorm.Schedule != "" {
		t.Fatal("this test is meaningless unless the brainstorm schedule is empty")
	}
	reader := &fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 1, Title: "open issue"}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	if _, _, _, _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}
	if got := len(listRefineQEs(t, proj.Name)); got != 1 {
		t.Fatalf("refine must run on its own cron with brainstorm unscheduled, got %d events", got)
	}
}

// A paused brainstorm must not stop the grooming. The two are decoupled.
func TestRefineCron_RunsWhileBrainstormIsPaused(t *testing.T) {
	proj := seedRefineCronProject(t, "refine-cron-3")
	now := metav1.Now()
	proj.Status.BrainstormPausedAt = &now
	proj.Status.BrainstormPauseReason = "exhausted"
	if err := k8sClient.Status().Update(context.Background(), proj); err != nil {
		t.Fatalf("stamp pause: %v", err)
	}
	reader := &fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 1, Title: "open issue"}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	if _, _, _, _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}
	if got := len(listRefineQEs(t, proj.Name)); got != 1 {
		t.Fatalf("refine must run while brainstorm is paused, got %d events", got)
	}
}

// A brainstorm backlog already at target must not stop the grooming either -
// refine's own cron block reads nothing about brainstorm's deficit/target
// state at all, so this is exhaustive coverage of the same decoupling, not a
// duplicate of the paused/no-schedule cases: it pins the same guarantee
// TestRefineBarrierRunsEvenWhenBrainstormBacklogIsAtTarget used to (an
// explicit TargetOpenProposals=0 means brainstorm() can never decide to
// refill) but from refine's OWN cron rather than the retired barrier.
func TestRefineCron_RunsWhileBrainstormIsAtTarget(t *testing.T) {
	proj := seedRefineCronProject(t, "refine-cron-6")
	zero := 0
	proj.Spec.Scm.Cron.Brainstorm.TargetOpenProposals = &zero
	if err := k8sClient.Update(context.Background(), proj); err != nil {
		t.Fatalf("disable brainstorm refill via target=0: %v", err)
	}
	reader := &fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 1, Title: "open issue"}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	if _, _, _, _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}
	if got := len(listRefineQEs(t, proj.Name)); got != 1 {
		t.Fatalf("refine must run while brainstorm's backlog is at target, got %d events", got)
	}
}

// One refine per due tick, not one per reconcile: a second pass inside the same
// cron period must find LastRefine stamped and create nothing.
func TestRefineCron_StampsLastRefineAndDoesNotRefire(t *testing.T) {
	proj := seedRefineCronProject(t, "refine-cron-4")
	ctx := context.Background()
	reader := &fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 1, Title: "open issue"}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	if _, _, _, _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans 1: %v", err)
	}
	if proj.Status.LastRefine == nil {
		t.Fatal("a due refine tick must stamp LastRefine")
	}
	if time.Since(proj.Status.LastRefine.Time) > time.Minute {
		t.Fatalf("LastRefine is stale: %v", proj.Status.LastRefine.Time)
	}
	if _, _, _, _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans 2: %v", err)
	}
	if got := len(listRefineQEs(t, proj.Name)); got != 1 {
		t.Fatalf("a second pass in the same period must create nothing, got %d events", got)
	}
}

// An empty refine schedule disables the activity outright.
func TestRefineCron_EmptyScheduleDisablesRefine(t *testing.T) {
	proj := seedRefineCronProject(t, "refine-cron-5")
	ctx := context.Background()
	proj.Spec.Scm.Cron.Refine.Schedule = ""
	if err := k8sClient.Update(ctx, proj); err != nil {
		t.Fatalf("clear refine schedule: %v", err)
	}
	reader := &fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 1, Title: "open issue"}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	if _, _, _, _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}
	if got := len(listRefineQEs(t, proj.Name)); got != 0 {
		t.Fatalf("an empty refine schedule must create nothing, got %d events", got)
	}
}
