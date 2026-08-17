package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// scanTaskExists reports whether a Task of that name is in etcd.
func scanTaskExists(t *testing.T, name string) bool {
	t.Helper()
	var task tatarav1alpha1.Task
	err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &task)
	if err == nil {
		return true
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("get task %s: %v", name, err)
	}
	return false
}

// seedDeadIntakeTwin creates a TERMINAL Task on the deterministic intake name
// for (project, repo, number) - the tombstone createTaskRaceSafe collides with
// and deletes, returning MintTombstoneDeleted with the mint still OWED.
func seedDeadIntakeTwin(t *testing.T, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int) string {
	t.Helper()
	ctx := context.Background()
	name := tatarav1alpha1.IntakeTaskName(proj.Name, SweepIssueKind, repo.Name, number)
	dead := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: SweepIssueKind, Goal: "dead"},
	}
	if err := k8sClient.Create(ctx, dead); err != nil {
		t.Fatalf("create dead twin: %v", err)
	}
	dead.Status.State = tatarav1alpha1.StateDone
	if err := k8sClient.Status().Update(ctx, dead); err != nil {
		t.Fatalf("stamp dead twin terminal: %v", err)
	}
	return name
}

// TestRunScans_TombstoneRequeueReSweepsTheOwedRepo is the assertion that would
// have caught the 30s requeue being a no-op. It drives the WHOLE loop:
// pass 1 deletes a tombstone (the mint is OWED and has NOT happened), the
// reconcile asks to be woken in sweepRemintDelay, and pass 2 must MINT.
//
// The failure mode it pins is not the returned duration. stampScan writes
// Status.LastIssueScan, which IS reposDueForScan's dueBase, so a pass that
// stamps leaves the owed repo not-due for a full cron period: the 30s wake
// then finds zero due repos, never calls SweepProject, and the owed mint waits
// the remaining ~59.5 minutes - the exact silent-next-pass shape issue #521
// exists to eliminate, wearing a 30s hat.
func TestRunScans_TombstoneRequeueReSweepsTheOwedRepo(t *testing.T) {
	ctx := context.Background()
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *"}}
	proj, repo := seedScanProject(t, "remint-requeue", cron)
	past := metav1.NewTime(time.Now().Add(-3 * time.Hour))
	proj.Status.LastIssueScan = &past
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("seed LastIssueScan: %v", err)
	}

	name := seedDeadIntakeTwin(t, proj, repo, 40)
	reader := &fakeReader{issues: []scm.IssueRef{{Number: 40, State: "open", Author: "szymonrychu"}}}
	r := newScanReconciler(reader)

	requeue, _, _, _, err := r.runScans(ctx, proj)
	if err != nil {
		t.Fatalf("runScans pass 1: %v", err)
	}
	if scanTaskExists(t, name) {
		t.Fatal("pass 1 should have deleted the tombstone and minted NOTHING yet")
	}
	if requeue > sweepRemintDelay {
		t.Fatalf("pass 1 requeue = %v, want at most %v", requeue, sweepRemintDelay)
	}

	// THE 30s WAKE. Nothing about the forge has changed; the only question is
	// whether the owed repo is due again.
	if _, _, _, _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans pass 2: %v", err)
	}
	if !scanTaskExists(t, name) {
		t.Fatal("the owed mint never happened: the tombstone requeue woke a pass with zero due repos")
	}
}

// TestRunScans_TombstoneRequeueLeavesPerRepoStampUntouched is the per-repo
// half of the sweepRequeue>0 contract (defect 2 fix): the skipped stamp is
// not just the project-wide Status.LastIssueScan, the per-repo one must stay
// untouched too, or the 30s wake's re-evaluation would anchor the owed repo
// on a fresh per-repo stamp instead of leaving it due.
func TestRunScans_TombstoneRequeueLeavesPerRepoStampUntouched(t *testing.T) {
	ctx := context.Background()
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *"}}
	proj, repo := seedScanProject(t, "remint-repo-stamp", cron)
	past := metav1.NewTime(time.Now().Add(-3 * time.Hour))
	proj.Status.LastIssueScan = &past
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("seed LastIssueScan: %v", err)
	}

	seedDeadIntakeTwin(t, proj, repo, 42)
	reader := &fakeReader{issues: []scm.IssueRef{{Number: 42, State: "open", Author: "szymonrychu"}}}
	r := newScanReconciler(reader)

	if _, _, _, _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans pass 1: %v", err)
	}
	var got tatarav1alpha1.Repository
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: repo.Name}, &got); err != nil {
		t.Fatalf("get repo: %v", err)
	}
	if got.Status.LastIssueScan != nil {
		t.Fatalf("pass 1 stamped the repo's own LastIssueScan to %v even though the mint is still owed; "+
			"the 30s wake will now anchor on a fresh stamp instead of leaving the repo due",
			got.Status.LastIssueScan)
	}
}

// TestRunScans_SuccessfulSweepStampsPerRepo is the positive-path wiring
// check: a pass that sweeps a due repo and returns no requeue must stamp that
// repo's OWN Status.LastIssueScan, not just the project-wide one - this is
// the write that makes repoIssueScanBase's per-repo anchoring possible.
func TestRunScans_SuccessfulSweepStampsPerRepo(t *testing.T) {
	ctx := context.Background()
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *"}}
	proj, repo := seedScanProject(t, "successful-sweep-stamp", cron)
	past := metav1.NewTime(time.Now().Add(-3 * time.Hour))
	proj.Status.LastIssueScan = &past
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("seed LastIssueScan: %v", err)
	}

	reader := &fakeReader{issues: []scm.IssueRef{{Number: 99, State: "open", Author: "szymonrychu"}}}
	r := newScanReconciler(reader)

	before := time.Now()
	if _, _, _, _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}
	var got tatarav1alpha1.Repository
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: repo.Name}, &got); err != nil {
		t.Fatalf("get repo: %v", err)
	}
	if got.Status.LastIssueScan == nil {
		t.Fatal("a successful sweep of a due repo must stamp the repo's own LastIssueScan")
	}
	// metav1.Time truncates to second precision, so compare against the
	// second-truncated start to avoid a false failure on the same-second race.
	if got.Status.LastIssueScan.Time.Before(before.Truncate(time.Second)) {
		t.Fatalf("repo LastIssueScan %v is before the pass started %v", got.Status.LastIssueScan.Time, before)
	}
}

// TestRunScans_StampsAgainOnceTheOwedMintLands bounds the fix. The skipped
// stamp is not a permanent state: the pass that actually mints advances
// Status.LastIssueScan, so the sweep returns to its cron cadence and cannot
// re-enter against the forge every 30s forever.
func TestRunScans_StampsAgainOnceTheOwedMintLands(t *testing.T) {
	ctx := context.Background()
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *"}}
	proj, repo := seedScanProject(t, "remint-bounded", cron)
	past := metav1.NewTime(time.Now().Add(-3 * time.Hour))
	proj.Status.LastIssueScan = &past
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("seed LastIssueScan: %v", err)
	}
	// Re-read what etcd actually stored: metav1.Time round-trips at second
	// precision, so the seeded literal is not what a comparison must use.
	seeded := proj.Status.LastIssueScan.DeepCopy()
	seedDeadIntakeTwin(t, proj, repo, 41)
	reader := &fakeReader{issues: []scm.IssueRef{{Number: 41, State: "open", Author: "szymonrychu"}}}
	r := newScanReconciler(reader)

	if _, _, _, _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans pass 1: %v", err)
	}
	if !proj.Status.LastIssueScan.Time.Equal(seeded.Time) {
		t.Fatalf("pass 1 advanced LastIssueScan to %v; the owed repo is no longer due", proj.Status.LastIssueScan)
	}
	if _, _, _, _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans pass 2: %v", err)
	}
	if proj.Status.LastIssueScan.Time.Equal(seeded.Time) {
		t.Fatal("pass 2 minted the owed Task but never stamped; the sweep would re-enter every 30s forever")
	}
}

// TestRunScans_ClearsStrandedWhenTheSweepIsDisabled: ClearSweepOrphanStranded
// runs at the top of sweepIssues, so a project whose sweep is switched OFF (or
// whose issueScan cron is emptied) can never clear its own series again. The
// intended alert is max by (project) over a threshold, so a frozen 19h series
// pages permanently until the pod rolls.
func TestRunScans_ClearsStrandedWhenTheSweepIsDisabled(t *testing.T) {
	ctx := context.Background()
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *"}}
	proj, repo := seedScanProject(t, "stranded-disabled", cron)
	proj.Annotations = map[string]string{SweepAnnotation: SweepDisabledValue}
	if err := k8sClient.Update(ctx, proj); err != nil {
		t.Fatalf("disable sweep: %v", err)
	}
	obs.SweepOrphanStrandedSeconds.WithLabelValues(proj.Name, repo.Name, "9").Set(68400)

	if _, _, _, _, err := newScanReconciler(&fakeReader{}).runScans(ctx, proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}
	if v := testutil.ToFloat64(obs.SweepOrphanStrandedSeconds.WithLabelValues(proj.Name, repo.Name, "9")); v != 0 {
		t.Fatalf("a disabled sweep left a frozen stranded series at %v", v)
	}
}

// TestRunScans_ClearsStrandedForAnUnenrolledRepo: the same leak via the other
// door. A Repository removed from the project is never swept again, so its
// stranded series freezes at its last value and is scraped forever.
func TestRunScans_ClearsStrandedForAnUnenrolledRepo(t *testing.T) {
	ctx := context.Background()
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *"}}
	proj, repo := seedScanProject(t, "stranded-unenrolled", cron)
	obs.SweepOrphanStrandedSeconds.WithLabelValues(proj.Name, "gone-repo", "9").Set(68400)
	obs.SweepOrphanStrandedSeconds.WithLabelValues(proj.Name, repo.Name, "8").Set(68400)

	if _, _, _, _, err := newScanReconciler(&fakeReader{}).runScans(ctx, proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}
	if v := testutil.ToFloat64(obs.SweepOrphanStrandedSeconds.WithLabelValues(proj.Name, "gone-repo", "9")); v != 0 {
		t.Fatalf("an unenrolled repo left a frozen stranded series at %v", v)
	}
	if v := testutil.ToFloat64(obs.SweepOrphanStrandedSeconds.WithLabelValues(proj.Name, repo.Name, "8")); v == 0 {
		t.Fatal("an ENROLLED repo's stranded series was wiped; only this pass's own sweep may clear it")
	}
}
