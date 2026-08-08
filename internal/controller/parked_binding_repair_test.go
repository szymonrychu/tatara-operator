package controller

// Fix for the 2026-07-19 production deadlock (task
// mt-r-tatara-operator-388-6e7958617d9d0119 / PR 388): a mint interrupted by
// informer-cache lag left an UNOWNED MergeRequest stub and a Task with empty
// refs; the 5m backstop parked it awaiting-human BEFORE the PR 388 repair
// path could run (or under an older operator without it). Once parked,
// reconcileMRBindingBackstop's own StageParked guard excluded it forever:
// the repair had a live target and no driver. reconcileParkedBindingRepair
// is that driver: repair the binding on reconcile, then unpark back to
// status.parkedFromStage through the stage machine.

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// pbTask is mbTask parked by the MR-binding watchdog: awaiting-human, empty
// refs, Source-bearing, with the stage it was interrupted in recorded in
// parkedFromStage - the exact shape the watchdog's r.enter leaves behind.
func pbTask(name string) *tatarav1alpha1.Task {
	task := mbTask(name, time.Hour)
	task.Status.State = tatarav1alpha1.StateAwaitingReview
	task.Status.ParkReason = stage.ReasonAwaitingHuman
	task.Status.ParkedFromState = tatarav1alpha1.StateAwaitingReview
	return task
}

// TestParkedBindingRepair_RepairsRefsButStaysParked is the primary self-heal
// case, driven through reconcileStage to prove the wiring runs BEFORE the
// terminal early-return that hands every parked Task to the reaper: a parked
// interrupted-mint Task with a repairable unowned MR stub is repaired (refs
// stamped, CR owned). #521: the repair NO LONGER UN-PARKS - park is a flag
// orthogonal to state, so there is nothing to derive a re-entry target from
// and no exception to make (see reconcileParkedBindingRepair's own doc
// comment) - it stamps the refs and leaves the Task parked exactly where it
// was; the ordinary awaiting-human re-entry rule resumes it on the Task's
// NEXT human comment, not automatically here.
func TestParkedBindingRepair_RepairsRefsButStaysParked(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	task := pbTask("mt-r-tatara-cli-87")
	mr := mdMR(task, "tatara-cli", 87)
	mr.OwnerReferences = nil // the interrupted mint's unbound stub
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("tatara-cli"), task, mr)
	r, _ := mbReconciler(c, &mbWriter{})
	r.Scheme = c.Scheme()

	if _, err := r.reconcileStage(ctx, proj, task, time.Now()); err != nil {
		t.Fatalf("reconcileStage: %v", err)
	}

	got := mdGetTask(t, c, task.Name)
	if got.Status.State != tatarav1alpha1.StateAwaitingReview || !tatarav1alpha1.Parked(got) ||
		got.Status.ParkReason != stage.ReasonAwaitingHuman {
		t.Fatalf("state=%q parked=%v reason=%q, want awaiting-review/parked/awaiting-human (repaired, but still parked)",
			got.Status.State, tatarav1alpha1.Parked(got), got.Status.ParkReason)
	}
	wantRef := tatarav1alpha1.MergeRequestName("tatara-cli", 87)
	if len(got.Status.MRRefs) != 1 || got.Status.MRRefs[0] != wantRef {
		t.Fatalf("mrRefs = %v, want [%s]", got.Status.MRRefs, wantRef)
	}
	gotMR := mdGetMR(t, c, wantRef)
	if owner, ok := own.ControllerOwner(gotMR); !ok || owner != task.Name {
		t.Fatalf("mr controller owner = %q, %v, want %s", owner, ok, task.Name)
	}
}

// TestParkedBindingRepair_NonEmptyRefsUntouched: a parked(awaiting-human)
// Task WITH refs is a genuine human wait (e.g. the review agent's verdict on
// a human PR) and must never be auto-unparked by this path.
func TestParkedBindingRepair_NonEmptyRefsUntouched(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	task := pbTask("t-genuine-wait")
	task.Status.MRRefs = []string{tatarav1alpha1.MergeRequestName("tatara-cli", 87)}
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("tatara-cli"), task, mdMR(task, "tatara-cli", 87))
	r, _ := mbReconciler(c, &mbWriter{})
	r.Scheme = c.Scheme()

	handled, err := r.reconcileParkedBindingRepair(ctx, proj, task, time.Now())
	if err != nil || handled {
		t.Fatalf("reconcileParkedBindingRepair = %v, %v, want handled=false, err=nil", handled, err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.State != tatarav1alpha1.StateAwaitingReview || !tatarav1alpha1.Parked(got) || got.Status.ParkReason != stage.ReasonAwaitingHuman {
		t.Fatalf("state=%q parked=%v reason=%q, want awaiting-review/untouched parked(awaiting-human)", got.Status.State, tatarav1alpha1.Parked(got), got.Status.ParkReason)
	}
}

// TestParkedBindingRepair_OtherReasonsUntouched: only the awaiting-human
// flavor the watchdog produces is eligible; any other park reason (here
// identity-unverified, the gate in front of writing code) must never be
// auto-unparked even with empty refs.
func TestParkedBindingRepair_OtherReasonsUntouched(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	task := pbTask("t-identity-unverified")
	task.Status.ParkReason = stage.ReasonIdentityUnverified
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("tatara-cli"), task)
	r, _ := mbReconciler(c, &mbWriter{})
	r.Scheme = c.Scheme()

	handled, err := r.reconcileParkedBindingRepair(ctx, proj, task, time.Now())
	if err != nil || handled {
		t.Fatalf("reconcileParkedBindingRepair = %v, %v, want handled=false, err=nil", handled, err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.State != tatarav1alpha1.StateAwaitingReview || !tatarav1alpha1.Parked(got) || got.Status.ParkReason != stage.ReasonIdentityUnverified {
		t.Fatalf("state=%q parked=%v reason=%q, want awaiting-review/untouched parked(identity-unverified)", got.Status.State, tatarav1alpha1.Parked(got), got.Status.ParkReason)
	}
}

// TestParkedBindingRepair_RepairFailureStaysParked: an unrepairable bind (the
// MR CR is controller-owned by ANOTHER Task - the repair never steals) leaves
// the Task exactly as parked as it was.
func TestParkedBindingRepair_RepairFailureStaysParked(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	task := pbTask("mt-r-tatara-cli-87")
	other := mbTask("mt-r-tatara-cli-87-other", time.Hour)
	mr := mdMR(other, "tatara-cli", 87) // controller-owned by the OTHER task
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("tatara-cli"), task, mr)
	r, _ := mbReconciler(c, &mbWriter{})
	r.Scheme = c.Scheme()

	handled, err := r.reconcileParkedBindingRepair(ctx, proj, task, time.Now())
	if err != nil || handled {
		t.Fatalf("reconcileParkedBindingRepair = %v, %v, want handled=false, err=nil", handled, err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.State != tatarav1alpha1.StateAwaitingReview || !tatarav1alpha1.Parked(got) || got.Status.ParkReason != stage.ReasonAwaitingHuman {
		t.Fatalf("state=%q parked=%v reason=%q, want awaiting-review/still parked(awaiting-human)", got.Status.State, tatarav1alpha1.Parked(got), got.Status.ParkReason)
	}
	if len(got.Status.MRRefs) != 0 {
		t.Fatalf("mrRefs = %v, want empty (foreign-owned CR is never stolen)", got.Status.MRRefs)
	}
}

// TestParkedBindingRepair_NoParkedFromStageStaysParked: without a recorded
// parkedFromStage this is not recognizably the watchdog's park (stage.Enter
// always stamps it on ->parked) and there is no re-entry target to derive,
// so the predicate refuses and the Task is left entirely untouched.
func TestParkedBindingRepair_NoParkedFromStageStaysParked(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	task := pbTask("mt-r-tatara-cli-87")
	task.Status.ParkedFromState = ""
	mr := mdMR(task, "tatara-cli", 87)
	mr.OwnerReferences = nil
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("tatara-cli"), task, mr)
	r, _ := mbReconciler(c, &mbWriter{})
	r.Scheme = c.Scheme()

	handled, err := r.reconcileParkedBindingRepair(ctx, proj, task, time.Now())
	if err != nil || handled {
		t.Fatalf("reconcileParkedBindingRepair = %v, %v, want handled=false, err=nil", handled, err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.State != tatarav1alpha1.StateAwaitingReview || !tatarav1alpha1.Parked(got) || got.Status.ParkReason != stage.ReasonAwaitingHuman {
		t.Fatalf("state=%q parked=%v reason=%q, want awaiting-review/still parked(awaiting-human)", got.Status.State, tatarav1alpha1.Parked(got), got.Status.ParkReason)
	}
}
