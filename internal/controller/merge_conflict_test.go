package controller

import (
	"context"
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// --- the conflict self-heal (merge gate) ----------------------------------
//
// The gate used to lump MergeStateDirty and MergeStateBlocked together and stall
// on both. They are OPPOSITES: dirty is a textual conflict an agent resolves by
// committing to the branch; blocked is policy an agent cannot touch. These tests
// pin the split, and pin that ownership is what decides whether an agent is put
// on the branch at all.

// mcDirtyTask is the fixture every case below starts from: a tatara-owned,
// approved, reviewed-at-the-live-head merge request whose mergeability the forge
// reports as ms.
func mcDirtyTask(t *testing.T, ms scm.MergeState) (*tatarav1alpha1.Task, *fakeForge, *StageDriver) {
	t.Helper()
	task := mdTask("t1", "implement", tatarav1alpha1.StateMerged)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.ReviewedSHA = "sha-a"
	mr.Status.Status = "approved"
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	f.head[7] = "sha-a"
	f.mergeState[7] = ms
	d := mdNewDriver(t, f, c)
	return task, f, d
}

// DIRTY + tatara-owned: the branch goes back to the implement agent. Nobody else
// is coming for it - Renovate will not rebase a branch an agent has committed to,
// and no human owns it - so stalling until merge-timeout is a permanent dead end.
func TestMergeDirtyTataraOwnedReEntersImplementation(t *testing.T) {
	task, f, d := mcDirtyTask(t, scm.MergeStateDirty)
	c := d.Client

	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	if f.mergeCalls != 0 {
		t.Fatalf("merge calls = %d, want 0: a dirty MR is never merged", f.mergeCalls)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.State != tatarav1alpha1.StateUnderImplementation {
		t.Fatalf("state = %q, want under-implementation", got.Status.State)
	}
	if tatarav1alpha1.Parked(got) {
		t.Fatalf("parked(%q); the first conflict must re-enter, not park", got.Status.ParkReason)
	}
	if got.Status.StateReason != stage.ReasonMergeConflict {
		t.Fatalf("stateReason = %q, want %q", got.Status.StateReason, stage.ReasonMergeConflict)
	}
	if got.Status.MergeConflictReentries != 1 {
		t.Fatalf("mergeConflictReentries = %d, want 1", got.Status.MergeConflictReentries)
	}
	// The bundle an implement pod renders carries no mergeability at all, so the
	// note is the ONLY thing that tells the agent this is a conflict-resolution
	// turn rather than a fresh implementation.
	var note string
	for _, n := range got.Status.Notes {
		if n.Agent == "operator" && strings.Contains(n.Body, "conflict") {
			note = n.Body
		}
	}
	if note == "" {
		t.Fatalf("no operator note naming the conflict; notes = %+v", got.Status.Notes)
	}
	for _, want := range []string{"tatara-operator", "!7", "main"} {
		if !strings.Contains(note, want) {
			t.Fatalf("note %q does not name %q", note, want)
		}
	}
}

// DIRTY + external: a HUMAN's branch. It keeps stalling exactly as before. The
// merge-allowed stand-down MR is the case that matters - it is external AND
// merge-eligible, so ownership, not merge eligibility, has to be the test.
func TestMergeDirtyExternalMRStillStalls(t *testing.T) {
	task := mdTask("t1", takeoverKind, tatarav1alpha1.StateMerged)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.ReviewedSHA = "sha-a"
	mr.Status.Ownership = tatarav1alpha1.OwnershipExternal
	mr.Status.OwnershipReason = "external-push:human-head"
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	f.head[7] = "sha-a"
	f.mergeState[7] = scm.MergeStateDirty
	d := mdNewDriver(t, f, c)

	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	if f.mergeCalls != 0 {
		t.Fatalf("merge calls = %d, want 0", f.mergeCalls)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.State != tatarav1alpha1.StateMerged {
		t.Fatalf("state = %q, want merged (stalled): an agent must never be put on a human's branch", got.Status.State)
	}
	if got.Status.MergeConflictReentries != 0 {
		t.Fatalf("mergeConflictReentries = %d, want 0", got.Status.MergeConflictReentries)
	}
}

// BLOCKED is policy - a missing approval, a protected branch, a required check.
// No commit an agent can write clears it, so it must keep stalling.
func TestMergeBlockedStillStalls(t *testing.T) {
	task, f, d := mcDirtyTask(t, scm.MergeStateBlocked)
	c := d.Client

	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	if f.mergeCalls != 0 {
		t.Fatalf("merge calls = %d, want 0", f.mergeCalls)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.State != tatarav1alpha1.StateMerged {
		t.Fatalf("state = %q, want merged (stalled)", got.Status.State)
	}
	if got.Status.MergeConflictReentries != 0 {
		t.Fatalf("mergeConflictReentries = %d, want 0: blocked is not a conflict", got.Status.MergeConflictReentries)
	}
}

// CLEAN still merges. The regression guard on the split.
func TestMergeCleanStillMerges(t *testing.T) {
	task, f, d := mcDirtyTask(t, scm.MergeStateClean)
	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	if f.mergeCalls != 1 {
		t.Fatalf("merge calls = %d, want 1", f.mergeCalls)
	}
}

// BEHIND is deliberately NOT in the self-heal. There is no conflict: the forge
// merges a behind branch itself, and the squash merge the operator performs does
// not care. Spawning a pod to reconcile a branch nothing is refusing would burn
// the budget on the state that changes fastest - main moves constantly - for no
// change in outcome.
func TestMergeBehindStillAttemptsTheMerge(t *testing.T) {
	task, f, d := mcDirtyTask(t, scm.MergeStateBehind)
	c := d.Client

	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	if f.mergeCalls != 1 {
		t.Fatalf("merge calls = %d, want 1: behind is not a conflict", f.mergeCalls)
	}
	if got := mdGetTask(t, c, "t1"); got.Status.MergeConflictReentries != 0 {
		t.Fatalf("mergeConflictReentries = %d, want 0", got.Status.MergeConflictReentries)
	}
}

// THE BOUND. main keeps moving, so conflict -> implement -> push -> conflict is a
// real loop. At the ceiling the Task parks merge-blocked, which is where the
// stall-and-time-out path landed it before this change existed: exhaustion
// degrades to today's behaviour, never to something worse.
func TestMergeDirtyReentriesExhaustedParksMergeBlocked(t *testing.T) {
	task, f, d := mcDirtyTask(t, scm.MergeStateDirty)
	task.Status.MergeConflictReentries = tatarav1alpha1.MaxMergeConflictReentries
	c := d.Client
	if err := c.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed the spent budget: %v", err)
	}

	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	if f.mergeCalls != 0 {
		t.Fatalf("merge calls = %d, want 0", f.mergeCalls)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.State != tatarav1alpha1.StateMerged || !tatarav1alpha1.Parked(got) ||
		got.Status.ParkReason != stage.ReasonMergeBlocked {
		t.Fatalf("state/park = %q/%q, want merged, parked(merge-blocked)",
			got.Status.State, got.Status.ParkReason)
	}
}
