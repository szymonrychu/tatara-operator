package controller

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// A STALLED TURN GETS THE SAME GRACEFUL STOP AS A TTL ROTATION.
//
// It used to get the opposite. The poll backstop called expireTimedOutTurn,
// which deleted the session, the Pod and the Service outright: no handoff turn,
// no handoff note, and everything the agent had written but not pushed died with
// the workspace. A Task whose FIRST turn stalled was handed back to the stage
// machine with an EMPTY notes journal, so the next pod started from nothing and
// redid the work. Live: an mtg-decks implement agent lost a decklist, a meta
// capture and an hour of sim runs to this, twice, before anything was pushed.
//
// The stop now runs from the Task reconciler - which already owns the identical
// TTL stop - rather than from the backstop, because the graceful sequence blocks
// on real timers and the backstop is one pass over every Task in the namespace.
func TestStalledTurn_StopsGracefullyAndLeavesAHandoffNote(t *testing.T) {
	proj, task, r, sess := newConversingExitFixture(t)
	task.Status.State = tatarav1alpha1.StateUnderImplementation
	task.Status.AgentKind = stage.AgentKindFor(tatarav1alpha1.StateUnderImplementation, "implement")

	// A turn submitted long ago that has reported no activity since: exactly what
	// turnTimedOut calls a stall.
	stale := time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339)
	task.Annotations = map[string]string{
		annCurrentTurn:   "turn-stuck",
		annTurnStartedAt: stale,
	}
	if err := r.Update(context.Background(), task); err != nil {
		t.Fatalf("seed annotations: %v", err)
	}

	if _, err := r.reconcilePodStage(context.Background(), proj, task,
		stage.AgentImplement, time.Now()); err != nil {
		t.Fatalf("reconcilePodStage: %v", err)
	}

	got := mdGetTask(t, r.Client, task.Name)

	// 1. The agent got its handoff turn, and the notes journal is NOT empty. This
	//    is the whole point: the next pod resumes from a note instead of nothing.
	if sess.handoffTurns != 1 {
		t.Fatalf("handoff turns = %d, want 1: a stalled turn must still be offered the handoff", sess.handoffTurns)
	}
	var handoff bool
	for _, n := range got.Status.Notes {
		if n.Kind == agent.NoteKindHandoff {
			handoff = true
		}
	}
	if !handoff {
		t.Fatalf("no kind=handoff note: notes=%d - the old hard teardown left exactly this gap", len(got.Status.Notes))
	}

	// 2. The turn is cleared, so a late callback cannot resolve the Task against
	//    a turn the operator has finished with. This is the one property the
	//    deleted expireTimedOutTurn existed to guarantee.
	for _, ann := range []string{annCurrentTurn, annTurnStartedAt, annTurnLastActivity, annTurnComplete} {
		if got.Annotations[ann] != "" {
			t.Errorf("annotation %s = %q, want cleared", ann, got.Annotations[ann])
		}
	}

	// 3. The pod clocks are re-armed so the next reconcile spawns a continuation
	//    pod - a stalled TURN is not a dead TASK.
	if got.Status.PodStartedAt != nil || got.Status.StateWorkStartedAt != nil {
		t.Errorf("pod clocks not re-armed: podStartedAt=%v workStartedAt=%v",
			got.Status.PodStartedAt, got.Status.StateWorkStartedAt)
	}
	if tatarav1alpha1.TaskDone(got) {
		t.Error("stopping a stalled TURN must never terminate the TASK")
	}
}

// A turn that is still reporting activity is NOT stalled, however long it has
// been running. This is the property the wrapper's anchor fix restores end to
// end: turnTimedOut anchors on max(started, lastActivity), so a long productive
// turn must survive a reconcile that a frozen anchor would have killed.
func TestStalledTurn_FreshActivityIsNotAStall(t *testing.T) {
	proj, task, r, sess := newConversingExitFixture(t)
	task.Status.State = tatarav1alpha1.StateUnderImplementation
	task.Status.AgentKind = stage.AgentKindFor(tatarav1alpha1.StateUnderImplementation, "implement")

	// Submitted two hours ago - far past turnTimeoutSeconds - but it reported
	// activity a second ago. That is a working agent, not a hung one.
	task.Annotations = map[string]string{
		annCurrentTurn:      "turn-busy",
		annTurnStartedAt:    time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339),
		annTurnLastActivity: time.Now().Add(-time.Second).UTC().Format(time.RFC3339),
	}
	if err := r.Update(context.Background(), task); err != nil {
		t.Fatalf("seed annotations: %v", err)
	}
	if _, err := r.reconcilePodStage(context.Background(), proj, task,
		stage.AgentImplement, time.Now()); err != nil {
		t.Fatalf("reconcilePodStage: %v", err)
	}

	// No handoff turn means no stop was attempted: the reconcile left the live
	// turn alone. (It goes on to do ordinary stage work, which is not what this
	// test is about - the assertion is precisely "it was not torn down".)
	if sess.handoffTurns != 0 {
		t.Fatalf("handoff turns = %d, want 0: a turn that is still streaming must not be stopped", sess.handoffTurns)
	}
	got := mdGetTask(t, r.Client, task.Name)
	if len(got.Status.Notes) != 0 {
		t.Errorf("notes = %d, want 0: no handoff note may be written for a turn that is still alive", len(got.Status.Notes))
	}
}
