package controller

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// setTaskLastTurn seeds the continuation state a previous turn captured.
func setTaskLastTurn(t *testing.T, name, finalText string, repos []string) {
	t.Helper()
	tk := getTask(t, name)
	tk.Status.LastTurnFinalText = finalText
	tk.Status.LastTurnPushedRepos = repos
	if err := k8sClient.Status().Update(context.Background(), tk); err != nil {
		t.Fatalf("seed last-turn state on %s: %v", name, err)
	}
}

// POLLONCE CARRIES TWO OVERLAPPING RESPONSIBILITIES, AND THEY MUST NOT EAT EACH
// OTHER.
//
// #566 gave it the orphaned-turn repair: when a turn has gone stale and the
// reconciler provably cannot reach it, retire the POD-SCOPED TURN ANNOTATIONS.
// #527 gave it the last-turn backstop: when the wrapper reports a turn finished
// and no callback arrived, persist the CONTINUATION STATE that finished turn
// produced.
//
// The two answer different questions - "is a turn running?" versus "what did the
// last turn produce?" - and this pins that they stay independent on the one loop
// that does both.
//
// The recovery half is where the #557 regression bites hardest: the backstop
// population is BY CONSTRUCTION the wrappers whose callback never landed, i.e.
// the degraded pods most likely to TTL-stop next, so an unconditional write here
// blanks the continuation state of exactly the Tasks that are about to need it.
func TestPollOnce_BackstopRecoveryDoesNotBlankThePreviousTurnsState(t *testing.T) {
	mkTaskProject(t, "p-pollturn", 3)
	mkTaskRepository(t, "r-pollturn", "p-pollturn")
	mkTask(t, "t-pollturn", "p-pollturn", "r-pollturn")
	setTaskStage(t, "t-pollturn", tatarav1alpha1.StateUnderImplementation)
	setTaskPodStartedAt(t, "t-pollturn", time.Now())
	setTaskLastTurn(t, "t-pollturn", "opened PR #91; the mirror suite is still red", []string{"tatara-operator"})

	// A LIVE turn (fresh activity, so it is not stalled and the orphan branch is
	// not taken) whose wrapper reports it FAILED with nothing to show for it.
	fresh := time.Now().UTC().Format(time.RFC3339)
	annotate(t, "t-pollturn", map[string]string{
		annCurrentTurn:      "turn-poll-1",
		annTurnStartedAt:    fresh,
		annTurnLastActivity: fresh,
	})

	sess := newFakeSession()
	sess.getResult["turn-poll-1"] = agent.TurnResult{State: "failed", FinalText: ""}
	cb := newCallbackServer()
	cb.Session = sess
	cb.PollOnce(context.Background())

	got := getTask(t, "t-pollturn")
	if got.Status.LastTurnFinalText != "opened PR #91; the mirror suite is still red" {
		t.Errorf("lastTurnFinalText = %q, want the previous turn's payload intact: a backstop-recovered "+
			"turn that produced nothing must not erase the newest turn that produced something (#557 regression)",
			got.Status.LastTurnFinalText)
	}
	if len(got.Status.LastTurnPushedRepos) != 1 {
		t.Errorf("lastTurnPushedRepos = %v, want the previous turn's repos intact", got.Status.LastTurnPushedRepos)
	}
	// The backstop still did its own job: the turn is recorded as complete.
	if got.Annotations[annTurnComplete] == "" {
		t.Error("annTurnComplete is empty: the guard must not stop the backstop recording the result")
	}
}

// The recovery half still WRITES when there is something to write. Without this
// the guard above could be satisfied by doing nothing at all, and the backstop
// population - degraded wrappers, the ones most likely to TTL-stop next - would
// silently report handoff="none" on payloads the operator was holding.
func TestPollOnce_BackstopRecoveryPersistsAFinalTextItDoesReceive(t *testing.T) {
	mkTaskProject(t, "p-pollturn2", 3)
	mkTaskRepository(t, "r-pollturn2", "p-pollturn2")
	mkTask(t, "t-pollturn2", "p-pollturn2", "r-pollturn2")
	setTaskStage(t, "t-pollturn2", tatarav1alpha1.StateUnderImplementation)
	setTaskPodStartedAt(t, "t-pollturn2", time.Now())

	fresh := time.Now().UTC().Format(time.RFC3339)
	annotate(t, "t-pollturn2", map[string]string{
		annCurrentTurn:      "turn-poll-2",
		annTurnStartedAt:    fresh,
		annTurnLastActivity: fresh,
	})

	sess := newFakeSession()
	sess.getResult["turn-poll-2"] = agent.TurnResult{
		State: "complete", FinalText: "rebased onto main; CI green",
	}
	cb := newCallbackServer()
	cb.Session = sess
	cb.PollOnce(context.Background())

	if got := getTask(t, "t-pollturn2").Status.LastTurnFinalText; got != "rebased onto main; CI green" {
		t.Errorf("lastTurnFinalText = %q, want the recovered turn's text: a turn whose callback never "+
			"landed is exactly the one whose payload the next TTL stop needs", got)
	}
}

// THE ORPHAN REPAIR RETIRES THE TURN, NOT THE WORK.
//
// The two mechanisms meet on a parked Task carrying both: #566's repair clears
// the pod-scoped turn annotations, and #527's continuation state must SURVIVE it
// for the same reason respawnLostPod keeps it - a pod that vanished wrote no
// handoff note, so the status is the only trace of what it did, and the Task can
// still un-park and hand it to the next pod.
func TestPollOnce_OrphanRepairClearsTheTurnButKeepsTheLastTurnPayload(t *testing.T) {
	mkTaskProject(t, "p-pollturn3", 3)
	mkTaskRepository(t, "r-pollturn3", "p-pollturn3")
	mkTask(t, "t-pollturn3", "p-pollturn3", "r-pollturn3")
	setTaskStage(t, "t-pollturn3", tatarav1alpha1.StateUnderImplementation)
	setTaskParkReason(t, "t-pollturn3", "awaiting-human")
	setTaskLastTurn(t, "t-pollturn3", "left the merge gate red; needs a human call", []string{"tatara-operator"})

	stale := time.Now().Add(-3 * time.Hour).UTC().Format(time.RFC3339)
	annotate(t, "t-pollturn3", map[string]string{
		annCurrentTurn:      "turn-orphan-lt",
		annTurnStartedAt:    stale,
		annTurnLastActivity: stale,
	})

	cb := newCallbackServer()
	cb.Session = newFakeSession()
	cb.PollOnce(context.Background())

	got := getTask(t, "t-pollturn3")
	assertNoTurnAnnotations(t, got, "the pod running this turn is gone (#566)")
	if got.Status.LastTurnFinalText != "left the merge gate red; needs a human call" {
		t.Errorf("lastTurnFinalText = %q, want it kept: retiring the TURN must not discard what that "+
			"turn PRODUCED - it is the only trace of the vanished pod's work (#527)", got.Status.LastTurnFinalText)
	}
	if len(got.Status.LastTurnPushedRepos) != 1 {
		t.Errorf("lastTurnPushedRepos = %v, want kept", got.Status.LastTurnPushedRepos)
	}
}
