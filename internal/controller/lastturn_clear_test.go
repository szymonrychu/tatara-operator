package controller

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// A G.7 STOP SPENDS THE LAST-TURN STATE, SO IT MUST ALSO RETIRE IT.
//
// status.lastTurn{FinalText,PushedRepos} exist for one consumer: the synthetic
// handoff note. Once a stop has written that note - or the agent has written its
// own - the payload is IN the notes journal, which is where the next pod reads
// its continuation state from. Leaving it on the status means the NEXT stop of
// the NEXT pod re-renders the same turn's text as though it were that pod's
// work.
//
// That failure is strictly worse than the empty note #527 was filed about. An
// empty note is recognisably empty; a stale one is confidently wrong, and the
// next agent has nothing to tell them apart with. Both stops that re-arm for a
// continuation pod therefore clear it in the same patch that nils podStartedAt.
//
// respawnLostPod deliberately does NOT: that path writes no note at all - a pod
// that vanished mid-turn produced nothing to write - so the status is the only
// surviving trace of its work, and clearing it there turns a recoverable crash
// into guaranteed loss.
func TestTTLStop_ClearsTheLastTurnStateItJustSpent(t *testing.T) {
	proj, task, r, _ := newConversingExitFixture(t)
	proj.Spec.AgentPodTTLSeconds = 60 // t0 is 9 minutes in the past for this fixture
	task.Status.State = tatarav1alpha1.StateUnderImplementation
	task.Status.AgentKind = stage.AgentKindFor(tatarav1alpha1.StateUnderImplementation, "implement")
	task.Status.LastTurnFinalText = "wired the reconciler; the mirror suite is still red"
	task.Status.LastTurnPushedRepos = []string{"tatara-operator"}
	if err := r.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed last-turn state: %v", err)
	}

	if _, err := r.ttlStop(context.Background(), proj, task,
		stage.AgentImplement, time.Now()); err != nil {
		t.Fatalf("ttlStop: %v", err)
	}

	got := mdGetTask(t, r.Client, task.Name)
	if got.Status.LastTurnFinalText != "" {
		t.Errorf("lastTurnFinalText = %q after a TTL stop spent it on a handoff note; "+
			"the next pod's stop would re-render this turn as its own", got.Status.LastTurnFinalText)
	}
	if len(got.Status.LastTurnPushedRepos) != 0 {
		t.Errorf("lastTurnPushedRepos = %v after a TTL stop, want cleared", got.Status.LastTurnPushedRepos)
	}
}

// The stalled-turn stop runs the identical G.7 sequence and re-arms the same
// clocks, so it carries the identical obligation.
func TestStalledTurnStop_ClearsTheLastTurnStateItJustSpent(t *testing.T) {
	proj, task, r, _ := newConversingExitFixture(t)
	task.Status.State = tatarav1alpha1.StateUnderImplementation
	task.Status.AgentKind = stage.AgentKindFor(tatarav1alpha1.StateUnderImplementation, "implement")
	task.Status.LastTurnFinalText = "pushed the fix; waiting on CI"
	task.Status.LastTurnPushedRepos = []string{"tatara-operator", "tatara-cli"}
	if err := r.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed last-turn state: %v", err)
	}

	if _, err := r.stalledTurnStop(context.Background(), proj, task,
		stage.AgentImplement, time.Now()); err != nil {
		t.Fatalf("stalledTurnStop: %v", err)
	}

	got := mdGetTask(t, r.Client, task.Name)
	if got.Status.LastTurnFinalText != "" {
		t.Errorf("lastTurnFinalText = %q after a stalled-turn stop, want cleared", got.Status.LastTurnFinalText)
	}
	if len(got.Status.LastTurnPushedRepos) != 0 {
		t.Errorf("lastTurnPushedRepos = %v after a stalled-turn stop, want cleared", got.Status.LastTurnPushedRepos)
	}
}
