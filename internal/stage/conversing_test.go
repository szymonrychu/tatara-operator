package stage_test

import (
	"testing"
	"time"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// conversing is POD-BEARING and NON-TERMINAL. Those two facts are what make it
// need NO exception in the reaper (which gates on TaskDone), the concurrency
// accountant (queueTaskHoldsSlot = !TaskDone && !StagePodless) or the transition
// table. Carving three exceptions into those subsystems is exactly the ad hoc
// pattern that produced the 2026-06/2026-07 warm-pod incident chain.
func TestConversingNeedsNoCarveOut(t *testing.T) {
	task := &v1alpha1.Task{}
	task.Status.Stage = v1alpha1.StageConversing

	if v1alpha1.StageTerminal(task) {
		t.Error("StageTerminal(conversing) = true: the reaper would collect its live pod as finished work")
	}
	if v1alpha1.TaskDone(task) {
		t.Error("TaskDone(conversing) = true: the reaper's orphanReason would reap the pod promptly")
	}
	if v1alpha1.StagePodless(v1alpha1.StageConversing) {
		t.Error("StagePodless(conversing) = true: queueTaskHoldsSlot would return false and the pod would run off the MaxConcurrentAgents books")
	}
	if got := stage.AgentKindFor(v1alpha1.StageConversing); got != stage.AgentClarify {
		t.Errorf("AgentKindFor(conversing) = %q, want %q", got, stage.AgentClarify)
	}
}

func TestConversingIsInEveryClosedTable(t *testing.T) {
	found := false
	for _, s := range stage.AllStages() {
		if s == v1alpha1.StageConversing {
			found = true
		}
	}
	if !found {
		t.Fatal("AllStages() does not contain conversing")
	}
	if len(stage.AllStages()) != 16 {
		t.Errorf("AllStages() has %d members, want 16", len(stage.AllStages()))
	}
	budget, ok := stage.Budget(v1alpha1.StageConversing)
	if !ok {
		t.Fatal("conversing has no budget row")
	}
	if budget != v1alpha1.ConversationIdleDefault {
		t.Errorf("conversing budget = %v, want %v", budget, v1alpha1.ConversationIdleDefault)
	}
	edge, ok := stage.OnElapse(v1alpha1.StageConversing)
	if !ok {
		t.Fatal("conversing has no OnElapse row")
	}
	if edge.To != v1alpha1.StageParked || edge.Reason != stage.ReasonAwaitingHuman {
		t.Errorf("conversing OnElapse = %s(%s), want parked(awaiting-human)", edge.To, edge.Reason)
	}
}

func TestConversingEdges(t *testing.T) {
	legal := [][2]string{
		{v1alpha1.StageClarifying, v1alpha1.StageConversing},
		{v1alpha1.StageReviewing, v1alpha1.StageConversing},
		{v1alpha1.StageParked, v1alpha1.StageConversing},
		{v1alpha1.StageConversing, v1alpha1.StageApproved},
		{v1alpha1.StageConversing, v1alpha1.StageReviewing},
		{v1alpha1.StageConversing, v1alpha1.StageParked},
		{v1alpha1.StageConversing, v1alpha1.StageRejected},
		{v1alpha1.StageConversing, v1alpha1.StageFailed},
	}
	for _, e := range legal {
		if !stage.Legal(e[0], e[1]) {
			t.Errorf("Legal(%s -> %s) = false, want true", e[0], e[1])
		}
	}
	illegal := [][2]string{
		{v1alpha1.StageConversing, v1alpha1.StageImplementing},
		{v1alpha1.StageConversing, v1alpha1.StageMerging},
		{v1alpha1.StageConversing, v1alpha1.StageDelivered},
		{v1alpha1.StageConversing, v1alpha1.StageDeploying},
	}
	for _, e := range illegal {
		if stage.Legal(e[0], e[1]) {
			t.Errorf("Legal(%s -> %s) = true, want false", e[0], e[1])
		}
	}
}

// A kind=review Task conversing must not reach implementing, merging, or
// approved by any path, exactly as from every other stage.
//
// The implementing/merging assertions below are VACUOUS on their own:
// Legal(conversing, implementing) and Legal(conversing, merging) are already
// false (neither pair is in the F.3 table AT ALL), so LegalFor would return
// false for ANY kind, guard or no guard - deleting GUARD 1's kind check
// entirely would not fail them. Legal(conversing, approved) IS in the table
// (a clarify agent's decision=implement moves ANY kind there), so that
// assertion is the one that actually exercises GUARD 1: without it, a
// kind=review Task that reached conversing via reviewing -> conversing and
// then submitted decision=implement would land in approved and sit there -
// unable to reach implementing (still blocked) and unable to un-park on its
// own - until the 24h admission-starved budget elapsed (2026-07-28 security
// review IMPORTANT 3).
func TestConversingReviewKindStillBarredFromImplementing(t *testing.T) {
	task := &v1alpha1.Task{}
	task.Spec.Kind = "review"
	task.Status.Stage = v1alpha1.StageConversing
	if stage.LegalFor(task, nil, v1alpha1.StageConversing, v1alpha1.StageImplementing) {
		t.Fatal("a kind=review Task reached implementing from conversing")
	}
	if stage.LegalFor(task, nil, v1alpha1.StageConversing, v1alpha1.StageMerging) {
		t.Fatal("a kind=review Task reached merging from conversing")
	}
	if stage.LegalFor(task, nil, v1alpha1.StageConversing, v1alpha1.StageApproved) {
		t.Fatal("a kind=review Task reached approved from conversing - it would wedge there, unable to advance or un-park, until admission-starved (24h)")
	}
}

// The idle-expiry exit is parked(awaiting-human), and Enter must accept it.
func TestConversingIdleExitEntersParkedAwaitingHuman(t *testing.T) {
	task := &v1alpha1.Task{}
	task.Spec.Kind = "clarify"
	task.Status.Stage = v1alpha1.StageConversing
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	if err := stage.Enter(task, nil, v1alpha1.StageParked, stage.ReasonAwaitingHuman, now); err != nil {
		t.Fatalf("Enter(conversing -> parked(awaiting-human)): %v", err)
	}
	if task.Status.ParkedFromStage != v1alpha1.StageConversing {
		t.Errorf("ParkedFromStage = %q, want conversing", task.Status.ParkedFromStage)
	}
}
