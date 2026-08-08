package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// idleExitNoAgentHandoff arranges the fixture so the wrapper REFUSES the G.7
// handoff turn (410 Gone, the wrapper past its own t0 - or, in production, a pod
// the idle reaper already deleted). The operator then writes the synthetic note
// and no AGENT-authored handoff note ever exists.
func idleExitNoAgentHandoff(t *testing.T) (*tatarav1alpha1.Project, *tatarav1alpha1.Task, *TaskReconciler) {
	t.Helper()
	proj, task, r, sess := newConversingExitFixture(t)
	// A refusal, so the stopper skips waitHandoffNote and goes straight to the
	// synthetic note. Without it the stopper polls on real wall-clock timers.
	sess.submitErr = &agent.HTTPError{Status: 410, Body: "gone"}
	proj.Spec.Scm.ConversationIdleMinutes = 5
	task.Status.ConversationLastEventAt = &metav1.Time{Time: time.Now().Add(-6 * time.Minute)}
	return proj, task, r
}

// TestIdleExitWithoutAgentHandoffReArmsInsteadOfFakingAHumanGate.
//
// awaiting-human un-parks ONLY on a non-bot comment (stage.UnparkHuman). A Task
// parked there having produced no agent handoff was never given anything to
// respond to, so no human ever replies and it sits until an unrelated comment
// happens to land on its issue. Measured live in the tatara namespace: 45 Tasks
// parked awaiting-human, of which 8 carried only the content-free synthetic note
// and 17 carried no handoff note at all - 25 of 45 were wreckage from pods dying,
// not human gates.
//
// A pod that ended without the agent saying anything is a pod to REPLACE, not a
// question to wait on.
func TestIdleExitWithoutAgentHandoffReArmsInsteadOfFakingAHumanGate(t *testing.T) {
	proj, task, r := idleExitNoAgentHandoff(t)
	task.Status.LastTurnFinalText = "refactored the reconciler; tests still red on the mirror suite"
	task.Status.LastTurnPushedRepos = []string{"tatara-operator"}

	_, handled, err := r.reconcileClocks(context.Background(), proj, task, time.Now())
	if err != nil {
		t.Fatalf("reconcileClocks: %v", err)
	}
	if !handled {
		t.Fatal("handled = false: the idle conversation did not age out")
	}

	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(task), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if tatarav1alpha1.Parked(fresh) {
		t.Fatalf("parked(%s): a pod that died without the agent asking anything is not a human gate",
			fresh.Status.ParkReason)
	}
	if fresh.Status.State != tatarav1alpha1.StateRefined {
		t.Fatalf("state = %s, want refined: re-arming never moves state", fresh.Status.State)
	}
	if fresh.Status.PodStartedAt != nil || fresh.Status.StateWorkStartedAt != nil {
		t.Fatalf("pod clocks not re-armed: podStartedAt=%v workStartedAt=%v",
			fresh.Status.PodStartedAt, fresh.Status.StateWorkStartedAt)
	}
	if fresh.Status.Stats.PodRecreations != 1 {
		t.Fatalf("podRecreations = %d, want 1: the re-arm MUST charge the recreation budget or it can spin forever",
			fresh.Status.Stats.PodRecreations)
	}

	// The synthetic note is still written, and under #527 it now carries the last
	// turn's actual continuation state rather than "(none)".
	var body string
	for _, n := range fresh.Status.Notes {
		if n.Kind == agent.NoteKindHandoff {
			body = n.Body
		}
	}
	if body == "" {
		t.Fatal("no handoff note at all: the continuation state was lost")
	}
	if !strings.Contains(body, "refactored the reconciler") || !strings.Contains(body, "tatara-operator") {
		t.Fatalf("synthetic handoff note carries no continuation state: %q", body)
	}
}

// TestIdleExitWithoutAgentHandoffParksOnceTheRecreationBudgetIsSpent: the re-arm
// is BOUNDED. Repeated pod death must terminate, and pod-recreation-exhausted is
// already the right terminal for it.
func TestIdleExitWithoutAgentHandoffParksOnceTheRecreationBudgetIsSpent(t *testing.T) {
	proj, task, r := idleExitNoAgentHandoff(t)
	// Persisted, not just set in memory: the re-arm re-reads the Task through
	// patchTaskStatus, so an in-memory-only budget would not be seen.
	task.Status.Stats.PodRecreations = taskMaxPodRecreations(proj)
	if err := r.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed the spent recreation budget: %v", err)
	}

	if _, _, err := r.reconcileClocks(context.Background(), proj, task, time.Now()); err != nil {
		t.Fatalf("reconcileClocks: %v", err)
	}

	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(task), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if !tatarav1alpha1.Parked(fresh) {
		t.Fatal("a Task that has burned its whole recreation budget must terminate, not re-arm again")
	}
	if fresh.Status.ParkReason != stage.ReasonPodRecreationExhausted {
		t.Fatalf("park reason = %s, want %s", fresh.Status.ParkReason, stage.ReasonPodRecreationExhausted)
	}
}
