package controller

import (
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// chainTask builds a Task with the given activity label and stage.
func chainTask(activity, stg string) *tatarav1alpha1.Task {
	t := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "demo", Kind: "brainstorm", Goal: "g"},
	}
	if activity != "" {
		t.Labels = map[string]string{labelActivity: activity}
	}
	t.Status.State = stg
	return t
}

// chainParkedTask builds a Task at stg, then parks it for reason. State is
// UNCHANGED by a park (stage.Park stamps the flag; it never moves State).
func chainParkedTask(activity, stg, reason string) *tatarav1alpha1.Task {
	t := chainTask(activity, stg)
	if err := stage.Park(t, reason, time.Now()); err != nil {
		panic(err)
	}
	return t
}

// TestBrainstormCycleFinishedPredicate is the whole contract of the
// Owns(&Task{}) edge, event type by event type.
//
// THE STAGE SET IS TaskDone, NOT StageTerminal. delivered is where all five of
// the 2026-07 mtg skips landed and it is deliberately NOT in terminalStages
// (task_types.go:207) - a predicate written against "terminal stage" would have
// missed every real case. TaskDone is also the exact predicate
// brainstormInFlightProject (projectscan.go:1045) counts in-flight cycles with,
// so the wake fires precisely when a Task stops counting against the deficit.
func TestBrainstormCycleFinishedPredicate(t *testing.T) {
	pred := brainstormCycleFinishedPredicate()

	t.Run("update", func(t *testing.T) {
		cases := map[string]struct {
			old, new *tatarav1alpha1.Task
			want     bool
		}{
			"THE SIGNAL: a skipped cycle lands on delivered": {
				chainTask("brainstorm", tatarav1alpha1.StateRefined),
				chainTask("brainstorm", tatarav1alpha1.StateDone), true},
			// #521: parked/failed are gone as stages; a park is now a FLAG
			// orthogonal to State, and TaskDone is state-only (done|rejected).
			// A park does not free the deficit slot - the Task may still
			// resume (un-park) and finish the SAME cycle, so counting it as
			// finished here would let a second cycle start under it. This
			// mirrors brainstormInFlightProject/documentationInFlightProject,
			// which count a parked Task as still in-flight for the same
			// reason.
			"a parked cycle does NOT free its slot: it may still resume": {
				chainTask("brainstorm", tatarav1alpha1.StateRefined),
				chainParkedTask("brainstorm", tatarav1alpha1.StateRefined, stage.ReasonAwaitingHuman), false},
			"a cycle parked with no re-entry ALSO does not free its slot; the reaper's retention timer collects it eventually": {
				chainTask("brainstorm", tatarav1alpha1.StateRefined),
				chainParkedTask("brainstorm", tatarav1alpha1.StateRefined, stage.ReasonStageDeadline), false},
			"mid-cycle movement is not the signal": {
				chainTask("brainstorm", tatarav1alpha1.StateRefined),
				chainTask("brainstorm", tatarav1alpha1.StateAwaitingReview), false},
			"an already-done cycle re-written is not a second signal": {
				chainTask("brainstorm", tatarav1alpha1.StateDone),
				chainTask("brainstorm", tatarav1alpha1.StateDone), false},
			"a NON-brainstorm Task finishing changes no brainstorm deficit": {
				chainTask("documentation", tatarav1alpha1.StateUnderImplementation),
				chainTask("documentation", tatarav1alpha1.StateDone), false},
			"an unlabelled Task finishing changes no brainstorm deficit": {
				chainTask("", tatarav1alpha1.StateUnderImplementation),
				chainTask("", tatarav1alpha1.StateDone), false},
		}
		for name, tc := range cases {
			t.Run(name, func(t *testing.T) {
				if got := pred.Update(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.new}); got != tc.want {
					t.Fatalf("Update = %v, want %v", got, tc.want)
				}
			})
		}
	})

	// THE SELF-TRIGGER GUARD. The Project reconcile is what CREATES brainstorm
	// Tasks; a Create that woke it would make the reconciler trigger itself on
	// every cycle it starts.
	t.Run("create is never admitted", func(t *testing.T) {
		for _, stg := range []string{
			tatarav1alpha1.StateRefined, tatarav1alpha1.StateDone, "",
		} {
			if pred.Create(event.CreateEvent{Object: chainTask("brainstorm", stg)}) {
				t.Fatalf("Create at stage %q was admitted; the parent creates these Tasks", stg)
			}
		}
	})

	t.Run("delete", func(t *testing.T) {
		if !pred.Delete(event.DeleteEvent{Object: chainTask("brainstorm", tatarav1alpha1.StateRefined)}) {
			t.Fatal("deleting an IN-FLIGHT cycle must be admitted: it frees the slot and raises the deficit")
		}
		if pred.Delete(event.DeleteEvent{Object: chainTask("brainstorm", tatarav1alpha1.StateDone)}) {
			t.Fatal("deleting an already-done cycle frees no slot; it must be dropped")
		}
		if pred.Delete(event.DeleteEvent{Object: chainTask("documentation", tatarav1alpha1.StateUnderImplementation)}) {
			t.Fatal("deleting a non-brainstorm Task must be dropped")
		}
	})

	t.Run("generic is always dropped", func(t *testing.T) {
		if pred.Generic(event.GenericEvent{Object: chainTask("brainstorm", tatarav1alpha1.StateDone)}) {
			t.Fatal("GenericFunc must always return false")
		}
	})
}
