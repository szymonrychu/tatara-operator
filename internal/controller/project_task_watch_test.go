package controller

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
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
	t.Status.Stage = stg
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
				chainTask("brainstorm", tatarav1alpha1.StageBrainstorming),
				chainTask("brainstorm", tatarav1alpha1.StageDelivered), true},
			"a parked cycle frees its slot too": {
				chainTask("brainstorm", tatarav1alpha1.StageBrainstorming),
				chainTask("brainstorm", tatarav1alpha1.StageParked), true},
			"a failed cycle frees its slot too": {
				chainTask("brainstorm", tatarav1alpha1.StageBrainstorming),
				chainTask("brainstorm", tatarav1alpha1.StageFailed), true},
			"mid-cycle movement is not the signal": {
				chainTask("brainstorm", tatarav1alpha1.StageBrainstorming),
				chainTask("brainstorm", tatarav1alpha1.StageReviewing), false},
			"an already-done cycle re-written is not a second signal": {
				chainTask("brainstorm", tatarav1alpha1.StageDelivered),
				chainTask("brainstorm", tatarav1alpha1.StageDelivered), false},
			"a NON-brainstorm Task finishing changes no brainstorm deficit": {
				chainTask("documentation", tatarav1alpha1.StageImplementing),
				chainTask("documentation", tatarav1alpha1.StageDelivered), false},
			"an unlabelled Task finishing changes no brainstorm deficit": {
				chainTask("", tatarav1alpha1.StageImplementing),
				chainTask("", tatarav1alpha1.StageDelivered), false},
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
			tatarav1alpha1.StageBrainstorming, tatarav1alpha1.StageDelivered, "",
		} {
			if pred.Create(event.CreateEvent{Object: chainTask("brainstorm", stg)}) {
				t.Fatalf("Create at stage %q was admitted; the parent creates these Tasks", stg)
			}
		}
	})

	t.Run("delete", func(t *testing.T) {
		if !pred.Delete(event.DeleteEvent{Object: chainTask("brainstorm", tatarav1alpha1.StageBrainstorming)}) {
			t.Fatal("deleting an IN-FLIGHT cycle must be admitted: it frees the slot and raises the deficit")
		}
		if pred.Delete(event.DeleteEvent{Object: chainTask("brainstorm", tatarav1alpha1.StageDelivered)}) {
			t.Fatal("deleting an already-done cycle frees no slot; it must be dropped")
		}
		if pred.Delete(event.DeleteEvent{Object: chainTask("documentation", tatarav1alpha1.StageImplementing)}) {
			t.Fatal("deleting a non-brainstorm Task must be dropped")
		}
	})

	t.Run("generic is always dropped", func(t *testing.T) {
		if pred.Generic(event.GenericEvent{Object: chainTask("brainstorm", tatarav1alpha1.StageDelivered)}) {
			t.Fatal("GenericFunc must always return false")
		}
	})
}
