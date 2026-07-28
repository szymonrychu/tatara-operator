package controller

import (
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// isBrainstormTask reports whether t is a brainstorm cycle, keyed on the SAME
// activity label brainstormInFlightProject (projectscan.go) counts with.
func isBrainstormTask(t *tatarav1alpha1.Task) bool {
	return t.Labels[labelActivity] == "brainstorm"
}

// brainstormCycleFinishedPredicate admits exactly the events that RAISE the
// brainstorm deficit, so the refill runs the moment a cycle stops counting
// against it instead of waiting out brainstormResyncInterval (15m). A cycle
// that SKIPS writes no Issue at all, so the Watches(&Issue{}) edge never sees
// it - that is the gap this closes, and it cost the mtg proposer five 15m
// waits in a row in 2026-07.
//
//	Update  admit iff isBrainstorm && !TaskDone(old) && TaskDone(new)
//	Delete  admit iff isBrainstorm && !TaskDone(obj)   (an in-flight cycle's slot is freed)
//	Create  NEVER  (the parent CREATES these Tasks; admitting self-triggers)
//	Generic NEVER
//
// It reuses tatarav1alpha1.TaskDone rather than defining a second stage set:
// TaskDone is StageTerminal || delivered, and delivered - deliberately absent
// from terminalStages (task_types.go:207) - is where every skipped cycle lands.
// A predicate written against StageTerminal alone would drop every real case.
//
// GenerationChangedPredicate MUST NOT be composed onto this edge. Task carries
// +kubebuilder:subresource:status and a stage change is a status write; that
// predicate's own godoc says a status-only update does not increase Generation
// and will not be reconciled, so it would silently drop every event this edge
// exists for while looking perfectly wired.
func brainstormCycleFinishedPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldT, ok1 := e.ObjectOld.(*tatarav1alpha1.Task)
			newT, ok2 := e.ObjectNew.(*tatarav1alpha1.Task)
			if !ok1 || !ok2 || !isBrainstormTask(newT) {
				return false
			}
			return !tatarav1alpha1.TaskDone(oldT) && tatarav1alpha1.TaskDone(newT)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			t, ok := e.Object.(*tatarav1alpha1.Task)
			return ok && isBrainstormTask(t) && !tatarav1alpha1.TaskDone(t)
		},
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// brainstormChainEdge registers the Task-completion wake edge on b. Owns rather
// than Watches+EnqueueRequestsFromMapFunc because Task genuinely IS controller-
// owned by Project (intake.go, takeover_mint.go, queue/enqueue.go all set the
// Project as controller), which is the documented dividing line between the two
// idioms - Owns supplies EnqueueRequestForOwner(OnlyControllerOwner()) for free.
//
// It is a function rather than two inline lines in SetupWithManager so the
// envtest (project_brainstorm_chain_test.go) can register the SAME edge the
// production wiring registers, instead of a hand-copied duplicate that would go
// on passing after someone deleted the real one.
func brainstormChainEdge(b *builder.Builder) *builder.Builder {
	return b.Owns(&tatarav1alpha1.Task{}, builder.WithPredicates(brainstormCycleFinishedPredicate()))
}
