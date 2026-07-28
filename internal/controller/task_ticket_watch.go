package controller

import (
	"context"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// ticketToTask maps an ADMISSION TICKET onto the Task it admits. A mint
// (payload.newTask, or a legacy flat payload) names no Task and maps to nothing:
// the dispatcher creates that Task itself, and its Create event is already a
// wake through For(&Task{}).
//
// This map func deliberately performs NO List and consults NO field index. The
// ticket's payload already names the target, and handler.MapFunc has no error
// return: a failed List inside one silently DROPS the event
// (controller-runtime#1996, open and lifecycle/frozen). A field-free map func
// cannot hit that, which is what lets admissionRequeue stay a backstop for
// leadership gaps rather than a correctness dependency.
//
// It is a METHOD, unlike project_issue_watch.go's free issueToProject, purely so
// it can reach r.Metrics: ticketAdmittedPredicate makes this the single
// EDGE-triggered point in the whole pipeline, so it is the only honest place to
// count a wake. Counting from the predicate would be a side effect inside a
// filter; counting from ensureTicket would be level-triggered and would keep
// counting for as long as the ticket reads Admitted.
func (r *TaskReconciler) ticketToTask(ctx context.Context, obj client.Object) []reconcile.Request {
	q, ok := obj.(*tatarav1alpha1.QueuedEvent)
	if !ok || !queue.IsAdmissionTicket(q) {
		return nil
	}
	if r.Metrics != nil {
		r.Metrics.AdmissionWake(q.Spec.Class, q.Spec.Payload.AgentKind)
	}
	log.FromContext(ctx).Info("admission ticket admitted, waking its task",
		"action", "admission_wake", "resource_id", q.Spec.Payload.TaskRef,
		"queued_event", q.Name, "class", q.Spec.Class,
		"agent_kind", q.Spec.Payload.AgentKind)
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{Namespace: q.Namespace, Name: q.Spec.Payload.TaskRef},
	}}
}

// ticketAdmittedPredicate admits only the events that mean "this ticket just
// became admittable work": the Queued -> Admitted transition, and a create that
// lands already Admitted.
//
// ALL FOUR funcs are set explicitly. An unset predicate.Funcs field defaults to
// returning TRUE, so a bare predicate.Funcs{UpdateFunc: ...} would pass every
// create, delete and generic event straight through to the workqueue.
//
// UpdateFunc is EDGE-triggered (old != Admitted && new == Admitted), not a bare
// read of the new value: the manager cache's 10h resync replays every live
// ticket as an Update whose old and new are identical, and admitting those would
// re-wake every admitted Task twice a day for nothing.
//
// CreateFunc admits an already-Admitted ticket because that is the RESTART case:
// on manager start or leader-election handover the informer replays existing
// objects as Create events, which carry no old state to diff. Without this, a
// Task admitted moments before a rollout would fall all the way back to
// admissionRequeue. Its cost is bounded by the number of live tickets.
//
// Do NOT add predicate.GenerationChangedPredicate to this edge. QueuedEvent
// declares +kubebuilder:subresource:status, so a Status.State patch never bumps
// metadata.generation and the wake would be filtered out entirely.
func ticketAdmittedPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			q, ok := e.Object.(*tatarav1alpha1.QueuedEvent)
			return ok && q.Status.State == tatarav1alpha1.QueueStateAdmitted
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldQ, ok1 := e.ObjectOld.(*tatarav1alpha1.QueuedEvent)
			newQ, ok2 := e.ObjectNew.(*tatarav1alpha1.QueuedEvent)
			return ok1 && ok2 &&
				oldQ.Status.State != tatarav1alpha1.QueueStateAdmitted &&
				newQ.Status.State == tatarav1alpha1.QueueStateAdmitted
		},
		// A deleted ticket admits nothing. The Task's own clocks and the
		// admissionRequeue backstop cover a ticket that vanishes.
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}
