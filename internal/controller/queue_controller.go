package controller

import (
	"context"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type DispatcherReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	Alloc   *queue.SeqAllocator
	Metrics *obs.OperatorMetrics
}

func taskByName(tasks []tatarav1alpha1.Task, name string) *tatarav1alpha1.Task {
	for i := range tasks {
		if tasks[i].Name == name {
			return &tasks[i]
		}
	}
	return nil
}

// poolInflight counts Admitted QueuedEvents of class whose Task is still
// non-terminal. Migration of unlabelled pre-queue Tasks is added in Task 7.
func (r *DispatcherReconciler) poolInflight(qes []tatarav1alpha1.QueuedEvent, tasks []tatarav1alpha1.Task, class string) int {
	n := 0
	for i := range qes {
		q := &qes[i]
		if q.Spec.Class != class || q.Status.State != tatarav1alpha1.QueueStateAdmitted {
			continue
		}
		t := taskByName(tasks, q.Status.TaskRef)
		if t != nil && !tatarav1alpha1.TaskTerminal(t) {
			n++
		}
	}
	return n
}

// reconcileDone flips Admitted events whose Task is terminal or gone to Done.
// Called by the Reconcile loop added in Task 6.
//
//nolint:unused
func (r *DispatcherReconciler) reconcileDone(ctx context.Context, qes []tatarav1alpha1.QueuedEvent, tasks []tatarav1alpha1.Task) (bool, error) {
	changed := false
	for i := range qes {
		q := &qes[i]
		if q.Status.State != tatarav1alpha1.QueueStateAdmitted {
			continue
		}
		t := taskByName(tasks, q.Status.TaskRef)
		if t == nil || tatarav1alpha1.TaskTerminal(t) {
			q.Status.State = tatarav1alpha1.QueueStateDone
			if err := r.Status().Update(ctx, q); err != nil {
				return changed, err
			}
			changed = true
			log.FromContext(ctx).Info("queue: event done", "action", "queue_done", "resource_id", q.Name, "class", q.Spec.Class)
		}
	}
	return changed, nil
}
