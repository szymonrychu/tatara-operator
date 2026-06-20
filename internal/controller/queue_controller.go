package controller

import (
	"context"
	"sort"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
// non-terminal, plus non-terminal Tasks created before the queue existed (no
// LabelQueuedEvent label) so capacity is not over-admitted at cutover.
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
	// Migration: non-terminal Tasks created before the queue (no queued-event
	// label) count toward their pool so capacity is not over-admitted at cutover.
	for i := range tasks {
		t := &tasks[i]
		if _, queued := t.Labels[LabelQueuedEvent]; queued {
			continue
		}
		if tatarav1alpha1.TaskTerminal(t) {
			continue
		}
		taskClass := tatarav1alpha1.QueueClassNormal
		if t.Spec.Kind == "incident" {
			taskClass = tatarav1alpha1.QueueClassAlert
		}
		if taskClass == class {
			n++
		}
	}
	return n
}

// admit drains the alert pool to AlertCapacity, then the normal pool to
// QueueCapacity, each in strict ascending seq order (pure global FIFO within a
// pool; head-of-line blocking accepted). Wired in Task 8 Reconcile.
//
//nolint:unused
func (r *DispatcherReconciler) admit(ctx context.Context, proj *tatarav1alpha1.Project,
	qes []tatarav1alpha1.QueuedEvent, tasks []tatarav1alpha1.Task) error {

	admitPool := func(class string, cap int) error {
		inflight := r.poolInflight(qes, tasks, class)
		queued := make([]*tatarav1alpha1.QueuedEvent, 0)
		for i := range qes {
			if qes[i].Spec.Class == class && qes[i].Status.State == tatarav1alpha1.QueueStateQueued {
				queued = append(queued, &qes[i])
			}
		}
		sort.Slice(queued, func(i, j int) bool { return queued[i].Spec.Seq < queued[j].Spec.Seq })
		for _, q := range queued {
			if inflight >= cap {
				break
			}
			task, err := buildTaskFromQueuedEvent(q, proj, r.Scheme)
			if err != nil {
				return err
			}
			if err := r.Create(ctx, task); err != nil && !apierrors.IsAlreadyExists(err) {
				// Leave Queued; requeue. Slot not consumed (inflight derives from Admitted).
				return err
			}
			q.Status.State = tatarav1alpha1.QueueStateAdmitted
			q.Status.TaskRef = task.Name
			now := metav1.Now()
			q.Status.AdmittedAt = &now
			if err := r.Status().Update(ctx, q); err != nil {
				return err
			}
			inflight++
			if r.Metrics != nil {
				r.Metrics.QueueAdmitted(class, q.Spec.Kind)
			}
			log.FromContext(ctx).Info("queue: admitted",
				"action", "queue_admit", "resource_id", q.Name, "task", task.Name,
				"class", class, "seq", q.Spec.Seq, "kind", q.Spec.Kind)
		}
		return nil
	}

	if err := admitPool(tatarav1alpha1.QueueClassAlert, proj.AlertCapacity()); err != nil {
		return err
	}
	return admitPool(tatarav1alpha1.QueueClassNormal, proj.QueueCapacity())
}

// reconcileDone flips Admitted events whose Task is terminal or gone to Done.
// Called by the Reconcile loop added in Task 8.
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
