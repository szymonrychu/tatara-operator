package controller

import (
	"context"
	"sort"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// isQueued reports whether a QueuedEvent state is effectively Queued.
// State=="" handles a QE whose Status().Update failed after Create (stranded ghost).
func isQueued(state string) bool {
	return state == tatarav1alpha1.QueueStateQueued || state == ""
}

type DispatcherReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
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
		if _, queued := t.Labels[queue.LabelQueuedEvent]; queued {
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
// QueueCapacity, each in strict ascending seq order (pure per-project FIFO within a
// pool; head-of-line blocking accepted). Wired in Task 8 Reconcile.
func (r *DispatcherReconciler) admit(ctx context.Context, proj *tatarav1alpha1.Project,
	qes []tatarav1alpha1.QueuedEvent, tasks []tatarav1alpha1.Task) error {

	admitPool := func(class string, cap int) error {
		inflight := r.poolInflight(qes, tasks, class)
		queued := make([]*tatarav1alpha1.QueuedEvent, 0)
		for i := range qes {
			if qes[i].Spec.Class == class && isQueued(qes[i].Status.State) {
				queued = append(queued, &qes[i])
			}
		}
		sort.Slice(queued, func(i, j int) bool { return queued[i].Spec.Seq < queued[j].Spec.Seq })
		for _, q := range queued {
			if inflight >= cap {
				break
			}
			task, err := queue.BuildTaskFromQueuedEvent(q, proj, r.Scheme)
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
func (r *DispatcherReconciler) reconcileDone(ctx context.Context, qes []tatarav1alpha1.QueuedEvent, tasks []tatarav1alpha1.Task) (bool, error) {
	changed := false
	for i := range qes {
		q := &qes[i]
		if q.Status.State != tatarav1alpha1.QueueStateAdmitted {
			continue
		}
		t := taskByName(tasks, q.Status.TaskRef)
		if t == nil || tatarav1alpha1.TaskTerminal(t) {
			log.FromContext(ctx).Info("queue: event done", "action", "queue_done", "resource_id", q.Name, "class", q.Spec.Class)
			if err := r.Delete(ctx, q); err != nil && !apierrors.IsNotFound(err) {
				return changed, err
			}
			changed = true
		}
	}
	return changed, nil
}

func (r *DispatcherReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var qe tatarav1alpha1.QueuedEvent
	if err := r.Get(ctx, req.NamespacedName, &qe); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	var proj tatarav1alpha1.Project
	if err := r.Get(ctx, types.NamespacedName{Namespace: qe.Namespace, Name: qe.Spec.ProjectRef}, &proj); err != nil {
		// Project gone (deleted mid-flight): nothing to admit against. Drop
		// the not-found so we do not loop on a zero-value Project whose
		// capacities read 0 and silently block all admission.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if proj.Name == "" {
		return ctrl.Result{}, nil
	}

	listProject := func() ([]tatarav1alpha1.QueuedEvent, []tatarav1alpha1.Task, error) {
		var qel tatarav1alpha1.QueuedEventList
		if err := r.List(ctx, &qel, client.InNamespace(qe.Namespace)); err != nil {
			return nil, nil, err
		}
		var tl tatarav1alpha1.TaskList
		if err := r.List(ctx, &tl, client.InNamespace(qe.Namespace)); err != nil {
			return nil, nil, err
		}
		return filterQEsByProject(qel.Items, proj.Name), filterTasksByProject(tl.Items, proj.Name), nil
	}

	qes, tasks, err := listProject()
	if err != nil {
		return ctrl.Result{}, err
	}
	if _, err := r.reconcileDone(ctx, qes, tasks); err != nil {
		return ctrl.Result{}, err
	}
	// Re-list after Done mutations so admission sees fresh QE and Task state.
	qes, tasks, err = listProject()
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.admit(ctx, &proj, qes, tasks); err != nil {
		return ctrl.Result{}, err
	}
	// Re-list after admission to get fresh state for gauge snapshot.
	qes, tasks, err = listProject()
	if err != nil {
		return ctrl.Result{}, err
	}
	if r.Metrics != nil {
		for _, class := range []string{tatarav1alpha1.QueueClassNormal, tatarav1alpha1.QueueClassAlert} {
			depth := 0
			for i := range qes {
				if qes[i].Spec.Class == class && isQueued(qes[i].Status.State) {
					depth++
				}
			}
			r.Metrics.SetQueueDepth(class, depth)
			r.Metrics.SetQueueInflight(class, r.poolInflight(qes, tasks, class))
		}
	}
	// Backstop: if any pool has waiting (Queued/empty-state) work and is at capacity,
	// requeue to catch missed Task-terminal watch events.
	waiting := false
	for _, class := range []string{tatarav1alpha1.QueueClassNormal, tatarav1alpha1.QueueClassAlert} {
		cap := proj.QueueCapacity()
		if class == tatarav1alpha1.QueueClassAlert {
			cap = proj.AlertCapacity()
		}
		if r.poolInflight(qes, tasks, class) >= cap {
			for i := range qes {
				if qes[i].Spec.Class == class && isQueued(qes[i].Status.State) {
					waiting = true
					break
				}
			}
		}
	}
	if waiting {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func queuedAutonomousCount(qes []tatarav1alpha1.QueuedEvent) int {
	n := 0
	for i := range qes {
		if qes[i].Spec.Autonomous && isQueued(qes[i].Status.State) {
			n++
		}
	}
	return n
}

func filterQEsByProject(in []tatarav1alpha1.QueuedEvent, project string) []tatarav1alpha1.QueuedEvent {
	out := make([]tatarav1alpha1.QueuedEvent, 0)
	for i := range in {
		if in[i].Spec.ProjectRef == project {
			out = append(out, in[i])
		}
	}
	return out
}

func filterTasksByProject(in []tatarav1alpha1.Task, project string) []tatarav1alpha1.Task {
	out := make([]tatarav1alpha1.Task, 0)
	for i := range in {
		if in[i].Spec.ProjectRef == project {
			out = append(out, in[i])
		}
	}
	return out
}

func (r *DispatcherReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mapTaskToQE := func(_ context.Context, obj client.Object) []reconcile.Request {
		qeName := obj.GetLabels()[queue.LabelQueuedEvent]
		if qeName == "" {
			return nil
		}
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: qeName}}}
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&tatarav1alpha1.QueuedEvent{}).
		Watches(&tatarav1alpha1.Task{}, handler.EnqueueRequestsFromMapFunc(mapTaskToQE)).
		Complete(r)
}
