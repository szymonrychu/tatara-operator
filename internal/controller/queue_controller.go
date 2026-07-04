package controller

import (
	"context"
	"sort"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/accountusage"
	"github.com/szymonrychu/tatara-operator/internal/budget"
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
	// BudgetDefaults is the operator-wide token-budget config (issue #189). Each
	// Project layers its spec.tokenBudget over this. The zero value is disabled,
	// so the admission gate is inert until configured.
	BudgetDefaults budget.Config
	// Usage is the fleet-wide Claude account usage snapshot (claudeSubscription
	// mode). Nil-safe: a nil store yields an empty snapshot (nothing per-kind held).
	Usage *accountusage.Store
}

// blockKindFunc returns a predicate that reports whether a queued event of a
// given kind must be held on the account usage gate. Subscription mode reads the
// fleet-wide store; other modes never per-kind block.
func (r *DispatcherReconciler) blockKindFunc(proj *tatarav1alpha1.Project, cfg budget.Config, now time.Time) func(string) bool {
	var sub budget.Subscription
	if r.Usage != nil {
		sub = r.Usage.Get().Subscription()
	}
	return func(kind string) bool {
		return budget.KindBlocked(cfg, sub, kind, now)
	}
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
// Returns requeue=true when a stale terminal Task was deleted so Reconcile can
// signal a prompt re-attempt via ctrl.Result{RequeueAfter: time.Second}.
func (r *DispatcherReconciler) admit(ctx context.Context, proj *tatarav1alpha1.Project,
	qes []tatarav1alpha1.QueuedEvent, tasks []tatarav1alpha1.Task, d budget.Decision, blockKind func(string) bool) (requeue bool, err error) {

	// Full project pause (issue: maxConcurrentTasks=0 must create NO agent work
	// at all, not fall back to the QueueCapacity hard floor of 3). This is the
	// sole chokepoint where a QueuedEvent becomes a Task (both normal and alert
	// pools), so gating here fully holds the project: no scan/lifecycle/
	// brainstorm/incident Task is ever created while paused. The >0
	// concurrency-cap semantics below are unchanged.
	if proj.Spec.MaxConcurrentTasks == 0 {
		for i := range qes {
			q := &qes[i]
			if !isQueued(q.Status.State) {
				continue
			}
			if r.Metrics != nil {
				r.Metrics.AdmissionBlocked(proj.Name, q.Spec.Class, "", "project_paused")
			}
			log.FromContext(ctx).Info("queue: admission held, project paused",
				"action", "project_paused_skip", "project", proj.Name, "repo", q.Spec.RepositoryRef,
				"resource_id", q.Name, "class", q.Spec.Class, "kind", q.Spec.Kind, "reason", "max_concurrent_tasks_zero")
		}
		return false, nil
	}

	admitPool := func(class string, cap int) error {
		queued := make([]*tatarav1alpha1.QueuedEvent, 0)
		for i := range qes {
			if qes[i].Spec.Class == class && isQueued(qes[i].Status.State) {
				queued = append(queued, &qes[i])
			}
		}
		// Token-budget gate (issue #189): hold this pool's work when window usage
		// has reached the pool's threshold. The normal pool (proactive/reactive
		// work) pauses at the proactive threshold; the alert pool (incidents) only
		// at the higher emergency threshold, so incidents keep flowing while
		// proactive work is paused. A disabled budget yields the zero Decision, so
		// neither flag is set and admission is unchanged.
		blocked := (class == tatarav1alpha1.QueueClassNormal && d.ProactiveBlocked) ||
			(class == tatarav1alpha1.QueueClassAlert && d.EmergencyBlocked)
		if blocked {
			if len(queued) > 0 {
				if r.Metrics != nil {
					r.Metrics.AdmissionBlocked(proj.Name, class, "", "token_budget")
				}
				log.FromContext(ctx).Info("queue: admission held by token budget",
					"action", "admission_blocked", "class", class, "reason", "token_budget",
					"used_percent", d.UsedPercent, "queued", len(queued))
			}
			return nil
		}
		inflight := r.poolInflight(qes, tasks, class)
		sort.Slice(queued, func(i, j int) bool { return queued[i].Spec.Seq < queued[j].Spec.Seq })
		for _, q := range queued {
			if inflight >= cap {
				break
			}
			if blockKind != nil && blockKind(q.Spec.Kind) {
				if r.Metrics != nil {
					r.Metrics.AdmissionBlocked(proj.Name, class, q.Spec.Kind, "kind_ceiling")
				}
				continue // held on its per-kind account-usage ceiling; leave Queued
			}
			task, err := queue.BuildTaskFromQueuedEvent(q, proj, r.Scheme)
			if err != nil {
				return err
			}
			if err := r.Create(ctx, task); err != nil {
				if !apierrors.IsAlreadyExists(err) {
					// Leave Queued; requeue. Slot not consumed (inflight derives from Admitted).
					return err
				}
				// AlreadyExists: get the existing Task to determine if it is terminal.
				existing := &tatarav1alpha1.Task{}
				if getErr := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, existing); getErr != nil {
					return getErr
				}
				if tatarav1alpha1.TaskTerminal(existing) {
					// Name collision with a dead Task: delete it and signal a prompt
					// requeue so the next pass creates a fresh Task. Continue processing
					// the rest of the pool's queued events in this same pass so sibling
					// events are not abandoned.
					if delErr := r.Delete(ctx, existing); delErr != nil && !apierrors.IsNotFound(delErr) {
						return delErr
					}
					log.FromContext(ctx).Info("queue: deleted stale terminal task on name collision",
						"action", "queue_stale_delete", "resource_id", q.Name, "task", task.Name)
					requeue = true
					continue
				}
				// Non-terminal Task with this name already exists: genuine idempotent
				// re-admit (e.g. Status().Update failed on a prior pass). Fall through
				// to mark Admitted pointing at the existing Task.
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
		return false, err
	}
	if err := admitPool(tatarav1alpha1.QueueClassNormal, proj.QueueCapacity()); err != nil {
		return false, err
	}
	return requeue, nil
}

// reconcileDone GC-deletes Admitted events whose Task is terminal or gone
// (completed QueuedEvents are removed, not tombstoned to Done).
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

// +kubebuilder:rbac:groups=tatara.dev,resources=queuedevents,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tatara.dev,resources=queuedevents/status,verbs=get;update;patch

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
	// Token-budget decision for this admit pass (issue #189): computed once from
	// the project's resolved config + persisted window/snapshot state, used both
	// to gate admission and to drive the boundary-aware requeue below.
	budgetCfg := proj.BudgetConfig(r.BudgetDefaults)
	sub := proj.BudgetSubscription()
	if budgetCfg.Mode == budget.ModeClaudeSubscription && r.Usage != nil {
		sub = r.Usage.Get().Subscription()
	}
	decision := budget.Evaluate(budgetCfg, proj.BudgetWindowState(), sub, time.Now())
	requeue, err := r.admit(ctx, &proj, qes, tasks, decision, r.blockKindFunc(&proj, budgetCfg, time.Now()))
	if err != nil {
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
			r.Metrics.SetQueueDepth(proj.Name, class, depth)
			r.Metrics.SetQueueInflight(proj.Name, class, r.poolInflight(qes, tasks, class))
		}
		// Token-budget gauges (issue #189): track usage against both thresholds so
		// a dashboard plots used vs proactive/emergency per project. Only emitted
		// when the budget is enabled, so disabled projects create no series.
		if budgetCfg.Enabled {
			r.Metrics.SetTokenBudgetUsedRatio(proj.Name, "used", decision.UsedPercent/100)
			pro, emg := budget.ResolvePercents(budgetCfg)
			r.Metrics.SetTokenBudgetUsedRatio(proj.Name, "proactive", float64(pro)/100)
			r.Metrics.SetTokenBudgetUsedRatio(proj.Name, "emergency", float64(emg)/100)
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
	// Budget-hold backstop: when a pool is paused on the token budget and still has
	// Queued work, re-check near the next window reset (the cron boundary, capped)
	// so the held work resumes promptly once the window rolls, even if no new
	// QueuedEvent arrives to re-trigger the dispatcher (issue #189).
	budgetHeld := (decision.ProactiveBlocked && poolHasQueued(qes, tatarav1alpha1.QueueClassNormal)) ||
		(decision.EmergencyBlocked && poolHasQueued(qes, tatarav1alpha1.QueueClassAlert))
	if requeue {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if budgetHeld {
		return ctrl.Result{RequeueAfter: budgetRequeueAfter(budgetCfg, time.Now())}, nil
	}
	if waiting {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

// poolHasQueued reports whether the pool of the given class has any Queued
// (not-yet-admitted) QueuedEvent.
func poolHasQueued(qes []tatarav1alpha1.QueuedEvent, class string) bool {
	for i := range qes {
		if qes[i].Spec.Class == class && isQueued(qes[i].Status.State) {
			return true
		}
	}
	return false
}

// budgetRequeueAfter returns how long to wait before re-checking a budget-held
// pool: the time to the next custom-window reset boundary (from the cron, plus a
// small slack and capped at 5m to bound staleness), or a 60s fallback when there
// is no parseable schedule (e.g. claudeSubscription mode, where the snapshot's
// own reset time drives unblocking).
func budgetRequeueAfter(cfg budget.Config, now time.Time) time.Duration {
	const fallback = 60 * time.Second
	const maxWait = 5 * time.Minute
	if cfg.ResetSchedule == "" {
		return fallback
	}
	sched, err := budget.ParseSchedule(cfg.ResetSchedule)
	if err != nil {
		return fallback
	}
	next := sched.Next(now)
	if next.IsZero() {
		return fallback
	}
	wait := next.Sub(now) + time.Second
	if wait <= 0 {
		return fallback
	}
	if wait > maxWait {
		return maxWait
	}
	return wait
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
