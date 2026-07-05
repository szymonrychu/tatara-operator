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
	// UsagePollInterval is the account-usage poll cadence (claudeSubscription
	// mode). It is the requeue delay after a per-kind/pool account-usage hold so
	// held work re-evaluates once the next poll refreshes the store; a zero value
	// falls back to 60s (see usageRequeueAfter).
	UsagePollInterval time.Duration
}

// RBAC note (Task A10, no-op): the accountusage poller/mirror (cmd/manager)
// needs get on the OAuth Secret and get;create;update on ConfigMaps in the
// operator namespace. No new +kubebuilder:rbac markers are added here because
// access is already granted: project_controller.go already carries a
// namespaced "" /secrets marker (get;list;watch;create;update;patch;delete)
// and repository_controller.go already carries a namespaced "" /configmaps
// marker (get;list;watch;create;update;patch;delete); both are coalesced into
// the SAME namespaced Role in charts/tatara-operator/templates/rbac.yaml,
// scoped to the operator's own namespace (Release.Namespace). That Role is
// the hand-maintained source of truth the chart renders (see
// hack/check-rbac-drift.sh); `make rbac-check` confirms the +kubebuilder
// markers still mirror it with zero drift, so no chart or marker change is
// required for the poller/mirror to read the OAuth Secret and manage the
// account-usage ConfigMap.

// ceilingKey resolves which SpawnCeilingByKind entry governs a queued event,
// mirroring modelForKind/effortForKind precedence (internal/agent/pod.go): a
// non-empty LabelActivity value that is itself a configured ceiling key wins, so
// healthCheck work (enqueued as Kind=brainstorm + activity=healthCheck) is
// governed by the healthCheck ceiling rather than brainstorm's. Otherwise the
// event's Spec.Kind is used.
func ceilingKey(cfg budget.Config, q *tatarav1alpha1.QueuedEvent) string {
	if act := q.Spec.Payload.Labels[tatarav1alpha1.LabelActivity]; act != "" {
		if _, ok := cfg.SpawnCeilingByKind[act]; ok {
			return act
		}
	}
	return q.Spec.Kind
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
		// Exclude Deploying Tasks (pod-less push-CD deploy-supervision): they are
		// non-terminal but run no agent pod, so counting them against the pool
		// re-creates the lane-starvation trap
		// (operator-laneoccupancy-starves-recovery-2026-06-15). They re-acquire a
		// lane only when rerolled to fix a cascade failure (back to a podful state).
		if t != nil && !tatarav1alpha1.TaskTerminal(t) && !tatarav1alpha1.TaskDeploying(t) {
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
		if tatarav1alpha1.TaskTerminal(t) || tatarav1alpha1.TaskDeploying(t) {
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
	qes []tatarav1alpha1.QueuedEvent, tasks []tatarav1alpha1.Task,
	d budget.Decision, cfg budget.Config, sub budget.Subscription, now time.Time) (requeue bool, heldOnUsage bool, err error) {

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
		return false, false, nil
	}

	// claudeSubscription mode decides the account-usage hold PER EVENT (below):
	// the pool-class Decision is fleet-percent-derived and would freeze whole
	// pools at the coarse proactive/emergency thresholds, pre-empting the per-kind
	// spawn ceilings that are the point of subscription mode. customWindow (and
	// disabled) mode keeps the wholesale per-pool short-circuit unchanged.
	subscription := cfg.Enabled && cfg.Mode == budget.ModeClaudeSubscription

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
		poolBlocked := (class == tatarav1alpha1.QueueClassNormal && d.ProactiveBlocked) ||
			(class == tatarav1alpha1.QueueClassAlert && d.EmergencyBlocked)
		if !subscription && poolBlocked {
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
			// claudeSubscription account-usage hold, decided per event: a kind with
			// a configured spawn ceiling is governed ONLY by that ceiling (so
			// incident:98 admits until ~98% while brainstorm:40 is already held); a
			// kind without one falls through to the pool-class proactive/emergency
			// decision. No-op in customWindow mode (handled by the short-circuit
			// above), so subscription==false leaves admission unchanged.
			if subscription {
				key := ceilingKey(cfg, q)
				held := poolBlocked
				reason := "token_budget"
				if _, hasCeiling := cfg.SpawnCeilingByKind[key]; hasCeiling {
					held = budget.KindBlocked(cfg, sub, key, now)
					reason = "kind_ceiling"
				}
				// refine is a scan-pipeline BARRIER (projectscan.go runScans defers
				// mrScan/issueScan/brainstorm/healthCheck until a refine Task reaches
				// a terminal state). A refine event held Queued here never runs,
				// never becomes terminal, and wedges that barrier - and every scan
				// behind it - forever. So refine always admits, regardless of any
				// configured spawnCeilingByKind["refine"].
				if key == "refine" {
					held = false
				}
				if held {
					heldOnUsage = true
					if r.Metrics != nil {
						r.Metrics.AdmissionBlocked(proj.Name, class, q.Spec.Kind, reason)
					}
					log.FromContext(ctx).Info("queue: admission held by account usage",
						"action", "admission_blocked", "class", class, "kind", q.Spec.Kind,
						"reason", reason, "used_percent", d.UsedPercent)
					continue // leave Queued; requeued to re-evaluate after next poll
				}
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
		return false, heldOnUsage, err
	}
	if err := admitPool(tatarav1alpha1.QueueClassNormal, proj.QueueCapacity()); err != nil {
		return false, heldOnUsage, err
	}
	return requeue, heldOnUsage, nil
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
	// claudeSubscription mode reads the fleet store's last-known snapshot even when
	// the poll has gone stale (Store.Healthy=false). The spec's "fall back to
	// customWindow when stale" is deliberately NOT implemented (F7): a
	// claudeSubscription Project has no customWindow inputs (TokenLimit/
	// ResetSchedule) to fall back to. Staleness is instead governed by
	// budget.active() - each window fails open once its own reset passes - made
	// visible by tatara_account_usage_poll_health (F3), with the wrapper's OTel 429
	// floor as the hard backstop. See MEMORY.md 2026-07-04.
	if budgetCfg.Mode == budget.ModeClaudeSubscription && r.Usage != nil {
		sub = r.Usage.Get().Subscription()
	}
	decision := budget.Evaluate(budgetCfg, proj.BudgetWindowState(), sub, time.Now())
	requeue, heldOnUsage, err := r.admit(ctx, &proj, qes, tasks, decision, budgetCfg, sub, time.Now())
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
	// Account-usage-held backstop (claudeSubscription mode): the fleet Store is not
	// a watched resource, so a per-kind or pool-class hold would never resume until
	// an unrelated QueuedEvent re-triggered the dispatcher. Requeue so the gate
	// re-evaluates once the next poll refreshes the store (F2).
	if heldOnUsage {
		return ctrl.Result{RequeueAfter: r.usageRequeueAfter()}, nil
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

// usageRequeueAfter is the delay before re-checking work held on the account-
// usage gate (claudeSubscription mode): the configured poll interval when known,
// else a 60s fallback. Held work resumes on the reconcile this schedules once the
// next poll has refreshed the fleet store (F2).
func (r *DispatcherReconciler) usageRequeueAfter() time.Duration {
	if r.UsagePollInterval > 0 {
		return r.UsagePollInterval
	}
	return 60 * time.Second
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
