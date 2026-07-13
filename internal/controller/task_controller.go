package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/prompt"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

const (
	memGateRequeue    = 15 * time.Second
	pollRequeue       = 30 * time.Second
	agentBootRequeue  = 5 * time.Second
	agentBootDeadline = 5 * time.Minute
	// busyRequeue paces re-submission after a wrapper 409 "session busy": the
	// session is processing a prior turn, so requeue on a short bounded interval
	// (backpressure) instead of erroring and tight-looping on reconcile backoff
	// (issue #168). A session stuck busy forever is bounded by the F.4 WORK clock.
	busyRequeue       = 15 * time.Second
	maxPodRecreations = 3
	turnTimeoutGrace  = 60 * time.Second

	annCurrentTurn           = tatarav1alpha1.AnnCurrentTurn
	annTurnComplete          = tatarav1alpha1.AnnTurnComplete
	annPodRecreations        = tatarav1alpha1.AnnPodRecreations
	annTurnStartedAt         = tatarav1alpha1.AnnTurnStartedAt
	annTurnLastActivity      = tatarav1alpha1.AnnTurnLastActivity
	annAgentUnreachableSince = "tatara.dev/agent-unreachable-since"
)

// TaskReconciler spawns one wrapper session per Task and drives it turn by
// turn through the F.1 stage machine.
type TaskReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Metrics   *obs.OperatorMetrics
	Session   agent.Session
	PodConfig agent.PodConfig
	// SCMFor returns an scm.SCMWriter for the given provider name ("github"|"gitlab").
	// Nil in tests that do not exercise write-back; replaced with a fake in
	// write-back tests.
	SCMFor func(provider string) (scm.SCMWriter, error)
	// ReaderFor returns a token-bound scm.SCMReader for title-level duplicate
	// detection in createProposal. Nil in tests that do not exercise reading;
	// wired in wire.go at runtime. When nil the title check is skipped gracefully.
	ReaderFor func(provider, token string) (scm.SCMReader, error)
	// SpillerFor resolves the A.7 byte-budget spiller for a Project. Every stage
	// write goes through objbudget.FitTask; a nil SpillerFor (unit tests) means an
	// over-budget write is refused rather than spilled.
	SpillerFor func(proj *tatarav1alpha1.Project) objbudget.Spiller
	// Seq allocates the per-project QueuedEvent sequence. Nil (unit tests) makes
	// the pod path run unqueued instead of wedging on an admission that never comes.
	Seq *queue.SeqSource
	// BundleMetrics is the E.5 render sink (operator_bundle_bytes /
	// operator_bundle_elided_total). Optional.
	BundleMetrics prompt.Metrics
}

// +kubebuilder:rbac:groups=tatara.dev,resources=tasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tatara.dev,resources=tasks/status,verbs=get;update;patch
// tasks/finalizers update is what lets the operator set blockOwnerDeletion:true on
// the Task ownerRefs of Issues and MergeRequests (contract B.2 rule 2): a custom
// controller does not get blockOwnerDeletion for free, the API server checks this
// exact permission on the OWNER when a dependent asks for it.
// +kubebuilder:rbac:groups=tatara.dev,resources=tasks/finalizers,verbs=update
// +kubebuilder:rbac:groups=tatara.dev,resources=issues;mergerequests,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tatara.dev,resources=issues/status;mergerequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=tatara.dev,resources=projects;repositories,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods;services,verbs=get;list;watch;create;update;patch;delete

// isFieldSelectorUnsupported reports whether a list error is "field label not
// supported", which happens when a direct (non-cached) client is used without a
// registered field index. In that case callers fall back to a full-namespace scan
// with in-Go filtering.
func isFieldSelectorUnsupported(err error) bool {
	return err != nil && strings.Contains(err.Error(), "field label not supported")
}

// Reconcile drives a Task through the F stage machine, and NOTHING ELSE. There
// is no phase and no per-kind prompt: the stage decides what
// happens, the F.4 clocks decide when it stops, and internal/prompt.Render
// decides what the agent sees.
//
//	mint          -> triaging (F.3's Create edge)
//	terminal      -> the REAPER owns it; return
//	clocks (F.4)  -> CLOCK 1 admission / CLOCK 2 readiness / CLOCK 3 work
//	caps  (F.4)   -> maxTurnsPerTask, maxPodRecreations, pod-stopped-no-outcome
//	pod-less stage-> the operator does the work
//	pod stage     -> ticket -> admission -> pod -> G.10 handshake -> turn-0
func (r *TaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var task tatarav1alpha1.Task
	if err := r.Get(ctx, req.NamespacedName, &task); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, fmt.Errorf("get task: %w", err)
	}
	if !task.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// The printcolumn-backed ShortDescription, kept fresh on every reconcile so
	// `kubectl get task` is scannable without a describe.
	if err := r.patchTaskStatus(ctx, &task, func(fresh *tatarav1alpha1.Task) bool {
		desc := shortDescription(fresh.Spec.Goal)
		if fresh.Status.ShortDescription == desc {
			return false
		}
		fresh.Status.ShortDescription = desc
		return true
	}); err != nil {
		l.Error(err, "task: update derived status (non-fatal)",
			"action", "task_derived_status", "resource_id", task.Name)
	}

	var project tatarav1alpha1.Project
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &project); err != nil {
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, fmt.Errorf("get project: %w", err)
	}

	res, err := r.reconcileStage(ctx, &project, &task, time.Now())
	if err != nil {
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, err
	}
	r.updateInflightGauge(ctx)
	r.Metrics.ReconcileResult("Task", "success")
	return res, nil
}

// reconcileStage is Reconcile's body, with the clock injectable.
func (r *TaskReconciler) reconcileStage(ctx context.Context, project *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, now time.Time) (ctrl.Result, error) {

	l := log.FromContext(ctx)

	// MINT (F.3's Create edges). The sweep mints straight into triaging or
	// parked(backlog-sweep) and the nightly batch straight into documenting. Those
	// targets are carried in the IMMUTABLE Spec.InitialStage (fix C5) so this edge
	// derives them here, with no racing post-create status write by the minter;
	// anything else starts at triaging.
	if task.Status.Stage == "" {
		initStage := task.Spec.InitialStage
		if initStage == "" {
			initStage = tatarav1alpha1.StageTriaging
		}
		if err := r.enter(ctx, project, task, nil, initStage, task.Spec.InitialStageReason, now); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// The REAPER owns a terminal Task (B.6). This reconciler never deletes one and
	// never resurrects one: a parked Task's ONLY exits are the narrow F.6 re-entry
	// rules, which stage.Unpark applies from the webhook and the sweep.
	if tatarav1alpha1.StageTerminal(task) {
		return ctrl.Result{}, nil
	}

	// THE THREE CLOCKS (F.4). Gap 5: nothing else in production calls
	// stage.ArmedClock, so without this every deadline in the contract is fiction.
	res, handled, err := r.reconcileClocks(ctx, project, task, now)
	if err != nil || handled {
		return res, err
	}

	// The caps EVERY pod stage carries on top of its clocks (F.4).
	handled, err = r.reconcileCaps(ctx, project, task, now)
	if err != nil || handled {
		return ctrl.Result{}, err
	}

	agentKind := stage.AgentKindFor(task.Status.Stage)
	if agentKind == "" {
		switch task.Status.Stage {
		case tatarav1alpha1.StageTriaging:
			return r.reconcileTriaging(ctx, project, task, now)

		case tatarav1alpha1.StageApproved:
			// POD-LESS, and yet it needs a ticket: F.3's approved -> implementing
			// edge IS the admission of the implement pod's ticket, and the
			// DISPATCHER applies it (queue_controller.go admitTicket). Enqueue the
			// ticket and wait; applying the edge here too would double-transition.
			if _, err := r.ensureTicket(ctx, project, task, stage.AgentImplement); err != nil {
				return ctrl.Result{}, err
			}
			return res, nil

		case tatarav1alpha1.StageMerging, tatarav1alpha1.StageDeploying:
			// StageReconciler (stage_controller.go) drives these through StageDriver -
			// the single merge egress. This reconciler owns only their CLOCKS, which
			// ran above: merging's 4h -> parked(merge-timeout), deploying's 2h ->
			// parked(deploy-timeout).
			return res, nil

		case tatarav1alpha1.StageDelivered:
			// Quasi-terminal. Its clock (48h) elapses to (reap) and the reaper
			// collects it once the nightly batch has documented it.
			return res, nil
		}
		l.Info("stage has no driver", "action", "stage_no_driver",
			"resource_id", task.Name, "stage", task.Status.Stage)
		return res, nil
	}

	// The memory gate is SPAWN-ONLY: a pod already working must not be torn down by
	// a memory blip, and a Task that has not been admitted has nothing to gate.
	if task.Status.PodStartedAt == nil && !memoryStablyReady(project, now) {
		if !tatarav1alpha1.InfraIncidentExempt(task.Spec) {
			l.Info("task gated: project memory not stably ready",
				"action", "task_memory_gate", "resource_id", task.Name, "project", project.Name)
			return ctrl.Result{RequeueAfter: memGateRequeue}, nil
		}
		// #236: an incident investigating the memory stack must not be gated on that
		// same stack being Ready, or infra-outage self-heal deadlocks.
		l.Info("task memory gate bypassed for infra incident",
			"action", "task_memory_gate_bypass", "resource_id", task.Name,
			"project", project.Name, "alert_rules", strings.Join(task.Spec.AlertRules, ","))
		r.Metrics.MemoryGateBypass(project.Name, task.Spec.Kind)
	}

	return r.reconcilePodStage(ctx, project, task, agentKind, now)
}

// patchTaskAnnotations Get-fresh + mutate + Update's a Task's annotations
// (metadata subresource), retrying on conflict. mutate returns whether a
// write is needed; when it returns false the fresh object is still copied
// back into task unconditionally (both on skip and on a successful write) so
// callers observe the current resourceVersion afterward instead of a stale
// one - this is what makes a "someone else already applied this" skip-write
// branch adopt the winner's state rather than risk a 409 on the next write in
// the same reconcile. Mirrors repository_controller.go's patchStatus.
func (r *TaskReconciler) patchTaskAnnotations(ctx context.Context, task *tatarav1alpha1.Task, mutate func(*tatarav1alpha1.Task) bool) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		if !mutate(fresh) {
			*task = *fresh
			return nil
		}
		if err := r.Update(ctx, fresh); err != nil {
			return err
		}
		*task = *fresh
		return nil
	})
}

// patchTaskStatus Get-fresh + mutate + Status().Update's a Task's status,
// retrying on conflict. Same skip/write copy-back contract as
// patchTaskAnnotations (and repository_controller.go's patchStatus): the
// unconditional `*task = *fresh` on both paths is what preserves the
// site-153 ledger-seed resourceVersion adoption (the #175 409-storm fix).
func (r *TaskReconciler) patchTaskStatus(ctx context.Context, task *tatarav1alpha1.Task, mutate func(*tatarav1alpha1.Task) bool) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		if !mutate(fresh) {
			*task = *fresh
			return nil
		}
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*task = *fresh
		return nil
	})
}

// projectRepos returns all Repositories belonging to a Project. Uses the
// cached field index on spec.projectRef when available, falling back to
// a full-namespace scan for direct clients (tests).
func (r *TaskReconciler) projectRepos(ctx context.Context, project *tatarav1alpha1.Project) ([]tatarav1alpha1.Repository, error) {
	var list tatarav1alpha1.RepositoryList
	err := r.List(ctx, &list,
		client.InNamespace(project.Namespace),
		client.MatchingFields{taskIndexRepositoryRef: project.Name},
	)
	if err != nil && isFieldSelectorUnsupported(err) {
		err = r.List(ctx, &list, client.InNamespace(project.Namespace))
		if err != nil {
			return nil, fmt.Errorf("list repositories: %w", err)
		}
		out := list.Items[:0]
		for i := range list.Items {
			if list.Items[i].Spec.ProjectRef == project.Name {
				out = append(out, list.Items[i])
			}
		}
		return out, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list repositories: %w", err)
	}
	return list.Items, nil
}

// taskHasInflightTurn reports whether the Task has an agent turn in flight: a
// current-turn id is set and its completion callback has not yet arrived.
func taskHasInflightTurn(task *tatarav1alpha1.Task) bool {
	return task.Annotations[annCurrentTurn] != "" && task.Annotations[annTurnComplete] == ""
}

// stampResolvedModel records the MODEL env resolved for this Task's agent pod
// onto Task.Status.ResolvedModel, so the token/terminal metrics can price by
// the model that actually ran. Best-effort: callers must not fail pod
// creation on a stamp error (the metric label degrades to "", fail-open).
func (r *TaskReconciler) stampResolvedModel(ctx context.Context, task *tatarav1alpha1.Task, model string) error {
	if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		if fresh.Status.ResolvedModel == model {
			return false
		}
		fresh.Status.ResolvedModel = model
		return true
	}); err != nil {
		return fmt.Errorf("stampResolvedModel: %w", err)
	}
	return nil
}

// handleTransientWrapper handles a SubmitTurn error that means "the wrapper is
// not ready yet" rather than a hard failure: either an agent UnreachableError
// (the turn server is still booting even though the pod is Ready) or a wrapper
// HTTPError with a transient status (503 session not ready/dead, 425 too early
// - see agent.IsTransientWrapper). Both stem from endpoint-readiness
// propagation lag and are the same condition, so it returns a short fixed
// requeue and does NOT return an error - returning an error would trip
// controller-runtime's exponential backoff and idle the task for minutes.
// handled=false means the error is not a transient wrapper condition and the
// caller surfaces it as a real failure.
//
// To bound a pod that never starts accepting turns (Ready but always
// unreachable/503), the first occurrence stamps annAgentUnreachableSince; once
// the wrapper has been not-ready for longer than agentBootDeadline the Task is
// terminated instead of requeued forever. The marker is cleared on the next
// successful turn submit (recordTurn) and on each lifecycle state reset.
func (r *TaskReconciler) handleTransientWrapper(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, err error) (ctrl.Result, error, bool) {

	if !agent.IsTransientWrapper(err) {
		return ctrl.Result{}, nil, false
	}
	l := log.FromContext(ctx)

	// A Ready pod that never accepts turns past the boot deadline is a DEAD POD,
	// not a dead Task (fix V7-7): tear it down and burn ONE podRecreation. The
	// Task fails only once that budget is spent, with the F.5 reason
	// pod-recreation-exhausted. An absent or unparseable marker is (re)stamped so
	// the deadline is always anchored to a parseable time.
	started, perr := time.Parse(time.RFC3339, task.Annotations[annAgentUnreachableSince])
	switch {
	case perr != nil:
		if serr := r.stampUnreachableSince(ctx, task); serr != nil {
			return ctrl.Result{}, fmt.Errorf("stamp agent-unreachable-since: %w", serr), true
		}
	case time.Now().After(started.Add(agentBootDeadline)):
		r.Metrics.AgentUnreachableTermination()
		l.Info("wrapper not ready past boot deadline; respawning its pod",
			"action", "agent_unreachable_respawn", "resource_id", task.Name,
			"since", started.Format(time.RFC3339),
			"deadline", agentBootDeadline.String(), "outcome", agent.SubmitOutcome(err))
		if derr := r.deleteWrapper(ctx, task); derr != nil {
			return ctrl.Result{}, derr, true
		}
		if aerr := r.patchTaskAnnotations(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
			if fresh.Annotations == nil || fresh.Annotations[annAgentUnreachableSince] == "" {
				return false
			}
			delete(fresh.Annotations, annAgentUnreachableSince)
			return true
		}); aerr != nil {
			return ctrl.Result{}, aerr, true
		}
		res, rerr := r.respawnLostPod(ctx, proj, task, time.Now())
		return res, rerr, true
	}

	r.Metrics.AgentBootRaceRequeue()
	l.Info("wrapper not yet accepting turns; requeuing",
		"task", task.Name, "requeueAfter", agentBootRequeue.String(),
		"outcome", agent.SubmitOutcome(err))
	return ctrl.Result{RequeueAfter: agentBootRequeue}, nil, true
}

// handleTurnSubmitFailure records the operator_turn_submit_total metric for a
// failed SubmitTurn and decides how the reconcile proceeds. A transient
// wrapper-not-ready error (boot-race or HTTP 503/425) is routed through
// handleTransientWrapper - it requeues without a reconcile error and is counted
// as result="transient" so the turn-submit failure-ratio alert (which keys on
// result="error") is not inflated by benign readiness races (issue #164). A
// hard failure is counted as result="error", logged at the failure site with
// the wrapper status/body (via err) and context, and returned wrapped so
// controller-runtime applies backoff. phase names the submit site (e.g. "turn0")
// and outcome carries the specific cause.
func (r *TaskReconciler) handleTurnSubmitFailure(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, err error, elapsed float64, phase string) (ctrl.Result, error) {
	outcome := agent.SubmitOutcome(err)
	// A wrapper 410 Gone means this pod is past its TTL t0 and will never take
	// another NORMAL turn (fix I10). Route it into the G.7 stop/handoff - the
	// operator captures the agent's handoff (or writes a synthetic one) and rotates
	// to a fresh pod - instead of returning a hard error that backoff-loops. It is
	// counted result="transient" because a 410 at TTL is expected, not a dispatch
	// failure. session.go's transient set covers only 503/425; 410 is distinct.
	if agent.IsTTLGone(err) {
		r.Metrics.TurnSubmit(task.Spec.Kind, "transient", outcome, elapsed)
		log.FromContext(ctx).Info("wrapper past TTL (410 Gone) on turn submit; running G.7 handoff",
			"action", "agent_turn_submit", "resource_id", task.Name, "phase", phase, "outcome", outcome)
		return r.ttlStop(ctx, proj, task, stage.AgentKindFor(task.Status.Stage), time.Now())
	}
	// A wrapper 409 "session busy" is expected backpressure, not a dispatch
	// failure: the session already has a turn in flight (the operator's view of
	// the in-flight turn raced the wrapper's session release). Requeue on a short
	// bounded interval and count it as result="transient" so the turn-submit
	// failure-ratio alert (which keys on result="error") is not inflated by
	// expected contention; returning an error here would tight-loop on
	// controller-runtime backoff (the retry storm in issue #168). It is NOT routed
	// through handleTransientWrapper because a busy session is running a prior
	// turn, not failing to boot, so it must not be bounded by agentBootDeadline; a
	// session stuck busy forever is caught by the F.4 WORK clock instead.
	if agent.IsSessionBusy(err) {
		r.Metrics.TurnSubmit(task.Spec.Kind, "transient", outcome, elapsed)
		r.Metrics.AgentSessionBusyRequeue()
		log.FromContext(ctx).Info("wrapper session busy; requeuing turn submit",
			"action", "agent_turn_submit", "resource_id", task.Name,
			"phase", phase, "requeueAfter", busyRequeue.String(), "outcome", outcome)
		return ctrl.Result{RequeueAfter: busyRequeue}, nil
	}
	if res, herr, handled := r.handleTransientWrapper(ctx, proj, task, err); handled {
		r.Metrics.TurnSubmit(task.Spec.Kind, "transient", outcome, elapsed)
		return res, herr
	}
	r.Metrics.TurnSubmit(task.Spec.Kind, "error", outcome, elapsed)
	log.FromContext(ctx).Error(err, "turn submit failed",
		"action", "agent_turn_submit", "resource_id", task.Name,
		"phase", phase, "outcome", outcome)
	return ctrl.Result{}, fmt.Errorf("submit %s turn: %w", phase, err)
}

// stampUnreachableSince records the first time the wrapper was found not ready
// for the current run, so handleTransientWrapper can enforce agentBootDeadline.
// An existing VALID timestamp is preserved (the deadline is anchored to the
// earliest sighting); an absent or unparseable value is overwritten with now.
func (r *TaskReconciler) stampUnreachableSince(ctx context.Context, task *tatarav1alpha1.Task) error {
	now := time.Now().UTC().Format(time.RFC3339)
	return r.patchTaskAnnotations(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		if cur := fresh.Annotations[annAgentUnreachableSince]; cur != "" {
			if _, perr := time.Parse(time.RFC3339, cur); perr == nil {
				return false
			}
		}
		fresh.Annotations[annAgentUnreachableSince] = now
		return true
	})
}

// shortDescription is the first line of goal, truncated to ~60 runes on a
// word boundary where possible, with an ellipsis when truncated.
func shortDescription(goal string) string {
	line := goal
	if i := strings.IndexByte(goal, '\n'); i >= 0 {
		line = goal[:i]
	}
	r := []rune(strings.TrimSpace(line))
	const maxLen = 60
	if len(r) <= maxLen {
		return string(r)
	}
	cut := maxLen
	if i := strings.LastIndexByte(string(r[:maxLen]), ' '); i > 0 {
		cut = i
	}
	return strings.TrimRight(string(r[:cut]), " ") + "..."
}

// inflightKinds are the Task ORIGIN kinds the per-kind in-flight gauge always
// emits, so a kind with no live Task reports 0 rather than dropping its series.
var inflightKinds = []string{"brainstorm", "incident", "clarify", "refine", "review", "documentation"}

// updateInflightGauge sets operator_tasks_inflight (aggregate) and
// tatara_tasks_inflight{kind} (per-kind) to the count of Tasks in a POD stage.
// A pod-less stage (triaging/approved/merging/deploying) and a finished Task
// hold no agent slot and are not counted: counting them re-creates the
// lane-starvation trap.
func (r *TaskReconciler) updateInflightGauge(ctx context.Context) {
	var list tatarav1alpha1.TaskList
	if err := r.List(ctx, &list, client.InNamespace(r.PodConfig.Namespace)); err != nil {
		return
	}
	n := 0
	byKind := map[string]int{}
	for i := range list.Items {
		t := &list.Items[i]
		if tatarav1alpha1.TaskDone(t) || stage.AgentKindFor(t.Status.Stage) == "" {
			continue
		}
		n++
		byKind[t.Spec.Kind]++
	}
	r.Metrics.SetTasksInflight(float64(n))
	for _, kind := range inflightKinds {
		r.Metrics.SetTasksInflightKind(kind, float64(byKind[kind]))
	}
	for kind, count := range byKind {
		if slices.Contains(inflightKinds, kind) {
			continue
		}
		r.Metrics.SetTasksInflightKind(kind, float64(count))
	}
}

// taskIndexProjectRef is the field index key for Task.Spec.ProjectRef.
const taskIndexProjectRef = ".spec.projectRef"

// taskIndexRepositoryRef is the field index key for Task.Spec.RepositoryRef.
const taskIndexRepositoryRef = ".spec.repositoryRef"

// registerFieldIndexes registers the field indexes required by TaskReconciler
// so hot-path list operations are O(matching) against the informer cache
// rather than O(all-tasks). Called from SetupWithManager and in test suites
// that start a manager.
func (r *TaskReconciler) registerFieldIndexes(idx client.FieldIndexer) error {
	if err := idx.IndexField(context.Background(), &tatarav1alpha1.Task{}, taskIndexProjectRef,
		func(obj client.Object) []string {
			t := obj.(*tatarav1alpha1.Task)
			if t.Spec.ProjectRef == "" {
				return nil
			}
			return []string{t.Spec.ProjectRef}
		}); err != nil {
		return fmt.Errorf("index Task.spec.projectRef: %w", err)
	}
	if err := idx.IndexField(context.Background(), &tatarav1alpha1.Repository{}, taskIndexRepositoryRef,
		func(obj client.Object) []string {
			repo := obj.(*tatarav1alpha1.Repository)
			if repo.Spec.ProjectRef == "" {
				return nil
			}
			return []string{repo.Spec.ProjectRef}
		}); err != nil {
		return fmt.Errorf("index Repository.spec.projectRef: %w", err)
	}
	// Contract A.3: the mirror's four indexes (the fifth, queuedEventDedupKey,
	// belongs to DispatcherReconciler, which owns the QueuedEvent controller).
	// Dedup is an indexed lookup on issueKey/mrKey, NEVER a hashed Task name and
	// NEVER a label selector - label VALUES reject ':' and '#'.
	//
	// "projectRef" duplicates the pre-existing ".spec.projectRef" index above by
	// design: the contract names it, and the old key stays until Task 20's
	// cutover repoints its callers.
	if err := idx.IndexField(context.Background(), &tatarav1alpha1.Issue{}, IssueKeyIndex, IssueKeyIndexer); err != nil {
		return fmt.Errorf("index Issue.issueKey: %w", err)
	}
	if err := idx.IndexField(context.Background(), &tatarav1alpha1.MergeRequest{}, MRKeyIndex, MRKeyIndexer); err != nil {
		return fmt.Errorf("index MergeRequest.mrKey: %w", err)
	}
	if err := idx.IndexField(context.Background(), &tatarav1alpha1.Task{}, TaskProjectRefIndex, TaskProjectRefIndexer); err != nil {
		return fmt.Errorf("index Task.projectRef: %w", err)
	}
	if err := idx.IndexField(context.Background(), &tatarav1alpha1.Task{}, TaskDocumentsTasksIndex, TaskDocumentsTasksIndexer); err != nil {
		return fmt.Errorf("index Task.documentsTasks: %w", err)
	}
	return nil
}

// SetupWithManager registers the Task reconciler, watching Tasks and the
// Pods/Services it owns, and registers field indexers so hot-path list
// operations (projectRepos) are O(matching) against the
// cache rather than O(all-tasks).
func (r *TaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := r.registerFieldIndexes(mgr.GetFieldIndexer()); err != nil {
		return err
	}
	// The pod-clock watch (F.4 clocks 2 and 3, plus the G.10 handshake). A SECOND
	// controller on Pods: the Owns(&corev1.Pod{}) below must keep firing on every
	// Pod event for handleBootCrash, so the Ready-predicated watch cannot be
	// folded into it. It acts only on Tasks carrying status.stage.
	podClocks := &PodWatchReconciler{
		Client:     r.Client,
		Session:    r.Session,
		Namespace:  r.PodConfig.Namespace,
		Metrics:    r.Metrics,
		SpillerFor: r.SpillerFor,
	}
	if err := podClocks.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup pod-clock watch: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&tatarav1alpha1.Task{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.Service{}).
		// MaxConcurrentReconciles: 1 serialises Task reconciles to avoid races in
		// read-then-write sequences (pod creation, status updates, seq accounting
		// in the admission queue). The admission queue is the sole concurrency gate.
		WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}

// deleteWrapper best-effort deletes the wrapper Pod and Service for a task.
// Idempotent: a missing object is not an error. Thin receiver-bound wrapper over
// the shared agent.DeleteWrapper so the webhook server (different receiver type)
// can reuse the same teardown.
func (r *TaskReconciler) deleteWrapper(ctx context.Context, task *tatarav1alpha1.Task) error {
	return agent.DeleteWrapper(ctx, r.Client, task.Namespace, task)
}
