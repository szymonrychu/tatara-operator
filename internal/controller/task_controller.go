package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

const (
	memGateRequeue    = 15 * time.Second
	pollRequeue       = 30 * time.Second
	agentBootRequeue  = 5 * time.Second
	agentBootDeadline = 5 * time.Minute
	// busyRequeue paces re-submission after a wrapper 409 "session busy": the
	// session is processing a prior turn, so requeue on a short bounded interval
	// (backpressure) instead of erroring and tight-looping on reconcile backoff
	// (issue #168). A session stuck busy forever is bounded by the turn-timeout /
	// planning-stall watchdogs, not this interval.
	busyRequeue       = 15 * time.Second
	maxPodRecreations = 3
	turnTimeoutGrace  = 60 * time.Second
	// planningStallDeadline bounds how long a Task may sit in Planning without
	// ever acquiring an in-flight turn before the spawn watchdog fails it. Set
	// well beyond the boot-crash budget (maxPodRecreations * agentBootDeadline)
	// so the watchdog never preempts a legitimately slow/crash-looping boot; it
	// is the last-resort catch for a Task wedged where boot-crash cannot act
	// (e.g. a duplicate lifecycle Task whose pod name collided with the live one).
	planningStallDeadline = 4 * agentBootDeadline

	annCurrentTurn             = tatarav1alpha1.AnnCurrentTurn
	annCurrentSubtask          = tatarav1alpha1.AnnCurrentSubtask
	annTurnComplete            = tatarav1alpha1.AnnTurnComplete
	annPodRecreations          = tatarav1alpha1.AnnPodRecreations
	annTurnStartedAt           = tatarav1alpha1.AnnTurnStartedAt
	annTurnLastActivity        = tatarav1alpha1.AnnTurnLastActivity
	annPlanningSince           = tatarav1alpha1.AnnPlanningSince
	annPendingHandoverResume   = tatarav1alpha1.AnnPendingHandoverResume
	annParentConversationKey   = tatarav1alpha1.AnnParentConversationKey
	annForkFromConversationKey = tatarav1alpha1.AnnForkFromConversationKey
	annAgentUnreachableSince   = "tatara.dev/agent-unreachable-since"
)

// TaskReconciler spawns one wrapper session per Task and drives it turn by
// turn over the Task's Subtasks.
type TaskReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	Metrics          *obs.OperatorMetrics
	LifecycleMetrics *obs.LifecycleMetrics
	Session          agent.Session
	PodConfig        agent.PodConfig
	// SCMFor returns an scm.SCMWriter for the given provider name ("github"|"gitlab").
	// Nil in tests that do not exercise write-back; replaced with a fake in
	// write-back tests.
	SCMFor func(provider string) (scm.SCMWriter, error)
	// ReaderFor returns a token-bound scm.SCMReader for title-level duplicate
	// detection in createProposal. Nil in tests that do not exercise reading;
	// wired in wire.go at runtime. When nil the title check is skipped gracefully.
	ReaderFor func(provider, token string) (scm.SCMReader, error)
}

// +kubebuilder:rbac:groups=tatara.dev,resources=tasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tatara.dev,resources=tasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=tatara.dev,resources=subtasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tatara.dev,resources=subtasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=tatara.dev,resources=projects;repositories,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods;services,verbs=get;list;watch;create;update;patch;delete

func isTerminal(phase string) bool { return phase == "Succeeded" || phase == "Failed" }
func isActive(phase string) bool   { return phase == "Planning" || phase == "Running" }

// isFieldSelectorUnsupported reports whether a list error is "field label not
// supported", which happens when a direct (non-cached) client is used without a
// registered field index. In that case callers fall back to a full-namespace scan
// with in-Go filtering.
func isFieldSelectorUnsupported(err error) bool {
	return err != nil && strings.Contains(err.Error(), "field label not supported")
}

// taskActive reports whether a Task is occupying an agent slot: an active
// phase (Planning/Running) that has NOT entered a terminal lifecycle state. A
// Task Parked at maxIterations keeps a stale Planning phase; counting it by
// phase alone (without the lifecycle check) would over-count in-flight agents.
//
// Conversation (awaiting-human) is excluded: a task blocked on human input is
// externally gated and is not consuming an autonomous agent slot.
func taskActive(t *tatarav1alpha1.Task) bool {
	if t.Status.DeployState == "Conversation" {
		return false
	}
	return isActive(t.Status.Phase) && !isLifecycleTerminal(t.Status.DeployState)
}

// Reconcile drives a Task through spawn -> plan turn -> subtask turns ->
// terminate. Turn results arrive via the /internal/turn-complete callback,
// which annotates the Task to trigger the next reconcile.
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

	// Lazy-seed the work-item ledger from Spec.Source when WorkItems is empty.
	// This provides backward-compat for in-flight Tasks created before the ledger
	// was introduced: first reconcile populates Status.WorkItems so Phase-2 dedup
	// (ledger-based) can operate without labels on old Tasks too.
	if len(task.Status.WorkItems) == 0 && task.Spec.Source != nil {
		seedLedgerFromSpec(&task)
		if len(task.Status.WorkItems) > 0 {
			if err := r.patchTaskStatus(ctx, &task, func(fresh *tatarav1alpha1.Task) bool {
				if len(fresh.Status.WorkItems) > 0 {
					// Another replica already seeded; skip-write adopts its state
					// (via patchTaskStatus's copy-back) so the downstream reconcile
					// starts from the current resourceVersion.
					return false
				}
				seedLedgerFromSpec(fresh)
				return true
			}); err != nil {
				l.Error(err, "task: seed ledger from spec (non-fatal)",
					"action", "ledger_seed", "resource_id", task.Name)
			}
		}
	}

	if task.Spec.Kind == "issueLifecycle" {
		return r.reconcileLifecycle(ctx, &task)
	}

	// clarify is the decomposed conversational front-half kind: it reuses the
	// lifecycle Triage/Conversation machinery via reconcileClarify but hands off
	// to implement (label swap) and terminates rather than driving the deploy
	// half. Routed parallel to the retained issueLifecycle bridge above.
	if task.Spec.Kind == "clarify" {
		return r.reconcileClarify(ctx, &task)
	}

	// Defensive: a pod-less Deploying Task must never drive an agent run. Only
	// issueLifecycle enters Deploying (handled above), but guard the generic path
	// so a Phase=Deploying Task of any kind is supervised, not re-spawned.
	if tatarav1alpha1.TaskDeploying(&task) {
		var project tatarav1alpha1.Project
		if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &project); err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, fmt.Errorf("get project: %w", err)
		}
		res, err := r.reconcileDeploying(ctx, &project, &task)
		if err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, err
		}
		r.Metrics.ReconcileResult("Task", "success")
		return res, nil
	}

	if isTerminal(task.Status.Phase) {
		// M5 write-back: if the task succeeded and SCMFor is set, open PR/MR.
		if task.Status.Phase == "Succeeded" && r.SCMFor != nil {
			if cond := apimeta.FindStatusCondition(task.Status.Conditions, "WritebackPending"); cond != nil && cond.Status == metav1.ConditionTrue {
				res, err := r.doWriteBack(ctx, &task)
				if err != nil {
					r.Metrics.ReconcileResult("Task", "error")
					return ctrl.Result{}, err
				}
				r.Metrics.ReconcileResult("Task", "success")
				return res, nil
			}
		}
		return ctrl.Result{}, nil
	}

	var project tatarav1alpha1.Project
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &project); err != nil {
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, fmt.Errorf("get project: %w", err)
	}

	// Memory gate: spawn-only. Apply only when there is no pod yet and no
	// in-flight turn. An already-running turn must keep being supervised, and a
	// completed turn must be torn down, during a memory blip - gating teardown
	// would let the reaper delete a mid-flight pod and then the same task get
	// re-spawned once memory recovers, wasting the in-progress work.
	taskNeedsSpawn := task.Status.Phase == "" && !taskHasInflightTurn(&task)
	if taskNeedsSpawn && !memoryStablyReady(&project, time.Now()) {
		// Infra-incident exemption (#236): an incident Task investigating the
		// core memory/storage infrastructure must NOT be gated on that same
		// subsystem being Ready, or infra-outage self-heal deadlocks (the agent
		// can never run to open/annotate an issue). Incident agents investigate
		// live via Grafana and do not need the memory graph, so admit it. All
		// other work keeps the gate.
		if tatarav1alpha1.InfraIncidentExempt(task.Spec) {
			l.Info("task memory gate bypassed for infra incident",
				"action", "task_memory_gate_bypass", "resource_id", task.Name,
				"project", project.Name, "alert_rule", task.Spec.AlertRule)
			r.Metrics.MemoryGateBypass(project.Name, task.Spec.Kind)
		} else {
			l.Info("task gated: project memory not stably ready",
				"action", "task_memory_gate", "resource_id", task.Name, "project", project.Name)
			return ctrl.Result{RequeueAfter: memGateRequeue}, nil
		}
	}

	// RepositoryRef contract guard: repo-scoped kinds require a non-empty
	// RepositoryRef; project-scoped kinds (brainstorm/healthCheck) require it
	// empty. The CRD schema cannot express this kind-conditional rule, so it is
	// enforced here. Terminate (not error) on violation: an errored reconcile
	// would hot-loop the Task forever, while a malformed spec is permanent.
	if !isActive(task.Status.Phase) {
		if verr := tatarav1alpha1.ValidateTaskSpec(task.Spec); verr != nil {
			l.Info("task invalid spec; terminating",
				"action", "task_invalid_spec", "resource_id", task.Name, "project", project.Name, "err", verr.Error())
			res, terr := r.terminate(ctx, &task, "Failed", "InvalidTaskSpec", verr.Error())
			if terr != nil {
				r.Metrics.ReconcileResult("Task", "error")
				return ctrl.Result{}, terr
			}
			r.Metrics.ReconcileResult("Task", "success")
			return res, nil
		}
	}

	// Proposal creation: a Task with a ProposedIssue and no Source yet.
	if task.Spec.Kind == "implement" && task.Spec.ProposedIssue != nil && task.Spec.Source == nil && r.SCMFor != nil {
		res, err := r.createProposal(ctx, &project, &task)
		if err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, err
		}
		r.Metrics.ReconcileResult("Task", "success")
		return res, nil
	}

	// Project-scoped kinds (brainstorm, healthCheck) have an empty RepositoryRef;
	// skip the single-repo Get and pass nil to driveAgentRun.
	var repoPtr *tatarav1alpha1.Repository
	if task.Spec.RepositoryRef != "" {
		var repo tatarav1alpha1.Repository
		if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, fmt.Errorf("get repository: %w", err)
		}
		repoPtr = &repo
	}

	// Review tasks (MR/PR review, issue #114 decision 4) get a review/test prompt
	// instead of the implement-oriented plan prompt.
	planText := planTurnText(task.Spec.Goal, taskBranch(&task), project.Name, task.Name)
	if task.Spec.Kind == "review" {
		// reviewText already injects the required-skills directive for review.
		planText = reviewText(task.Spec.Goal, project.Name, task.Name)
	} else if d := skillsDirective(task.Spec.Kind); d != "" {
		planText += "\n\n" + d
	}
	// Turn-0 umbrella context bundle for the project-scoped cross-repo kinds: prepend
	// every umbrella member's body + thread + state + per-repo checkout instructions
	// so implement/review pods get the full cross-repo context upfront (skills assume
	// it complete and must not re-crawl SCM). Only assembled before turn-0 is
	// submitted (annCurrentTurn unset); planText drives no later turn, so re-fetching
	// SCM on every post-turn-0 reconcile would be wasted work.
	if (task.Spec.Kind == "implement" || task.Spec.Kind == "review") && task.Annotations[annCurrentTurn] == "" {
		planText = r.buildUmbrellaPromptFor(ctx, &project, &task, planText)
	}
	res, err := r.driveAgentRun(ctx, &project, repoPtr, &task, planText)
	if err != nil {
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, err
	}
	r.updateInflightGauge(ctx)
	r.Metrics.ReconcileResult("Task", "success")
	return res, nil
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

// driveAgentRun is the shared agent-spawn + drive-turns sequence. It handles
// ensurePodAndService, the Planning phase transition, pod readiness wait, and
// driveTurns. Used by the generic Reconcile and by lifecycle state handlers.
func (r *TaskReconciler) driveAgentRun(ctx context.Context, project *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, task *tatarav1alpha1.Task, planText string) (ctrl.Result, error) {
	exhausted, err := r.ensurePodAndService(ctx, project, repo, task)
	if err != nil {
		return ctrl.Result{}, err
	}
	if exhausted {
		const detail = "wrapper pod lost; recreation budget exhausted"
		res, terr := r.terminate(ctx, task, "Failed", "PodLost", detail)
		if terr == nil {
			r.commentTerminalDiagnostics(ctx, task, "PodLost", detail)
		}
		return res, terr
	}

	// Set Planning on first spawn. RetryOnConflict: lifecycle handlers write
	// status (the iteration counter) immediately before this, and the cached
	// client can lag the API server, so a plain Update races. Absorbing the
	// conflict here (instead of erroring the reconcile) is essential: a
	// reconcile-level error would re-enter the handler at Phase=="" and
	// re-run its iteration increment, spinning the count to the backstop.
	if task.Status.Phase == "" {
		// Stamp planning-since (metadata) before the status flip so the spawn
		// watchdog can detect a Task wedged in Planning that never acquires a
		// turn. Annotations are metadata, so this is a separate Update from the
		// status write below. Overwrite unconditionally: every spawn re-enters
		// here at Phase=="" and must measure from its own start, not a prior one.
		if err := r.patchTaskAnnotations(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
			if fresh.Status.Phase != "" {
				return false // already advanced by a prior attempt
			}
			if fresh.Annotations == nil {
				fresh.Annotations = map[string]string{}
			}
			fresh.Annotations[annPlanningSince] = time.Now().UTC().Format(time.RFC3339)
			return true
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("stamp planning-since: %w", err)
		}
		if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
			if fresh.Status.Phase != "" {
				return false // already advanced by a prior attempt
			}
			fresh.Status.Phase = "Planning"
			fresh.Status.PodName = agent.PodName(task)
			return true
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("set planning phase: %w", err)
		}
		task.Status.Phase = "Planning"
		// updateInflightGauge is called by Reconcile on the shared success path;
		// calling it here again would produce two full-namespace TaskList calls
		// per reconcile on the Planning transition (the only case where both this
		// branch and Reconcile's success path both run).
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}

	ready, err := r.podReady(ctx, task)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !ready {
		// Boot-crash detection: a wrapper that exits non-zero before /readyz comes
		// up leaves the Task wedged in Planning with no turn in flight, so neither
		// the reconcile nor the poll-backstop turn-timeout ever fires (both need a
		// turn-started-at). Detect a Failed/CrashLoopBackOff pod, or one that never
		// became Ready within the boot deadline, and fail the boot fast -> respawn
		// (bounded) instead of requeuing every 2s forever.
		if res, herr, handled := r.handleBootCrash(ctx, task); handled {
			return res, herr
		}
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	return r.driveTurns(ctx, project, task, planText)
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

// ensurePodAndService creates the wrapper Pod+Service if absent. For an
// already-active Task it counts recreations; when the budget is exhausted it
// returns exhausted=true so the caller fails the Task.
func (r *TaskReconciler) ensurePodAndService(ctx context.Context, project *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, task *tatarav1alpha1.Task) (bool, error) {
	// Fail fast with a clear operator-side error when a required secret ref is
	// missing, instead of letting the kubelet surface an opaque
	// CreateContainerConfigError after the Pod is already created.
	if err := agent.ValidatePodSecretRefs(project, r.PodConfig); err != nil {
		return false, err
	}
	repos, err := r.projectRepos(ctx, project)
	if err != nil {
		return false, err
	}
	// Memory endpoint is normally non-nil here (the readiness gate passed). It can
	// be nil/degraded only on the infra-incident exemption path (#236), where an
	// incident agent runs with the memory stack down; it investigates via Grafana
	// and does not need memory, so an empty endpoint is acceptable.
	memEndpoint := ""
	if project.Status.Memory != nil {
		memEndpoint = project.Status.Memory.Endpoint
	}
	pod := agent.BuildPod(project, repo, task, repos, memEndpoint, r.PodConfig)
	existing := &corev1.Pod{}
	err = r.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, existing)
	switch {
	case apierrors.IsNotFound(err):
		if isActive(task.Status.Phase) {
			if r.podRecreations(task) >= maxPodRecreations {
				return true, nil
			}
			if err := r.bumpRecreations(ctx, task); err != nil {
				return false, err
			}
		}
		if err := r.Create(ctx, pod); err != nil {
			return false, fmt.Errorf("create wrapper pod: %w", err)
		}
		model := agent.ModelForKind(project, task.Spec.Kind, task.Labels[tatarav1alpha1.LabelActivity])
		_ = r.stampResolvedModel(ctx, task, model)
	case err != nil:
		return false, fmt.Errorf("get wrapper pod: %w", err)
	}

	svc := agent.BuildService(project, repo, task, r.PodConfig)
	existingSvc := &corev1.Service{}
	err = r.Get(ctx, types.NamespacedName{Namespace: svc.Namespace, Name: svc.Name}, existingSvc)
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, svc); err != nil {
			return false, fmt.Errorf("create wrapper service: %w", err)
		}
	} else if err != nil {
		return false, fmt.Errorf("get wrapper service: %w", err)
	}
	return false, nil
}

func (r *TaskReconciler) podRecreations(task *tatarav1alpha1.Task) int {
	n, _ := strconv.Atoi(task.Annotations[annPodRecreations])
	return n
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

func (r *TaskReconciler) bumpRecreations(ctx context.Context, task *tatarav1alpha1.Task) error {
	return r.patchTaskAnnotations(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		n, _ := strconv.Atoi(fresh.Annotations[annPodRecreations])
		fresh.Annotations[annPodRecreations] = strconv.Itoa(n + 1)
		return true
	})
}

// podReady reports whether the wrapper Pod has the Ready condition true.
func (r *TaskReconciler) podReady(ctx context.Context, task *tatarav1alpha1.Task) (bool, error) {
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: agent.PodName(task)}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get pod for readiness: %w", err)
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true, nil
		}
	}
	return false, nil
}

// postTerminalComment posts a single human-readable cause to the Task's linked
// issue, so a human (or the loop) can read WHY the run ended even after the
// terminal Task CR is garbage-collected (#81). It is the shared plumbing behind
// the boot-crash comment (#115) and the generic terminal-Failed comment (#116).
// Best-effort: a missing SCM wiring / source, a project-scoped task (no repo),
// or a comment failure is logged and ignored - the terminal condition message
// and structured log still carry the cause.
func (r *TaskReconciler) postTerminalComment(ctx context.Context, task *tatarav1alpha1.Task, body string) {
	if r.SCMFor == nil || task.Spec.Source == nil || task.Spec.Source.IssueRef == "" {
		return
	}
	l := log.FromContext(ctx)
	proj, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		l.Error(err, "terminal-diagnostics: scm context for comment (non-fatal)", "resource_id", task.Name)
		return
	}
	posted, cerr := r.gatedComment(ctx, &proj, &repo, writer, token, provider,
		task.Spec.Source.Number, task.Spec.Source.IsPR, task.Spec.Source.AuthorLogin,
		task.Spec.Source.IssueRef, body)
	if cerr != nil {
		l.Error(cerr, "terminal-diagnostics: post comment (non-fatal)",
			"resource_id", task.Name, "issue_ref", task.Spec.Source.IssueRef)
		return
	}
	if posted {
		l.Info("terminal diagnostics commented on issue",
			"action", "task_terminal_commented", "resource_id", task.Name, "issue_ref", task.Spec.Source.IssueRef)
	}
}

// commentTerminalDiagnostics posts the cause of a terminal Failed run to the
// Task's linked issue, generalizing the boot-crash comment (#115) to the other
// terminal-Failed reasons surfaced from a reconcile - AgentUnreachable,
// TurnTimeout, PodLost (#116) - so each leaves a durable cause on the issue
// (#81). Callers invoke it once, only after terminate has committed Failed, so a
// terminate retry cannot double-post (the boot-crash ordering).
func (r *TaskReconciler) commentTerminalDiagnostics(ctx context.Context, task *tatarav1alpha1.Task, reason, detail string) {
	body := fmt.Sprintf("Task run terminated (`Failed` / `%s`).", reason)
	if detail != "" {
		body += "\n\n" + detail
	}
	r.postTerminalComment(ctx, task, body)
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
func (r *TaskReconciler) handleTransientWrapper(ctx context.Context, task *tatarav1alpha1.Task, err error) (ctrl.Result, error, bool) {
	if !agent.IsTransientWrapper(err) {
		return ctrl.Result{}, nil, false
	}
	l := log.FromContext(ctx)

	// A valid, in-range marker that is older than the boot deadline terminates
	// the run: a Ready pod that never accepts turns must not requeue forever.
	// An absent or unparseable marker is (re)stamped so the deadline is always
	// anchored to a parseable time.
	started, perr := time.Parse(time.RFC3339, task.Annotations[annAgentUnreachableSince])
	switch {
	case perr != nil:
		if serr := r.stampUnreachableSince(ctx, task); serr != nil {
			return ctrl.Result{}, fmt.Errorf("stamp agent-unreachable-since: %w", serr), true
		}
	case time.Now().After(started.Add(agentBootDeadline)):
		r.Metrics.AgentUnreachableTermination()
		l.Info("wrapper not ready past boot deadline; failing task",
			"task", task.Name, "since", started.Format(time.RFC3339),
			"deadline", agentBootDeadline.String(), "outcome", agent.SubmitOutcome(err))
		detail := fmt.Sprintf("wrapper agent unreachable for over %s", agentBootDeadline)
		res, terr := r.terminate(ctx, task, "Failed", "AgentUnreachable", detail)
		if terr == nil {
			r.commentTerminalDiagnostics(ctx, task, "AgentUnreachable", detail)
		}
		return res, terr, true
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
// controller-runtime applies backoff. phase is "plan" or "subtask"; subtask is
// the subtask name ("" for the plan turn). outcome carries the specific cause.
func (r *TaskReconciler) handleTurnSubmitFailure(ctx context.Context, task *tatarav1alpha1.Task, err error, elapsed float64, phase, subtask string) (ctrl.Result, error) {
	outcome := agent.SubmitOutcome(err)
	// A wrapper 409 "session busy" is expected backpressure, not a dispatch
	// failure: the session already has a turn in flight (the operator's view of
	// the in-flight turn raced the wrapper's session release). Requeue on a short
	// bounded interval and count it as result="transient" so the turn-submit
	// failure-ratio alert (which keys on result="error") is not inflated by
	// expected contention; returning an error here would tight-loop on
	// controller-runtime backoff (the retry storm in issue #168). It is NOT routed
	// through handleTransientWrapper because a busy session is running a prior
	// turn, not failing to boot, so it must not be bounded by agentBootDeadline; a
	// session stuck busy forever is caught by the turn-timeout / planning-stall
	// watchdogs instead.
	if agent.IsSessionBusy(err) {
		r.Metrics.TurnSubmit(task.Spec.Kind, "transient", outcome, elapsed)
		r.Metrics.AgentSessionBusyRequeue()
		log.FromContext(ctx).Info("wrapper session busy; requeuing turn submit",
			"action", "agent_turn_submit", "resource_id", task.Name,
			"subtask", subtask, "phase", phase,
			"requeueAfter", busyRequeue.String(), "outcome", outcome)
		return ctrl.Result{RequeueAfter: busyRequeue}, nil
	}
	if res, herr, handled := r.handleTransientWrapper(ctx, task, err); handled {
		r.Metrics.TurnSubmit(task.Spec.Kind, "transient", outcome, elapsed)
		return res, herr
	}
	r.Metrics.TurnSubmit(task.Spec.Kind, "error", outcome, elapsed)
	log.FromContext(ctx).Error(err, "turn submit failed",
		"action", "agent_turn_submit", "resource_id", task.Name,
		"subtask", subtask, "phase", phase, "outcome", outcome)
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

// driveTurns runs the callback-driven turn loop: plan turn first, then one
// Subtask per delivered turn-complete callback. planText is the turn-0 prompt;
// callers supply it so lifecycle states can inject their own prompts.
func (r *TaskReconciler) driveTurns(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, planText string) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	baseURL := agent.BaseURL(task, task.Namespace)
	cbURL := strings.TrimSuffix(r.PodConfig.CallbackURL, "/") + "/internal/turn-complete"

	current := task.Annotations[annCurrentTurn]

	// No turn yet -> submit the plan turn (turn 0).
	if current == "" {
		t0 := time.Now()
		id, err := r.Session.SubmitTurn(ctx, baseURL, planText, cbURL)
		elapsed := time.Since(t0).Seconds()
		if err != nil {
			return r.handleTurnSubmitFailure(ctx, task, err, elapsed, "plan", "")
		}
		r.Metrics.TurnSubmit(task.Spec.Kind, "ok", "ok", elapsed)
		l.Info("turn submitted", "action", "agent_turn_submit", "resource_id", task.Name,
			"turn_id", id, "subtask", "", "duration_ms", int64(elapsed*1000))
		res, err := r.recordTurn(ctx, task, id, "")
		if err != nil {
			return ctrl.Result{}, err
		}
		// Clear pending-handover-resume annotation now that turn-0 has been submitted.
		if task.Annotations[annPendingHandoverResume] != "" {
			if cerr := r.patchTaskAnnotations(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
				if fresh.Annotations == nil {
					return false
				}
				delete(fresh.Annotations, annPendingHandoverResume)
				return true
			}); cerr != nil {
				return ctrl.Result{}, fmt.Errorf("clear pending-handover-resume: %w", cerr)
			}
		}
		return res, nil
	}

	// Turn in flight, no callback yet -> check for timeout, otherwise wait.
	if task.Annotations[annTurnComplete] == "" {
		if r.isTurnTimedOut(project, task) {
			r.Metrics.TurnTimeout("reconcile")
			detail := fmt.Sprintf("turn %s exceeded timeout", current)
			res, terr := r.terminate(ctx, task, "Failed", "TurnTimeout", detail)
			if terr == nil {
				r.commentTerminalDiagnostics(ctx, task, "TurnTimeout", detail)
			}
			return res, terr
		}
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}

	// A callback arrived. Mark the executing Subtask Done (if any).
	if prev := task.Annotations[annCurrentSubtask]; prev != "" {
		if err := r.markSubtaskDone(ctx, task.Namespace, prev, current); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Check maxTurns cap before picking next subtask. Implementation kinds run
	// uncapped (capped=false), so the cap branch is skipped entirely.
	if limit, capped := turnCap(project, task); capped && task.Status.TurnsCompleted >= limit {
		return r.terminate(ctx, task, "Succeeded", "MaxTurnsReached",
			fmt.Sprintf("reached turn cap %d", limit))
	}

	// Per-Task token runaway backstop (token conservation, component 5): the
	// uncapped implementation kinds fail cleanly when they cross the per-Task
	// output-token ceiling, so a looping agent cannot burn unbounded tokens. The
	// terminate path records operator_task_terminal_total{reason=TokenBudgetExceeded}.
	if taskTokenBudgetExceeded(project, task) {
		return r.terminate(ctx, task, "Failed", "TokenBudgetExceeded",
			fmt.Sprintf("cumulative tokens %d reached the per-task budget %d",
				task.Status.CumulativeTokens, project.Spec.Agent.MaxTaskTokens))
	}

	// Pick the next Pending Subtask. Uses the field index when available
	// (production cached client), falls back to full scan for direct clients (tests).
	var subs tatarav1alpha1.SubtaskList
	subErr := r.List(ctx, &subs,
		client.InNamespace(task.Namespace),
		client.MatchingFields{subtaskIndexTaskRef: task.Name},
	)
	if subErr != nil && isFieldSelectorUnsupported(subErr) {
		var allSubs tatarav1alpha1.SubtaskList
		if subErr = r.List(ctx, &allSubs, client.InNamespace(task.Namespace)); subErr != nil {
			return ctrl.Result{}, fmt.Errorf("list subtasks: %w", subErr)
		}
		mine := allSubs.Items[:0]
		for i := range allSubs.Items {
			if allSubs.Items[i].Spec.TaskRef == task.Name {
				mine = append(mine, allSubs.Items[i])
			}
		}
		subs.Items = mine
	} else if subErr != nil {
		return ctrl.Result{}, fmt.Errorf("list subtasks: %w", subErr)
	}
	next, ok := nextPendingSubtask(subs.Items)
	if !ok {
		return r.terminate(ctx, task, "Succeeded", "NoPendingSubtasks", "all subtasks complete")
	}

	t0 := time.Now()
	id, err := r.Session.SubmitTurn(ctx, baseURL, turnText(*next, taskBranch(task), task.Name), cbURL)
	elapsed := time.Since(t0).Seconds()
	if err != nil {
		return r.handleTurnSubmitFailure(ctx, task, err, elapsed, "subtask", next.Name)
	}
	r.Metrics.TurnSubmit(task.Spec.Kind, "ok", "ok", elapsed)
	l.Info("turn submitted", "action", "agent_turn_submit", "resource_id", task.Name,
		"turn_id", id, "subtask", next.Name, "duration_ms", int64(elapsed*1000))
	// Persist the new turn id BEFORE flipping phases so that if either
	// Status().Update below conflicts (the callback server may write the same
	// Task's status subresource concurrently), the turn id is already recorded
	// and a retry does not re-enter the 'callback arrived' branch with the old
	// turn, which would skip this subtask entirely.
	res, rerr := r.recordTurn(ctx, task, id, next.Name)
	if rerr != nil {
		return ctrl.Result{}, rerr
	}
	// Flip subtask and task to Running. Both are wrapped in RetryOnConflict
	// to match the pattern used everywhere else in this controller: the
	// callback server writes status concurrently on the turn-complete path,
	// so a plain update races and may return a conflict.
	if next.Status.Phase != "Running" {
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh := &tatarav1alpha1.Subtask{}
			if err := r.Get(ctx, types.NamespacedName{Namespace: next.Namespace, Name: next.Name}, fresh); err != nil {
				return err
			}
			if fresh.Status.Phase == "Running" {
				return nil
			}
			fresh.Status.Phase = "Running"
			return r.Status().Update(ctx, fresh)
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("set subtask running: %w", err)
		}
	}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, fresh); err != nil {
			return err
		}
		if fresh.Status.Phase == "Running" {
			return nil
		}
		fresh.Status.Phase = "Running"
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("set task running: %w", err)
	}
	return res, nil
}

// turnCap returns the maximum turns allowed for this Task and whether a cap
// applies at all. An explicit task.Spec.MaxTurns always wins. Otherwise the
// implementation kinds (implement, issueLifecycle) run uncapped - long but
// healthy coding runs must not be cut off; per-turn timeout and
// maxPodRecreations remain the runaway bounds. All other kinds keep the cap.
func turnCap(project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (int, bool) {
	if task.Spec.MaxTurns > 0 {
		return task.Spec.MaxTurns, true
	}
	if task.Spec.Kind == "implement" || task.Spec.Kind == "issueLifecycle" {
		return 0, false
	}
	if project.Spec.Agent.MaxTurnsPerTask > 0 {
		return project.Spec.Agent.MaxTurnsPerTask, true
	}
	return 50, true
}

// taskTokenBudgetExceeded reports whether an uncapped implementation Task has
// burned past its per-Task output-token backstop. Only implement/issueLifecycle
// are gated (they run turn-uncapped per turnCap); every other kind keeps the turn
// cap and is never token-gated here. A zero MaxTaskTokens disables the backstop.
func taskTokenBudgetExceeded(project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) bool {
	if task.Spec.Kind != "implement" && task.Spec.Kind != "issueLifecycle" {
		return false
	}
	limit := project.Spec.Agent.MaxTaskTokens
	if limit <= 0 {
		return false
	}
	return task.Status.CumulativeTokens >= limit
}

// recordTurn writes the in-flight turn id + executing subtask onto the Task,
// clears the turn-complete marker, and bumps turnsCompleted when a turn closed.
// Both the metadata update and the status increment are wrapped in
// RetryOnConflict: the callback server writes the same Task's annotations and
// status subresource concurrently, so plain updates race and lose the increment.
func (r *TaskReconciler) recordTurn(ctx context.Context, task *tatarav1alpha1.Task, turnID, subtaskName string) (ctrl.Result, error) {
	startedAt := time.Now().UTC().Format(time.RFC3339)
	var hadCallback bool
	if err := r.patchTaskAnnotations(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		hadCallback = fresh.Annotations[annTurnComplete] != ""
		fresh.Annotations[annCurrentTurn] = turnID
		fresh.Annotations[annCurrentSubtask] = subtaskName
		fresh.Annotations[annTurnStartedAt] = startedAt
		delete(fresh.Annotations, annTurnLastActivity)
		delete(fresh.Annotations, annTurnComplete)
		delete(fresh.Annotations, annAgentUnreachableSince)
		delete(fresh.Annotations, annBootCrashAttempts)
		delete(fresh.Annotations, annBootCrashDiagnostics)
		delete(fresh.Annotations, annBootCrashLastPodUID)
		return true
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("record turn annotations: %w", err)
	}
	if hadCallback {
		if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
			fresh.Status.TurnsCompleted++
			return true
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("record turns completed: %w", err)
		}
	}
	return ctrl.Result{RequeueAfter: pollRequeue}, nil
}

// markSubtaskDone sets a Subtask Done, recording the turn id (its result is
// written by the callback before this reconcile runs). Wrapped in
// RetryOnConflict because the callback's recordResult writes the same
// Subtask's status subresource concurrently and may race the reconcile.
func (r *TaskReconciler) markSubtaskDone(ctx context.Context, taskNamespace, name, turnID string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		st := &tatarav1alpha1.Subtask{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: taskNamespace, Name: name}, st); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("get subtask %s: %w", name, err)
		}
		st.Status.Phase = "Done"
		st.Status.TurnID = turnID
		if err := r.Status().Update(ctx, st); err != nil {
			return fmt.Errorf("mark subtask done: %w", err)
		}
		return nil
	})
}

// deriveResultSummary fills task.Status.ResultSummary from Done subtasks when
// the agent has not explicitly set it. Called before write-back so PR/MR bodies
// and issue comments are meaningful.
func (r *TaskReconciler) deriveResultSummary(ctx context.Context, task *tatarav1alpha1.Task) {
	if task.Status.ResultSummary != "" {
		return
	}
	var subs tatarav1alpha1.SubtaskList
	if err := r.List(ctx, &subs, client.InNamespace(task.Namespace)); err != nil {
		return
	}
	done := 0
	lastResult := ""
	for i := range subs.Items {
		st := &subs.Items[i]
		if st.Spec.TaskRef != task.Name {
			continue
		}
		if st.Status.Phase == "Done" {
			done++
			if st.Status.Result != "" {
				lastResult = st.Status.Result
			}
		}
	}
	if lastResult != "" {
		task.Status.ResultSummary = lastResult
	} else if done > 0 {
		task.Status.ResultSummary = fmt.Sprintf("Completed %d subtask(s).", done)
	}
}

// terminate ends the Task: set phase, record turns, delete the wrapper
// session + Pod + Service, and leave the M5 write-back hook marker.
// The terminal status write is wrapped in RetryOnConflict: by the time
// terminate runs, the callback server may have updated the Task's status
// (CumulativeTokens, LastTurnInputTokens), bumping the resourceVersion.
// Every other terminal-state transition must win despite the concurrent write,
// and the teardown (deleteWrapper) above is idempotent so a retry is safe.
func (r *TaskReconciler) terminate(ctx context.Context, task *tatarav1alpha1.Task, phase, reason, msg string) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	baseURL := agent.BaseURL(task, task.Namespace)
	if err := r.Session.DeleteSession(ctx, baseURL); err != nil {
		// Best-effort: the pod is about to be deleted anyway. Classify the error
		// before recording a failure condition. An UnreachableError means the
		// wrapper pod is already gone (reaped on a prior turn, evicted, or the
		// Service has no endpoints). For a DELETE whose goal is to make the session
		// not exist, a gone pod IS the desired terminal state, so that is a clean
		// teardown, not a failure. Only a reachable-but-refused wrapper (HTTPError)
		// or a timeout leaves the session possibly still alive and worth surfacing.
		var unreachable *agent.UnreachableError
		if errors.As(err, &unreachable) {
			l.Info("terminate: session already gone", "action", "task_terminate", "resource_id", task.Name)
		} else {
			l.Error(err, "terminate: delete session (non-fatal)", "resource_id", task.Name)
			apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
				Type: "SessionDeleteFailed", Status: metav1.ConditionTrue,
				Reason: "DeleteError", Message: err.Error(),
			})
		}
	}

	if err := r.deleteWrapper(ctx, task); err != nil {
		return ctrl.Result{}, err
	}

	if phase == "Succeeded" {
		r.deriveResultSummary(ctx, task)
	}

	if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		// Carry over any ResultSummary derived before the retry.
		if task.Status.ResultSummary != "" && fresh.Status.ResultSummary == "" {
			fresh.Status.ResultSummary = task.Status.ResultSummary
		}
		// Preserve any SessionDeleteFailed condition set above.
		for _, c := range task.Status.Conditions {
			if c.Type == "SessionDeleteFailed" {
				apimeta.SetStatusCondition(&fresh.Status.Conditions, c)
			}
		}
		fresh.Status.Phase = phase
		apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionTrue, Reason: reason, Message: msg,
			ObservedGeneration: fresh.Generation,
		})
		if phase == "Succeeded" {
			apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
				Type: "WritebackPending", Status: metav1.ConditionTrue,
				Reason: "AwaitingM5", Message: "agent run complete; SCM write-back handled in M5",
				ObservedGeneration: fresh.Generation,
			})
		}
		return true
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("set terminal status: %w", err)
	}
	l.Info("task terminated", "action", "task_terminate", "resource_id", task.Name, "phase", phase, "reason", reason)
	// Meter every terminal transition in this single chokepoint: the uniform
	// loop success/failure denominator. operator_reconcile_total cannot stand in
	// because terminal-failure reconciles return (Result{}, nil) and count as
	// result="success".
	if r.Metrics != nil {
		r.Metrics.TaskTerminal(task.Spec.Kind, phase, reason)
	}
	r.updateInflightGauge(ctx)
	return ctrl.Result{}, nil
}

// isTurnTimedOut reports whether the in-flight turn has stalled: no agent
// activity for project.spec.agent.turnTimeoutSeconds + turnTimeoutGrace, anchored
// on max(turn-started-at, turn-last-activity-at). It returns false when the
// start annotation is absent or unparseable (safe default: keep waiting).
// Delegates to the free function turnTimedOut (turncallback.go) so the two
// receivers share the same deadline arithmetic (finding 3/r3).
func (r *TaskReconciler) isTurnTimedOut(project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) bool {
	return turnTimedOut(task.Annotations[annTurnStartedAt], task.Annotations[annTurnLastActivity], project.Spec.Agent.TurnTimeoutSeconds)
}

// updateInflightGauge sets operator_tasks_inflight (aggregate) and
// tatara_tasks_inflight{kind} (per-kind) to the count of active Tasks.
func (r *TaskReconciler) updateInflightGauge(ctx context.Context) {
	var list tatarav1alpha1.TaskList
	if err := r.List(ctx, &list, client.InNamespace(r.PodConfig.Namespace)); err != nil {
		return
	}
	n := 0
	byKind := map[string]int{}
	for i := range list.Items {
		if taskActive(&list.Items[i]) {
			n++
			byKind[list.Items[i].Spec.Kind]++
		}
	}
	r.Metrics.SetTasksInflight(float64(n))
	// Emit per-kind gauge for all known kinds, zeroing kinds with no in-flight tasks.
	for _, kind := range []string{"implement", "review", "clarify", "triageIssue", "brainstorm", "issueLifecycle", "documentation"} {
		r.Metrics.SetTasksInflightKind(kind, float64(byKind[kind]))
	}
	// Also emit any kinds seen in the list that are not in the known set.
	for kind, count := range byKind {
		switch kind {
		case "implement", "review", "clarify", "triageIssue", "brainstorm", "issueLifecycle", "documentation":
			continue
		}
		r.Metrics.SetTasksInflightKind(kind, float64(count))
	}
}

// taskIndexProjectRef is the field index key for Task.Spec.ProjectRef.
const taskIndexProjectRef = ".spec.projectRef"

// taskIndexRepositoryRef is the field index key for Task.Spec.RepositoryRef.
const taskIndexRepositoryRef = ".spec.repositoryRef"

// subtaskIndexTaskRef is the field index key for Subtask.Spec.TaskRef.
const subtaskIndexTaskRef = ".spec.taskRef"

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
	if err := idx.IndexField(context.Background(), &tatarav1alpha1.Subtask{}, subtaskIndexTaskRef,
		func(obj client.Object) []string {
			st := obj.(*tatarav1alpha1.Subtask)
			if st.Spec.TaskRef == "" {
				return nil
			}
			return []string{st.Spec.TaskRef}
		}); err != nil {
		return fmt.Errorf("index Subtask.spec.taskRef: %w", err)
	}
	return nil
}

// SetupWithManager registers the Task reconciler, watching Tasks and the
// Pods/Services it owns, and registers field indexers so hot-path list
// operations (projectRepos, subtask listing) are O(matching) against the
// cache rather than O(all-tasks).
func (r *TaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := r.registerFieldIndexes(mgr.GetFieldIndexer()); err != nil {
		return err
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
