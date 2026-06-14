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
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

const (
	capRequeue        = 15 * time.Second
	pollRequeue       = 30 * time.Second
	agentBootRequeue  = 5 * time.Second
	agentBootDeadline = 5 * time.Minute
	maxPodRecreations = 3
	turnTimeoutGrace  = 60 * time.Second

	annCurrentTurn           = tatarav1alpha1.AnnCurrentTurn
	annCurrentSubtask        = tatarav1alpha1.AnnCurrentSubtask
	annTurnComplete          = tatarav1alpha1.AnnTurnComplete
	annPodRecreations        = tatarav1alpha1.AnnPodRecreations
	annTurnStartedAt         = tatarav1alpha1.AnnTurnStartedAt
	annPendingHandoverResume = "tatara.dev/pending-handover-resume"
	annAgentUnreachableSince = "tatara.dev/agent-unreachable-since"
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
// +kubebuilder:rbac:groups=tatara.dev,resources=subtasks,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=tatara.dev,resources=subtasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=tatara.dev,resources=projects;repositories,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods;services,verbs=get;list;watch;create;delete

func isTerminal(phase string) bool { return phase == "Succeeded" || phase == "Failed" }
func isActive(phase string) bool   { return phase == "Planning" || phase == "Running" }

// taskActive reports whether a Task occupies a concurrency slot: an active
// phase (Planning/Running) that has NOT entered a terminal lifecycle state. A
// Task Parked at maxIterations keeps a stale Planning phase; counting it by
// phase alone (without the lifecycle check) deadlocks the concurrency cap.
func taskActive(t *tatarav1alpha1.Task) bool {
	return isActive(t.Status.Phase) && !isLifecycleTerminal(t.Status.LifecycleState)
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

	if task.Spec.Kind == "issueLifecycle" {
		return r.reconcileLifecycle(ctx, &task)
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

	if project.Status.Memory == nil || project.Status.Memory.Phase != "Ready" {
		l.Info("task gated: project memory not ready",
			"action", "task_memory_gate", "resource_id", task.Name, "project", project.Name)
		return ctrl.Result{RequeueAfter: capRequeue}, nil
	}

	// Concurrency gate: only applies to Tasks not yet active.
	if !isActive(task.Status.Phase) {
		atCap, err := r.atConcurrencyCap(ctx, &project, task.Name)
		if err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, err
		}
		if atCap {
			l.Info("task gated at concurrency cap",
				"action", "task_gate", "resource_id", task.Name, "project", project.Name)
			return ctrl.Result{RequeueAfter: capRequeue}, nil
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

	// Authorship gate (security boundary): never spawn an agent for a
	// selfImprove Task whose PR/MR is not actually authored by the bot. The
	// webhook's AuthorLogin is only a hint; GetPRState is authoritative.
	if task.Spec.Kind == "selfImprove" && !isActive(task.Status.Phase) && r.SCMFor != nil {
		authored, gerr := r.selfImproveBotAuthored(ctx, &project, &task)
		if gerr != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, gerr
		}
		if !authored {
			res, terr := r.terminate(ctx, &task, "Failed", "NotBotAuthored",
				"selfImprove PR/MR is not authored by the project bot login")
			if terr != nil {
				r.Metrics.ReconcileResult("Task", "error")
				return ctrl.Result{}, terr
			}
			r.Metrics.ReconcileResult("Task", "success")
			return res, nil
		}
	}

	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); err != nil {
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, fmt.Errorf("get repository: %w", err)
	}

	planText := planTurnText(task.Spec.Goal, taskBranch(&task), project.Name, task.Name)
	res, err := r.driveAgentRun(ctx, &project, &repo, &task, planText)
	if err != nil {
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, err
	}
	r.updateInflightGauge(ctx)
	r.Metrics.ReconcileResult("Task", "success")
	return res, nil
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
		return r.terminate(ctx, task, "Failed", "PodLost", "wrapper pod lost; recreation budget exhausted")
	}

	// Set Planning on first spawn. RetryOnConflict: lifecycle handlers write
	// status (the iteration counter) immediately before this, and the cached
	// client can lag the API server, so a plain Update races. Absorbing the
	// conflict here (instead of erroring the reconcile) is essential: a
	// reconcile-level error would re-enter the handler at Phase=="" and
	// re-run its iteration increment, spinning the count to the backstop.
	if task.Status.Phase == "" {
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh := &tatarav1alpha1.Task{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
				return err
			}
			if fresh.Status.Phase != "" {
				return nil // already advanced by a prior attempt
			}
			fresh.Status.Phase = "Planning"
			fresh.Status.PodName = agent.PodName(task)
			return r.Status().Update(ctx, fresh)
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("set planning phase: %w", err)
		}
		task.Status.Phase = "Planning"
		r.updateInflightGauge(ctx)
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}

	ready, err := r.podReady(ctx, task)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !ready {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	return r.driveTurns(ctx, project, task, planText)
}

// atConcurrencyCap reports whether the Project already has maxConcurrentTasks
// active Tasks, excluding self.
func (r *TaskReconciler) atConcurrencyCap(ctx context.Context, project *tatarav1alpha1.Project, self string) (bool, error) {
	max := project.Spec.MaxConcurrentTasks
	if max <= 0 {
		max = 3
	}
	var list tatarav1alpha1.TaskList
	if err := r.List(ctx, &list, client.InNamespace(project.Namespace)); err != nil {
		return false, fmt.Errorf("list tasks: %w", err)
	}
	active := 0
	for i := range list.Items {
		it := list.Items[i]
		if it.Spec.ProjectRef == project.Name && it.Name != self && taskActive(&it) {
			active++
		}
	}
	return active >= max, nil
}

// projectRepos returns all Repositories belonging to a Project.
func (r *TaskReconciler) projectRepos(ctx context.Context, project *tatarav1alpha1.Project) ([]tatarav1alpha1.Repository, error) {
	var list tatarav1alpha1.RepositoryList
	if err := r.List(ctx, &list, client.InNamespace(project.Namespace)); err != nil {
		return nil, fmt.Errorf("list repositories: %w", err)
	}
	var out []tatarav1alpha1.Repository
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef == project.Name {
			out = append(out, list.Items[i])
		}
	}
	return out, nil
}

// ensurePodAndService creates the wrapper Pod+Service if absent. For an
// already-active Task it counts recreations; when the budget is exhausted it
// returns exhausted=true so the caller fails the Task.
func (r *TaskReconciler) ensurePodAndService(ctx context.Context, project *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, task *tatarav1alpha1.Task) (bool, error) {
	repos, err := r.projectRepos(ctx, project)
	if err != nil {
		return false, err
	}
	pod := agent.BuildPod(project, repo, task, repos, project.Status.Memory.Endpoint, r.PodConfig)
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

func (r *TaskReconciler) bumpRecreations(ctx context.Context, task *tatarav1alpha1.Task) error {
	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, fresh); err != nil {
		return fmt.Errorf("reload task for recreation bump: %w", err)
	}
	if fresh.Annotations == nil {
		fresh.Annotations = map[string]string{}
	}
	n, _ := strconv.Atoi(fresh.Annotations[annPodRecreations])
	fresh.Annotations[annPodRecreations] = strconv.Itoa(n + 1)
	return r.Update(ctx, fresh)
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

// handleAgentUnreachable handles a SubmitTurn error: when it is an agent
// UnreachableError (the wrapper pod's turn server is still booting even though
// the pod is Ready), it returns a short fixed requeue so the reconcile does
// NOT return an error - returning an error would trip controller-runtime's
// exponential backoff and idle the task for minutes. handled=false means the
// error is not a boot-race and the caller surfaces it as a real failure.
//
// To bound a pod that is permanently unreachable (Ready but never accepting
// turns), the first boot-race stamps annAgentUnreachableSince; once the agent
// has been unreachable for longer than agentBootDeadline the Task is
// terminated instead of requeued forever. The marker is cleared on the next
// successful turn submit (recordTurn) and on each lifecycle state reset.
func (r *TaskReconciler) handleAgentUnreachable(ctx context.Context, task *tatarav1alpha1.Task, err error) (ctrl.Result, error, bool) {
	var unreachable *agent.UnreachableError
	if !errors.As(err, &unreachable) {
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
		l.Info("agent unreachable past boot deadline; failing task",
			"task", task.Name, "since", started.Format(time.RFC3339), "deadline", agentBootDeadline.String())
		res, terr := r.terminate(ctx, task, "Failed", "AgentUnreachable",
			fmt.Sprintf("wrapper agent unreachable for over %s", agentBootDeadline))
		return res, terr, true
	}

	r.Metrics.AgentBootRaceRequeue()
	l.Info("agent not yet accepting turns; requeuing",
		"task", task.Name, "requeueAfter", agentBootRequeue.String())
	return ctrl.Result{RequeueAfter: agentBootRequeue}, nil, true
}

// stampUnreachableSince records the first time the agent was found unreachable
// for the current run, so handleAgentUnreachable can enforce agentBootDeadline.
// An existing VALID timestamp is preserved (the deadline is anchored to the
// earliest sighting); an absent or unparseable value is overwritten with now.
func (r *TaskReconciler) stampUnreachableSince(ctx context.Context, task *tatarav1alpha1.Task) error {
	now := time.Now().UTC().Format(time.RFC3339)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, fresh); err != nil {
			return err
		}
		if cur := fresh.Annotations[annAgentUnreachableSince]; cur != "" {
			if _, perr := time.Parse(time.RFC3339, cur); perr == nil {
				return nil
			}
		}
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations[annAgentUnreachableSince] = now
		return r.Update(ctx, fresh)
	})
}

// driveTurns runs the callback-driven turn loop: plan turn first, then one
// Subtask per delivered turn-complete callback. planText is the turn-0 prompt;
// callers supply it so lifecycle states can inject their own prompts.
func (r *TaskReconciler) driveTurns(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, planText string) (ctrl.Result, error) {
	baseURL := agent.BaseURL(task, task.Namespace)
	cbURL := strings.TrimSuffix(r.PodConfig.CallbackURL, "/") + "/internal/turn-complete"

	current := task.Annotations[annCurrentTurn]

	// No turn yet -> submit the plan turn (turn 0).
	if current == "" {
		id, err := r.Session.SubmitTurn(ctx, baseURL, planText, cbURL)
		if err != nil {
			if res, herr, handled := r.handleAgentUnreachable(ctx, task, err); handled {
				return res, herr
			}
			return ctrl.Result{}, fmt.Errorf("submit plan turn: %w", err)
		}
		res, err := r.recordTurn(ctx, task, id, "")
		if err != nil {
			return ctrl.Result{}, err
		}
		// Clear pending-handover-resume annotation now that turn-0 has been submitted.
		if task.Annotations[annPendingHandoverResume] != "" {
			if cerr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				fresh := &tatarav1alpha1.Task{}
				if err2 := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, fresh); err2 != nil {
					return err2
				}
				if fresh.Annotations == nil {
					return nil
				}
				delete(fresh.Annotations, annPendingHandoverResume)
				return r.Update(ctx, fresh)
			}); cerr != nil {
				return ctrl.Result{}, fmt.Errorf("clear pending-handover-resume: %w", cerr)
			}
		}
		return res, nil
	}

	// Turn in flight, no callback yet -> check for timeout, otherwise wait.
	if task.Annotations[annTurnComplete] == "" {
		if r.isTurnTimedOut(project, task) {
			return r.terminate(ctx, task, "Failed", "TurnTimeout",
				fmt.Sprintf("turn %s exceeded timeout", current))
		}
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}

	// A callback arrived. Mark the executing Subtask Done (if any).
	if prev := task.Annotations[annCurrentSubtask]; prev != "" {
		if err := r.markSubtaskDone(ctx, task.Namespace, prev, current); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Check maxTurns cap before picking next subtask.
	if task.Status.TurnsCompleted >= turnCap(project, task) {
		return r.terminate(ctx, task, "Succeeded", "MaxTurnsReached",
			fmt.Sprintf("reached turn cap %d", turnCap(project, task)))
	}

	// Pick the next Pending Subtask.
	var subs tatarav1alpha1.SubtaskList
	if err := r.List(ctx, &subs, client.InNamespace(task.Namespace)); err != nil {
		return ctrl.Result{}, fmt.Errorf("list subtasks: %w", err)
	}
	mine := make([]tatarav1alpha1.Subtask, 0, len(subs.Items))
	for i := range subs.Items {
		if subs.Items[i].Spec.TaskRef == task.Name {
			mine = append(mine, subs.Items[i])
		}
	}
	next, ok := nextPendingSubtask(mine)
	if !ok {
		return r.terminate(ctx, task, "Succeeded", "NoPendingSubtasks", "all subtasks complete")
	}

	id, err := r.Session.SubmitTurn(ctx, baseURL, turnText(*next, taskBranch(task), task.Name), cbURL)
	if err != nil {
		if res, herr, handled := r.handleAgentUnreachable(ctx, task, err); handled {
			return res, herr
		}
		return ctrl.Result{}, fmt.Errorf("submit subtask turn: %w", err)
	}
	if next.Status.Phase != "Running" {
		next.Status.Phase = "Running"
		if err := r.Status().Update(ctx, next); err != nil {
			return ctrl.Result{}, fmt.Errorf("set subtask running: %w", err)
		}
	}
	task.Status.Phase = "Running"
	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{}, fmt.Errorf("set task running: %w", err)
	}
	return r.recordTurn(ctx, task, id, next.Name)
}

// turnCap returns the maximum turns allowed for this Task.
func turnCap(project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) int {
	if task.Spec.MaxTurns > 0 {
		return task.Spec.MaxTurns
	}
	if project.Spec.Agent.MaxTurnsPerTask > 0 {
		return project.Spec.Agent.MaxTurnsPerTask
	}
	return 50
}

// recordTurn writes the in-flight turn id + executing subtask onto the Task,
// clears the turn-complete marker, and bumps turnsCompleted when a turn closed.
func (r *TaskReconciler) recordTurn(ctx context.Context, task *tatarav1alpha1.Task, turnID, subtaskName string) (ctrl.Result, error) {
	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, fresh); err != nil {
		return ctrl.Result{}, fmt.Errorf("reload task: %w", err)
	}
	if fresh.Annotations == nil {
		fresh.Annotations = map[string]string{}
	}
	hadCallback := fresh.Annotations[annTurnComplete] != ""
	fresh.Annotations[annCurrentTurn] = turnID
	fresh.Annotations[annCurrentSubtask] = subtaskName
	fresh.Annotations[annTurnStartedAt] = time.Now().UTC().Format(time.RFC3339)
	delete(fresh.Annotations, annTurnComplete)
	delete(fresh.Annotations, annAgentUnreachableSince)
	if err := r.Update(ctx, fresh); err != nil {
		return ctrl.Result{}, fmt.Errorf("record turn annotations: %w", err)
	}
	if hadCallback {
		fresh.Status.TurnsCompleted++
		if err := r.Status().Update(ctx, fresh); err != nil {
			return ctrl.Result{}, fmt.Errorf("record turns completed: %w", err)
		}
	}
	return ctrl.Result{RequeueAfter: pollRequeue}, nil
}

// markSubtaskDone sets a Subtask Done, recording the turn id (its result is
// written by the callback before this reconcile runs).
func (r *TaskReconciler) markSubtaskDone(ctx context.Context, taskNamespace, name, turnID string) error {
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
func (r *TaskReconciler) terminate(ctx context.Context, task *tatarav1alpha1.Task, phase, reason, msg string) (ctrl.Result, error) {
	baseURL := agent.BaseURL(task, task.Namespace)
	if err := r.Session.DeleteSession(ctx, baseURL); err != nil {
		// Best-effort: the pod is about to be deleted anyway.
		apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
			Type: "SessionDeleteFailed", Status: metav1.ConditionTrue,
			Reason: "DeleteError", Message: err.Error(),
		})
	}

	pod := &corev1.Pod{}
	pod.Name = agent.PodName(task)
	pod.Namespace = task.Namespace
	if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("delete wrapper pod: %w", err)
	}
	svc := &corev1.Service{}
	svc.Name = agent.PodName(task)
	svc.Namespace = task.Namespace
	if err := r.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("delete wrapper service: %w", err)
	}

	if phase == "Succeeded" {
		r.deriveResultSummary(ctx, task)
	}
	task.Status.Phase = phase
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type: "Ready", Status: metav1.ConditionTrue, Reason: reason, Message: msg,
		ObservedGeneration: task.Generation,
	})
	// M5 write-back hook: the SCM PR/MR + issue comment path keys off this
	// condition. M4 only sets it; M5 clears it once the change is landed.
	if phase == "Succeeded" {
		apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
			Type: "WritebackPending", Status: metav1.ConditionTrue,
			Reason: "AwaitingM5", Message: "agent run complete; SCM write-back handled in M5",
			ObservedGeneration: task.Generation,
		})
	}
	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{}, fmt.Errorf("set terminal status: %w", err)
	}
	r.updateInflightGauge(ctx)
	return ctrl.Result{}, nil
}

// isTurnTimedOut reports whether the in-flight turn has exceeded
// project.spec.agent.turnTimeoutSeconds + turnTimeoutGrace. It returns false
// when the annotation is absent or unparseable (safe default: keep waiting).
func (r *TaskReconciler) isTurnTimedOut(project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) bool {
	raw := task.Annotations[annTurnStartedAt]
	if raw == "" {
		return false
	}
	startedAt, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return false
	}
	timeout := project.Spec.Agent.TurnTimeoutSeconds
	if timeout <= 0 {
		timeout = 1800
	}
	deadline := startedAt.Add(time.Duration(timeout)*time.Second + turnTimeoutGrace)
	return time.Now().After(deadline)
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
	for _, kind := range []string{"implement", "review", "selfImprove", "triageIssue", "brainstorm", "issueLifecycle"} {
		r.Metrics.SetTasksInflightKind(kind, float64(byKind[kind]))
	}
	// Also emit any kinds seen in the list that are not in the known set.
	for kind, count := range byKind {
		switch kind {
		case "implement", "review", "selfImprove", "triageIssue", "brainstorm", "issueLifecycle":
			continue
		}
		r.Metrics.SetTasksInflightKind(kind, float64(count))
	}
}

// SetupWithManager registers the Task reconciler, watching Tasks and the
// Pods/Services it owns.
func (r *TaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tatarav1alpha1.Task{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
