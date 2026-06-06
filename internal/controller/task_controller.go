package controller

import (
	"context"
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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

const (
	capRequeue        = 15 * time.Second
	pollRequeue       = 30 * time.Second
	maxPodRecreations = 3
	turnTimeoutGrace  = 60 * time.Second

	annCurrentTurn    = "tatara.dev/current-turn"
	annCurrentSubtask = "tatara.dev/current-subtask"
	annTurnComplete   = "tatara.dev/turn-complete"
	annPodRecreations = "tatara.dev/pod-recreations"
	annTurnStartedAt  = "tatara.dev/turn-started-at"
)

// TaskReconciler spawns one wrapper session per Task and drives it turn by
// turn over the Task's Subtasks.
type TaskReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Metrics   *obs.OperatorMetrics
	Session   agent.Session
	PodConfig agent.PodConfig
	// SCMFor returns a Writer for the given provider name ("github"|"gitlab").
	// Nil in tests that do not exercise write-back; replaced with a fake in
	// write-back tests.
	SCMFor func(provider string) (Writer, error)
}

// +kubebuilder:rbac:groups=tatara.dev,resources=tasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tatara.dev,resources=tasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=tatara.dev,resources=subtasks,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=tatara.dev,resources=subtasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=tatara.dev,resources=projects;repositories,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods;services,verbs=get;list;watch;create;delete

func isTerminal(phase string) bool { return phase == "Succeeded" || phase == "Failed" }
func isActive(phase string) bool   { return phase == "Planning" || phase == "Running" }

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

	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); err != nil {
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, fmt.Errorf("get repository: %w", err)
	}

	exhausted, err := r.ensurePodAndService(ctx, &project, &repo, &task)
	if err != nil {
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, err
	}
	if exhausted {
		res, err := r.terminate(ctx, &task, "Failed", "PodLost", "wrapper pod lost; recreation budget exhausted")
		if err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, err
		}
		r.Metrics.ReconcileResult("Task", "success")
		return res, nil
	}

	// Set Planning on first spawn.
	if task.Status.Phase == "" {
		task.Status.Phase = "Planning"
		task.Status.PodName = agent.PodName(&task)
		if err := r.Status().Update(ctx, &task); err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, fmt.Errorf("set planning phase: %w", err)
		}
		r.updateInflightGauge(ctx)
		r.Metrics.ReconcileResult("Task", "success")
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}

	ready, err := r.podReady(ctx, &task)
	if err != nil {
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, err
	}
	if !ready {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	res, err := r.driveTurns(ctx, &project, &task)
	if err != nil {
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, err
	}
	r.updateInflightGauge(ctx)
	r.Metrics.ReconcileResult("Task", "success")
	return res, nil
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
		if it.Spec.ProjectRef == project.Name && it.Name != self && isActive(it.Status.Phase) {
			active++
		}
	}
	return active >= max, nil
}

// ensurePodAndService creates the wrapper Pod+Service if absent. For an
// already-active Task it counts recreations; when the budget is exhausted it
// returns exhausted=true so the caller fails the Task.
func (r *TaskReconciler) ensurePodAndService(ctx context.Context, project *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, task *tatarav1alpha1.Task) (bool, error) {
	pod := agent.BuildPod(project, repo, task, r.PodConfig)
	existing := &corev1.Pod{}
	err := r.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, existing)
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

// driveTurns runs the callback-driven turn loop: plan turn first, then one
// Subtask per delivered turn-complete callback.
func (r *TaskReconciler) driveTurns(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	baseURL := agent.BaseURL(task, task.Namespace)
	cbURL := strings.TrimSuffix(r.PodConfig.CallbackURL, "/") + "/internal/turn-complete"

	current := task.Annotations[annCurrentTurn]

	// No turn yet -> submit the plan turn (turn 0).
	if current == "" {
		id, err := r.Session.SubmitTurn(ctx, baseURL, planTurnText(task.Spec.Goal), cbURL)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("submit plan turn: %w", err)
		}
		return r.recordTurn(ctx, task, id, "")
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

	id, err := r.Session.SubmitTurn(ctx, baseURL, turnText(*next), cbURL)
	if err != nil {
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

// updateInflightGauge sets operator_tasks_inflight to the count of active Tasks.
func (r *TaskReconciler) updateInflightGauge(ctx context.Context) {
	var list tatarav1alpha1.TaskList
	if err := r.List(ctx, &list, client.InNamespace(r.PodConfig.Namespace)); err != nil {
		return
	}
	n := 0
	for i := range list.Items {
		if isActive(list.Items[i].Status.Phase) {
			n++
		}
	}
	r.Metrics.SetTasksInflight(float64(n))
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
