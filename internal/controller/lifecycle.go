// Copyright 2026 tatara authors.

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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// setLifecycleState updates task.Status.LifecycleState to `to`, retrying on
// conflict (same pattern as clearWritebackPending). It logs the transition at
// INFO and increments tatara_lifecycle_transition_total{from,to}.
func (r *TaskReconciler) setLifecycleState(ctx context.Context, task *tatarav1alpha1.Task, to, reason string) error {
	l := log.FromContext(ctx)
	from := task.Status.LifecycleState

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		from = fresh.Status.LifecycleState
		fresh.Status.LifecycleState = to
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		return fmt.Errorf("setLifecycleState: %w", err)
	}

	l.Info("lifecycle transition",
		"action", "lifecycle_transition",
		"resource_id", task.Name,
		"from", from,
		"to", to,
		"reason", reason,
	)

	if r.LifecycleMetrics != nil {
		r.LifecycleMetrics.RecordTransition(from, to)
	}

	task.Status.LifecycleState = to
	return nil
}

// resetAgentRun clears the agent-run state on the Task so the next lifecycle
// state can spawn a fresh session. It:
//   - sets Status.Phase = ""
//   - deletes the turn annotations (current-turn, current-subtask, turn-complete, turn-started-at, pod-recreations)
//   - removes the WritebackPending condition (sets it False)
//   - deletes the wrapper Pod + Service (belt-and-suspenders; terminate already does this on success)
func (r *TaskReconciler) resetAgentRun(ctx context.Context, task *tatarav1alpha1.Task) error {
	// Delete wrapper pod + service (best-effort; may already be gone from terminate).
	pod := &corev1.Pod{}
	pod.Name = agent.PodName(task)
	pod.Namespace = task.Namespace
	if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("resetAgentRun: delete pod: %w", err)
	}
	svc := &corev1.Service{}
	svc.Name = agent.PodName(task)
	svc.Namespace = task.Namespace
	if err := r.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("resetAgentRun: delete service: %w", err)
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.Phase = ""
		// Clear WritebackPending.
		apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               "WritebackPending",
			Status:             metav1.ConditionFalse,
			Reason:             "LifecycleReset",
			Message:            "agent run reset for next lifecycle state",
			ObservedGeneration: fresh.Generation,
		})
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		// Clear turn annotations (requires a metadata update, separate from status).
		fresh2 := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh2); err != nil {
			return err
		}
		if fresh2.Annotations != nil {
			delete(fresh2.Annotations, annCurrentTurn)
			delete(fresh2.Annotations, annCurrentSubtask)
			delete(fresh2.Annotations, annTurnComplete)
			delete(fresh2.Annotations, annTurnStartedAt)
			delete(fresh2.Annotations, annPodRecreations)
		}
		task.Status.Phase = ""
		return r.Update(ctx, fresh2)
	})
}

// needsSpawn reports whether the lifecycle state requires starting a new agent
// run. Only these states need the memory + concurrency gates.
func needsSpawn(lifecycleState, phase string) bool {
	switch lifecycleState {
	case "", "Triage", "Implement":
		// Gate only when the run has NOT yet finished (no terminal phase).
		return !isTerminal(phase)
	}
	return false
}

// reconcileLifecycle is the dispatch function for issueLifecycle Tasks. It
// applies the memory-ready and concurrency gates ONLY on the spawn path (i.e.
// when about to start a new agent run). Terminal-phase outcome consumption,
// poll states, and terminal lifecycle states bypass the gates so a finished
// run can always be torn down and its outcome consumed regardless of cap.
func (r *TaskReconciler) reconcileLifecycle(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var project tatarav1alpha1.Project
	if err := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &project); err != nil {
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, fmt.Errorf("lifecycle: get project: %w", err)
	}

	// Memory + concurrency gates: apply only when about to spawn a new agent run.
	if needsSpawn(task.Status.LifecycleState, task.Status.Phase) {
		if project.Status.Memory == nil || project.Status.Memory.Phase != "Ready" {
			l.Info("lifecycle task gated: project memory not ready",
				"action", "task_memory_gate", "resource_id", task.Name, "project", project.Name)
			return ctrl.Result{RequeueAfter: capRequeue}, nil
		}

		if !isActive(task.Status.Phase) {
			atCap, err := r.atConcurrencyCap(ctx, &project, task.Name)
			if err != nil {
				r.Metrics.ReconcileResult("Task", "error")
				return ctrl.Result{}, err
			}
			if atCap {
				l.Info("lifecycle task gated at concurrency cap",
					"action", "task_gate", "resource_id", task.Name, "project", project.Name)
				return ctrl.Result{RequeueAfter: capRequeue}, nil
			}
		}
	}

	switch task.Status.LifecycleState {
	case "":
		// First reconcile: initialize from the lifecycle-entry annotation set at
		// create time by the binder/mrScan; default to Triage when absent.
		entry := task.Annotations[tatarav1alpha1.LifecycleEntryAnnotation]
		if entry == "" {
			entry = "Triage"
		}
		if err := r.setLifecycleState(ctx, task, entry, "initial"); err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, err
		}
		r.Metrics.ReconcileResult("Task", "success")
		return ctrl.Result{RequeueAfter: pollRequeue}, nil

	case "Triage":
		res, err := r.handleTriage(ctx, &project, task)
		if err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, err
		}
		r.Metrics.ReconcileResult("Task", "success")
		return res, nil

	case "Implement":
		res, err := r.handleImplement(ctx, &project, task)
		if err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, err
		}
		r.Metrics.ReconcileResult("Task", "success")
		return res, nil

	case "Conversation", "MRCI", "Merge", "MainCI":
		// M1/M2: poll states - stub requeue
		r.Metrics.ReconcileResult("Task", "success")
		return ctrl.Result{RequeueAfter: pollRequeue}, nil

	case "Done", "Stopped", "Parked":
		r.Metrics.ReconcileResult("Task", "success")
		return ctrl.Result{}, nil

	default:
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, fmt.Errorf("lifecycle: unknown lifecycleState %q for task %s", task.Status.LifecycleState, task.Name)
	}
}

// handleTriage drives the Triage agent-run state. On a finished run it reads
// IssueOutcome and transitions: close->Done, discuss->Conversation, implement->Implement.
func (r *TaskReconciler) handleTriage(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	// Run finished -> act on the outcome.
	if isTerminal(task.Status.Phase) {
		return r.finishTriage(ctx, project, task)
	}
	// Run in progress (or not yet started) -> drive another step.
	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); err != nil {
		return ctrl.Result{}, fmt.Errorf("triage: get repo: %w", err)
	}
	return r.driveAgentRun(ctx, project, &repo, task, lifecycleTriageText(task))
}

// finishTriage consumes Status.IssueOutcome after a completed Triage agent run.
func (r *TaskReconciler) finishTriage(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	if task.Status.Phase == "Failed" {
		l.Info("triage agent run failed; parking task",
			"action", "lifecycle_triage_failed", "resource_id", task.Name)
		if err := r.setLifecycleState(ctx, task, "Parked", "triage-failed"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.resetAgentRun(ctx, task)
	}

	// Phase == Succeeded: read outcome.
	outcome := task.Status.IssueOutcome
	action := "implement" // default when agent did not set outcome
	comment := ""
	if outcome != nil {
		action = outcome.Action
		comment = outcome.Comment
	}

	// Clear IssueOutcome before acting so stale outcome is never re-consumed.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.IssueOutcome = nil
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("triage: clear IssueOutcome: %w", err)
	}
	task.Status.IssueOutcome = nil

	switch action {
	case "close":
		if err := r.triageCloseIssue(ctx, project, task, comment); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setLifecycleState(ctx, task, "Done", "triage-close"); err != nil {
			return ctrl.Result{}, err
		}

	case "discuss":
		if err := r.triagePostComment(ctx, project, task, comment); err != nil {
			return ctrl.Result{}, err
		}
		idleMinutes := 60
		if project.Spec.Scm != nil && project.Spec.Scm.ConversationIdleMinutes > 0 {
			idleMinutes = project.Spec.Scm.ConversationIdleMinutes
		}
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh := &tatarav1alpha1.Task{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
				return err
			}
			now := metav1.Now()
			deadline := metav1.NewTime(now.Add(time.Duration(idleMinutes) * time.Minute))
			fresh.Status.DeadlineAt = &deadline
			fresh.Status.LastActivityAt = &now
			return r.Status().Update(ctx, fresh)
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("triage: set deadline: %w", err)
		}
		if err := r.setLifecycleState(ctx, task, "Conversation", "triage-discuss"); err != nil {
			return ctrl.Result{}, err
		}

	default: // "implement" and anything else
		if err := r.setLifecycleState(ctx, task, "Implement", "triage-implement"); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, r.resetAgentRun(ctx, task)
}

// triageCloseIssue calls CloseIssue for the task's source issue.
func (r *TaskReconciler) triageCloseIssue(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, comment string) error {
	if task.Spec.Source == nil {
		return nil
	}
	_, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return fmt.Errorf("triage close: %w", err)
	}
	repoSlug, _, perr := repoSlugFromURL(repo.Spec.URL, provider)
	if perr != nil {
		return perr
	}
	if cerr := writer.CloseIssue(ctx, token, repoSlug, task.Spec.Source.Number, comment); cerr != nil {
		r.recordSCM(provider, "close_issue", cerr)
		return fmt.Errorf("triage close issue: %w", cerr)
	}
	r.recordSCM(provider, "close_issue", nil)
	if r.Metrics != nil {
		r.Metrics.IssueOutcome("close")
	}
	log.FromContext(ctx).Info("lifecycle triage: issue closed",
		"action", "scm_issue_outcome", "resource_id", task.Name, "number", task.Spec.Source.Number)
	return nil
}

// triagePostComment posts the discuss comment to the source issue.
func (r *TaskReconciler) triagePostComment(_ context.Context, _ *tatarav1alpha1.Project, task *tatarav1alpha1.Task, comment string) error {
	if task.Spec.Source == nil {
		return nil
	}
	ctx := context.Background()
	_, _, writer, token, _, err := r.scmContext(ctx, task)
	if err != nil {
		return fmt.Errorf("triage discuss: %w", err)
	}
	if cerr := writer.Comment(ctx, token, task.Spec.Source.IssueRef, comment); cerr != nil {
		return fmt.Errorf("triage discuss comment: %w", cerr)
	}
	log.FromContext(ctx).Info("lifecycle triage: discuss comment posted",
		"action", "scm_issue_discuss", "resource_id", task.Name)
	return nil
}

// handleImplement drives the Implement agent-run state. On a finished run it
// calls writeBackOpenChange to open the MR, then transitions to MRCI.
func (r *TaskReconciler) handleImplement(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	if isTerminal(task.Status.Phase) {
		return r.finishImplement(ctx, task)
	}
	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); err != nil {
		return ctrl.Result{}, fmt.Errorf("implement: get repo: %w", err)
	}
	planText := planTurnText(task.Spec.Goal, taskBranch(task), project.Name, task.Name)
	return r.driveAgentRun(ctx, project, &repo, task, planText)
}

// finishImplement opens the MR after the Implement run completes.
func (r *TaskReconciler) finishImplement(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	if task.Status.Phase == "Failed" {
		l.Info("implement agent run failed; parking task",
			"action", "lifecycle_implement_failed", "resource_id", task.Name)
		if err := r.setLifecycleState(ctx, task, "Parked", "implement-failed"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.resetAgentRun(ctx, task)
	}

	// Phase == Succeeded: open MR via the shared writeBackOpenChange path.
	// writeBackOpenChange sets task.Status.PrURL when a PR was opened.
	if _, err := r.writeBackOpenChange(ctx, task); err != nil {
		return ctrl.Result{}, fmt.Errorf("implement: open change: %w", err)
	}

	// Re-read task to pick up PrURL written by writeBackOpenChange.
	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
		return ctrl.Result{}, fmt.Errorf("implement: re-get task: %w", err)
	}

	if fresh.Status.PrURL == "" {
		// No PR opened (no diff / branch absent) -> park.
		l.Info("implement: no PR opened; parking", "action", "lifecycle_implement_no_change", "resource_id", task.Name)
		if err := r.setLifecycleState(ctx, fresh, "Parked", "no-change"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.resetAgentRun(ctx, fresh)
	}

	// Record head branch, PR number, and transition to MRCI in one RetryOnConflict
	// block to minimise conflict surface (was two separate round-trips).
	prNumber := parsePRNumber(fresh.Status.PrURL)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		t2 := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), t2); err != nil {
			return err
		}
		t2.Status.HeadBranch = taskBranch(task)
		t2.Status.PRNumber = prNumber
		t2.Status.LifecycleIterations++
		t2.Status.LifecycleState = "MRCI"
		return r.Status().Update(ctx, t2)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("implement: record pr fields + MRCI transition: %w", err)
	}

	l.Info("lifecycle transition",
		"action", "lifecycle_transition",
		"resource_id", task.Name,
		"from", "Implement",
		"to", "MRCI",
		"reason", "implement-done",
	)
	if r.LifecycleMetrics != nil {
		r.LifecycleMetrics.RecordTransition("Implement", "MRCI")
	}

	// Re-get for resetAgentRun.
	fresh2 := &tatarav1alpha1.Task{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh2); err != nil {
		return ctrl.Result{}, fmt.Errorf("implement: get for reset: %w", err)
	}
	return ctrl.Result{}, r.resetAgentRun(ctx, fresh2)
}

// parsePRNumber extracts the trailing integer from a PR/MR URL
// (e.g. https://github.com/o/r/pull/42 -> 42).
func parsePRNumber(prURL string) int {
	if prURL == "" {
		return 0
	}
	parts := strings.Split(strings.TrimRight(prURL, "/"), "/")
	if len(parts) == 0 {
		return 0
	}
	n, _ := strconv.Atoi(parts[len(parts)-1])
	return n
}
