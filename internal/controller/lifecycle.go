// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

const babysitDefaultDeadlineMinutes = 60

// lifecyclePR returns the PR number and URL for a lifecycle task. When the task
// was opened via an issue (issue path), finishImplement writes PrNumber/PrURL;
// when entered directly from a bot PR (IsPR=true), the Source fields carry them.
func lifecyclePR(task *tatarav1alpha1.Task) (number int, url string) {
	if task.Status.PRNumber != 0 {
		return task.Status.PRNumber, task.Status.PrURL
	}
	if task.Spec.Source != nil && task.Spec.Source.IsPR {
		return task.Spec.Source.Number, task.Spec.Source.URL
	}
	return 0, ""
}

// deadlinePassed reports whether the task's DeadlineAt has been reached.
func deadlinePassed(task *tatarav1alpha1.Task) bool {
	if task.Status.DeadlineAt == nil {
		return false
	}
	return time.Now().After(task.Status.DeadlineAt.Time)
}

// ensureDeadline sets DeadlineAt on first entry to a poll state when unset.
func (r *TaskReconciler) ensureDeadline(ctx context.Context, task *tatarav1alpha1.Task, project *tatarav1alpha1.Project) error {
	if task.Status.DeadlineAt != nil {
		return nil
	}
	minutes := babysitDefaultDeadlineMinutes
	if project.Spec.Scm != nil && project.Spec.Scm.BabysitDeadlineMinutes > 0 {
		minutes = project.Spec.Scm.BabysitDeadlineMinutes
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		if fresh.Status.DeadlineAt != nil {
			task.Status.DeadlineAt = fresh.Status.DeadlineAt
			return nil
		}
		dl := metav1.NewTime(time.Now().Add(time.Duration(minutes) * time.Minute))
		fresh.Status.DeadlineAt = &dl
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		task.Status.DeadlineAt = fresh.Status.DeadlineAt
		return nil
	})
}

// clearDeadline clears DeadlineAt on transition out of a poll state.
func (r *TaskReconciler) clearDeadline(ctx context.Context, task *tatarav1alpha1.Task) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		if fresh.Status.DeadlineAt == nil {
			return nil
		}
		fresh.Status.DeadlineAt = nil
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		task.Status.DeadlineAt = nil
		return nil
	})
}

// parkWithComment posts a comment on the PR/issue and transitions to Parked.
// For issue-linked tasks it comments on the issue (IssueRef). For bot-PR-entry
// tasks with no issue ref, it falls back to the PR ref derived from lifecyclePR.
func (r *TaskReconciler) parkWithComment(ctx context.Context, task *tatarav1alpha1.Task, writer scm.SCMWriter, token, reason, msg string) error {
	l := log.FromContext(ctx)
	if task.Spec.Source != nil {
		commentRef := task.Spec.Source.IssueRef
		// For bot-PR-entry tasks the binder sets IssueRef to "owner/repo#N" (the PR
		// ref). In the rare case it is empty, fall back to URL from lifecyclePR so
		// the park is never silent.
		if commentRef == "" && task.Spec.Source.IsPR {
			_, prURL := lifecyclePR(task)
			if prURL == "" {
				prURL = task.Spec.Source.URL
			}
			commentRef = prURL
		}
		if commentRef != "" {
			if cerr := writer.Comment(ctx, token, commentRef, msg); cerr != nil {
				l.Error(cerr, "lifecycle: park comment (non-fatal)", "resource_id", task.Name)
			}
		}
	}
	return r.setLifecycleState(ctx, task, "Parked", reason)
}

// deleteWrapper best-effort deletes the wrapper Pod and Service for a task.
// Idempotent: a missing object is not an error. Used by terminate (terminal
// phase), resetAgentRun (re-spawn), and setLifecycleState (terminal lifecycle).
// Thin receiver-bound wrapper over the shared agent.DeleteWrapper so the
// webhook server (different receiver type) can reuse the same teardown.
func (r *TaskReconciler) deleteWrapper(ctx context.Context, task *tatarav1alpha1.Task) error {
	return agent.DeleteWrapper(ctx, r.Client, task.Namespace, task)
}

// setLifecycleState updates task.Status.LifecycleState to `to`, retrying on
// conflict (same pattern as clearWritebackPending). It logs the transition at
// INFO and increments tatara_lifecycle_transition_total{from,to}. On a
// transition into a terminal lifecycle state (Done/Stopped/Parked) it also
// tears down the wrapper Pod+Service so idle agent sessions do not accumulate.
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
		// A fresh entry into Implement (triage-implement, or a CI-failure/merge-
		// conflict re-entry, or a human revival) starts a new implementation
		// attempt, so reset the empty-run retry budget. The empty-run retry loop
		// re-spawns via resetAgentRun (which never calls setLifecycleState), so
		// this cannot clobber an in-progress retry count.
		if to == "Implement" {
			fresh.Status.ImplementEmptyRetries = 0
		}
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
		// Track live task counts per state via delta adjustments on the gauge.
		if from != "" {
			r.LifecycleMetrics.AddLifecycleState(from, -1)
		}
		r.LifecycleMetrics.AddLifecycleState(to, 1)
	}

	task.Status.LifecycleState = to

	// Terminal lifecycle states have no further agent run: tear down the wrapper
	// Pod+Service so idle sessions do not leak CPU/mem + a work PVC. Best-effort;
	// a failure here must not block the (already-applied) state transition.
	if isLifecycleTerminal(to) {
		if err := r.deleteWrapper(ctx, task); err != nil {
			l.Error(err, "lifecycle: delete wrapper on terminal transition (non-fatal)",
				"resource_id", task.Name, "to", to)
		}
	}

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
	if err := r.deleteWrapper(ctx, task); err != nil {
		return fmt.Errorf("resetAgentRun: %w", err)
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
			delete(fresh2.Annotations, annAgentUnreachableSince)
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

// ensurePhaseLabel sets the desired managed phase label on the task's source
// issue (no-op for PR sources or missing source). phase is one of
// "brainstorming"|"approved"|"implementation"|"declined"; it resolves the
// configured label name and delegates to setLifecycleLabel (idempotent).
func (r *TaskReconciler) ensurePhaseLabel(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, phase string) error {
	if task.Spec.Source == nil || task.Spec.Source.IsPR || task.Spec.Source.IssueRef == "" {
		return nil
	}
	brainstorming, approved, implementation, declined := lifecycleLabels(project.Spec.Scm)
	var desired string
	switch phase {
	case "brainstorming":
		desired = brainstorming
	case "approved":
		desired = approved
	case "implementation":
		desired = implementation
	case "declined":
		desired = declined
	default:
		return nil
	}
	return r.setLifecycleLabel(ctx, project, task, desired)
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

	// Drain agent-queued free-form comments (from the comment MCP tool) to the
	// linked issue before anything else, then clear and requeue. Comments are
	// posted in order; each posted comment is dequeued under RetryOnConflict
	// (preserving any concurrently-appended comment) BEFORE returning, so a
	// post failure can never re-post an already-delivered comment.
	if pending := task.Status.PendingComments; len(pending) > 0 && task.Spec.Source != nil && task.Spec.Source.IssueRef != "" {
		_, _, writer, token, provider, err := r.scmContext(ctx, task)
		if err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, fmt.Errorf("lifecycle drain comments: %w", err)
		}
		posted := 0
		var postErr error
		for _, c := range pending {
			cerr := writer.Comment(ctx, token, task.Spec.Source.IssueRef, c)
			r.recordSCM(provider, "comment", cerr)
			if cerr != nil {
				postErr = cerr
				break
			}
			posted++
			l.Info("lifecycle: agent comment posted",
				"action", "scm_agent_comment", "resource_id", task.Name)
		}
		if posted > 0 {
			if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				var fresh tatarav1alpha1.Task
				if gerr := r.Get(ctx, client.ObjectKeyFromObject(task), &fresh); gerr != nil {
					return gerr
				}
				if len(fresh.Status.PendingComments) >= posted {
					fresh.Status.PendingComments = fresh.Status.PendingComments[posted:]
				} else {
					fresh.Status.PendingComments = nil
				}
				return r.Status().Update(ctx, &fresh)
			}); err != nil {
				r.Metrics.ReconcileResult("Task", "error")
				return ctrl.Result{}, fmt.Errorf("lifecycle clear comments: %w", err)
			}
		}
		if postErr != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, fmt.Errorf("lifecycle drain comment: %w", postErr)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Drain webhook-queued interjections into the live wrapper session: new
	// issue/MR comments that arrived while a turn was in flight must reach the
	// running agent as mid-session input (issue #25). Delivered in order, then
	// cleared under RetryOnConflict so a concurrently-appended interjection is
	// preserved. Skipped when r.Session is nil (tests without a session).
	if len(task.Status.PendingInterjections) > 0 && r.Session != nil {
		// Stale if no turn is in flight: the next turn/triage re-reads the thread,
		// so drop the queue rather than injecting into a session with nothing running.
		if !taskHasInflightTurn(task) {
			if err := r.clearPendingInterjections(ctx, task, len(task.Status.PendingInterjections)); err != nil {
				r.Metrics.ReconcileResult("Task", "error")
				return ctrl.Result{}, fmt.Errorf("lifecycle drop stale interjections: %w", err)
			}
			l.Info("lifecycle: dropped stale interjections (no in-flight turn)",
				"action", "interject_stale", "resource_id", task.Name)
			return ctrl.Result{Requeue: true}, nil
		}
		baseURL := agent.BaseURL(task, task.Namespace)
		total := len(task.Status.PendingInterjections)
		delivered := 0
		var deliverErr error
		for _, text := range task.Status.PendingInterjections {
			ierr := r.Session.Interject(ctx, baseURL, text)
			if ierr != nil {
				var unreachable *agent.UnreachableError
				if errors.As(ierr, &unreachable) {
					// Pod/turn server still booting: keep the queue intact, retry soon.
					l.Info("lifecycle: interject unreachable; keeping queue",
						"action", "interject_retry", "resource_id", task.Name)
					return ctrl.Result{RequeueAfter: pollRequeue}, nil
				}
				deliverErr = ierr
				l.Error(ierr, "lifecycle: interject failed", "resource_id", task.Name)
				break
			}
			delivered++
			l.Info("lifecycle: interjection delivered",
				"action", "interject", "resource_id", task.Name)
		}
		if delivered > 0 {
			if err := r.clearPendingInterjections(ctx, task, delivered); err != nil {
				r.Metrics.ReconcileResult("Task", "error")
				return ctrl.Result{}, fmt.Errorf("lifecycle clear interjections: %w", err)
			}
		}
		if deliverErr != nil || delivered < total {
			// Interjections remain after a delivery error: back off, retry later.
			return ctrl.Result{RequeueAfter: pollRequeue}, nil
		}
		return ctrl.Result{Requeue: true}, nil
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
		if entry == "Triage" {
			if err := r.ensurePhaseLabel(ctx, &project, task, "brainstorming"); err != nil {
				r.Metrics.ReconcileResult("Task", "error")
				return ctrl.Result{}, err
			}
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

	case "Conversation":
		res, err := r.handleConversation(ctx, task)
		if err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, err
		}
		r.Metrics.ReconcileResult("Task", "success")
		return res, nil

	case "MRCI":
		res, err := r.handleMRCI(ctx, &project, task)
		if err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, err
		}
		r.Metrics.ReconcileResult("Task", "success")
		return res, nil

	case "Merge":
		res, err := r.handleMerge(ctx, &project, task)
		if err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, err
		}
		r.Metrics.ReconcileResult("Task", "success")
		return res, nil

	case "MainCI":
		res, err := r.handleMainCI(ctx, &project, task)
		if err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, err
		}
		r.Metrics.ReconcileResult("Task", "success")
		return res, nil

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
	// Idempotent: ensure the brainstorming label is set (covers reactivation where
	// the task re-enters Triage without going through the case "" initializer).
	if !isTerminal(task.Status.Phase) {
		if err := r.ensurePhaseLabel(ctx, project, task, "brainstorming"); err != nil {
			return ctrl.Result{}, err
		}
	}
	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); err != nil {
		return ctrl.Result{}, fmt.Errorf("triage: get repo: %w", err)
	}
	prompt := r.buildTriagePromptFor(ctx, project, task)
	return r.driveAgentRun(ctx, project, &repo, task, prompt)
}

// buildTriagePromptFor fetches issue content and comments via ReaderFor (if wired) and
// builds the full triage turn-0 prompt with real title, body, and comment thread included.
// On any error it falls back gracefully with empty title/body.
func (r *TaskReconciler) buildTriagePromptFor(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) string {
	l := log.FromContext(ctx)
	if r.ReaderFor == nil || task.Spec.Source == nil {
		return lifecycleTriageText(task, "", "")
	}
	provider := task.Spec.Source.Provider
	if provider == "" && project.Spec.Scm != nil {
		provider = project.Spec.Scm.Provider
	}
	token, err := r.scmToken(ctx, task.Namespace, project.Spec.ScmSecretRef)
	if err != nil {
		l.Info("triage: could not fetch token for comment thread (non-fatal)", "resource_id", task.Name)
		return lifecycleTriageText(task, "", "")
	}
	reader, err := r.ReaderFor(provider, token)
	if err != nil {
		l.Info("triage: could not get reader for comment thread (non-fatal)", "resource_id", task.Name)
		return lifecycleTriageText(task, "", "")
	}
	owner, repoName, parseErr := scm.OwnerRepo(r.repoURLForTask(ctx, task))
	if parseErr != nil {
		return lifecycleTriageText(task, "", "")
	}
	content, err := reader.GetIssue(ctx, owner, repoName, task.Spec.Source.Number)
	if err != nil {
		l.Info("triage: GetIssue failed (non-fatal)", "resource_id", task.Name, "err", err.Error())
	}
	comments, err := reader.ListIssueComments(ctx, owner, repoName, task.Spec.Source.Number)
	if err != nil {
		l.Info("triage: ListIssueComments failed (non-fatal)", "resource_id", task.Name, "err", err.Error())
		comments = nil
	}
	return buildTriagePrompt(task, content.Title, content.Body, comments)
}

// repoURLForTask fetches the Repository URL for the task's RepositoryRef.
// Returns "" on error (caller falls back gracefully).
func (r *TaskReconciler) repoURLForTask(ctx context.Context, task *tatarav1alpha1.Task) string {
	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); err != nil {
		return ""
	}
	return repo.Spec.URL
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

	// IssueOutcome is cleared only AFTER the action arm commits a state
	// transition (see clearIssueOutcome calls below). Clearing before acting
	// would, on any mid-arm SCM error, strand the task with a nil outcome and
	// silently default a close/discuss to implement on the next reconcile.
	// Accepted tradeoff: if the post-SCM status transition (RetryOnConflict)
	// exhausts its retries after the comment/close already landed, the next
	// reconcile re-runs the arm and may post a duplicate triage comment. That
	// is rare and cosmetic, and preferred over the wrong-implement downgrade.
	brainstorming, approved, _, declined := lifecycleLabels(project.Spec.Scm)

	switch action {
	case "close":
		if hasUnmergedChange(task) {
			// Invariant: never close an issue that has an unmerged code change.
			// A human-comment re-triage of an issue whose MR is open/conflicting
			// can yield a "close" outcome; closing here would orphan the unmerged
			// change. Keep the issue open (brainstorming) and await the change being
			// merged-green or abandoned.
			l.Info("triage close withheld: unmerged change exists; keeping issue open",
				"action", "lifecycle_close_withheld", "resource_id", task.Name,
				"pr_url", task.Status.PrURL, "head_branch", task.Status.HeadBranch)
			if err := r.setLifecycleLabel(ctx, project, task, brainstorming); err != nil {
				return ctrl.Result{}, err
			}
			note := comment
			if note != "" {
				note += "\n\n"
			}
			note += "tatara: not closing - this issue has an unmerged change that must be merged (with green main CI) or abandoned first."
			if err := r.triagePostComment(ctx, project, task, note); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.enterConversation(ctx, project, task, "close-withheld-unmerged"); err != nil {
				return ctrl.Result{}, err
			}
			break
		}
		if err := r.setLifecycleLabel(ctx, project, task, declined); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.triageCloseIssue(ctx, project, task, comment); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setLifecycleState(ctx, task, "Done", "triage-close"); err != nil {
			return ctrl.Result{}, err
		}

	case "discuss":
		if err := r.setLifecycleLabel(ctx, project, task, brainstorming); err != nil {
			return ctrl.Result{}, err
		}
		// Silence gate: for tatara-authored issues with no human reply, do not
		// post a repeated "still awaiting go-ahead" comment on every triage cycle.
		// Only post when a human has actually replied since the issue was opened.
		// Human-filed issues always get the comment (authorship check returns false).
		skipComment := false
		authored, aerr := r.tataraAuthoredIssue(ctx, project, task)
		if aerr != nil {
			l.Info("triage discuss: authorship check failed; posting comment (fail open)",
				"action", "lifecycle_discuss_silence_check", "resource_id", task.Name, "err", aerr.Error())
		} else if authored {
			human, herr := r.hasHumanComment(ctx, project, task)
			if herr != nil {
				l.Info("triage discuss: hasHumanComment failed; posting comment (fail open)",
					"action", "lifecycle_discuss_silence_check", "resource_id", task.Name, "err", herr.Error())
			} else if !human {
				skipComment = true
				l.Info("triage discuss: tatara-authored issue with no human reply; suppressing comment",
					"action", "lifecycle_discuss_silent_hold", "resource_id", task.Name)
			}
		}
		if !skipComment {
			if err := r.triagePostComment(ctx, project, task, comment); err != nil {
				return ctrl.Result{}, err
			}
		}
		if err := r.enterConversation(ctx, project, task, "triage-discuss"); err != nil {
			return ctrl.Result{}, err
		}

	default: // "implement" and anything else
		// Self-approve guard (R1/R2): tatara never approves its OWN idea before a
		// human has engaged. Authorship is detected via the tatara-authored marker
		// in the issue body - reliable and egress-verified, unlike Source.AuthorLogin
		// which is empty for cron-scanned issues and untrusted on the webhook path.
		authored, aerr := r.tataraAuthoredIssue(ctx, project, task)
		if aerr != nil {
			l.Info("triage: authorship check failed; treating as tatara-authored (fail closed)",
				"action", "lifecycle_triage_guard", "resource_id", task.Name, "err", aerr.Error())
			authored = true
		}
		if authored {
			human, herr := r.hasHumanComment(ctx, project, task)
			if herr != nil {
				l.Info("triage: hasHumanComment failed; parking as brainstorming (fail closed)",
					"action", "lifecycle_triage_guard", "resource_id", task.Name, "err", herr.Error())
				human = false
			}
			if !human {
				if err := r.setLifecycleLabel(ctx, project, task, brainstorming); err != nil {
					return ctrl.Result{}, err
				}
				if err := r.enterConversation(ctx, project, task, "triage-await-approval"); err != nil {
					return ctrl.Result{}, err
				}
				if err := r.clearIssueOutcome(ctx, task); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{}, r.resetAgentRun(ctx, task)
			}
		}
		if err := r.setLifecycleLabel(ctx, project, task, approved); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setLifecycleState(ctx, task, "Implement", "triage-implement"); err != nil {
			return ctrl.Result{}, err
		}
	}

	if err := r.clearIssueOutcome(ctx, task); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.resetAgentRun(ctx, task)
}

// clearPendingInterjections removes the first n delivered interjections from
// Status.PendingInterjections under RetryOnConflict, preserving any
// concurrently-appended interjection (same trim pattern as PendingComments).
func (r *TaskReconciler) clearPendingInterjections(ctx context.Context, task *tatarav1alpha1.Task, n int) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh tatarav1alpha1.Task
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), &fresh); err != nil {
			return err
		}
		if len(fresh.Status.PendingInterjections) >= n {
			fresh.Status.PendingInterjections = fresh.Status.PendingInterjections[n:]
		} else {
			fresh.Status.PendingInterjections = nil
		}
		return r.Status().Update(ctx, &fresh)
	})
}

// clearIssueOutcome nils Status.IssueOutcome (RetryOnConflict). Called only
// after the triage action arm has committed its state transition, so a mid-arm
// error retries the same outcome rather than defaulting to implement.
func (r *TaskReconciler) clearIssueOutcome(ctx context.Context, task *tatarav1alpha1.Task) error {
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.IssueOutcome = nil
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		return fmt.Errorf("triage: clear IssueOutcome: %w", err)
	}
	task.Status.IssueOutcome = nil
	return nil
}

// enterConversation sets the conversation idle deadline + LastActivityAt and
// transitions the task to Conversation with the given reason. Shared by the
// discuss and bot-await-approval triage outcomes.
func (r *TaskReconciler) enterConversation(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, reason string) error {
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
		return fmt.Errorf("enter conversation: set deadline: %w", err)
	}
	return r.setLifecycleState(ctx, task, "Conversation", reason)
}

// handleConversation manages the idle wait state. No pod is ever spawned here.
// If the deadline has passed the task transitions to Stopped (idle-stop, resumable).
// If DeadlineAt is nil (safety net for tasks whose deadline was never set), set it
// once and requeue so the normal deadline path runs on the next reconcile.
// Otherwise it requeues until the deadline.
func (r *TaskReconciler) handleConversation(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	if task.Status.DeadlineAt == nil {
		// Safety net: set deadline once rather than returning false from
		// deadlinePassed forever and requeuing without bound.
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh := &tatarav1alpha1.Task{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
				return err
			}
			if fresh.Status.DeadlineAt != nil {
				task.Status.DeadlineAt = fresh.Status.DeadlineAt
				return nil
			}
			dl := metav1.NewTime(time.Now().Add(60 * time.Minute))
			fresh.Status.DeadlineAt = &dl
			if err := r.Status().Update(ctx, fresh); err != nil {
				return err
			}
			task.Status.DeadlineAt = fresh.Status.DeadlineAt
			return nil
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("conversation: set nil deadline: %w", err)
		}
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}
	if deadlinePassed(task) {
		if err := r.setLifecycleState(ctx, task, "Stopped", "idle"); err != nil {
			return ctrl.Result{}, err
		}
		if r.LifecycleMetrics != nil {
			r.LifecycleMetrics.RecordIdleStop()
		}
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: pollRequeue}, nil
}

// hasUnmergedChange reports whether the task produced a code artifact (a pushed
// branch or an opened PR/MR). An issue with an unmerged change must NOT be
// closed by an agent-driven outcome; only handleMainCI (merge recorded + main
// CI green) may close such an issue.
func hasUnmergedChange(task *tatarav1alpha1.Task) bool {
	return task.Status.PrURL != "" || task.Status.HeadBranch != ""
}

// triageCloseIssue calls CloseIssue for the task's source issue. It refuses to
// close when an unmerged change exists (defence-in-depth for the invariant that
// only a merged-and-green lifecycle may close an issue).
func (r *TaskReconciler) triageCloseIssue(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, comment string) error {
	if task.Spec.Source == nil {
		return nil
	}
	if hasUnmergedChange(task) {
		log.FromContext(ctx).Info("triage close withheld: issue has an unmerged change",
			"action", "scm_close_withheld", "resource_id", task.Name,
			"number", task.Spec.Source.Number, "pr_url", task.Status.PrURL, "head_branch", task.Status.HeadBranch)
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
func (r *TaskReconciler) triagePostComment(ctx context.Context, _ *tatarav1alpha1.Project, task *tatarav1alpha1.Task, comment string) error {
	if task.Spec.Source == nil {
		return nil
	}
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

// implementPrompt builds the turn-0 prompt for the Implement state.
//   - When Status.ImplementContext is set, appends a "## Re-entry context" block.
//   - When the pending-handover-resume annotation is set, prepends a
//     "## Resume from handover" block so the agent resumes with full context.
func implementPrompt(task *tatarav1alpha1.Task) string {
	base := planTurnText(task.Spec.Goal, taskBranch(task), task.Spec.ProjectRef, task.Name)
	if task.Status.ImplementContext != "" {
		base += "\n\n## Re-entry context\n" + task.Status.ImplementContext
	}
	if task.Annotations[annPendingHandoverResume] == "true" && task.Status.Handover != "" {
		base += "\n\n## Resume from handover\n" + task.Status.Handover
	}
	return base
}

// handleImplement drives the Implement agent-run state. On a finished run it
// calls writeBackOpenChange to open the MR, then transitions to MRCI.
func (r *TaskReconciler) handleImplement(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	if isTerminal(task.Status.Phase) {
		return r.finishImplement(ctx, task)
	}

	// Fresh-spawn path: Phase == "". Apply backstop + increment iterations.
	if task.Status.Phase == "" {
		maxIter := 10
		if project.Spec.Agent.MaxLifecycleIterations > 0 {
			maxIter = project.Spec.Agent.MaxLifecycleIterations
		}
		if task.Status.LifecycleIterations >= maxIter {
			// Backstop: too many attempts. Post comment and park.
			_, _, writer, token, _, scmErr := r.scmContext(ctx, task)
			if scmErr == nil && task.Spec.Source != nil && task.Spec.Source.IssueRef != "" {
				msg := "max lifecycle iterations reached; leaving for a human"
				_ = writer.Comment(ctx, token, task.Spec.Source.IssueRef, msg)
			}
			if err := r.setLifecycleState(ctx, task, "Parked", "maxIterations"); err != nil {
				return ctrl.Result{}, err
			}
			if r.LifecycleMetrics != nil {
				r.LifecycleMetrics.RecordGiveup("maxIterations")
			}
			return ctrl.Result{}, nil
		}
		// Increment LifecycleIterations on fresh spawn, then re-read task so
		// driveAgentRun has the current resourceVersion.
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh := &tatarav1alpha1.Task{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
				return err
			}
			fresh.Status.LifecycleIterations++
			return r.Status().Update(ctx, fresh)
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("implement: increment iterations: %w", err)
		}
		// Re-read after increment so driveAgentRun uses the latest resourceVersion.
		refreshed := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), refreshed); err != nil {
			return ctrl.Result{}, fmt.Errorf("implement: re-get after iteration increment: %w", err)
		}
		// Copy mutable pointers back so callers see the new values.
		task.ResourceVersion = refreshed.ResourceVersion
		task.Status = refreshed.Status
		// Idempotent: set implementation label on fresh Implement spawn.
		if err := r.ensurePhaseLabel(ctx, project, task, "implementation"); err != nil {
			return ctrl.Result{}, err
		}
	}

	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); err != nil {
		return ctrl.Result{}, fmt.Errorf("implement: get repo: %w", err)
	}
	// Build the prompt using the current ImplementContext (may contain re-entry
	// instructions). Do NOT clear it here - it must persist until the pod is ready
	// and driveTurns submits the turn-0 prompt. Clearing happens in finishImplement,
	// after the run has completed and the context has been used.
	planText := implementPrompt(task)
	return r.driveAgentRun(ctx, project, &repo, task, planText)
}

// finishImplement opens the MR after the Implement run completes.
func (r *TaskReconciler) finishImplement(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	// Clear ImplementContext: by the time finishImplement runs the turn-0 prompt
	// has already been submitted (or the run failed), so the context is stale.
	// Each re-entry overwrites it fresh, so clearing here is safe.
	// After the write, refresh the in-memory task so subsequent status updates use
	// the current resourceVersion and do not conflict.
	if task.Status.ImplementContext != "" {
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh := &tatarav1alpha1.Task{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
				return err
			}
			if fresh.Status.ImplementContext == "" {
				task.ResourceVersion = fresh.ResourceVersion
				task.Status = fresh.Status
				return nil
			}
			fresh.Status.ImplementContext = ""
			if err := r.Status().Update(ctx, fresh); err != nil {
				return err
			}
			task.ResourceVersion = fresh.ResourceVersion
			task.Status = fresh.Status
			return nil
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("implement: clear ImplementContext: %w", err)
		}
	}

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
		// Implement run produced no commit -> no PR. Retry with a re-entry nudge
		// up to the cap, then comment on the issue and park with a distinct reason.
		const emptyRetryCap = 2
		if fresh.Status.ImplementEmptyRetries < emptyRetryCap {
			if err := r.setImplementEmptyRetries(ctx, fresh, fresh.Status.ImplementEmptyRetries+1); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.setImplementContext(ctx, fresh, emptyImplementReentryPrompt); err != nil {
				return ctrl.Result{}, err
			}
			l.Info("implement: no commit produced; retrying with re-entry nudge",
				"action", "lifecycle_implement_empty_retry", "resource_id", task.Name,
				"attempt", fresh.Status.ImplementEmptyRetries, "cap", emptyRetryCap)
			// resetAgentRun clears phase to "" and leaves LifecycleState=Implement,
			// so the next reconcile re-spawns the Implement run with ImplementContext.
			return ctrl.Result{}, r.resetAgentRun(ctx, fresh)
		}
		l.Info("implement: no commit after retry cap; commenting + parking",
			"action", "lifecycle_implement_empty_parked", "resource_id", task.Name)
		msg := "The implement agent produced no change after " +
			strconv.Itoa(emptyRetryCap) + " attempts. Leaving this for a human - " +
			"the fix may be unclear, blocked, or already present."
		// parkWithComment posts the comment (with the IsPR ref fallback) and parks
		// atomically. If the SCM context is unavailable, still park so the task does
		// not loop, just without a comment.
		if _, _, writer, token, _, scmErr := r.scmContext(ctx, fresh); scmErr == nil {
			if err := r.parkWithComment(ctx, fresh, writer, token, "implement-empty", msg); err != nil {
				return ctrl.Result{}, err
			}
		} else {
			l.Error(scmErr, "implement: scm context for empty-park comment (parking without comment)",
				"resource_id", task.Name)
			if err := r.setLifecycleState(ctx, fresh, "Parked", "implement-empty"); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, r.resetAgentRun(ctx, fresh)
	}
	// PR opened: clear any prior empty-retry count so a later re-entry into
	// Implement starts fresh.
	if fresh.Status.ImplementEmptyRetries > 0 {
		if err := r.setImplementEmptyRetries(ctx, fresh, 0); err != nil {
			l.Error(err, "implement: reset empty-retry counter (non-fatal)", "resource_id", task.Name)
		}
	}

	// M4: open a follow-up issue when RemainingScope is set and not already done.
	if err := r.maybeOpenFollowupIssue(ctx, fresh); err != nil {
		// Non-fatal: log and continue so the lifecycle does not stall.
		l.Error(err, "implement: open follow-up issue (non-fatal)", "resource_id", task.Name)
	}

	// Re-read after follow-up so subsequent writes use the latest resourceVersion.
	if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
		return ctrl.Result{}, fmt.Errorf("implement: re-get after followup: %w", err)
	}

	// Record head branch, PR number, clear any stale deadline (e.g. from a prior
	// Conversation idle deadline), and transition to MRCI in one RetryOnConflict
	// block to minimise conflict surface.
	prNumber := parsePRNumber(fresh.Status.PrURL)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		t2 := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), t2); err != nil {
			return err
		}
		t2.Status.HeadBranch = taskBranch(task)
		t2.Status.PRNumber = prNumber
		t2.Status.DeadlineAt = nil // clear stale Conversation/Implement deadline; MRCI sets its own via ensureDeadline
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
	task.Status.LifecycleState = "MRCI"

	// Re-get for resetAgentRun.
	fresh2 := &tatarav1alpha1.Task{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh2); err != nil {
		return ctrl.Result{}, fmt.Errorf("implement: get for reset: %w", err)
	}
	return ctrl.Result{}, r.resetAgentRun(ctx, fresh2)
}

// maybeOpenFollowupIssue creates a follow-up issue when ChangeSummary.RemainingScope
// is non-empty and Status.FollowupIssueURL is not yet set (idempotency guard).
// It appends the new issue URL to Status.DiscoveredIssues and records it in
// Status.FollowupIssueURL so re-entry does not open a duplicate.
// Non-fatal: if CreateIssue fails the caller logs and continues.
func (r *TaskReconciler) maybeOpenFollowupIssue(ctx context.Context, task *tatarav1alpha1.Task) error {
	cs := task.Status.ChangeSummary
	if cs == nil || cs.RemainingScope == "" {
		return nil
	}
	// Idempotency guard: already opened.
	if task.Status.FollowupIssueURL != "" {
		return nil
	}

	_, repo, writer, token, _, err := r.scmContext(ctx, task)
	if err != nil {
		return fmt.Errorf("followup: scm context: %w", err)
	}

	issueTitle := "Follow-up: " + firstLine(task.Spec.Goal) + " (remaining scope)"
	prURL := task.Status.PrURL
	issueBody := cs.RemainingScope + "\n\nOpened as a follow-up to: " + prURL + "\n\n" + tataraAuthoredMarker

	created, cerr := writer.CreateIssue(ctx, repo.Spec.URL, token, scm.IssueReq{
		Title: issueTitle,
		Body:  issueBody,
	})
	if cerr != nil {
		return fmt.Errorf("followup: create issue: %w", cerr)
	}

	log.FromContext(ctx).Info("lifecycle implement: follow-up issue opened",
		"action", "scm_followup_issue",
		"resource_id", task.Name,
		"issue_url", created.URL,
	)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		// Append to DiscoveredIssues only if not already present.
		alreadyPresent := false
		for _, u := range fresh.Status.DiscoveredIssues {
			if u == created.URL {
				alreadyPresent = true
				break
			}
		}
		if !alreadyPresent {
			fresh.Status.DiscoveredIssues = append(fresh.Status.DiscoveredIssues, created.URL)
		}
		fresh.Status.FollowupIssueURL = created.URL
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		task.Status.DiscoveredIssues = fresh.Status.DiscoveredIssues
		task.Status.FollowupIssueURL = fresh.Status.FollowupIssueURL
		return nil
	})
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

// handleMRCI polls the MR CI status, enforces the authorship gate, and
// transitions to Merge (green), Implement (failure), or Parked (deadline/not-bot).
func (r *TaskReconciler) handleMRCI(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	_, repo, writer, token, _, err := r.scmContext(ctx, task)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("mrci: %w", err)
	}

	number, _ := lifecyclePR(task)

	// Guard: PR number 0 means no PR was opened; calling GetPRState(0) is invalid.
	if number == 0 {
		l.Info("mrci: PR number is 0; parking task",
			"action", "lifecycle_mrci_no_pr", "resource_id", task.Name)
		msg := "lifecycle: no PR number available for MRCI; parking."
		if err := r.parkWithComment(ctx, task, writer, token, "no-pr-number", msg); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Authorship gate: PR must be bot-authored.
	st, serr := writer.GetPRState(ctx, repo.Spec.URL, token, number)
	if serr != nil {
		return ctrl.Result{}, fmt.Errorf("mrci: get pr state: %w", serr)
	}
	botLogin := ""
	if project.Spec.Scm != nil {
		botLogin = project.Spec.Scm.BotLogin
	}
	if botLogin != "" && st.Author != botLogin {
		l.Info("mrci: PR not bot-authored; parking",
			"action", "lifecycle_mrci_not_bot", "resource_id", task.Name, "author", st.Author)
		msg := fmt.Sprintf("lifecycle: PR #%d is not authored by the bot (%s); parking.", number, botLogin)
		if err := r.parkWithComment(ctx, task, writer, token, "not-bot-authored", msg); err != nil {
			return ctrl.Result{}, err
		}
		if r.LifecycleMetrics != nil {
			r.LifecycleMetrics.RecordGiveup("not-bot-authored")
		}
		return ctrl.Result{}, nil
	}

	// Set DeadlineAt on first entry.
	if err := r.ensureDeadline(ctx, task, project); err != nil {
		return ctrl.Result{}, fmt.Errorf("mrci: ensure deadline: %w", err)
	}

	// Deadline check (do after authorship so a non-bot PR parks immediately).
	if deadlinePassed(task) {
		msg := fmt.Sprintf("lifecycle: MRCI deadline reached for PR #%d; parking.", number)
		if err := r.parkWithComment(ctx, task, writer, token, "deadline", msg); err != nil {
			return ctrl.Result{}, err
		}
		if r.LifecycleMetrics != nil {
			r.LifecycleMetrics.RecordGiveup("deadline")
		}
		return ctrl.Result{}, nil
	}

	switch st.CIStatus {
	case "pending":
		return ctrl.Result{RequeueAfter: pollRequeue}, nil

	case "success":
		if r.LifecycleMetrics != nil && task.Status.DeadlineAt != nil {
			minutes := babysitDefaultDeadlineMinutes
			if project.Spec.Scm != nil && project.Spec.Scm.BabysitDeadlineMinutes > 0 {
				minutes = project.Spec.Scm.BabysitDeadlineMinutes
			}
			elapsed := time.Duration(minutes)*time.Minute - time.Until(task.Status.DeadlineAt.Time)
			if elapsed > 0 {
				r.LifecycleMetrics.ObserveMRCIWait(elapsed.Seconds())
			}
		}
		if err := r.clearDeadline(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setLifecycleState(ctx, task, "Merge", "mrci-success"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	case "failure":
		ctx2 := fmt.Sprintf("MR pipeline failed for PR #%d. Fix the failures and push.", number)
		if err := r.setImplementContext(ctx, task, ctx2); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.clearDeadline(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.maybeMarkHandoverResume(ctx, project, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setLifecycleState(ctx, task, "Implement", "mrci-failure"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	default: // "" - no CI configured
		if err := r.clearDeadline(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setLifecycleState(ctx, task, "Merge", "mrci-no-ci"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
}

// setImplementContext persists ImplementContext on the task via RetryOnConflict.
func (r *TaskReconciler) setImplementContext(ctx context.Context, task *tatarav1alpha1.Task, msg string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.ImplementContext = msg
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		task.Status.ImplementContext = msg
		return nil
	})
}

// emptyImplementReentryPrompt nudges a re-spawned Implement agent that produced
// no diff on the prior turn to either deliver the change or stop and explain.
const emptyImplementReentryPrompt = "Your previous attempt finished without " +
	"committing any change, so no PR could be opened and the issue is still " +
	"open. Re-read the issue and the repository, then EITHER implement the fix " +
	"and commit it, OR if no code change is genuinely needed, state clearly why " +
	"in your final summary so a human can close the issue."

// setImplementEmptyRetries persists Status.ImplementEmptyRetries via RetryOnConflict.
func (r *TaskReconciler) setImplementEmptyRetries(ctx context.Context, task *tatarav1alpha1.Task, n int) error {
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.ImplementEmptyRetries = n
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		task.Status.ImplementEmptyRetries = n
		return nil
	}); err != nil {
		return fmt.Errorf("setImplementEmptyRetries: %w", err)
	}
	return nil
}

// clearMergedChangeState resets the per-MR write-back fields (MergeCommitSHA,
// PrURL, PRNumber) when the lifecycle re-enters Implement AFTER a PR was already
// merged - i.e. only from the MainCI failure route. The merged PR is closed, so
// the fix must land as a brand new MR; leaving these fields set traps the task in
// a non-converging loop:
//   - PrURL set -> writeBackOpenChange short-circuits ("AlreadyWritten") and never
//     opens the new MR, so finishImplement re-enters MRCI against the stale PR.
//   - MergeCommitSHA set -> handleMerge's "already-merged" idempotency guard skips
//     the merge and bounces straight to MainCI, re-checking the stale failing SHA.
//
// Clearing them lets the next Implement->MRCI->Merge cycle open and merge a real
// new change. NOT called on the MRCI-failure or Merge-conflict routes: there the
// PR is still open (MergeCommitSHA is unset) and the fix is pushed to the same PR.
func (r *TaskReconciler) clearMergedChangeState(ctx context.Context, task *tatarav1alpha1.Task) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.MergeCommitSHA = ""
		fresh.Status.PrURL = ""
		fresh.Status.PRNumber = 0
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		task.Status.MergeCommitSHA = ""
		task.Status.PrURL = ""
		task.Status.PRNumber = 0
		return nil
	})
}

// maybeMarkHandoverResume checks whether the last implement run consumed enough
// context to warrant a handover on the NEXT fresh run. When the threshold is
// reached it:
//   - ensures Status.Handover is set (uses the agent-submitted doc if present,
//     otherwise builds one from ResultSummary + ImplementContext + head branch)
//   - stamps tatara.dev/pending-handover-resume=true on the Task annotations
//   - calls LifecycleMetrics.RecordHandover()
//
// Called after EACH failure->Implement transition (MRCI failure, Merge 405,
// MainCI failure). A ContextWindowTokens<=0 is treated as "no guard" to avoid
// div-by-zero.
func (r *TaskReconciler) maybeMarkHandoverResume(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) error {
	ctxWin := project.Spec.Agent.ContextWindowTokens
	if ctxWin <= 0 {
		return nil
	}
	threshold := project.Spec.Agent.HandoverThresholdPercent
	if threshold <= 0 {
		threshold = 50
	}
	pct := task.Status.LastTurnInputTokens * 100 / int64(ctxWin)
	if pct < int64(threshold) {
		return nil
	}

	// Build handover doc when Status.Handover is empty.
	handover := task.Status.Handover
	if handover == "" {
		parts := []string{}
		if task.Status.ResultSummary != "" {
			parts = append(parts, "## Prior work summary\n"+task.Status.ResultSummary)
		}
		if task.Status.ImplementContext != "" {
			parts = append(parts, "## Re-entry context\n"+task.Status.ImplementContext)
		}
		if task.Status.HeadBranch != "" {
			parts = append(parts, "Prior work is on branch `"+task.Status.HeadBranch+"`; read its diff.")
		}
		if len(parts) > 0 {
			handover = strings.Join(parts, "\n\n")
		} else {
			handover = "Resume from prior implement run."
		}
	}

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations[annPendingHandoverResume] = "true"
		if err := r.Update(ctx, fresh); err != nil {
			return err
		}
		task.ResourceVersion = fresh.ResourceVersion
		return nil
	}); err != nil {
		return fmt.Errorf("maybeMarkHandoverResume: set annotation: %w", err)
	}

	// Persist Handover on status.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.Handover = handover
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		task.Status.Handover = handover
		return nil
	}); err != nil {
		return fmt.Errorf("maybeMarkHandoverResume: set handover status: %w", err)
	}

	if r.LifecycleMetrics != nil {
		r.LifecycleMetrics.RecordHandover()
	}
	return nil
}

// handleMerge attempts to merge the PR. Handles 405-conflict as a re-implement
// signal (MUST NOT return the error to avoid controller-runtime backoff loop).
func (r *TaskReconciler) handleMerge(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	_, repo, writer, token, _, err := r.scmContext(ctx, task)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("merge: %w", err)
	}

	number, _ := lifecyclePR(task)

	// Idempotency: if the PR is already merged (MergeCommitSHA already set on the
	// task), skip straight to MainCI without calling Merge again. This handles the
	// case where setLifecycleState("MainCI") failed after a successful Merge on a
	// prior reconcile, which would otherwise re-merge -> 405 -> bogus conflict path.
	if task.Status.MergeCommitSHA != "" {
		// PR was merged in a prior reconcile; advance to MainCI directly.
		if err := r.clearDeadline(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setLifecycleState(ctx, task, "MainCI", "already-merged"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Set DeadlineAt on first entry.
	if err := r.ensureDeadline(ctx, task, project); err != nil {
		return ctrl.Result{}, fmt.Errorf("merge: ensure deadline: %w", err)
	}

	// Check mergeAllowed policy.
	allowed, merr := r.mergeAllowed(ctx, project, repo, writer, token, number)
	if merr != nil {
		return ctrl.Result{}, merr
	}
	if !allowed {
		if deadlinePassed(task) {
			msg := fmt.Sprintf("lifecycle: merge deadline reached for PR #%d; parking.", number)
			if err := r.parkWithComment(ctx, task, writer, token, "deadline", msg); err != nil {
				return ctrl.Result{}, err
			}
			if r.LifecycleMetrics != nil {
				r.LifecycleMetrics.RecordGiveup("deadline")
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}

	// Attempt merge.
	sha, mergeErr := writer.Merge(ctx, repo.Spec.URL, token, number, "squash")
	if mergeErr == nil {
		// Success: record SHA and advance.
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh := &tatarav1alpha1.Task{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
				return err
			}
			fresh.Status.MergeCommitSHA = sha
			return r.Status().Update(ctx, fresh)
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("merge: record sha: %w", err)
		}
		task.Status.MergeCommitSHA = sha
		if err := r.clearDeadline(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setLifecycleState(ctx, task, "MainCI", "merged"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// 405 or body contains "conflict" -> re-implement with resolve instruction.
	var he *scm.HTTPError
	if errors.As(mergeErr, &he) {
		if he.Status == 405 || strings.Contains(strings.ToLower(he.Body), "conflict") {
			branch := task.Status.HeadBranch
			ctxMsg := fmt.Sprintf("Merge conflict on branch `%s`. Rebase the default branch into it, resolve conflicts, and push.", branch)
			if err := r.setImplementContext(ctx, task, ctxMsg); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.clearDeadline(ctx, task); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.maybeMarkHandoverResume(ctx, project, task); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.setLifecycleState(ctx, task, "Implement", "merge-conflict"); err != nil {
				return ctrl.Result{}, err
			}
			// MUST return nil error - not returning the error prevents controller-runtime backoff loop.
			return ctrl.Result{}, nil
		}
	}

	// Transient error: requeue or deadline park.
	if deadlinePassed(task) {
		msg := fmt.Sprintf("lifecycle: merge deadline reached (error: %v) for PR #%d; parking.", mergeErr, number)
		if err := r.parkWithComment(ctx, task, writer, token, "deadline", msg); err != nil {
			return ctrl.Result{}, err
		}
		if r.LifecycleMetrics != nil {
			r.LifecycleMetrics.RecordGiveup("deadline")
		}
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: pollRequeue}, nil
}

// handleMainCI polls the default-branch CI for the merge commit SHA,
// closes the issue on green, and re-enters Implement on failure.
func (r *TaskReconciler) handleMainCI(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	_, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("mainci: %w", err)
	}

	// Set DeadlineAt on first entry.
	if err := r.ensureDeadline(ctx, task, project); err != nil {
		return ctrl.Result{}, fmt.Errorf("mainci: ensure deadline: %w", err)
	}

	if deadlinePassed(task) {
		msg := "lifecycle: MainCI deadline reached; parking."
		if err := r.parkWithComment(ctx, task, writer, token, "deadline", msg); err != nil {
			return ctrl.Result{}, err
		}
		if r.LifecycleMetrics != nil {
			r.LifecycleMetrics.RecordGiveup("deadline")
		}
		return ctrl.Result{}, nil
	}

	// Get the CI status for the merge commit.
	sha := task.Status.MergeCommitSHA
	// Guard: an empty SHA means the Merge state wrote the SHA but the status update
	// was lost. Requeue to allow Merge to re-run and populate the SHA rather than
	// polling "" until the deadline parks the task.
	if sha == "" {
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}
	if r.ReaderFor == nil {
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}
	reader, rerr := r.ReaderFor(provider, token)
	if rerr != nil {
		return ctrl.Result{}, fmt.Errorf("mainci: reader: %w", rerr)
	}
	// Derive the commit-status target provider-aware: GitLab needs the full project
	// path (group/sub/project), GitHub needs owner/repo separately.
	var ciOwner, ciRepo string
	if provider == "gitlab" {
		ciOwner, err = scm.GitLabProjectPath(repo.Spec.URL)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("mainci: parse gitlab project path: %w", err)
		}
		ciRepo = ""
	} else {
		ciOwner, ciRepo, err = scm.OwnerRepo(repo.Spec.URL)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("mainci: parse repo url: %w", err)
		}
	}
	ciStatus, cerr := reader.GetCommitCIStatus(ctx, ciOwner, ciRepo, sha)
	if cerr != nil {
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}

	switch ciStatus {
	case "pending", "":
		return ctrl.Result{RequeueAfter: pollRequeue}, nil

	case "success":
		// Close the originating issue (idempotent: swallow 404 / already-closed).
		if task.Spec.Source != nil && task.Spec.Source.IssueRef != "" && !task.Spec.Source.IsPR {
			repoSlug, _, slugErr := repoSlugFromURL(repo.Spec.URL, provider)
			if slugErr == nil {
				closeErr := writer.CloseIssue(ctx, token, repoSlug, task.Spec.Source.Number, "")
				if closeErr != nil {
					var closeHE *scm.HTTPError
					if !errors.As(closeErr, &closeHE) || (closeHE.Status != 404 && closeHE.Status != 422) {
						return ctrl.Result{}, fmt.Errorf("mainci: close issue: %w", closeErr)
					}
					// 404/422: already closed; continue.
				}
			}
		}
		if err := r.setLifecycleState(ctx, task, "Done", "mainci-success"); err != nil {
			return ctrl.Result{}, err
		}
		if r.LifecycleMetrics != nil {
			elapsed := time.Since(task.CreationTimestamp.Time)
			r.LifecycleMetrics.ObserveLifecycle(elapsed.Seconds())
		}
		return ctrl.Result{}, nil

	case "failure":
		ctxMsg := fmt.Sprintf("Default-branch pipeline failed after merge (SHA %s). The previous MR is already merged; open a NEW MR with the fix and push.", sha)
		if err := r.setImplementContext(ctx, task, ctxMsg); err != nil {
			return ctrl.Result{}, err
		}
		// The merged PR is closed: clear MergeCommitSHA/PrURL/PRNumber so the next
		// Implement->MRCI->Merge cycle opens and merges a fresh MR instead of
		// short-circuiting on the stale merged PR (writeback AlreadyWritten guard)
		// and stale SHA (handleMerge already-merged guard).
		if err := r.clearMergedChangeState(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.clearDeadline(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.maybeMarkHandoverResume(ctx, project, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setLifecycleState(ctx, task, "Implement", "mainci-failure"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	default:
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}
}
