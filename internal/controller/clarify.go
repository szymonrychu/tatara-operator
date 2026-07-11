// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"fmt"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// reconcileClarify drives a clarify-kind Task: the conversational front-half
// decomposed out of the issueLifecycle state machine (absorbing the old Triage +
// Conversation states and the standalone triageIssue kind). It spawns a live pod
// that converses on the issue thread and, on an issue_outcome action=implement,
// hands off to implement via a managed-label swap (see clarifyImplementAction /
// handoffToImplement) and terminates - it NEVER enters the deploy-half
// (Implement/MRCI/Merge/MainCI/Deploying) itself. Discrete pods structurally
// enforce "the implementer never approves its own clarification".
//
// It shares the queue-drain preamble, the front-half agent-run driver
// (handleFrontHalf), and the idle Conversation handler (handleConversation) with
// the retained issueLifecycle bridge; only the implement-outcome terminal action
// differs (finishClarify vs finishTriage).
func (r *TaskReconciler) reconcileClarify(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var project tatarav1alpha1.Project
	if err := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &project); err != nil {
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, fmt.Errorf("clarify: get project: %w", err)
	}

	// Drain agent-queued comments + webhook-queued interjections to the live
	// session before dispatching. Shared with reconcileLifecycle.
	if handled, res, err := r.drainLifecycleQueues(ctx, task); handled {
		return res, err
	}

	// Memory gate: apply only when about to spawn a new agent run. Clarify only
	// ever spawns in the "" (init) and Triage states, so needsSpawn's Implement
	// case is never reached here.
	if needsSpawn(task.Status.DeployState, task.Status.Phase) {
		if !memoryStablyReady(&project, time.Now()) {
			l.Info("clarify task gated: project memory not stably ready",
				"action", "task_memory_gate", "resource_id", task.Name, "project", project.Name)
			return ctrl.Result{RequeueAfter: memGateRequeue}, nil
		}
	}

	dispatch := func(h func() (ctrl.Result, error)) (ctrl.Result, error) {
		res, err := h()
		if err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, err
		}
		r.Metrics.ReconcileResult("Task", "success")
		return res, nil
	}

	switch task.Status.DeployState {
	case "":
		// First reconcile: clarify always starts at Triage (it has no
		// Implement/MRCI entry) and stamps the brainstorming label so the
		// exactly-one-of-4-managed-labels invariant holds from the outset.
		if err := r.setDeployState(ctx, task, "Triage", "initial"); err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, err
		}
		if err := r.ensurePhaseLabel(ctx, &project, task, "brainstorming"); err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, err
		}
		r.Metrics.ReconcileResult("Task", "success")
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	case "Triage":
		return dispatch(func() (ctrl.Result, error) { return r.handleClarify(ctx, &project, task) })
	case "Conversation":
		return dispatch(func() (ctrl.Result, error) { return r.handleClarifyConversation(ctx, &project, task) })
	case "Done", "Stopped", "Parked":
		r.Metrics.ReconcileResult("Task", "success")
		return ctrl.Result{}, nil
	default:
		// A clarify Task must never reach a deploy-half state (Implement/MRCI/
		// Merge/MainCI/Deploying): finishClarify hands off to implement via label
		// swap and terminates instead of transitioning there. Surface any such
		// state as an error rather than silently driving the wrong machinery.
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, fmt.Errorf("clarify: unexpected lifecycleState %q for task %s", task.Status.DeployState, task.Name)
	}
}

// clarifyDiscussTimeoutMinutes is the fixed wall-clock budget a clarify pod waits
// in the live discuss (Conversation) state for a human answer before the operator
// kills it. Unlike the issueLifecycle idle window it is NOT project-configurable: a
// clarify pod is an expensive live opus session, so the 1h cap is a hard platform
// invariant (plan Phase 7 / risk "Live clarify pod cost"). enterConversation stamps
// this window for clarify tasks; handleClarifyConversation enforces the kill.
const clarifyDiscussTimeoutMinutes = 60

// handleClarifyConversation is the clarify-specific live-discuss handler, replacing
// the shared handleConversation for clarify tasks. It differs in two load-bearing
// ways:
//
//   - It enforces a FIXED 1h wall-clock kill: on the deadline it tears the LIVE pod
//     down (deleteWrapper) and stops the task with reason "clarify-timeout", rather
//     than the shared handler's project-configurable idle-Stopped (which leaves pod
//     teardown to the reaper). "Waiting" vs "stalled" is distinguished by pending
//     interjections: a human answer that landed just before the deadline extends the
//     window (waiting); an empty queue at the deadline is a genuine stall (killed).
//   - It deliberately does NOT run observeProposalLabelReadback. That readback drives
//     an approved proposal into the "Implement" lifecycle state, which is a deploy-half
//     state a clarify task must never reach (reconcileClarify's default arm errors on
//     it). Clarify hands off to implement via the agent's issue_outcome + label swap
//     (handoffToImplement), never via a label readback, so the readback is both
//     unnecessary and unsafe here.
//
// Interjections are drained to the live session by drainLifecycleQueues at the top of
// reconcileClarify before this runs, so a pending queue here means "a human just spoke,
// re-drive rather than kill".
func (r *TaskReconciler) handleClarifyConversation(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	// Safety net: entered Conversation without a deadline (e.g. a reactivation path
	// that bypassed enterConversation). Stamp the fixed clarify window once and requeue.
	if task.Status.DeadlineAt == nil {
		if err := r.ensureDeadlineMinutes(ctx, task, clarifyDiscussTimeoutMinutes); err != nil {
			return ctrl.Result{}, fmt.Errorf("clarify conversation: set deadline: %w", err)
		}
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}

	if deadlinePassed(task) {
		// Waiting: a human comment arrived just before the deadline. Extend the window
		// for another turn rather than killing; the drain re-drives the pod next reconcile.
		if len(task.Status.PendingInterjections) > 0 {
			if err := r.setDeadlineMinutes(ctx, task, clarifyDiscussTimeoutMinutes); err != nil {
				return ctrl.Result{}, err
			}
			l.Info("clarify: discuss deadline passed but interjections pending; extending",
				"action", "clarify_extend_pending", "resource_id", task.Name,
				"pending", len(task.Status.PendingInterjections))
			return ctrl.Result{RequeueAfter: pollRequeue}, nil
		}
		// Stalled: no human answer within the 1h wall-clock. Kill the live pod and park
		// the task with a durable reason (a later human comment re-creates the
		// deterministic clarify task). Parked (not Stopped) is used so the
		// "clarify-timeout" reason persists on Status.ParkReason for observability;
		// setDeployState also tears the wrapper down on entering a terminal state,
		// but the explicit deleteWrapper makes the kill legible and independent of that.
		if err := r.deleteWrapper(ctx, task); err != nil {
			l.Error(err, "clarify: kill live pod on timeout (non-fatal)", "resource_id", task.Name)
		}
		// Liveness finding #2: park with an issue comment so the reporter can tell
		// "timed out / dead" from "still thinking", not a silent Parked state.
		msg := "tatara: this thread timed out after 1h with no reply, so I paused work on it. " +
			"Comment here to resume - I'll pick the conversation back up."
		if _, _, writer, token, _, scmErr := r.parkSCMContext(ctx, task); scmErr == nil {
			if err := r.parkWithComment(ctx, task, writer, token, "clarify-timeout", msg); err != nil {
				return ctrl.Result{}, err
			}
		} else if err := r.setDeployState(ctx, task, "Parked", "clarify-timeout"); err != nil {
			return ctrl.Result{}, err
		}
		if r.LifecycleMetrics != nil {
			r.LifecycleMetrics.RecordGiveup("clarify-timeout")
		}
		l.Info("clarify: discuss window elapsed with no human answer; killed pod and parked",
			"action", "clarify_timeout", "resource_id", task.Name)
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: pollRequeue}, nil
}

// handleClarify drives the clarify agent-run: it reuses the shared front-half
// driver, delegating the completed-run outcome to finishClarify (which flips the
// handoff label + terminates on implement, instead of transitioning into the
// Implement lifecycle state).
func (r *TaskReconciler) handleClarify(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	return r.handleFrontHalf(ctx, project, task, r.finishClarify)
}

// finishClarify consumes Status.IssueOutcome after a completed clarify agent run.
// close/discuss/guard/default arms are identical to finishTriage; the implement
// arm delegates to clarifyImplementAction (the label-swap handoff + terminate)
// rather than transitioning the Task into the Implement lifecycle state.
func (r *TaskReconciler) finishClarify(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	return r.finishFrontHalf(ctx, project, task, r.clarifyImplementAction)
}
