// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"strings"

	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// clarifyImplementAction is the clarify implement-outcome terminal action (the
// front-half -> back-half kind handoff). Unlike issueLifecycle - which
// transitions the SAME Task into the Implement lifecycle state - clarify is a
// discrete kind: it hands off to a separate implement stream via a managed-label
// swap and then TERMINATES (returns terminal=true so finishFrontHalf skips its
// shared resetAgentRun tail, which would otherwise resurrect the Task).
//
// Handoff contract (see CROSS-REPO-CONTRACT.md):
//   - remove tatara-brainstorming, add tatara-implementation (via setLifecycleLabel,
//     preserving the exactly-one-of-4-managed-labels invariant);
//   - carry the conversation warm-resume pointer (ConversationObjectKey/SessionID/
//     Handover) so the implement side resumes with clarify's context instead of a
//     cold rehydrate - the pointer already lives on this Task's status; this
//     ensures Status.Handover is populated as the durable handoff artifact;
//   - terminate the clarify Task (Done), tearing down its pod.
func (r *TaskReconciler) clarifyImplementAction(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, bool, error) {
	if err := r.handoffToImplement(ctx, project, task); err != nil {
		return ctrl.Result{}, false, err
	}
	if task.Status.IssueOutcome != nil && task.Status.IssueOutcome.Locked {
		if err := r.setImplementationLocked(ctx, task); err != nil {
			return ctrl.Result{}, false, err
		}
		if r.Metrics != nil {
			r.Metrics.ImplementationLocked()
		}
	}
	r.Metrics.IssueOutcome("implement")
	// Terminate the clarify Task: setDeployState(Done) tears down the wrapper
	// pod/service. A separate implement Task picks up the tatara-implementation
	// label to continue the stream.
	if err := r.setDeployState(ctx, task, "Done", "clarify-handoff"); err != nil {
		return ctrl.Result{}, false, err
	}
	return ctrl.Result{}, true, nil
}

// setImplementationLocked durably records Status.ImplementationLocked=true
// (item Request C/d): the clarify agent declared no open questions and every
// decision settled via issue_outcome{action=implement, locked=true}. Read
// later by systemic-group approval fan-out (filterSystemicGroupByApproval) to
// decide whether an approved lead's release extends to this sibling.
func (r *TaskReconciler) setImplementationLocked(ctx context.Context, task *tatarav1alpha1.Task) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		if fresh.Status.ImplementationLocked {
			task.Status.ImplementationLocked = true
			return nil
		}
		fresh.Status.ImplementationLocked = true
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		task.Status.ImplementationLocked = true
		return nil
	})
}

// handoffToImplement performs the clarify->implement label flip and ensures the
// warm-resume handoff artifact (Status.Handover) is populated. It is idempotent:
// setLifecycleLabel is idempotent, and a non-empty Handover is left untouched.
func (r *TaskReconciler) handoffToImplement(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) error {
	_, _, implementation, _ := lifecycleLabels(project.Spec.Scm)
	if err := r.setLifecycleLabel(ctx, project, task, implementation); err != nil {
		return err
	}
	return r.ensureHandoffArtifact(ctx, task)
}

// ensureHandoffArtifact populates Status.Handover with a durable warm-resume doc
// when the clarify pod did not submit one, so a downstream implement pod can
// resume from clarify's context (ConversationObjectKey/SessionID are already
// persisted on status by the wrapper). No-op when Handover is already set.
func (r *TaskReconciler) ensureHandoffArtifact(ctx context.Context, task *tatarav1alpha1.Task) error {
	if strings.TrimSpace(task.Status.Handover) != "" {
		return nil
	}
	handover := buildClarifyHandover(task)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		if strings.TrimSpace(fresh.Status.Handover) != "" {
			task.Status.Handover = fresh.Status.Handover
			return nil
		}
		fresh.Status.Handover = handover
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		task.Status.Handover = handover
		return nil
	})
}

// buildClarifyHandover assembles a compact warm-resume doc for the implement side
// from the clarify Task's durable status (the issue_outcome comment carrying the
// agreed plan, plus any result summary).
func buildClarifyHandover(task *tatarav1alpha1.Task) string {
	var parts []string
	if task.Status.IssueOutcome != nil && strings.TrimSpace(task.Status.IssueOutcome.Comment) != "" {
		parts = append(parts, "## Clarification outcome\n"+task.Status.IssueOutcome.Comment)
	}
	if strings.TrimSpace(task.Status.ResultSummary) != "" {
		parts = append(parts, "## Clarify run summary\n"+task.Status.ResultSummary)
	}
	if len(parts) == 0 {
		return "Resume from the clarify conversation on the linked issue thread."
	}
	return strings.Join(parts, "\n\n")
}
