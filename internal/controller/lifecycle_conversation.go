// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// observeProposalLabelReadback checks the task's source issue for a human-applied
// approved or declined label and reflects the result onto the role:proposed ledger
// entry. This is the P4 "read label changes back" path: when a human relabels a
// brainstorm proposal issue, the operator writes it onto the ledger entry so the
// backlog cap and any tooling that reads the ledger see the updated state.
// Returns the observed state ("approved"|"declined"|"") and any error.
// Non-fatal errors are logged by the caller; nil reader or no source is a no-op.
func (r *TaskReconciler) observeProposalLabelReadback(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (string, error) {
	if r.ReaderFor == nil || task.Spec.Source == nil || task.Spec.Source.IssueRef == "" || task.Spec.Source.IsPR {
		return "", nil
	}
	// Only tasks with an UNDECIDED role:proposed entry (still WIProposed) are
	// subject to readback. Once a proposal is approved/declined the decision is
	// terminal, so skipping the SCM ListOpenIssues for already-decided proposals
	// avoids a redundant repo-wide issue list on every idle-conversation reconcile.
	hasUndecidedProposed := false
	for _, wi := range task.Status.WorkItems {
		if wi.Role == tatarav1alpha1.RoleProposed && wi.State == tatarav1alpha1.WIProposed {
			hasUndecidedProposed = true
			break
		}
	}
	if !hasUndecidedProposed {
		return "", nil
	}

	provider := task.Spec.Source.Provider
	if provider == "" && project.Spec.Scm != nil {
		provider = project.Spec.Scm.Provider
	}
	token, err := r.scmToken(ctx, task.Namespace, project.Spec.ScmSecretRef)
	if err != nil {
		return "", fmt.Errorf("observe label: token: %w", err)
	}
	reader, err := r.ReaderFor(provider, token)
	if err != nil {
		return "", fmt.Errorf("observe label: reader: %w", err)
	}
	_, repo, _, _, _, sctxErr := r.scmContext(ctx, task)
	if sctxErr != nil {
		return "", fmt.Errorf("observe label: scm context: %w", sctxErr)
	}
	owner, name, oerr := scm.OwnerRepo(repo.Spec.URL)
	if oerr != nil {
		return "", fmt.Errorf("observe label: repo url: %w", oerr)
	}
	issues, lerr := reader.ListOpenIssues(ctx, owner, name)
	if lerr != nil {
		return "", fmt.Errorf("observe label: list: %w", lerr)
	}
	issueRef := task.Spec.Source.IssueRef
	_, approvedLabel, _, declinedLabel := lifecycleLabels(project.Spec.Scm)
	for _, iss := range issues {
		if fmt.Sprintf("%s#%d", iss.Repo, iss.Number) != issueRef {
			continue
		}
		for _, lb := range iss.Labels {
			switch lb {
			case approvedLabel:
				if uErr := r.upsertProposedEntryState(ctx, task, issueRef, tatarav1alpha1.WIApproved); uErr != nil {
					return "approved", fmt.Errorf("observe label: upsert approved: %w", uErr)
				}
				return "approved", nil
			case declinedLabel:
				if uErr := r.upsertProposedEntryState(ctx, task, issueRef, tatarav1alpha1.WIDeclined); uErr != nil {
					return "declined", fmt.Errorf("observe label: upsert declined: %w", uErr)
				}
				return "declined", nil
			}
		}
		break // issue found, no approved/declined label
	}
	return "", nil
}

// handleConversation manages the idle wait state. No pod is ever spawned here.
// If the deadline has passed the task transitions to Stopped (idle-stop, resumable).
// If DeadlineAt is nil (safety net for tasks whose deadline was never set), set it
// once using project.Spec.Scm.ConversationIdleMinutes (same logic as enterConversation)
// and requeue so the normal deadline path runs on the next reconcile.
// Otherwise it requeues until the deadline.
func (r *TaskReconciler) handleConversation(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	// P4 label readback: if the source issue has been relabeled by a human
	// (tatara-approved or tatara-declined) since the last reconcile, reflect the
	// state onto the role:proposed ledger entry AND drive the decision:
	//   approved -> transition to Implement (mirrors the triage approve path),
	//   declined -> park with "human-declined".
	// A readback ERROR is best-effort and does not block the idle path. A clean
	// approved/declined observation supersedes the idle deadline path (return).
	state, rbErr := r.observeProposalLabelReadback(ctx, project, task)
	if rbErr != nil {
		l.Info("conversation: proposal label readback failed (non-fatal)",
			"action", "conversation_label_readback", "resource_id", task.Name, "err", rbErr.Error())
	} else {
		switch state {
		case tatarav1alpha1.WIApproved:
			if err := r.setLifecycleState(ctx, task, "Implement", "human-approved"); err != nil {
				return ctrl.Result{}, fmt.Errorf("conversation: approve readback to implement: %w", err)
			}
			l.Info("conversation: human-approved proposal; driving implementation",
				"action", "conversation_label_approved", "resource_id", task.Name)
			return ctrl.Result{}, nil
		case tatarav1alpha1.WIDeclined:
			if err := r.setLifecycleState(ctx, task, "Parked", "human-declined"); err != nil {
				return ctrl.Result{}, fmt.Errorf("conversation: decline readback to park: %w", err)
			}
			l.Info("conversation: human-declined proposal; parking",
				"action", "conversation_label_declined", "resource_id", task.Name)
			return ctrl.Result{}, nil
		}
	}

	if task.Status.DeadlineAt == nil {
		// Safety net: set deadline once rather than returning false from
		// deadlinePassed forever and requeuing without bound. Delegates to the
		// shared ensureDeadlineMinutes so this logic cannot drift from the primary
		// path in ensureDeadline (finding 15).
		idleMinutes := conversationDefaultIdleMinutes
		if project.Spec.Scm != nil && project.Spec.Scm.ConversationIdleMinutes > 0 {
			idleMinutes = project.Spec.Scm.ConversationIdleMinutes
		}
		if err := r.ensureDeadlineMinutes(ctx, task, idleMinutes); err != nil {
			return ctrl.Result{}, fmt.Errorf("conversation: set nil deadline: %w", err)
		}
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}
	if deadlinePassed(task) {
		// Before stopping, check for pending interjections (finding 10): a human
		// comment that arrived just before the deadline should not be dropped. If
		// PendingInterjections is non-empty, refresh the deadline and re-drive rather
		// than stopping. The interjection drain at the top of reconcileLifecycle will
		// route them to the session (or drop as stale) on the next reconcile.
		if len(task.Status.PendingInterjections) > 0 {
			idleMinutes := conversationDefaultIdleMinutes
			if project.Spec.Scm != nil && project.Spec.Scm.ConversationIdleMinutes > 0 {
				idleMinutes = project.Spec.Scm.ConversationIdleMinutes
			}
			// Reset deadline so the conversation continues for another idle window.
			// setDeadlineMinutes replaces the two-write clearDeadline+ensureDeadlineMinutes
			// pattern with a single RetryOnConflict (finding 3/r3).
			if err := r.setDeadlineMinutes(ctx, task, idleMinutes); err != nil {
				return ctrl.Result{}, err
			}
			log.FromContext(ctx).Info("conversation: deadline passed but interjections pending; extending",
				"action", "conversation_extend_pending", "resource_id", task.Name,
				"pending", len(task.Status.PendingInterjections))
			return ctrl.Result{RequeueAfter: pollRequeue}, nil
		}
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
