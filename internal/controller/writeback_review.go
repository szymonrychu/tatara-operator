package controller

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// recordReviewQuality records the G4 quality-proxy signal for a review
// verdict that was just written back successfully: the verdict itself
// (operator_review_outcome_total) and its finding count
// (operator_review_findings_total), keyed by the model that ran the review.
// Finding count is len(Suggestions) - the only per-review count field on
// ReviewVerdict; AddReviewFindings is a no-op when it is 0.
func (r *TaskReconciler) recordReviewQuality(task *tatarav1alpha1.Task, verdict string, findingCount int) {
	if r.Metrics == nil {
		return
	}
	project, repo, _, _, model := taskTokenLabels(task)
	r.Metrics.RecordReviewOutcome(project, repo, model, verdict)
	r.Metrics.AddReviewFindings(project, repo, model, findingCount)
}

// writeBackReview reads Status.ReviewVerdict and posts exactly one verb set.
// Never calls OpenChange.
func (r *TaskReconciler) writeBackReview(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	v := task.Status.ReviewVerdict
	if v == nil || task.Spec.Source == nil {
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "NoVerdict", "review task without a verdict")
	}
	proj, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return ctrl.Result{}, err
	}
	_, approvedLabel, implementationLabel, _ := lifecycleLabels(proj.Spec.Scm)
	number := task.Spec.Source.Number
	var verbSent bool

	// Phase 6: an approve verdict is withheld when the PR is not mergeable (a
	// conflict or a mergeable-blocked state such as failing required checks).
	// Instead of approving, the stream routes back to implement via the managed
	// tatara-implementation label - the same terminal effect as request_changes.
	// Fail-open on a merge-state read error: a transient read must not block a
	// genuine approval (the deploy supervisor re-checks mergeability at merge time).
	decision := v.Decision
	if decision == "approve" {
		if ms, mserr := writer.GetMergeState(ctx, repo.Spec.URL, token, number); mserr == nil &&
			(ms == scm.MergeStateDirty || ms == scm.MergeStateBlocked) {
			l.Info("review: approval withheld; PR unmergeable, routing back to implement",
				"action", "scm_review_unmergeable", "resource_id", task.Name, "pr", number, "merge_state", string(ms))
			decision = "unmergeable"
		}
	}

	switch decision {
	case "approve":
		// Approve applies the native PR approval AND the tatara-approved managed
		// label. It NEVER merges: the deploy supervisor is the sole merge caller,
		// gated on green + tatara-approved (CROSS-REPO-CONTRACT handoff transitions).
		err = writer.Approve(ctx, repo.Spec.URL, token, number, v.Body)
		r.recordSCM(provider, "approve", err)
		verbSent = err == nil
		if err == nil {
			r.applyReviewLabel(ctx, &proj, task, approvedLabel)
			r.recordReviewQuality(task, "approved", len(v.Suggestions))
		}
	case "unmergeable":
		// No PR-review verb: withhold approval and re-add tatara-implementation to
		// route the stream back to implement. This is the only egress action here,
		// so its error propagates for a requeue (the label add is idempotent).
		if lerr := r.setLifecycleLabel(ctx, &proj, task, implementationLabel); lerr != nil {
			return ctrl.Result{}, fmt.Errorf("writeback review unmergeable relabel: %w", lerr)
		}
		r.recordReviewQuality(task, "unmergeable", len(v.Suggestions))
	case "request_changes":
		err = writer.RequestChanges(ctx, repo.Spec.URL, token, number, v.Body)
		r.recordSCM(provider, "request_changes", err)
		verbSent = err == nil
		if err == nil && len(v.Suggestions) > 0 {
			serr := writer.Suggest(ctx, repo.Spec.URL, token, number, toSCMSuggestions(v.Suggestions))
			r.recordSCM(provider, "suggest", serr)
		}
		if err == nil {
			// request_changes re-adds tatara-implementation (routes back to implement).
			r.applyReviewLabel(ctx, &proj, task, implementationLabel)
			r.recordReviewQuality(task, "changes_requested", len(v.Suggestions))
		}
	case "comment":
		// Build the comment target from repo URL + PR number (same addressing as
		// approve/request_changes). IssueRef may be the originating issue rather
		// than the PR, or empty, so derive a consistent ref here.
		slug, _, serr := repoSlugFromURL(repo.Spec.URL, provider)
		if serr != nil {
			return ctrl.Result{}, fmt.Errorf("writeback review comment: derive slug: %w", serr)
		}
		// This is always an MR/PR review. GitLab MRs use the '!' separator and a
		// distinct notes endpoint; a '#' ref routes to /issues/{iid}/notes which
		// 404s (issues and MRs have separate iid spaces). GitHub shares the issue
		// endpoint for PRs, so it stays on '#'.
		sep := "#"
		if provider == "gitlab" {
			sep = "!"
		}
		prRef := fmt.Sprintf("%s%s%d", slug, sep, number)
		err = writer.Comment(ctx, token, prRef, v.Body)
		r.recordSCM(provider, "comment", err)
		verbSent = err == nil
	default:
		err = fmt.Errorf("unknown review decision %q", v.Decision)
	}
	if err != nil {
		// If the verb reached the server (verbSent) but a later persistence step
		// fails, clear WritebackPending before returning so a requeue does not
		// re-post the same non-idempotent verb (duplicate approve/request_changes).
		if verbSent {
			// Propagate the clear error: if the clear fails the reconciler will
			// requeue; the verbSent guard above means on requeue we detect the
			// verb already landed and will not re-post it. Without propagating,
			// WritebackPending stays True and the verb is re-sent on every reconcile.
			if cerr := r.clearWritebackPending(ctx, task, "Reviewed", "review verdict posted: "+v.Decision); cerr != nil {
				return ctrl.Result{}, cerr
			}
		}
		return ctrl.Result{}, fmt.Errorf("writeback review: %w", err)
	}
	l.Info("review verdict posted", "action", "scm_review", "resource_id", task.Name, "decision", v.Decision)
	return ctrl.Result{}, r.clearWritebackPending(ctx, task, "Reviewed", "review verdict posted: "+v.Decision)
}

// applyReviewLabel sets the given managed phase label on the review Task's PR via
// setLifecycleLabel (preserving the exactly-one-of-4-managed-labels invariant). It
// is best-effort: the native review verb (Approve/RequestChanges) has already
// landed, so a label failure is logged non-fatally rather than re-sending the
// non-idempotent verb on a requeue. The unmergeable path, which sends NO verb,
// calls setLifecycleLabel directly so its error can propagate.
func (r *TaskReconciler) applyReviewLabel(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, label string) {
	if lerr := r.setLifecycleLabel(ctx, proj, task, label); lerr != nil {
		log.FromContext(ctx).Error(lerr, "review: apply managed label (non-fatal)",
			"action", "scm_review_label", "resource_id", task.Name, "label", label)
	}
}

func toSCMSuggestions(in []tatarav1alpha1.Suggestion) []scm.Suggestion {
	out := make([]scm.Suggestion, 0, len(in))
	for _, s := range in {
		out = append(out, scm.Suggestion{Path: s.Path, Line: s.Line, Body: s.Body})
	}
	return out
}
