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
	_, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return ctrl.Result{}, err
	}
	number := task.Spec.Source.Number
	var verbSent bool
	switch v.Decision {
	case "approve":
		err = writer.Approve(ctx, repo.Spec.URL, token, number, v.Body)
		r.recordSCM(provider, "approve", err)
		verbSent = err == nil
		if err == nil {
			r.recordReviewQuality(task, "approved", len(v.Suggestions))
		}
	case "request_changes":
		err = writer.RequestChanges(ctx, repo.Spec.URL, token, number, v.Body)
		r.recordSCM(provider, "request_changes", err)
		verbSent = err == nil
		if err == nil && len(v.Suggestions) > 0 {
			serr := writer.Suggest(ctx, repo.Spec.URL, token, number, toSCMSuggestions(v.Suggestions))
			r.recordSCM(provider, "suggest", serr)
		}
		if err == nil {
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

func toSCMSuggestions(in []tatarav1alpha1.Suggestion) []scm.Suggestion {
	out := make([]scm.Suggestion, 0, len(in))
	for _, s := range in {
		out = append(out, scm.Suggestion{Path: s.Path, Line: s.Line, Body: s.Body})
	}
	return out
}
