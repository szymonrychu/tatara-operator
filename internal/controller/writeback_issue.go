package controller

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// writeBackIssue applies a triageIssue Task's IssueOutcome: close calls
// CloseIssue with the agent's comment; implement records the marker only (the
// PR opened during the agent run is the artifact, re-entering the author-gated
// path). Never calls OpenChange.
func (r *TaskReconciler) writeBackIssue(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	out := task.Status.IssueOutcome
	if out == nil || task.Spec.Source == nil {
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "NoOutcome", "triageIssue task without an outcome")
	}
	// Safety gate: triageIssue must never close a PR.
	if task.Spec.Source.IsPR {
		l.Error(fmt.Errorf("triageIssue source is a PR"), "writeback issue: refusing to close a PR",
			"action", "scm_issue_refused_pr", "resource_id", task.Name, "number", task.Spec.Source.Number)
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "IssueRefusedPR", "triageIssue source is a PR; CloseIssue withheld")
	}
	// Re-assert kind (defence-in-depth).
	if task.Spec.Kind != "triageIssue" {
		l.Error(fmt.Errorf("unexpected kind %q in writeBackIssue", task.Spec.Kind), "writeback issue: wrong kind",
			"action", "scm_issue_wrong_kind", "resource_id", task.Name)
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "IssueWrongKind", "writeBackIssue called for non-triageIssue task")
	}
	if out.Action == "implement" {
		r.Metrics.IssueOutcome("implement")
		l.Info("issue outcome implement: opening PR from agent branch", "action", "scm_issue_outcome", "resource_id", task.Name, "outcome", "implement")
		// Route through the shared OpenChange path so the agent's pushed branch
		// becomes a tatara-authored PR re-entering the author-gated review/merge path.
		return r.writeBackOpenChange(ctx, task)
	}
	// close
	// Invariant: never close an issue that has an unmerged code change. Only the
	// merged-and-green lifecycle (handleMainCI) may close such an issue.
	if hasUnmergedChange(task) {
		l.Info("issue close withheld: triageIssue has an unmerged change",
			"action", "scm_close_withheld", "resource_id", task.Name, "number", task.Spec.Source.Number,
			"pr_url", task.Status.PrURL, "head_branch", task.Status.HeadBranch)
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "CloseWithheldUnmerged", "issue has an unmerged change; close withheld")
	}
	_, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return ctrl.Result{}, err
	}
	repoSlug, _, perr := repoSlugFromURL(repo.Spec.URL, provider)
	if perr != nil {
		return ctrl.Result{}, perr
	}
	if cerr := writer.CloseIssue(ctx, token, repoSlug, task.Spec.Source.Number, out.Comment); cerr != nil {
		r.recordSCM(provider, "close_issue", cerr)
		return ctrl.Result{}, fmt.Errorf("writeback issue close: %w", cerr)
	}
	r.recordSCM(provider, "close_issue", nil)
	r.Metrics.IssueOutcome("close")
	l.Info("issue closed", "action", "scm_issue_outcome", "resource_id", task.Name, "outcome", "close", "number", task.Spec.Source.Number)
	return ctrl.Result{}, r.clearWritebackPending(ctx, task, "IssueClosed", "issue closed with comment")
}
