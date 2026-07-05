package controller

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// writeBackSelfImprove reads Status.PROutcome and merges or closes the PR per policy.
// Never calls OpenChange.
func (r *TaskReconciler) writeBackSelfImprove(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	out := task.Status.PROutcome
	if out == nil || task.Spec.Source == nil {
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "NoOutcome", "selfImprove task without an outcome")
	}
	proj, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return ctrl.Result{}, err
	}
	number := task.Spec.Source.Number

	// Authorship gate (security boundary): hard-require the live PR/MR author to
	// be the project bot before merging OR closing, regardless of MergePolicy.
	// The agent must never act on a PR it does not own.
	if proj.Spec.Scm == nil || proj.Spec.Scm.BotLogin == "" {
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "AuthorshipWithheld", "project has no scm.botLogin")
	}
	st, perr := writer.GetPRState(ctx, repo.Spec.URL, token, number)
	r.recordSCM(provider, "get_pr_state", perr)
	if perr != nil {
		return ctrl.Result{}, fmt.Errorf("writeback selfImprove: authorship gate: %w", perr)
	}
	if st.Author != proj.Spec.Scm.BotLogin {
		l.Info("self-improve write-back withheld: PR not bot-authored",
			"action", "scm_authorship_withheld", "resource_id", task.Name, "author", st.Author)
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "AuthorshipWithheld",
			"PR/MR author is not the project bot login")
	}

	switch out.Action {
	case "close":
		// Short-circuit: if the PR is already closed (e.g. clearWritebackPending
		// failed on a prior reconcile), skip ClosePR to avoid re-posting the close
		// comment on an already-closed PR. st was fetched by the authorship gate above.
		if !st.Closed {
			err = writer.ClosePR(ctx, repo.Spec.URL, token, number, out.Reason)
			r.recordSCM(provider, "close", err)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("writeback selfImprove: %w", err)
			}
		}
		// Clear WritebackPending immediately after a successful state change so
		// a requeue triggered by a transient comment failure does not re-call
		// ClosePR (which would re-post the close comment on an already-closed PR).
		l.Info("self-improve outcome applied", "action", "scm_pr_outcome", "resource_id", task.Name, "outcome", out.Action)
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "PROutcomeApplied", "pr outcome applied: "+out.Action)
	case "merge":
		// push-CD (D5): agents must NOT self-merge - native auto-merge owns
		// merging. The bot-authored PR had auto-merge enabled at open time, so the
		// forge squash-merges it once required checks pass. pr_outcome=merge is no
		// longer a direct merge; honor it only as a policy confirmation and defer
		// the merge itself to the forge. pr_outcome is retained for close.
		if !r.mergeAllowed(&proj, st) {
			l.Info("self-improve merge withheld: policy not satisfied", "action", "scm_merge_withheld", "resource_id", task.Name)
			return ctrl.Result{}, r.clearWritebackPending(ctx, task, "MergeWithheld", "merge policy not satisfied")
		}
		l.Info("self-improve merge deferred to native auto-merge", "action", "scm_merge_deferred", "resource_id", task.Name)
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "PROutcomeApplied", "pr outcome applied: merge (deferred to auto-merge)")
	default:
		return ctrl.Result{}, fmt.Errorf("writeback selfImprove: unknown pr outcome %q", out.Action)
	}
}

// selfImproveBotAuthored reports whether the selfImprove PR/MR is actually
// authored by the project's bot login, by consulting the live PR state. It is
// the authoritative pre-spawn authorship gate: the agent must never be allowed
// to push to / merge / close a PR it does not own.
func (r *TaskReconciler) selfImproveBotAuthored(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (bool, error) {
	if proj.Spec.Scm == nil || proj.Spec.Scm.BotLogin == "" {
		return false, fmt.Errorf("authorship gate: project %q has no scm.botLogin", proj.Name)
	}
	if task.Spec.Source == nil {
		return false, fmt.Errorf("authorship gate: selfImprove task %q has no source", task.Name)
	}
	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); err != nil {
		return false, fmt.Errorf("authorship gate: get repository: %w", err)
	}
	provider := task.Spec.Source.Provider
	if provider == "" {
		provider = providerForRemote(ctx, repo.Spec.URL)
	}
	writer, err := r.SCMFor(provider)
	if err != nil {
		return false, fmt.Errorf("authorship gate: scm writer: %w", err)
	}
	token, err := r.scmToken(ctx, task.Namespace, proj.Spec.ScmSecretRef)
	if err != nil {
		return false, fmt.Errorf("authorship gate: scm token: %w", err)
	}
	st, err := writer.GetPRState(ctx, repo.Spec.URL, token, task.Spec.Source.Number)
	r.recordSCM(provider, "get_pr_state", err)
	if err != nil {
		return false, fmt.Errorf("authorship gate: get pr state: %w", err)
	}
	return st.Author == proj.Spec.Scm.BotLogin, nil
}

// mergeAllowed enforces MergePolicy. autoMergeOnGreenCI merges only when CI is
// present and green; CI absent falls back to afterApproval (trusts pr_outcome=merge
// as the agent's relay of an approving signal).
// st is the PR state already fetched by the authorship gate; passing it avoids
// a second GetPRState call on the hot merge path.
//
// afterApproval is an intentional trust-the-agent policy: the bot's pr_outcome=merge
// signal is treated as the agent relaying an approving signal (human review happened
// outside this gate). It does NOT consult live PR review state. If real approval
// gating is required, use autoMergeOnGreenCI combined with a branch protection rule
// requiring an approved review before CI can pass.
func (r *TaskReconciler) mergeAllowed(proj *tatarav1alpha1.Project, st scm.PRState) bool {
	policy := "afterApproval"
	if proj.Spec.Scm != nil && proj.Spec.Scm.MergePolicy != "" {
		policy = proj.Spec.Scm.MergePolicy
	}
	if policy == "autoMergeOnGreenCI" {
		if st.CIStatus == "success" {
			return true
		}
		if st.CIStatus != "" {
			return false // CI present but not green
		}
		// CI absent -> fall back to afterApproval below.
	}
	// afterApproval: trust pr_outcome=merge as the agent's relay of an approving signal.
	return true
}
