// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// mergeAllowed enforces MergePolicy. autoMergeOnGreenCI waits for green CI;
// absent CI falls back to afterApproval (trust pr_outcome=merge as the agent's
// relay of an approving signal). st is the PR state fetched by the caller.
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

// handleMerge attempts to merge the PR. Handles 405-conflict as a re-implement
// signal (MUST NOT return the error to avoid controller-runtime backoff loop).
func (r *TaskReconciler) handleMerge(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	_, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("merge: %w", err)
	}

	number, _ := lifecyclePR(task)

	// Idempotency: if the PR is already merged (MergeCommitSHA already set on the
	// task), skip straight to MainCI without calling Merge again. This handles the
	// case where setDeployState("MainCI") failed after a successful Merge on a
	// prior reconcile, which would otherwise re-merge -> 405 -> bogus conflict path.
	if task.Status.MergeCommitSHA != "" {
		// PR was merged in a prior reconcile; advance to MainCI directly.
		if err := r.clearDeadline(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setDeployState(ctx, task, "MainCI", "already-merged"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Set DeadlineAt on first entry.
	if err := r.ensureDeadline(ctx, task, project); err != nil {
		return ctrl.Result{}, fmt.Errorf("merge: ensure deadline: %w", err)
	}

	// Check mergeAllowed policy. Fetch PR state once here; reuse HeadSHA below to
	// avoid a second round-trip (findings 3 & 4).
	prSt, pserr := writer.GetPRState(ctx, repo.Spec.URL, token, number)
	r.recordSCM(provider, "get_pr_state", pserr)
	if pserr != nil {
		return ctrl.Result{}, fmt.Errorf("merge: get pr state: %w", pserr)
	}
	if !r.mergeAllowed(project, prSt) {
		if deadlinePassed(task) {
			msg := fmt.Sprintf("lifecycle: merge deadline reached for PR #%d; parking.", number)
			return ctrl.Result{}, r.parkOnDeadline(ctx, task, writer, token, msg)
		}
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}

	// Reuse the already-fetched HeadSHA to detect a later MRCI re-proposal of the
	// same already-merged commits, without a second GetPRState round-trip (finding 3).
	mergedHead := prSt.HeadSHA

	// push-CD gate (issue #229): the lifecycle Merge phase merges bot PRs directly
	// with the bot token, bypassing the writeback path's semver-label stamping
	// (applySemverAutoMerge). A directly-merged PR carrying no semver:<level> label
	// makes push-CD's release tag step fail closed ("no semver label; refusing to
	// tag"), so the change never cuts a tag or deploys until the next labeled merge.
	// Stamp the label from the declared significance (else patch) before the merge
	// egress so every lifecycle-merged change rides the cascade.
	r.ensureSemverLabelBeforeMerge(ctx, project, repo, writer, token, provider, number, task.Status.ChangeSummary)

	// Attempt merge via the shared egress (the sole writer.Merge call site;
	// superviseApprovedPRs is the other caller). The issueLifecycle drain keeps its
	// existing gate (mergeAllowed) here; the review-approved path gates on green +
	// tatara-approved in superviseApprovedPRs.
	sha, mergeErr := mergePRSquash(ctx, writer, repo.Spec.URL, token, number)
	r.recordSCM(provider, "merge", mergeErr)
	if mergeErr == nil {
		// Success: record SHA and advance.
		// Derive repo slug once outside the closure for the ledger upsert.
		mergeRepoSlug, _, _ := repoSlugFromURL(repo.Spec.URL, provider)
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh := &tatarav1alpha1.Task{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
				return err
			}
			fresh.Status.MergeCommitSHA = sha
			fresh.Status.MergedHeadSHA = mergedHead
			// Project the merge event onto the ledger: flip the openedPR entry to
			// state:merged so the backstop and dedup helpers see live state.
			if mergeRepoSlug != "" && number > 0 {
				UpsertWorkItem(fresh, tatarav1alpha1.WorkItemRef{
					Provider: provider,
					Repo:     mergeRepoSlug,
					Number:   number,
					Kind:     tatarav1alpha1.WorkItemPR,
					Role:     tatarav1alpha1.RoleOpenedPR,
					State:    tatarav1alpha1.WIMerged,
					HeadSHA:  mergedHead,
				})
			}
			return r.Status().Update(ctx, fresh)
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("merge: record sha: %w", err)
		}
		task.Status.MergeCommitSHA = sha
		task.Status.MergedHeadSHA = mergedHead
		if err := r.clearDeadline(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setDeployState(ctx, task, "MainCI", "merged"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// ErrMergeConflict -> re-implement with a merge-not-rebase, resolve-or-close
	// mandate. Rebase would need a force-push (hard-denied in the wrapper), so we
	// instruct a merge of the default branch. The message seeds the Implement
	// turn (via ImplementContext, folded into Status.Handover by
	// maybeMarkHandoverResume) with the PR ref, branch, issue scope, and the
	// binary terminal mandate.
	if errors.Is(mergeErr, scm.ErrMergeConflict) {
		branch := task.Status.HeadBranch
		defaultBranch := repo.Spec.DefaultBranch
		ctxMsg := fmt.Sprintf(
			"Merge conflict on PR #%d (branch `%s`). Reach ONE terminal outcome this turn - never park.\n\n"+
				"RESOLVE: `git fetch origin && git merge origin/%s` on the branch. Use git merge only (force-push is denied in this pod). "+
				"Resolve each conflict guided by the issue intent below, commit, and `git push` (no --force). The lifecycle re-attempts the merge once you finish.\n\n"+
				"CLOSE-AS-SUPERSEDED: after merging origin/%s in, if `git diff origin/%s...HEAD` is empty (all changes already landed), the PR is superseded - call `pr_outcome` action `close` with a superseded reason.\n\n"+
				"CLOSE-AS-OBSOLETE: if the PR is unwanted or the conflict is genuinely unresolvable, still call `pr_outcome` action `close` with the reason.\n\n"+
				"Originating issue scope:\n%s",
			number, branch, defaultBranch, defaultBranch, defaultBranch, task.Spec.Goal)
		if err := r.setImplementContext(ctx, task, ctxMsg); err != nil {
			return ctrl.Result{}, fmt.Errorf("merge: set implement context: %w", err)
		}
		if err := r.clearDeadline(ctx, task); err != nil {
			return ctrl.Result{}, fmt.Errorf("merge: clear deadline: %w", err)
		}
		if err := r.maybeMarkHandoverResume(ctx, project, task); err != nil {
			return ctrl.Result{}, fmt.Errorf("merge: mark handover resume: %w", err)
		}
		if err := r.setDeployState(ctx, task, "Implement", "merge-conflict"); err != nil {
			return ctrl.Result{}, fmt.Errorf("merge: set implement state: %w", err)
		}
		log.FromContext(ctx).Info("merge: conflict escalated to merge-not-rebase resolve-or-close",
			"action", "lifecycle_merge_conflict_selfheal", "resource_id", task.Name, "pr", number, "branch", branch)
		// MUST return nil error to avoid controller-runtime backoff loop.
		return ctrl.Result{}, nil
	}

	// Transient error: requeue or deadline park.
	if deadlinePassed(task) {
		msg := fmt.Sprintf("lifecycle: merge deadline reached (error: %v) for PR #%d; parking.", mergeErr, number)
		return ctrl.Result{}, r.parkOnDeadline(ctx, task, writer, token, msg)
	}
	return ctrl.Result{RequeueAfter: pollRequeue}, nil
}

// ensureSemverLabelBeforeMerge stamps a semver:<level> label on the bot PR the
// lifecycle Merge phase is about to merge, closing the push-CD gap where a
// directly-merged unlabeled PR leaves the release tag step failing closed (issue
// #229). The level comes from the declared change significance when present, else
// defaults to patch. It is idempotent and best-effort: an existing semver:* label
// (writeback- or human-applied) is respected and never double-stamped, and every
// SCM error is logged non-fatally so a labeling hiccup never blocks the merge. The
// cd-release cascade is GitHub-only (see applySemverAutoMerge), so this no-ops on
// other providers.
func (r *TaskReconciler) ensureSemverLabelBeforeMerge(ctx context.Context, proj *tatarav1alpha1.Project, repo tatarav1alpha1.Repository, writer scm.SCMWriter, token, provider string, number int, cs *tatarav1alpha1.ChangeSummary) {
	if provider != "github" || number <= 0 {
		return
	}
	l := log.FromContext(ctx)
	slug, _, serr := repoSlugFromURL(repo.Spec.URL, provider)
	if serr != nil || slug == "" {
		return
	}
	// Respect an existing semver:* label: reading the PR's current labels lets us
	// skip when one is already present (writeback stamps it at PR-open time, and a
	// human may set a specific level), so we never add a second, conflicting label
	// the tag step would have to disambiguate. Fail-open (proceed to stamp) when no
	// reader is wired or the read fails - an unlabeled merge is the failure we are
	// closing, so erring toward stamping is correct.
	if r.ReaderFor != nil {
		if reader, rerr := r.ReaderFor(provider, token); rerr == nil {
			if owner, name, oerr := scm.OwnerRepo(repo.Spec.URL); oerr == nil {
				if prs, lerr := reader.ListOpenPRs(ctx, owner, name); lerr == nil {
					for _, pr := range prs {
						if pr.Number != number {
							continue
						}
						for _, lb := range pr.Labels {
							if strings.HasPrefix(lb, "semver:") {
								return
							}
						}
						break
					}
				}
			}
		}
	}

	significance := "patch"
	if cs != nil && cs.Significance != "" {
		significance = cs.Significance
	}
	label := semverLabel(significance)
	color := managedLabelColors(proj.Spec.Scm)[label]
	r.ensureSemverLabelColor(ctx, writer, repo.Spec.URL, token, provider, label, color,
		"merge: ensure semver label (non-fatal)",
		"action", "scm_merge_semver_label", "resource_id", repo.Name, "label", label)
	prRef := fmt.Sprintf("%s#%d", slug, number)
	if aerr := r.addSemverLabelToPR(ctx, writer, token, provider, prRef, label,
		"merge: add semver label (non-fatal)",
		"action", "scm_merge_semver_label", "resource_id", repo.Name, "pr_ref", prRef, "label", label); aerr != nil {
		return
	}
	l.Info("merge: semver label stamped before lifecycle merge",
		"action", "scm_merge_semver_label", "resource_id", repo.Name,
		"pr_ref", prRef, "significance", significance)
}
