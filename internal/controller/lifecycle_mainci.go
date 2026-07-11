// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

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
		return ctrl.Result{}, r.parkOnDeadline(ctx, task, writer, token, "lifecycle: MainCI deadline reached; parking.")
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
	r.recordSCM(provider, "get_commit_ci_status", cerr)
	if cerr != nil {
		// Log at Error: a persistent CI-status read failure can silently burn the
		// MainCI deadline with zero observability. The requeue keeps it non-fatal
		// but the error level surfaces it for alerting (finding 12; the prior comment
		// said WARN but used l.Info - corrected to l.Error).
		log.FromContext(ctx).Error(cerr, "mainci: GetCommitCIStatus failed; requeueing",
			"action", "scm_ci_status_error",
			"resource_id", task.Name,
			"sha", sha,
		)
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}

	switch ciStatus {
	case "pending", "":
		return ctrl.Result{RequeueAfter: pollRequeue}, nil

	case "success":
		// push-CD (D8): a change that declared its significance does not go terminal
		// at merge. It enters the pod-less Deploying phase; the operator drives the
		// deploy cascade to a tatara-helmfile apply (deploy_supervision.go), then
		// closes the originating issue with the deployed version. A change with no
		// declared significance (legacy / non-cascade) keeps the close+Done path.
		if pushCDEligible(task) {
			return r.enterDeploying(ctx, project, task, &repo, provider)
		}
		// Close the originating issue (idempotent: swallow 404 / already-closed).
		if task.Spec.Source != nil && task.Spec.Source.IssueRef != "" && !task.Spec.Source.IsPR {
			repoSlug, _, slugErr := repoSlugFromURL(repo.Spec.URL, provider)
			if slugErr == nil {
				closeStart := time.Now()
				closeErr := writer.CloseIssue(ctx, token, repoSlug, task.Spec.Source.Number, "")
				if closeErr != nil {
					// 404/410 (target gone / deleted) and 422 (already closed) are
					// terminal, not genuine write failures: record result="gone" (not
					// "error", which inflated the SCM write-failure-ratio alert, issue
					// #268) and continue. Everything else requeues.
					var closeHE *scm.HTTPError
					if isPermanentTargetGone(closeErr) || (errors.As(closeErr, &closeHE) && closeHE.Status == 422) {
						r.recordSCMGone(provider, "close_issue", closeErr)
					} else {
						r.recordSCM(provider, "close_issue", closeErr)
						return ctrl.Result{}, fmt.Errorf("mainci: close issue: %w", closeErr)
					}
				} else {
					r.recordSCM(provider, "close_issue", nil)
					// Log the merge-driven close as a distinct business action (finding 8:
					// the generic lifecycle_transition log from setDeployState fires but
					// does not distinguish issue-closed-on-merge from other Done transitions).
					log.FromContext(ctx).Info("mainci: issue closed on merge",
						"action", "scm_issue_closed_on_merge",
						"resource_id", task.Name,
						"number", task.Spec.Source.Number,
						"duration_ms", time.Since(closeStart).Milliseconds())
				}
			}
		}
		// Project the issue-close event onto the ledger: set all source/closes
		// issue entries to state:closed so dedup and backstop see live state.
		// Best-effort: a conflict here does not stall the lifecycle transition.
		_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh2 := &tatarav1alpha1.Task{}
			if ferr := r.Get(ctx, client.ObjectKeyFromObject(task), fresh2); ferr != nil {
				return ferr
			}
			closeSourceIssueLedger(fresh2)
			return r.Status().Update(ctx, fresh2)
		})
		if err := r.setDeployState(ctx, task, "Done", "mainci-success"); err != nil {
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
		if err := r.setDeployState(ctx, task, "Implement", "mainci-failure"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	default:
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}
}
