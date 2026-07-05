// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"fmt"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// handleMRCI polls the MR CI status, enforces the authorship gate, and
// transitions to Merge (green), Implement (failure), or Parked (deadline/not-bot).
func (r *TaskReconciler) handleMRCI(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	_, repo, writer, token, provider, err := r.scmContext(ctx, task)
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
		if r.LifecycleMetrics != nil {
			r.LifecycleMetrics.RecordGiveup("no-pr-number")
		}
		return ctrl.Result{}, nil
	}

	// Authorship gate: PR must be bot-authored.
	st, serr := writer.GetPRState(ctx, repo.Spec.URL, token, number)
	r.recordSCM(provider, "get_pr_state", serr)
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

	// Duplicate-of-merged guard. After a post-merge MainCI failure,
	// clearMergedChangeState clears PrURL/PRNumber so finishImplement opens a fresh
	// MR. But the deterministic task branch (tatara/task-<name>) is reused, and if
	// the re-implement did not advance it past the already-merged head, the new PR
	// re-proposes the SAME already-merged commits. Observed in-repo: tatara-operator
	// PR #50 duplicated merged PR #46 with an identical head SHA. Nursing it would
	// re-merge identical code and fail MainCI again, bouncing until maxLifecycleIterations
	// parks the task with no diagnostic. Detect it by the head SHA equaling the last
	// merged head, close the duplicate, and park for a human now.
	if task.Status.MergedHeadSHA != "" && st.HeadSHA != "" && st.HeadSHA == task.Status.MergedHeadSHA {
		l.Info("mrci: PR re-proposes the already-merged head; parking",
			"action", "lifecycle_mrci_duplicate_merged", "resource_id", task.Name,
			"pr", number, "head_sha", st.HeadSHA)
		closeMsg := fmt.Sprintf("Closing as a duplicate: this branch was already merged (head %s) and the post-merge default-branch pipeline failure needs a genuinely new fix, not a re-proposal of the same commits.", st.HeadSHA)
		cerr := writer.ClosePR(ctx, repo.Spec.URL, token, number, closeMsg)
		r.recordSCM(provider, "close_pr", cerr)
		if cerr != nil {
			l.Error(cerr, "mrci: close duplicate PR (non-fatal)", "resource_id", task.Name, "pr", number)
		}
		msg := fmt.Sprintf("lifecycle: PR #%d re-proposes the already-merged change (head %s) with no new fix after the post-merge pipeline failure; parking for a human.", number, st.HeadSHA)
		if err := r.parkWithComment(ctx, task, writer, token, "duplicate-merged-change", msg); err != nil {
			return ctrl.Result{}, err
		}
		if r.LifecycleMetrics != nil {
			r.LifecycleMetrics.RecordGiveup("duplicate-merged-change")
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
		return ctrl.Result{}, r.parkOnDeadline(ctx, task, writer, token, msg)
	}

	switch st.CIStatus {
	case "pending":
		return ctrl.Result{RequeueAfter: pollRequeue}, nil

	case "success":
		if r.LifecycleMetrics != nil && task.Status.DeadlineAt != nil {
			// Elapsed is re-derived from the current BabysitDeadlineMinutes config rather
			// than a stored entry timestamp. If BabysitDeadlineMinutes changed between
			// ensureDeadline and now the elapsed will be slightly skewed, but config rarely
			// changes mid-task and the deviation is bounded to one deadline-window length.
			// Accepted drift; a stored entry timestamp would require a separate status field
			// (finding 16).
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
		if r.Metrics != nil {
			project, repo, kind, _, model := taskTokenLabels(task)
			r.Metrics.RecordImplementCI(project, repo, kind, model, "pass")
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
		if r.Metrics != nil {
			project, repo, kind, _, model := taskTokenLabels(task)
			r.Metrics.RecordImplementCI(project, repo, kind, model, "fail")
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
