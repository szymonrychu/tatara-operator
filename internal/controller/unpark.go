package controller

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// THE F.6 RE-ENTRY DRIVER (fix W3).
//
// stage.Unpark carries a full re-entry body for SIX park reasons, but before
// this file its ONLY production caller was the webhook's reverifyParked, gated to
// identity-unverified. unparkFires (reaper.go) merely CHECKS whether a park would
// re-enter, on a DeepCopy, so the reaper does not collect a re-entryable Task - it
// never APPLIED the transition. So awaiting-human, merge-timeout, deploy-timeout,
// no-outcome and backlog-sweep all had a re-entry body and NO driver: a
// reviewed+approved delivery whose CI stayed red past its budget parked at
// merge-timeout and was stranded forever.
//
// This driver applies stage.Unpark. identity-unverified is EXCLUDED: it re-enters
// only against a freshly SYNCED forge thread evaluated by the C.6 grammar
// (ReVerifyParked), which is the webhook's job; driving it here with
// GrammarPassed=false would never pass.

// ApplyUnpark runs stage.Unpark for one parked Task and persists the re-entry
// under optimistic concurrency. It is the SINGLE application of stage.Unpark,
// shared by the project-reconcile driver (driveUnparks, the time-based reasons)
// and the webhook comment-driven paths (awaiting-human, backlog-sweep), so every
// F.6 re-entry flows through one place. activeTasks / maxOpen are supplied by the
// caller (computed once per pass) so a bulk promotion cannot exceed maxOpenTasks
// on a stale count. target is "" when the park did not re-enter.
func ApplyUnpark(ctx context.Context, c client.Client, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, activeTasks, maxOpen int, grammarPassed bool, now time.Time) (string, error) {

	issues, err := loadTaskIssues(ctx, c, task)
	if err != nil {
		return "", err
	}
	mrs, err := loadTaskMRs(ctx, c, task)
	if err != nil {
		return "", err
	}
	maxTurns := taskMaxTurns(proj, task)
	botLogin := botLoginOf(proj)

	var target string
	key := client.ObjectKeyFromObject(task)
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := c.Get(ctx, key, fresh); err != nil {
			return err
		}
		// Raced past this park by another writer (or already un-parked): nothing to
		// do. The reason must also still match, or a different park is in play.
		if fresh.Status.Stage != tatarav1alpha1.StageParked ||
			fresh.Status.StageReason != task.Status.StageReason {
			target = ""
			return nil
		}
		to, ok := stage.Unpark(stage.UnparkInput{
			Task:            fresh,
			Issues:          issues,
			MRs:             mrs,
			ActiveTasks:     activeTasks,
			MaxOpenTasks:    maxOpen,
			BotLogin:        botLogin,
			GrammarPassed:   grammarPassed,
			MaxTurnsPerTask: maxTurns,
			Now:             now,
		})
		if !ok {
			target = ""
			return nil
		}
		if err := c.Status().Update(ctx, fresh); err != nil {
			return err
		}
		target = to
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("unpark: apply on %s: %w", task.Name, err)
	}
	if target != "" {
		log.FromContext(ctx).Info("unparked task",
			"action", "unpark", "resource_id", task.Name, "stage", target,
			"reason_from", task.Status.StageReason)
	}
	return target, nil
}

// driveUnparks applies stage.Unpark to every parked Task in proj whose park
// reason has an F.6 re-entry rule, EXCEPT identity-unverified (webhook-driven,
// grammar-gated). activeTasks is computed ONCE and then advanced as each Task
// re-enters an active stage, so a bulk re-entry never exceeds maxOpenTasks (H8).
func (r *ProjectReconciler) driveUnparks(ctx context.Context, proj *tatarav1alpha1.Project, now time.Time) error {
	var tl tatarav1alpha1.TaskList
	if err := r.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		return fmt.Errorf("unpark: list tasks: %w", err)
	}
	active, err := r.activeTaskCount(ctx, proj)
	if err != nil {
		return err
	}
	maxOpen := proj.Spec.MaxOpenTasks
	if maxOpen <= 0 {
		maxOpen = 6
	}

	var firstErr error
	for i := range tl.Items {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		t := &tl.Items[i]
		if t.Spec.ProjectRef != proj.Name || t.Status.Stage != tatarav1alpha1.StageParked {
			continue
		}
		if t.Status.StageReason == stage.ReasonIdentityUnverified {
			continue
		}
		target, err := ApplyUnpark(ctx, r.Client, proj, t, active, maxOpen, false, now)
		if err != nil {
			log.FromContext(ctx).Error(err, "unpark: apply failed",
				"action", "unpark_error", "resource_id", t.Name, "reason", t.Status.StageReason)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if target != "" {
			active++ // the re-entered Task is now active; keep the cap honest this pass.
		}
	}
	return firstErr
}

// loadTaskIssues / loadTaskMRs resolve status.issueRefs / status.mrRefs to their
// CRs. A ref whose CR is gone is skipped (the mirror is not authoritative). They
// are the standalone twins of ProjectReconciler.ownedIssues/ownedMRs so the
// webhook package can drive an un-park without a ProjectReconciler.
func loadTaskIssues(ctx context.Context, c client.Client, t *tatarav1alpha1.Task) ([]tatarav1alpha1.Issue, error) {
	out := make([]tatarav1alpha1.Issue, 0, len(t.Status.IssueRefs))
	for _, name := range t.Status.IssueRefs {
		var iss tatarav1alpha1.Issue
		err := c.Get(ctx, types.NamespacedName{Namespace: t.Namespace, Name: name}, &iss)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("unpark: get issue %s: %w", name, err)
		}
		out = append(out, iss)
	}
	return out, nil
}

func loadTaskMRs(ctx context.Context, c client.Client, t *tatarav1alpha1.Task) ([]tatarav1alpha1.MergeRequest, error) {
	out := make([]tatarav1alpha1.MergeRequest, 0, len(t.Status.MRRefs))
	for _, name := range t.Status.MRRefs {
		var mr tatarav1alpha1.MergeRequest
		err := c.Get(ctx, types.NamespacedName{Namespace: t.Namespace, Name: name}, &mr)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("unpark: get mergerequest %s: %w", name, err)
		}
		out = append(out, mr)
	}
	return out, nil
}

// CountActiveTasks counts the non-terminal Tasks in proj. It is the standalone
// twin of ProjectReconciler.activeTaskCount, for the webhook's backlog-sweep cap
// check (H8): a promotion is not a mint, so the cap must be re-checked at re-entry.
func CountActiveTasks(ctx context.Context, c client.Client, proj *tatarav1alpha1.Project) (int, error) {
	var tl tatarav1alpha1.TaskList
	if err := c.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		return 0, fmt.Errorf("unpark: list tasks for active count: %w", err)
	}
	n := 0
	for i := range tl.Items {
		if tl.Items[i].Spec.ProjectRef == proj.Name && StageActive(&tl.Items[i]) {
			n++
		}
	}
	return n, nil
}
