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

// UnparkDecline classifies WHY ApplyUnpark refused to re-enter (target==""),
// so callers can tell an anomalous drift bail from a normal steady-state
// refusal apart (finding: guard-decline and rule-decline used to collapse
// into the same target=="",err==nil shape, which is exactly what let the
// cache-lag decline hide as an unremarkable steady-state outcome).
type UnparkDecline string

const (
	// DeclineNone means ApplyUnpark did not decline (target != "").
	DeclineNone UnparkDecline = ""
	// DeclineGuard means the live Task's Stage/StageReason no longer matched
	// what the caller believed was parked (raced past by another writer, or
	// re-parked under a different reason). Rare and anomalous: the caller's
	// view of the world had already drifted from the apiserver.
	DeclineGuard UnparkDecline = "guard"
	// DeclineRule means stage.Unpark's re-entry rule was evaluated against the
	// live Task and was simply not satisfied yet. Normal steady state - most
	// parked Tasks decline on most passes.
	DeclineRule UnparkDecline = "rule"
)

// ApplyUnpark runs stage.Unpark for one parked Task and persists the re-entry
// under optimistic concurrency. It is the SINGLE application of stage.Unpark,
// shared by the project-reconcile driver (driveUnparks, the time-based reasons)
// and the webhook comment-driven paths (awaiting-human, backlog-sweep), so every
// F.6 re-entry flows through one place. activeTasks / maxOpen are supplied by the
// caller (computed once per pass) so a bulk promotion cannot exceed maxOpenTasks
// on a stale count. target is "" when the park did not re-enter; decline then
// says why (DeclineGuard vs DeclineRule) so the caller can log/count them
// differently. decline is always DeclineNone when target != "" or err != nil.
//
// reader is the manager's UNCACHED APIReader (same idiom as TaskReconciler's
// mintedAlready/refreshTaskFromAPI, #347/#348). The in-loop Get MUST use it,
// not the cached c: driveCommentUnpark's caller just wrote a pendingEvent via
// AppendTaskEvent's Status().Update microseconds earlier, and the cached
// informer has not observed that write yet. A cached Get here silently threw
// the fresh state away - fresh.Status.PendingEvents came back empty,
// hasNonBotEvent returned false, and the un-park was refused with ok=false, a
// normal (non-error) outcome, for a Task a human had just told to proceed
// (issue: comment-driven unpark lost the cache-lag race 2/2 in prod). Nil
// reader (unit tests that do not wire one) falls back to c.
func ApplyUnpark(ctx context.Context, c client.Client, reader client.Reader, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, activeTasks, maxOpen int, grammarPassed bool, now time.Time) (string, UnparkDecline, error) {

	// c (cached) is safe here, unlike the retry-loop Task Get below: by the time
	// driveCommentUnpark reaches this call the owning Issue CR is guaranteed to
	// already exist (resolveMirrorTarget/deliverPendingEvent already Got it and
	// early-returns on NotFound), the same-request write that preceded this call
	// only touches the Issue's Status.Comments, and stage.Unpark's
	// openIssues/allApproved read Status.State/Status.Status - fields that write
	// never touched - so no cache-lag can make this Get see a stale approval
	// state; a stale (or genuinely unapproved) read only ever routes into
	// clarifying, never into the silent decline this fix is about.
	issues, err := loadTaskIssues(ctx, c, task)
	if err != nil {
		return "", DeclineNone, err
	}
	mrs, err := loadTaskMRs(ctx, c, task)
	if err != nil {
		return "", DeclineNone, err
	}
	maxTurns := taskMaxTurns(proj, task)
	botLogin := botLoginOf(proj)

	getter := reader
	if getter == nil {
		getter = c
	}

	var target string
	var decline UnparkDecline
	key := client.ObjectKeyFromObject(task)
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := getter.Get(ctx, key, fresh); err != nil {
			return err
		}
		// Raced past this park by another writer (or already un-parked): nothing to
		// do. The reason must also still match, or a different park is in play.
		if fresh.Status.Stage != tatarav1alpha1.StageParked ||
			fresh.Status.StageReason != task.Status.StageReason {
			target = ""
			decline = DeclineGuard
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
			decline = DeclineRule
			return nil
		}
		if err := c.Status().Update(ctx, fresh); err != nil {
			return err
		}
		target = to
		decline = DeclineNone
		return nil
	})
	if err != nil {
		return "", DeclineNone, fmt.Errorf("unpark: apply on %s: %w", task.Name, err)
	}
	if target != "" {
		log.FromContext(ctx).Info("unparked task",
			"action", "unpark", "resource_id", task.Name, "stage", target,
			"reason_from", task.Status.StageReason)
	}
	return target, decline, nil
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
		target, decline, err := ApplyUnpark(ctx, r.Client, r.APIReader, proj, t, active, maxOpen, false, now)
		if err != nil {
			log.FromContext(ctx).Error(err, "unpark: apply failed",
				"action", "unpark_error", "resource_id", t.Name, "reason", t.Status.StageReason)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		// GUARD declines are always anomalous (the live object had already
		// drifted from what this pass believed was parked) and worth surfacing
		// here too, unlike RULE declines: driveUnparks sweeps every parked Task
		// every pass, and a RULE decline is the expected steady state for most
		// of them (e.g. merge-timeout still waiting) - logging every one would
		// be pure log spam, not a signal.
		if decline == DeclineGuard {
			log.FromContext(ctx).Info("unpark: declined (drift guard)",
				"action", "unpark_declined", "resource_id", t.Name, "reason", t.Status.StageReason, "decline", string(decline))
		}
		if decline != DeclineNone && r.Metrics != nil {
			r.Metrics.UnparkDeclined(t.Status.StageReason, string(decline))
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
