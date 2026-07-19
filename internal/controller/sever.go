package controller

import (
	"context"
	"fmt"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
)

// SeverMode is how SeverIssueFromTask detaches an Issue CR from its owning Task.
type SeverMode int

const (
	// SeverDeleteCR deletes the Issue CR outright (WS3-I3). The mirror is a
	// rebuildable projection; the reopen mint re-creates it via SyncIssue. This is
	// the leak fix: a bare ownerRef drop would leave the closed CR un-owned AND
	// un-cascadable, leaking forever with the IssueReconciler mirror-syncing it.
	SeverDeleteCR SeverMode = iota
	// SeverOrphan detaches the Task from the Issue and strips the mirror's
	// tatara-parked label (WS3-I4). If task is the CONTROLLER owner and another
	// live Task still holds a plain ownerRef, the controller flag is handed over
	// to the oldest surviving owner FIRST (B.2 rule 5: never leave the CR with
	// zero controller owners) - the bare drop only happens when no live heir
	// exists, leaving the still-OPEN CR the ownerless orphan the fresh
	// MintForItem re-adopts.
	SeverOrphan
)

// SeverIssueFromTask detaches issueName from task WITHOUT the split state the
// reviewer found: the Task side (Status.IssueRefs), the CR side (ownerRef), and
// the reaper's label bookkeeping move together, or the reaper walks stale refs
// (spurious terminal comment + label re-stamp) and orphaned CRs leak. I3 and I4
// both use this ONE op instead of two bespoke partial severances.
//
// LEADER-ONLY. It is called by ApplyIssueClosedStop (SeverDeleteCR) and the I4
// no-re-entry resume driver (SeverOrphan); both run leader-side.
//
// ORDER: the Task side is cleared FIRST, so the worst crash-state after step 1
// is "CR still owner-reffed but not listed by the Task": the CR keeps a valid
// controller owner (no B.2-rule-5 zero-owner violation), ownedIssues skips it
// (not in IssueRefs, so NO spurious terminal comment / label re-stamp), and the
// leader re-reconciles to finish step 2 idempotently. It stamps NO
// AnnTerminalReleased and NEVER deleteReapedTasks the Task: it detaches ONE
// issue and leaves the Task's other artifacts (MRs) to its normal terminal path.
//
// The FORGE-side tatara-parked label removal (F.6 operator-on-promotion) is the
// caller's job when it holds an SCM writer; this op strips the MIRROR label so
// the ForgeItem the mint is built from - and MintStage's tatara-parked check -
// never see it.
func SeverIssueFromTask(ctx context.Context, c client.Client, task *tatarav1alpha1.Task,
	issueName string, mode SeverMode) error {

	l := log.FromContext(ctx)

	// STEP 1 (Task side FIRST): drop issueName from Status.IssueRefs. A gone Task
	// (NotFound) is SUCCESS, not an error: its collision-delete already ran (the I4
	// re-mint stale-terminal path), so its refs are moot - but we STILL fall through
	// to step 2 so the issue's OWN ownerRef is dropped and it does not cascade with
	// the deleted Task.
	taskKey := client.ObjectKeyFromObject(task)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := c.Get(ctx, taskKey, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				return nil // task gone: refs moot; step 2 still orphans the issue.
			}
			return err
		}
		before := len(fresh.Status.IssueRefs)
		fresh.Status.IssueRefs = slices.DeleteFunc(fresh.Status.IssueRefs, func(n string) bool { return n == issueName })
		if len(fresh.Status.IssueRefs) == before {
			*task = *fresh
			return nil // already detached; idempotent
		}
		if err := c.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*task = *fresh
		return nil
	}); err != nil {
		return fmt.Errorf("sever: clear issueRef %s from task %s: %w", issueName, task.Name, err)
	}

	// STEP 2 (CR side).
	issKey := types.NamespacedName{Namespace: task.Namespace, Name: issueName}
	switch mode {
	case SeverDeleteCR:
		var iss tatarav1alpha1.Issue
		if err := c.Get(ctx, issKey, &iss); err != nil {
			if apierrors.IsNotFound(err) {
				return nil // already gone; idempotent
			}
			return fmt.Errorf("sever: get issue %s for delete: %w", issueName, err)
		}
		if err := c.Delete(ctx, &iss); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("sever: delete issue %s: %w", issueName, err)
		}
		l.Info("severed and deleted an issue mirror from its task",
			"action", "sever_issue_delete", "resource_id", issueName, "task", task.Name)
		return nil

	case SeverOrphan:
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			var iss tatarav1alpha1.Issue
			if err := c.Get(ctx, issKey, &iss); err != nil {
				return err
			}
			owner, ok := own.ControllerOwner(&iss)
			if !ok || owner != task.Name {
				// task holds no controller flag to hand over: a bare drop of its
				// (plain, or absent) ref cannot orphan a controller owner.
				dropOwnerRef(&iss, task.Name)
				return c.Update(ctx, &iss)
			}
			live := make(map[string]bool)
			for _, r := range iss.GetOwnerReferences() {
				if r.Kind != "Task" || r.APIVersion != tatarav1alpha1.GroupVersion.String() || r.Name == task.Name {
					continue
				}
				var other tatarav1alpha1.Task
				err := c.Get(ctx, types.NamespacedName{Namespace: iss.Namespace, Name: r.Name}, &other)
				switch {
				case err == nil:
					live[r.Name] = true
				case apierrors.IsNotFound(err):
					live[r.Name] = false
				default:
					return err
				}
			}
			heir, hasHeir := own.OldestSurvivingOwner(&iss, live)
			if hasHeir {
				if err := own.HandOverController(&iss, task, &tatarav1alpha1.Task{
					ObjectMeta: metav1.ObjectMeta{Name: heir, Namespace: iss.Namespace},
				}); err != nil {
					return err
				}
				return c.Update(ctx, &iss)
			}
			dropOwnerRef(&iss, task.Name)
			return c.Update(ctx, &iss)
		}); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("sever: drop ownerRef of %s from issue %s: %w", task.Name, issueName, err)
		}
		// Strip the mirror tatara-parked label so the fresh mint lands ACTIVE via
		// humanHasLastWord instead of parked(backlog-sweep). Best-effort on a status
		// subresource; a missing label is a no-op.
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			var iss tatarav1alpha1.Issue
			if err := c.Get(ctx, issKey, &iss); err != nil {
				return err
			}
			if !slices.Contains(iss.Status.Labels, TataraParkedLabel) {
				return nil
			}
			iss.Status.Labels = slices.DeleteFunc(iss.Status.Labels, func(n string) bool { return n == TataraParkedLabel })
			return c.Status().Update(ctx, &iss)
		}); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("sever: strip %s from issue %s: %w", TataraParkedLabel, issueName, err)
		}
		l.Info("severed (orphaned) an issue mirror from its task",
			"action", "sever_issue_orphan", "resource_id", issueName, "task", task.Name)
		return nil

	default:
		return fmt.Errorf("sever: unknown mode %d", mode)
	}
}
