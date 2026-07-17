package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ApplyReviewChangesRequested re-enters implementing when a maintainer requests
// changes on a Tatara-owned MR that is NOT yet merged. An already-merged MR is
// finished (no rewind); a kind=review or terminal Task is not driven (both are
// refused by ReenterImplementingOnReview / the merged check below). It is the
// mirror of the review pod's request_changes verdict, but sourced from a human
// review.
func ApplyReviewChangesRequested(ctx context.Context, c client.Client, sp objbudget.Spiller,
	proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, now time.Time) (bool, error) {

	mrs, err := ownedMergeRequests(ctx, c, task)
	if err != nil {
		return false, err
	}
	// The merged/finished boundary is the MergeRequest CR's merged state, NOT the
	// Task stage: any owned merged MR means the change shipped and must not rewind.
	for i := range mrs {
		if mrs[i].Status.State == "merged" || mrs[i].Status.MergedAt != nil {
			return false, nil
		}
	}

	key := client.ObjectKeyFromObject(task)
	reentered := false
	var prevStage string
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		reentered = false
		fresh := &tatarav1alpha1.Task{}
		if err := c.Get(ctx, key, fresh); err != nil {
			return err
		}
		prevStage = fresh.Status.Stage
		if !stage.ReenterImplementingOnReview(fresh, mrs, now) {
			return nil
		}
		if err := c.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*task = *fresh
		reentered = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("review: re-enter implementing on %s: %w", task.Name, err)
	}
	if reentered {
		// The stage being LEFT may still be running a pod (reviewing is one); a
		// human review event races the driver's own reconcile loop, so unlike
		// EnterStage's callers this write is not made from inside it and must tear
		// the old pod down itself or it leaks as an orphan.
		if stage.AgentKindFor(prevStage) != "" {
			if err := agent.DeleteWrapper(ctx, c, task.Namespace, task); err != nil {
				return true, fmt.Errorf("review: delete wrapper pod for %s: %w", task.Name, err)
			}
		}
		log.FromContext(ctx).Info("review: maintainer requested changes; re-entering implementing",
			"action", "review_reenter_implementing", "resource_id", task.Name)
	}
	return reentered, nil
}

// ApplyReviewApproval applies the reviewing -> merging edge on a maintainer's
// approval. A maintainer approval is authoritative and short-circuits any
// pending bot review: it clears PendingReview and stamps approved + reviewedSHA
// on every owned MR, opening reviewGateOpen so the edge is legal. The actual
// merge still waits on CI-green + mergeability in ReconcileMerging.
func ApplyReviewApproval(ctx context.Context, c client.Client, sp objbudget.Spiller,
	proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, reviewCommitSHA string, now time.Time) (bool, error) {

	if task.Spec.Kind == "review" {
		return false, nil // a kind=review Task never merges (LegalFor guard 1)
	}
	if task.Status.Stage != tatarav1alpha1.StageReviewing {
		return false, nil // approval arrived off reviewing; fold to the comment path
	}
	mrs, err := ownedMergeRequests(ctx, c, task)
	if err != nil {
		return false, err
	}
	if len(mrs) == 0 {
		return false, nil
	}
	for i := range mrs {
		mrKey := client.ObjectKeyFromObject(&mrs[i])
		thisSHA := reviewCommitSHA
		if err := objbudget.FitMergeRequest(ctx, c, sp, mrKey, func(m *tatarav1alpha1.MergeRequest) {
			m.Status.PendingReview = nil
			m.Status.Status = "approved"
			if thisSHA != "" {
				m.Status.ReviewedSHA = thisSHA
			}
		}); err != nil {
			return false, fmt.Errorf("review: settle mr %s: %w", mrKey.Name, err)
		}
	}
	fresh, err := ownedMergeRequests(ctx, c, task) // reload so reviewGateOpen sees the cleared copies
	if err != nil {
		return false, err
	}

	key := client.ObjectKeyFromObject(task)
	advanced := false
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		advanced = false
		t := &tatarav1alpha1.Task{}
		if err := c.Get(ctx, key, t); err != nil {
			return err
		}
		if t.Status.Stage != tatarav1alpha1.StageReviewing {
			return nil
		}
		if err := stage.Enter(t, fresh, tatarav1alpha1.StageMerging, "", now); err != nil {
			return nil // guard refused (e.g. gate still closed); leave untouched
		}
		if err := c.Status().Update(ctx, t); err != nil {
			return err
		}
		*task = *t
		advanced = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("review: enter merging on %s: %w", task.Name, err)
	}
	if advanced {
		// reviewing is always a pod stage (AgentReview); tear it down for the same
		// reason ApplyReviewChangesRequested does - this write races the driver's
		// own reconcile loop, outside EnterStage.
		if err := agent.DeleteWrapper(ctx, c, task.Namespace, task); err != nil {
			return true, fmt.Errorf("review: delete wrapper pod for %s: %w", task.Name, err)
		}
		log.FromContext(ctx).Info("review: maintainer approved; entering merging",
			"action", "review_enter_merging", "resource_id", task.Name)
	}
	return advanced, nil
}
