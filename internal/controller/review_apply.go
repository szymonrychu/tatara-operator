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

// ApplyReviewChangesRequested routes a maintainer's changes_requested on a
// Tatara-owned MR that is NOT yet merged back onto the stage machine
// (stage.ReenterOnReviewChangesRequested). A non-parked reviewing/merging Task
// re-enters implementing with a fresh merge budget; a parked Task is routed by
// its park reason (merge-timeout -> merging, no-outcome -> implementing behind
// its guards, everything else folds). An already-merged MR is finished (no
// rewind); a kind=review or terminal Task is not driven (refused by
// ReenterOnReviewChangesRequested / the merged check below).
//
// reader is the manager's UNCACHED APIReader. The merged-boundary read and every
// retry-loop Get use it, not the cached c: stampMerged writes status straight to
// the apiserver and the informer cache can lag it, so a cached read here could
// pass the merged gate and rewind a Task whose MR just shipped (F2). Nil reader
// (unit tests that wire none) falls back to c.
func ApplyReviewChangesRequested(ctx context.Context, c client.Client, reader client.Reader,
	proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, now time.Time) (bool, error) {

	rdr := reader
	if rdr == nil {
		rdr = c
	}
	maxTurns := taskMaxTurns(proj, task)

	key := client.ObjectKeyFromObject(task)
	reentered := false
	var prevStage string
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		reentered = false
		fresh := &tatarav1alpha1.Task{}
		if err := rdr.Get(ctx, key, fresh); err != nil {
			return err
		}
		// Reload owned MRs from the UNCACHED reader INSIDE the loop and re-check
		// merged, so a merge landing mid-retry cannot rewind shipped work (F2). The
		// merged/finished boundary is the MergeRequest CR's merged state, NOT the
		// Task stage: any owned merged MR means the change shipped and must not rewind.
		mrs, err := ownedMergeRequests(ctx, rdr, fresh)
		if err != nil {
			return err
		}
		for i := range mrs {
			if mrs[i].Status.State == "merged" || mrs[i].Status.MergedAt != nil {
				return nil
			}
		}
		prevStage = fresh.Status.Stage
		if !stage.ReenterOnReviewChangesRequested(fresh, mrs, maxTurns, now) {
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
		log.FromContext(ctx).Info("review: maintainer requested changes; re-entered stage machine",
			"action", "review_reenter_implementing", "resource_id", task.Name,
			"from_stage", prevStage, "to_stage", task.Status.Stage)
	}
	return reentered, nil
}

// ApplyReviewApproval applies the reviewing -> merging edge on a maintainer's
// approval. A maintainer approval is authoritative and short-circuits any
// pending bot review: it clears PendingReview and stamps approved + reviewedSHA
// on every owned MR, opening reviewGateOpen so the edge is legal. The actual
// merge still waits on CI-green + mergeability in ReconcileMerging.
func ApplyReviewApproval(ctx context.Context, c client.Client, reader client.Reader, sp objbudget.Spiller,
	proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, reviewCommitSHA string, now time.Time) (bool, error) {

	if task.Spec.Kind == "review" {
		return false, nil // a kind=review Task never merges (LegalFor guard 1)
	}
	rdr := reader
	if rdr == nil {
		rdr = c
	}

	// Live-confirm reviewing BEFORE tearing anything down. A cached passed-in task
	// could be stale; the uncached read decides whether this is genuinely the
	// reviewing pod we may delete.
	key := client.ObjectKeyFromObject(task)
	live := &tatarav1alpha1.Task{}
	if err := rdr.Get(ctx, key, live); err != nil {
		return false, err
	}
	if live.Status.Stage != tatarav1alpha1.StageReviewing {
		return false, nil // approval arrived off reviewing; fold to the comment path
	}
	mrs, err := ownedMergeRequests(ctx, rdr, task)
	if err != nil {
		return false, err
	}
	if len(mrs) == 0 {
		return false, nil
	}

	// F5: delete the in-flight review pod FIRST, before clearing PendingReview.
	// Otherwise the pod's own /outcome can re-arm PendingReview in the window
	// between our clear and merging, and DrainPendingReview then posts a redundant
	// bot review that overwrites reviewedSHA - which fires a spurious
	// merging -> reviewing head-moved bounce. The maintainer's approval is
	// authoritative; the bot review is moot. reviewing is always a pod stage
	// (AgentReview), so there is always a pod to tear down, and this write races
	// the driver's own reconcile loop outside EnterStage.
	//
	// Accepted degradation bound: if the pod delete succeeds but the subsequent
	// clear/enter fails and the forge exhausts redelivery, recovery is bounded and
	// self-healing. On any redelivery both appliers are idempotent (the clear and
	// the reviewing -> merging edge re-apply cleanly), so a later delivery finishes
	// the transition; absent redelivery, the deleted review pod is recreated by the
	// reconciler and its fresh verdict simply overrides - the Task never strands.
	if err := agent.DeleteWrapper(ctx, c, task.Namespace, task); err != nil {
		return false, fmt.Errorf("review: delete wrapper pod for %s: %w", task.Name, err)
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
	fresh, err := ownedMergeRequests(ctx, rdr, task) // reload so reviewGateOpen sees the cleared copies
	if err != nil {
		return false, err
	}

	advanced := false
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		advanced = false
		t := &tatarav1alpha1.Task{}
		if err := rdr.Get(ctx, key, t); err != nil {
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
		log.FromContext(ctx).Info("review: maintainer approved; entering merging",
			"action", "review_enter_merging", "resource_id", task.Name)
	}
	return advanced, nil
}
