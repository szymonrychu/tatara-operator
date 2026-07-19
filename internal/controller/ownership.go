package controller

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ownershipForAuthor classifies a never-seen MR: bot author -> tatara,
// anything else (human, Renovate, other bot, or an unknown/empty author) ->
// external.
func ownershipForAuthor(proj *tatarav1alpha1.Project, author string) string {
	if author != "" && author == botLoginOf(proj) {
		return tatarav1alpha1.OwnershipTatara
	}
	return tatarav1alpha1.OwnershipExternal
}

// ReconcileOwnership is the single convergence function for MR ownership,
// called from the leader MergeRequestReconciler (webhook fast path, via the
// mirror resourceVersion bump) and the cron sweep (convergence path). It:
//
//  1. backfills ownership on a never-classified or pre-upgrade mirror,
//  2. flips tatara -> external on unattributable head drift, and
//  3. redelivers missed comments on an external MR (sweep only; newComments
//     nil on the webhook path).
//
// It NEVER flips external -> tatara: that is the gated takeover REST
// endpoint's job (agent judgment alone can never flip state). Terminal MRs
// are frozen.
func (d *StageDriver) ReconcileOwnership(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest, liveHead string,
	newComments []scm.IssueComment) (bool, error) {

	if mr.Status.State != "open" {
		return false, nil
	}
	key := client.ObjectKeyFromObject(mr)
	sp := d.spiller(proj)

	if mr.Status.Ownership == "" {
		cls := ownershipForAuthor(proj, mr.Status.Author)
		// A backfilled tatara classification also seeds LastBotHeadSHA to the
		// current live head: there is no bot-push history to compare against
		// before this point (pre-upgrade mirror, or a mirror never reconciled
		// through OP7's stamp points yet), so the best available baseline is
		// "whatever is on the branch right now was put there by the bot". Without
		// this, the very next check below would read an EMPTY LastBotHeadSHA,
		// see liveHead != "", and immediately flip a freshly classified bot MR to
		// external on its own backfill - which is not a drift, it is day one.
		botHead := mr.Status.LastBotHeadSHA
		if cls == tatarav1alpha1.OwnershipTatara && liveHead != "" {
			botHead = liveHead
		}
		if err := objbudget.FitMergeRequest(ctx, d.Client, sp, key, func(m *tatarav1alpha1.MergeRequest) {
			if m.Status.Ownership == "" {
				m.Status.Ownership = cls
				m.Status.OwnershipReason = "initial"
				if cls == tatarav1alpha1.OwnershipTatara && liveHead != "" {
					m.Status.LastBotHeadSHA = botHead
				}
			}
		}); err != nil {
			return false, err
		}
		mr.Status.Ownership = cls
		mr.Status.OwnershipReason = "initial"
		if cls == tatarav1alpha1.OwnershipTatara && liveHead != "" {
			mr.Status.LastBotHeadSHA = botHead
		}
		log.FromContext(ctx).Info("ownership classified", "action", "ownership_initial",
			"resource_id", mr.Name, "ownership", cls)
		// Initial classification is not a flip; do not announce or count.
	}

	if mr.Status.Ownership == tatarav1alpha1.OwnershipTatara &&
		liveHead != "" && liveHead != mr.Status.LastBotHeadSHA {
		return d.flipToExternal(ctx, proj, repo, mr, liveHead)
	}

	if mr.Status.Ownership == tatarav1alpha1.OwnershipExternal && len(newComments) > 0 {
		return false, d.redeliverMRComments(ctx, proj, repo, mr, newComments) // OP12
	}
	return false, nil
}

// flipToExternal records the tatara -> external flip, parks the bound
// takeover Task ownership-lost, and hands the MR mirror's controller
// ownership back to the review Task so review rounds and hand-back comments
// continue to route. The stand-down announcement is posted by the drain
// (OP11), keyed on the ownershipChangedAt marker this stamps. The parked
// takeover Task is RETAINED (not reaped while its MR is open) as the durable
// merge-driver: an approved review on this stood-down MR re-drives it
// parked(ownership-lost) -> merging via DrainStandDownMerge (OP11), because
// merge-on-approve CONTINUES after a stand-down (spec Section 1: external +
// external-push keeps review + merge).
func (d *StageDriver) flipToExternal(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest, liveHead string) (bool, error) {

	now := metav1.Now()
	reason := "external-push:" + liveHead
	key := client.ObjectKeyFromObject(mr)
	if err := objbudget.FitMergeRequest(ctx, d.Client, d.spiller(proj), key, func(m *tatarav1alpha1.MergeRequest) {
		m.Status.Ownership = tatarav1alpha1.OwnershipExternal
		m.Status.OwnershipReason = reason
		m.Status.OwnershipChangedAt = &now
	}); err != nil {
		return false, err
	}
	mr.Status.Ownership = tatarav1alpha1.OwnershipExternal
	mr.Status.OwnershipReason = reason
	mr.Status.OwnershipChangedAt = &now

	// Park the bound takeover Task (the current controller owner, if any). Only
	// a takeover-kind Task gets the special "parked ownership-lost, retained as
	// the durable merge driver" treatment; some other kind of controller owner
	// (or none at all) is left alone here.
	if ownerName, ok := own.ControllerOwner(mr); ok {
		var task tatarav1alpha1.Task
		if err := d.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: ownerName}, &task); err == nil {
			if task.Spec.Kind == takeoverKind && !tatarav1alpha1.StageTerminal(&task) {
				if err := d.enterStage(ctx, proj, &task, tatarav1alpha1.StageParked, stage.ReasonOwnershipLost, nil); err != nil {
					return false, fmt.Errorf("flip: park takeover task: %w", err)
				}
			}
		} else if !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("flip: get owner task %s: %w", ownerName, err)
		}
		if err := d.handBackToReviewTask(ctx, proj, repo, mr); err != nil {
			return false, err
		}
	}

	obs.OwnershipFlip("to-external", "external-push")
	log.FromContext(ctx).Info("ownership flipped to external", "action", "ownership_flip",
		"resource_id", mr.Name, "direction", "to-external", "reason", reason)
	return true, nil
}

// handBackToReviewTask moves the MR mirror's controller ownership to the
// kind=review Task for this MR (re-minting it if it was never minted, or was
// reaped), so an external MR's review rounds and its next "take over" comment
// route to the review agent.
func (d *StageDriver) handBackToReviewTask(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest) error {

	reviewName := tatarav1alpha1.IntakeTaskName(proj.Name, SweepReviewKind, repo.Name, mr.Spec.Number)
	var review tatarav1alpha1.Task
	err := d.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: reviewName}, &review)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("flip: get review task %s: %w", reviewName, err)
		}
		// No review Task has ever been minted for this MR (the takeover path
		// bypasses it entirely). Re-mint via the shared intake funnel below.
		return d.reMintReviewOwner(ctx, proj, repo, mr)
	}
	prev, _ := own.ControllerOwner(mr)
	var from *tatarav1alpha1.Task
	if prev != "" && prev != reviewName {
		from = &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: prev, Namespace: proj.Namespace}}
	}
	own.AddPlainOwner(mr, &review)
	if err := own.HandOverController(mr, from, &review); err != nil {
		return fmt.Errorf("flip: hand back to review task: %w", err)
	}
	return d.Update(ctx, mr)
}

// reMintReviewOwner mints the MR's review Task via the shared intake funnel
// (Minter.MintReviewTask, reusing MintReviewStage exactly as the sweep/webhook
// do) when handBackToReviewTask finds none. MintReviewTask's own bind
// (Minter.ownMergeRequest) refuses to steal a controller ref it does not
// recognize as its own Task's - a guard built for the ORPHAN-mint case, where
// an unrecognized controller owner is a bug, not this flip's hand-back case,
// where the CURRENT controller (the takeover Task just parked above) is
// expected and about to be superseded. demoteMRController clears that
// controller flag first (leaving the old owner as a plain ref, same end state
// own.HandOverController's demote-then-promote would leave it in), so the
// funnel's own guard sees no existing controller and proceeds normally.
func (d *StageDriver) reMintReviewOwner(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest) error {

	if err := d.demoteMRController(ctx, mr); err != nil {
		return fmt.Errorf("flip: demote controller before review re-mint: %w", err)
	}
	pr := prRefFromMR(repo, mr)
	stg, reason := MintReviewStage(mr)
	if _, _, err := d.minter().MintReviewTask(ctx, proj, repo, pr, mr, stg, reason, d.spiller(proj)); err != nil {
		return fmt.Errorf("flip: re-mint review task: %w", err)
	}
	return nil
}

// demoteMRController clears the controller=true flag on mr's current owner
// ref, if any, leaving it as a plain owner. It is a no-op when mr carries no
// controller owner.
func (d *StageDriver) demoteMRController(ctx context.Context, mr *tatarav1alpha1.MergeRequest) error {
	if _, ok := own.ControllerOwner(mr); !ok {
		return nil
	}
	key := client.ObjectKeyFromObject(mr)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh tatarav1alpha1.MergeRequest
		if err := d.Get(ctx, key, &fresh); err != nil {
			return err
		}
		refs := fresh.GetOwnerReferences()
		for i := range refs {
			if refs[i].Controller != nil && *refs[i].Controller {
				f := false
				refs[i].Controller = &f
			}
		}
		fresh.SetOwnerReferences(refs)
		if err := d.Update(ctx, &fresh); err != nil {
			return err
		}
		*mr = fresh
		return nil
	})
}

// minter builds a Minter from this StageDriver's own fields, so flip-driven
// hand-back can re-mint a review Task through the SAME shared intake funnel
// the sweep and webhook use, instead of duplicating Task construction here.
func (d *StageDriver) minter() *Minter {
	return &Minter{
		Client:     d.Client,
		APIReader:  d.APIReader,
		Scheme:     d.Scheme(),
		Metrics:    d.Metrics,
		SpillerFor: d.SpillerFor,
	}
}

// redeliverMRComments is OP12's job: replaying comments an external MR missed
// while it had no owning Task in scope. Stubbed here so ReconcileOwnership
// compiles and is fully testable ahead of OP12; OP12 replaces this body.
func (d *StageDriver) redeliverMRComments(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest, newComments []scm.IssueComment) error {
	return nil
}
