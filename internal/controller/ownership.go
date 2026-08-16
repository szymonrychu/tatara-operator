package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

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

// externalPushReasonPrefix tags an OwnershipReason stamped by flipToExternal
// (reason is this prefix plus the drifted head SHA). ReconcileOwnership's
// resumeFlipToExternal greps for it to recognize its OWN flip - as opposed to
// an external-owned MR that was never tatara's to begin with (reason
// "initial") - and knows it is safe to resume that flip's park+hand-back.
const externalPushReasonPrefix = "external-push:"

// adoptedPushReasonPrefix is externalPushReasonPrefix's twin for a flip whose
// owner is an ADOPTED dependency-upgrade Task (stage.AdoptedUpgrade), and the whole point
// of it being a DIFFERENT string is that mergeAllowedForOwnership and
// DrainStandDownMerge both test for `external-push:` exactly and therefore
// REFUSE this one.
//
// WHY THE TWO CANNOT SHARE A REASON. Merge-after-stand-down exists for a
// TAKEOVER: a maintainer explicitly asked the platform to take a merge request
// over, so their later push to it is part of a collaboration they started, and
// continuing to review and merge across it is the behaviour they asked for.
// NOBODY ASKS FOR AN ADOPTION - the sweep does it automatically to every merge
// request the dependency engine opens. Reusing `external-push:` there would mean
// that a human pushing a commit onto the engine's branch hands their commit to a
// pod-less merge corridor on the next approve, on the strength of a decision no
// human ever made. So an adopted merge request a human pushed to becomes
// HUMAN-MERGED-ONLY, which is the same disposition an ordinary external merge
// request has carried all along.
//
// It shares no PREFIX with externalPushReasonPrefix either ("external-push-"
// would make HasPrefix("external-push:") false but invites a future
// HasPrefix("external-push") that silently re-admits it). isExternalPushReason
// is the ONE place that treats them alike, and it is used only by
// resumeFlipToExternal, which converges a half-completed flip and cares that a
// flip happened, not which kind.
const adoptedPushReasonPrefix = "adopted-external-push:"

// isExternalPushReason reports whether reason was stamped by flipToExternal, in
// either of its two shapes. It is deliberately NOT what the merge corridor asks:
// mergeAllowedForOwnership and DrainStandDownMerge test externalPushReasonPrefix
// alone, because only the takeover shape may be merged.
func isExternalPushReason(reason string) bool {
	return strings.HasPrefix(reason, externalPushReasonPrefix) ||
		strings.HasPrefix(reason, adoptedPushReasonPrefix)
}

// ownershipForAuthor classifies a never-seen MR: an author this project owns -
// the platform bot, or an allowlisted upgradePolicy.upgradeEngineLogins
// dependency-upgrade engine - is `tatara`; anything else (a human, an
// un-allowlisted bot, or an unknown/empty author) is `external`.
//
// IT DELEGATES TO adoptableAuthor ON PURPOSE, AND THE TWO MUST STAY ONE
// FUNCTION. AdoptUpgradeMR decides which merge requests the platform TAKES and
// this decides which it is ALLOWED TO MERGE (mergeAllowedForOwnership accepts
// `tatara` unconditionally and refuses external/"initial" outright). An author
// accepted by one and refused by the other adopts a merge request the merge
// corridor then declines - after the review agent has already approved it on
// the forge. Retiring the mint-time ownership flip is sound only while these
// two answer the same question, so they ask it in the same place.
func ownershipForAuthor(proj *tatarav1alpha1.Project, author string) string {
	if adoptableAuthor(proj, author) {
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

	if mr.Status.Ownership == tatarav1alpha1.OwnershipTatara && liveHead != "" {
		// NO BASELINE IS NOT A DRIFT. The backfill above seeds LastBotHeadSHA so
		// this check cannot fire on the classification's own backfill, but that
		// seed only runs when liveHead is already known - and a mirror is
		// routinely classified on a reconcile that has not synced the head yet
		// (a freshly opened PR whose head arrives a beat later). LastBotHeadSHA
		// then stayed EMPTY, and the first reconcile that did know the head read
		// liveHead != "" as drift and flipped the MR against tatara's OWN commit.
		//
		// That is not cosmetic: flipToExternal parks the owning Task
		// ownership-lost, hands the MR to the review Task, and the issue restarts
		// from scratch. Live, on tatara's own work: mr-mtg-decks-19 and
		// mr-mtg-decks-15 both ended external with reason=external-push:<tatara's
		// own commit> and lastBotHeadSHA empty, and a finished 4199-line deck PR
		// was closed and rebuilt from nothing.
		//
		// With no baseline there is nothing to compare against, so seed the same
		// way the backfill does - "whatever is on the branch right now was put
		// there by the bot" - and let the NEXT head move be judged against it.
		// The alternative, flipping on an unknowable question, is what the bug is.
		if mr.Status.LastBotHeadSHA == "" {
			if err := objbudget.FitMergeRequest(ctx, d.Client, sp, key, func(m *tatarav1alpha1.MergeRequest) {
				if m.Status.LastBotHeadSHA == "" {
					m.Status.LastBotHeadSHA = liveHead
				}
			}); err != nil {
				return false, err
			}
			mr.Status.LastBotHeadSHA = liveHead
			log.FromContext(ctx).Info("ownership: seeded the bot-head baseline on first known head",
				"action", "ownership_seed_bot_head", "resource_id", mr.Name, "head", liveHead)
			return false, nil
		}
		if liveHead != mr.Status.LastBotHeadSHA {
			return d.flipToExternal(ctx, proj, repo, mr, liveHead)
		}
	}

	if mr.Status.Ownership == tatarav1alpha1.OwnershipExternal {
		// Convergence hole fix: flipToExternal stamps Status.Ownership=external
		// BEFORE parking the owner Task and handing control to the review Task.
		// A non-conflict error partway through that second half leaves the
		// stamp committed but the park+hand-back unfinished - and, once
		// committed, this function's own tatara->external trigger above never
		// fires again for this MR, so nothing would otherwise retry it. Detect
		// that half-completed state (reason carries this flip's own prefix, and
		// the current controller owner is still a pushing-capable, non-review
		// Task - the review hand-back has not landed) and resume it. Gated on
		// the reason prefix, not unconditionally re-run: park+handBackToReviewTask
		// both always write, so running them on every reconcile of every
		// external MR would churn a resourceVersion for nothing once converged.
		if isExternalPushReason(mr.Status.OwnershipReason) {
			if err := d.resumeFlipToExternal(ctx, proj, repo, mr); err != nil {
				return false, err
			}
		}
		if len(newComments) > 0 {
			return false, d.redeliverMRComments(ctx, proj, repo, mr, newComments) // OP12
		}
	}
	return false, nil
}

// resumeFlipToExternal finishes an interrupted flipToExternal's park+hand-back
// half. It is a no-op (zero API calls beyond the one Get) once the MR's
// controller owner is already the review Task - the steady state after a
// clean flip - so calling it on every reconcile of an external-push MR costs
// nothing once converged.
func (d *StageDriver) resumeFlipToExternal(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest) error {

	ownerName, ok := own.ControllerOwner(mr)
	if !ok {
		return nil
	}
	var task tatarav1alpha1.Task
	err := d.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: ownerName}, &task)
	if err == nil && task.Spec.Kind == SweepReviewKind {
		return nil // hand-back already completed; nothing to resume
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("resume flip: get owner task %s: %w", ownerName, err)
	}
	return d.parkAndHandBack(ctx, proj, repo, mr)
}

// flipToExternal records the tatara -> external flip, parks the bound
// pushing-capable owner Task ownership-lost, and hands the MR mirror's
// controller ownership back to the review Task so review rounds and
// hand-back comments continue to route. The stand-down announcement is
// posted by the drain (OP11), keyed on the ownershipChangedAt marker this
// stamps. The parked owner Task is RETAINED (not reaped while its MR is
// open) as the durable merge-driver: an approved review on this stood-down
// MR re-drives it parked(ownership-lost) -> merging via DrainStandDownMerge
// (OP11), because merge-on-approve CONTINUES after a stand-down (spec
// Section 1: external + external-push keeps review + merge).
func (d *StageDriver) flipToExternal(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest, liveHead string) (bool, error) {

	now := metav1.Now()
	prefix := externalPushReasonPrefix
	adopted, aerr := d.ownerIsAdoptedMR(ctx, proj, mr)
	if aerr != nil {
		return false, aerr
	}
	if adopted {
		prefix = adoptedPushReasonPrefix
	}
	reason := prefix + liveHead
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

	// Park the bound owner Task and hand control to the review Task. Shared
	// with resumeFlipToExternal (above), which re-runs this exact step to
	// converge a flip interrupted between the Status write above and here.
	if err := d.parkAndHandBack(ctx, proj, repo, mr); err != nil {
		return false, err
	}

	obs.OwnershipFlip("to-external", "external-push")
	log.FromContext(ctx).Info("ownership flipped to external", "action", "ownership_flip",
		"resource_id", mr.Name, "direction", "to-external", "reason", reason, "adopted", adopted)
	return true, nil
}

// ownerIsAdoptedMR reports whether mr's CURRENT controller owner is an ADOPTED
// dependency-upgrade Task (stage.AdoptedUpgrade). It is flipToExternal's only
// discriminator, and it is keyed on the OWNER rather than on the head branch on
// purpose: a maintainer may legitimately request a takeover of a merge request
// that happens to sit under the adopt prefix, and that takeover keeps the
// mergeable stand-down reason it has always had.
//
// stage.AdoptedUpgrade, NEVER stage.AdoptedMR: a takeover Task and a kind=review
// Task are both minted onto a merge request and both satisfy AdoptedMR, so the
// looser predicate would strip merge-after-stand-down from every takeover on the
// platform.
//
// AN UNRESOLVABLE OWNER FALLS BACK TO THE TAKEOVER REASON, which is the
// pre-existing behaviour rather than a new one. It is not reachable for an
// adopted merge request in practice: the mint makes the adopted Task the
// mirror's controller owner in the same call that creates the mirror, and the
// only thing that removes that ref is the reap - which also orphans the mirror,
// at which point there is no owner left for a flip to park.
func (d *StageDriver) ownerIsAdoptedMR(ctx context.Context, proj *tatarav1alpha1.Project,
	mr *tatarav1alpha1.MergeRequest) (bool, error) {

	ownerName, ok := own.ControllerOwner(mr)
	if !ok {
		return false, nil
	}
	var task tatarav1alpha1.Task
	err := d.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: ownerName}, &task)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("flip: get owner task %s: %w", ownerName, err)
	}
	return stage.AdoptedUpgrade(&task), nil
}

// parkAndHandBack parks mr's current controller-owning Task (if any,
// pushing-capable) ownership-lost, then hands the MR mirror's controller
// ownership to the review Task. It is the shared body behind flipToExternal's
// own park+hand-back and resumeFlipToExternal's convergent retry of it -
// factored out so the two never drift, and so the retry inherits exactly the
// idempotency parkOwnerTask and handBackToReviewTask already carry.
func (d *StageDriver) parkAndHandBack(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest) error {

	// Park the bound owner Task (the current controller owner, if any) when it
	// is pushing-capable - i.e. any kind EXCEPT review: a kind=review Task never
	// pushes to the MR it owns (LegalFor structurally refuses it implementing or
	// merging), so it is never the thing an external push displaces and is never
	// parked here. A takeover-kind Task and a normal full-lifecycle (kind=clarify
	// -> implementing) Task both push, and both get the same "parked
	// ownership-lost, retained as the durable merge driver" treatment - the spec
	// draws the line on push-capability, not on takeover-vs-normal provenance.
	if ownerName, ok := own.ControllerOwner(mr); ok {
		var task tatarav1alpha1.Task
		if err := d.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: ownerName}, &task); err == nil {
			if err := d.parkOwnerTask(ctx, proj, &task); err != nil {
				return err
			}
		} else if !apierrors.IsNotFound(err) {
			return fmt.Errorf("flip: get owner task %s: %w", ownerName, err)
		}
		if err := d.handBackToReviewTask(ctx, proj, repo, mr); err != nil {
			return err
		}
	}
	return nil
}

// parkOwnerTask parks task ownership-lost when it is pushing-capable
// (kind != review), not done, and not ALREADY parked - stage.Park is idempotent
// (first reason wins) but the early return keeps the metric honest.
//
// THE PER-STAGE EDGE TABLE IS GONE (#521). Parking is orthogonal to state now,
// so there is no "does this stage carry an ownership-lost park edge" question to
// ask and no stage that can be left running because the answer was no. Every
// non-done state can hold the flag; hasOwnershipLostParkEdge and its
// ownership_flip_park_skipped log line are deleted with the question they
// answered.
//
// ONE PARK IS UPGRADED RATHER THAN LEFT ALONE (#604 review). A takeover Task
// parked implement-declined is the ordinary stand-down arriving agent-first: the
// pod's divergence signal is a local git call and beats the operator's flip,
// which waits on webhook -> mirror headSHA -> here. Left at implement-declined
// it is UnparkNever, so the re-take is refused, DrainStandDownMerge cannot
// re-drive an approved external head, and a branch a human merely took back
// sits until ParkRetention. See stage.UpgradeDeclineToOwnershipLost for why the
// upgrade is narrow on both kind and reason, and why it is not a repark.
//
// THE DISCRIMINATOR IS THE RECORDED BOT HEAD, NOT THE PUSHER, and the rule this
// upgrade implements ("terminal while the branch is still tatara's") is only as
// good as that baseline. ReconcileOwnership never sees a pusher identity: the
// flip fires on liveHead != Status.LastBotHeadSHA, and exactly two writers
// ADVANCE that baseline after a push - the synchronize webhook when
// isUpgradeEngineActor matches the pusher (webhook.stampMRHead) and /outcome's
// record_bot_head, which is on the SUBMITTED path only. (Three more SEED it
// once: restapi.mrTakeover at the takeover itself, and this function's own
// classification backfill and empty-baseline seed. A seed is not a refresh -
// mrTakeover's is the very value that goes stale below.) So a takeover that
// pushed, lost its synchronize delivery or pushed under a login
// isUpgradeEngineActor does not match, and then DELIBERATELY
// declined, is flipped against tatara's OWN sha - and its deliberate decline is
// upgraded here with no human having touched the branch, after which
// DrainStandDownMerge may merge, on an approved review, commits the agent
// refused to stand behind.
//
// The stale baseline is PRE-EXISTING and already accepted (handleMRSynchronize
// argues it out); what #604 adds is that a spurious flip now rewrites a terminal
// instead of hitting the Parked early return above.
//
// IT IS NOT FIXED BY REFRESHING LastBotHeadSHA ON THE DECLINE ARM, which is the
// first remedy anyone proposes and is worse than the hole. The decline arm holds
// no information this flip does not already have - anything it could record is a
// function of (liveHead, LastBotHeadSHA) - so the stamp is a no-op exactly when
// it is safe and decisive exactly when it is ambiguous. On a DIVERGENCE decline
// the live head is the HUMAN's, so stamping it pins the baseline to the human's
// sha, this flip never fires, and the ordinary stand-down is a permanent terminal
// again: the #604-review bug, restored. Every field-renamed variant of it
// (declinedAtHeadSHA and friends) is isomorphic. And as usually worded it is
// kind-agnostic - outcome.go's decline arm serves implement and upgrade too - so
// it would suppress the stand-down flip on tatara's OWN merge requests as well.
//
// What WOULD close it is positive attribution of the drifted head: a
// LastExternalHeadSHA stamped on stampMRHead's non-attributable branch, with only
// this upgrade - never the flip - gated on it. DECLINED, not missed. A new CRD
// field is a major API change by rule 7; it still cannot tell a MISattributed
// push apart, by construction; and it makes #604's fix contingent on the
// human-push delivery actually arriving, with no sweep repair possible, since the
// sweep's live head read carries no attribution. That trades a narrow laundering
// window for a narrow stranding window on a path that has never carried a single
// Task. Status.HeadSHA is NOT the free version of it: mirror.go writes that
// unattributed on the sync cadence, so it means "the mirror synced", not "a
// non-bot pushed".
func (d *StageDriver) parkOwnerTask(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task) error {
	if task.Spec.Kind == SweepReviewKind || tatarav1alpha1.TaskDone(task) {
		return nil
	}
	if tatarav1alpha1.Parked(task) {
		if task.Spec.Kind == takeoverKind && task.Status.ParkReason == stage.ReasonImplementDeclined {
			return d.upgradeDeclinedTakeoverPark(ctx, proj, task)
		}
		return nil
	}
	if err := d.parkTask(ctx, proj, task, stage.ReasonOwnershipLost); err != nil {
		return fmt.Errorf("flip: park owner task: %w", err)
	}
	return nil
}

// handBackToReviewTask moves the MR mirror's controller ownership to the
// kind=review Task for this MR (re-minting it if it was never minted, or was
// reaped), so an external MR's review rounds and its next "take over" comment
// route to the review agent.
//
// The owner-ref write goes through mutateOwnerRefs, NOT a direct Update on
// the caller's mr: by the time flipToExternal calls this, it has already
// written mr's Status via objbudget.FitMergeRequest, which Gets, mutates, and
// Status().Updates a FRESH server copy - bumping the object's resourceVersion
// without ever touching the caller's local mr. A direct d.Update(ctx, mr)
// here would carry that now-stale resourceVersion and 409 DETERMINISTICALLY
// on the mainline path (a normal bot MR already controller-owned by its
// review Task): the flip would return an error before obs.OwnershipFlip
// fires, yet the server's Ownership status is already external, so the next
// reconcile's `Ownership == tatara` guard is false and hand-back never
// retries. mutateOwnerRefs re-Gets before mutating, sidestepping this.
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
	if err := d.mutateOwnerRefs(ctx, mr, func(fresh *tatarav1alpha1.MergeRequest) error {
		prev, _ := own.ControllerOwner(fresh)
		var from *tatarav1alpha1.Task
		if prev != "" && prev != reviewName {
			from = &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: prev, Namespace: proj.Namespace}}
		}
		own.AddPlainOwner(fresh, &review)
		return own.HandOverController(fresh, from, &review)
	}); err != nil {
		return fmt.Errorf("flip: hand back to review task: %w", err)
	}
	return nil
}

// reMintReviewOwner mints the MR's review Task via the shared intake funnel
// (Minter.MintReviewTask, reusing MintReviewStage exactly as the sweep/webhook
// do) when handBackToReviewTask finds none. MintReviewTask's own bind
// (Minter.ownMergeRequest) refuses to hand the controller flag over from
// anyone it is not explicitly told to expect - a guard built for the
// ORPHAN-mint case, where an unrecognized controller owner is a bug, not this
// flip's hand-back case, where the CURRENT controller (the takeover Task just
// parked above) is expected and about to be superseded. Fix #408: capture
// that owner as prevOwner BEFORE minting and thread it through as
// expectFrom, so ownMergeRequest hands the flag over atomically in its own
// single Update - no standalone Controller=false demote Update first, so no
// zero-controller window a RepairZeroController race could jump into.
func (d *StageDriver) reMintReviewOwner(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest) error {

	prevOwner, _ := own.ControllerOwner(mr) // "" when mr carries no controller yet
	pr := prRefFromMR(repo, mr)
	stg, reason := MintReviewStage(mr)
	_, outcome, err := d.minter().MintReviewTask(ctx, proj, repo, pr, mr, stg, reason, d.spiller(proj), prevOwner)
	if err != nil {
		return fmt.Errorf("flip: re-mint review task: %w", err)
	}
	if outcome == MintTombstoneDeleted {
		// The mint is still OWED: this MR has no review owner yet. Return an
		// error so the MergeRequest reconciler requeues rather than reporting a
		// hand-back that did not happen.
		return fmt.Errorf("flip: re-mint review task for %s deleted a stale terminal task; the mint is still owed", mr.Name)
	}
	return nil
}

// mutateOwnerRefs re-Gets mr FRESH from the server, applies mutate to that
// fresh copy's owner refs, and writes it back under RetryOnConflict - so an
// owner-ref handover always lands on a CURRENT resourceVersion, never a
// caller's possibly-stale local copy. It is the shared primitive behind
// handBackToReviewTask's existing-review-Task branch: it mutates owner refs
// on an mr whose Status a sibling call (flipToExternal's
// objbudget.FitMergeRequest) may have JUST written server-side, which bumps
// resourceVersion without the caller's local copy knowing - a direct
// d.Update(ctx, mr) in that situation 409s deterministically. On success, *mr
// is overwritten with the server's fresh post-write copy.
func (d *StageDriver) mutateOwnerRefs(ctx context.Context, mr *tatarav1alpha1.MergeRequest,
	mutate func(fresh *tatarav1alpha1.MergeRequest) error) error {
	return MutateOwnerRefs(ctx, d.Client, mr, mutate)
}

// MutateOwnerRefs is the exported, client-only form of mutateOwnerRefs (see
// its docs for why a fresh-Get+RetryOnConflict loop is required instead of a
// bare Update on a possibly-stale mr). OP9's takeover REST endpoint uses this
// directly - it has no StageDriver, only a client.Client - to move the MR
// mirror's controller ownership onto the takeover Task under the exact same
// conflict-safe discipline every other owner-ref write in this package uses.
func MutateOwnerRefs(ctx context.Context, c client.Client, mr *tatarav1alpha1.MergeRequest,
	mutate func(fresh *tatarav1alpha1.MergeRequest) error) error {

	return MutateArtifactOwnerRefs(ctx, c, mr, mutate)
}

// ErrOwnerRefsUnchanged is the sentinel a MutateArtifactOwnerRefs mutation
// returns when, HAVING SEEN THE FRESH COPY, it decides there is nothing to
// write. The helper swallows it and returns nil without issuing an Update.
//
// It exists because the fresh Get is what makes the decision trustworthy: a
// caller that pre-decides on its own (possibly cached) copy and then only
// mutates inside the loop is back to acting on stale state, which is the whole
// defect issue #524 documents. Deciding INSIDE the mutation and bailing out
// with this sentinel keeps the read and the decision on the same object
// version.
var ErrOwnerRefsUnchanged = errors.New("owner refs unchanged")

// MutateArtifactOwnerRefs is MutateOwnerRefs for ANY mirrored artifact - Issue
// as well as MergeRequest. It re-Gets obj FRESH from the server, applies mutate
// to that fresh copy, and writes it back under RetryOnConflict; on success *obj
// is overwritten with the server's post-write copy.
//
// Issue #524: the reaper's B.5 handover (releaseOwnership) was the ONE owner-ref
// write in the tree that did not do this - it mutated a copy handed to it by the
// controller-runtime CACHE and issued a bare Update - and it is the one that left
// mr-tatara-operator-504 with zero controller owners. MutateOwnerRefs could not
// be reused there because it was typed to *MergeRequest and the reaper releases
// Issues too, so the discipline was generalised rather than duplicated.
func MutateArtifactOwnerRefs[T any, PT interface {
	client.Object
	*T
}](ctx context.Context, c client.Client, obj PT, mutate func(fresh PT) error) error {

	key := client.ObjectKeyFromObject(obj)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh T
		p := PT(&fresh)
		if err := c.Get(ctx, key, p); err != nil {
			return err
		}
		if err := mutate(p); err != nil {
			if errors.Is(err, ErrOwnerRefsUnchanged) {
				return nil
			}
			return err
		}
		if err := c.Update(ctx, p); err != nil {
			return err
		}
		*obj = fresh
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
