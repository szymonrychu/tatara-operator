package controller

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// redeliverMRComments is OP12's job: replaying comments an external MR missed
// while it had no owning Task in scope, or that simply arrived between sweeps.
// Called only by ReconcileOwnership, with incoming already narrowed to
// comments newer than the mirror's LastMirroredCommentID cursor
// (listPRCommentsAfter, sweep.go) - never a re-derivation of that filter here.
//
// For every new comment it: mirrors it onto the MR CR
// (AppendCommentToMirror - the SAME C.5 write-back helper the webhook uses),
// and - if the MR carries a controller owner - delivers it as an mr_comment
// TaskEvent via AppendTaskEvent, the SAME capped-append the webhook fast path
// uses (OP12 relocated it to this package precisely so the two paths share
// one implementation, never two that can drift). The cursor advances to the
// newest comment actually processed, in ONE write at the end, so a crash
// mid-loop re-runs as a partial no-op (already-mirrored comments dedup on
// ExternalID) rather than a lost cursor advance.
//
// Belt-and-suspenders (the OP6 convergence amendment): sweepPRs already mints
// the review Task for an in-scope orphan MR on the PRReview classification
// (ClassifyPR -> MintReviewTask) - the SAME rule EnsureTaskForMRComment (OP6)
// uses on the webhook fast path - so by the time this runs, an owner normally
// already exists. If ControllerOwner is still empty (a caller reaching here
// without going through that mint), EnsureTaskForMRComment - the exact
// function the webhook calls - is run here too, so the webhook fast path and
// this sweep convergence path never diverge on how an owner gets minted.
func (d *StageDriver) redeliverMRComments(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest, incoming []scm.IssueComment) error {

	// LIVENESS, not presence (issue #521). own.ControllerOwner alone accepts a
	// ref naming a reaped Task, so this path skipped the mint below AND then
	// tried to deliver every replayed comment to a Task that does not exist -
	// each one a silent NotFound. The resolver drops the ref, so hasOwner goes
	// false and EnsureTaskForMRComment mints the owner this batch needs.
	ownerName, lerr := d.minter().resolveLiveMROwner(ctx, proj, mr, SweepActivity)
	if lerr != nil {
		return lerr
	}
	hasOwner := ownerName != ""
	if !hasOwner && len(incoming) > 0 {
		// incoming[0].Author is the mint's author-of-record for this belt-and-
		// suspenders path; it relies on the sweep's prior PRReview
		// classification (ClassifyPR -> MintReviewTask, see above) having
		// already filtered out bot-first pages, making a bot incoming[0] here
		// unreachable in practice.
		newOwner, _, err := d.minter().EnsureTaskForMRComment(ctx, proj, repo, mr, incoming[0].Author)
		if err != nil {
			return err
		}
		if newOwner != "" {
			ownerName, hasOwner = newOwner, true
		}
	}

	newest := mr.Status.LastMirroredCommentID
	delivered := false
	for _, c := range incoming {
		if c.ExternalID == "" || c.ExternalID == mr.Status.LastMirroredCommentID {
			continue
		}
		if proj.Spec.Scm != nil && c.Author == proj.Spec.Scm.BotLogin {
			newest = c.ExternalID
			continue // never redeliver the bot's own comment
		}
		cmt := tatarav1alpha1.Comment{
			ExternalID: c.ExternalID, Author: c.Author, Body: c.Body, CreatedAt: metav1.NewTime(c.CreatedAt),
		}
		if err := AppendCommentToMirror(ctx, d.Client, d.spiller(proj), mr, cmt); err != nil {
			return err
		}
		if hasOwner {
			var task tatarav1alpha1.Task
			if err := d.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: ownerName}, &task); err == nil {
				ev := tatarav1alpha1.TaskEvent{
					At: metav1.Now(), Kind: "mr_comment", Repo: repo.Name,
					Number: mr.Spec.Number, Author: c.Author, Body: c.Body,
				}
				if err := AppendTaskEvent(ctx, d.Client, &task, ev); err != nil {
					return err
				}
				delivered = true
			} else if !apierrors.IsNotFound(err) {
				return err
			}
		}
		newest = c.ExternalID
	}
	// #519: redeliverMRComments used to stop at AppendTaskEvent, leaving a
	// parked(awaiting-human) review Task's own re-entry rule (#511/#515's
	// anyExternallyOwned round-cap bypass) with nothing to drive it - the
	// webhook fast path calls driveCommentUnpark right after its own
	// AppendTaskEvent (pending_events.go), but this sweep-convergence path
	// never called the ApplyUnpark equivalent at all. A "take over" comment
	// delivered ONLY through OP12 (a missed/late webhook delivery) then never
	// re-engaged the bot, no matter how many sweeps ran, because nothing here
	// ever re-evaluated the park. Mirror the webhook's own gate: an owner
	// Task parked for a comment-driven reason gets exactly one ApplyUnpark
	// pass per redelivery batch.
	if delivered {
		d.driveOwnerReentry(ctx, proj, ownerName)
	}
	if newest != mr.Status.LastMirroredCommentID {
		key := client.ObjectKeyFromObject(mr)
		return objbudget.FitMergeRequest(ctx, d.Client, d.spiller(proj), key, func(m *tatarav1alpha1.MergeRequest) {
			m.Status.LastMirroredCommentID = newest
		})
	}
	return nil
}

// driveOwnerReentry is redeliverMRComments' counterpart to the webhook fast
// path's driveCommentUnpark (pending_events.go): after delivering a fresh
// non-bot mr_comment TaskEvent to ownerName, re-evaluate that Task's park
// exactly once, for the SAME three comment-driven reasons the webhook drives
// (awaiting-human, backlog-sweep, identity-unverified) - most importantly
// awaiting-human on a kind=review Task, which is where #511/#515's
// anyExternallyOwned round-cap bypass lives. A live (non-parked) owner is left
// alone: OP12 has no EnterConversing counterpart here, and the webhook's own
// delivery already covers a live pod's follow-up turn.
//
// Best-effort, matching every other side effect ReconcileOwnership's callers
// tolerate: a failure here is logged and never fails the sweep/webhook
// request that triggered the redelivery, since the comment is already
// mirrored and queued and a later pass (this same function, or the periodic
// driveUnparks backstop) gets another chance.
func (d *StageDriver) driveOwnerReentry(ctx context.Context, proj *tatarav1alpha1.Project, ownerName string) {
	var task tatarav1alpha1.Task
	if err := d.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: ownerName}, &task); err != nil {
		if !apierrors.IsNotFound(err) {
			log.FromContext(ctx).Error(err, "redeliver: get owner task for reentry failed",
				"action", "redeliver_reentry_get_failed", "resource_id", ownerName)
		}
		return
	}
	if !tatarav1alpha1.Parked(&task) ||
		(task.Status.ParkReason != stage.ReasonAwaitingHuman &&
			task.Status.ParkReason != stage.ReasonBacklogSweep &&
			task.Status.ParkReason != stage.ReasonIdentityUnverified) {
		return
	}
	active, err := CountActiveTasks(ctx, d.Client, proj)
	if err != nil {
		log.FromContext(ctx).Error(err, "redeliver: count active tasks failed",
			"action", "redeliver_reentry_count_failed", "resource_id", ownerName)
		return
	}
	maxOpen := proj.Spec.MaxOpenTasks
	if maxOpen <= 0 {
		maxOpen = 6
	}
	liveRoom := false
	if NeedsLiveRoom(task.Status.ParkReason) {
		room, roomErr := LiveHasRoom(ctx, d.Client, proj)
		if roomErr != nil {
			log.FromContext(ctx).Error(roomErr, "redeliver: live capacity check failed; treating as no room",
				"action", "redeliver_reentry_room_failed", "resource_id", ownerName)
		} else {
			liveRoom = room
		}
	}
	unparked, decline, err := ApplyUnpark(ctx, d.Client, d.APIReader, proj, &task, active, maxOpen, liveRoom, time.Now())
	if err != nil {
		log.FromContext(ctx).Error(err, "redeliver: comment-driven unpark failed",
			"action", "redeliver_reentry_failed", "resource_id", ownerName)
		return
	}
	if !unparked {
		log.FromContext(ctx).Info("redeliver: comment-driven unpark declined",
			"action", "redeliver_reentry_declined", "resource_id", ownerName,
			"park_reason", task.Status.ParkReason, "decline_kind", string(decline))
		return
	}
	log.FromContext(ctx).Info("redeliver: unparked task on sweep-delivered comment",
		"action", "redeliver_reentry", "resource_id", ownerName, "state", task.Status.State,
		"reason_from", task.Status.ParkReason)
}
