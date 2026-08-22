// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// mrOnlyUnparkOwnership answers the ONE question that routes a no-re-entry park
// away from resumeOne: does this Task own ZERO Issue mirrors and AT LEAST ONE
// MergeRequest mirror?
//
// IT ASKS WITH r.ownedIssues, NOT WITH len(t.Status.IssueRefs), and the two are
// not the same test. ownedIssues SKIPS a ref whose CR is gone, and resumeOne's
// own bail is computed from that same helper - so a cheaper predicate here could
// route a Task into this arm that resumeOne would also have declined, or the
// reverse, and leave it served by NEITHER. That is the defect being fixed, one
// population over. The cost is bounded: for the MR-only population IssueRefs is
// empty, so the loop does zero Gets.
func (r *ProjectReconciler) mrOnlyUnparkOwnership(ctx context.Context, t *tatarav1alpha1.Task) (
	[]tatarav1alpha1.MergeRequest, bool, error) {

	issues, err := r.ownedIssues(ctx, t)
	if err != nil {
		return nil, false, err
	}
	if len(issues) > 0 {
		return nil, false, nil
	}
	mrs, err := r.ownedMRs(ctx, t)
	if err != nil {
		return nil, false, err
	}
	return mrs, len(mrs) > 0, nil
}

// driveMROnlyUnpark picks ONE of two dispositions for a parked Task carrying a
// maintainer's comment that resumeOne would have swallowed:
//
//	RELEASE   the park is an implementation-phase one, so the flag is cleared in
//	          place and the Task resumes where it stopped;
//	REFUSE    the park was written AT OR AFTER THE MERGE, so re-driving it risks
//	          re-issuing a merge that may have partially succeeded (#597) - the
//	          Task is left exactly as it is and the merge request is told why.
//
// The two are total over the input, and that totality IS the fix. One comment
// gets one answer: an un-park, or an explanation of why not. Silence is what
// containers!1300 got, and silence is what this exists to remove.
func (r *ProjectReconciler) driveMROnlyUnpark(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest, now time.Time) error {

	// READ BEFORE THE RELEASE. applyMROnlyUnpark writes the un-parked object back
	// over t and the release clears parkReason, so a reason read afterwards is
	// unconditionally empty - the bug ownership_redeliver.go carried for a
	// release, and the one driveCIRecoveryUnparks names in its own comment.
	reason := t.Status.ParkReason
	if stage.IsMergeStagePark(reason) {
		return r.refuseMROnlyUnpark(ctx, proj, t, mrs, now)
	}
	released, err := r.applyMROnlyUnpark(ctx, proj, t, now)
	if err != nil {
		return err
	}
	if !released {
		// Something else moved this Task between the List and the live re-read -
		// most plausibly driveCIRecoveryUnparks, which shares implement-declined
		// with this arm and runs earlier in the same Reconcile pass. Nothing was
		// spent and nothing is owed, so this is not logged as a release.
		return nil
	}
	r.Metrics.MROnlyUnpark(proj.Name, reason, obs.MROnlyUnparkReleased)
	// The shared "every park release" series (operator_task_unparked_total),
	// under its own class label. driveRetiredUnparks and driveCIRecoveryUnparks
	// both feed it under their own class on every release they make; a release
	// this driver performs that never touched it would be an observability blind
	// spot on the one series that already answers "how many parks were released,
	// and by what".
	r.Metrics.TaskUnparked(reason, obs.UnparkClassMROnly)
	log.FromContext(ctx).Info("released an mr-only park on a maintainer comment",
		"action", "mr_only_unpark", "resource_id", t.Name, "park_reason", reason,
		"kind", t.Spec.Kind, "state", t.Status.State, "project", proj.Name)
	return nil
}

// applyMROnlyUnpark persists stage.UnparkMaintainerComment under optimistic
// concurrency, re-reading through the UNCACHED APIReader and re-checking the
// park under the retry, exactly as applyRetiredUnpark and applyCIRecoveryUnpark
// do and for the same cache-lag reason.
//
// THE EVENT IS RE-CHECKED TOO, and that conjunct is not decoration: this driver
// has no annotation latch, so the ONLY thing that stops it releasing the same
// park twice is the UnparkConsumedAt stamp. Re-reading the live copy inside the
// retry is what makes the check see a stamp another writer just landed.
//
// released is false, with no error, when the live Task has drifted. There is
// nothing to log and nothing to count: a release that did not happen must never
// read as one.
func (r *ProjectReconciler) applyMROnlyUnpark(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, now time.Time) (bool, error) {

	getter := client.Reader(r.APIReader)
	if getter == nil {
		getter = r.Client
	}
	key := client.ObjectKeyFromObject(t)
	want := t.Status.ParkReason
	bot := botLoginOf(proj)
	released := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		released = false
		fresh := &tatarav1alpha1.Task{}
		if err := getter.Get(ctx, key, fresh); err != nil {
			return err
		}
		if !tatarav1alpha1.Parked(fresh) || fresh.Status.ParkReason != want ||
			!hasNonBotPendingEvent(fresh, bot) {
			return nil
		}
		if err := stage.UnparkMaintainerComment(fresh, bot, now); err != nil {
			return err
		}
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*t = *fresh
		released = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("resume: release the mr-only park on %s: %w", key.Name, err)
	}
	return released, nil
}

// mrOnlyRefusalPendingCap bounds status.pendingComments on a merge request
// mirror the same way restapi's pendingCommentsCap and the deploy-timeout
// enqueue do. It is a literal rather than a shared constant because the
// restapi copy is unexported and hoisting it into api/v1alpha1 to be read
// twice is the abstraction hard rule 2 says not to build.
const mrOnlyRefusalPendingCap = 20

// refuseMROnlyUnpark is the arm that says NO, out loud. A merge-stage park plus
// a maintainer comment gets one notice on each OPEN owned merge request naming
// the blocker, a latch so it stays one, and the comment SPENT - because one
// comment gets one answer, and an explanation is an answer.
//
// THE ORDER IS enqueue, latch, spend, and every step of it fails in the harmless
// direction. The enqueue is idempotent twice over (the requestId is already in
// the pending list, and DrainPendingComments dedups the same id on the forge
// thread), so latching AFTER it costs at worst a duplicate entry the mirror
// drops, and never a lost notice - the direction AnnRetryExhaustedCommented
// picks, and the opposite of driveRetiredUnparks' latch, which guards a
// TOKEN-BURNING loop rather than a comment. Spending last means a crash before
// it leaves the comment unspent and the notice latched, so the next pass is a
// no-op that has already said its piece.
//
// IT DOES NOT CLOSE, LABEL OR TOUCH THE MERGE REQUEST. That is the whole point:
// the work is finished and reviewed, and the blocker is outside the code.
func (r *ProjectReconciler) refuseMROnlyUnpark(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest, now time.Time) error {

	reason := t.Status.ParkReason
	latch := parkedAt(t).UTC().Format(time.RFC3339)
	if t.Annotations[tatarav1alpha1.AnnMROnlyUnparkRefused] == latch {
		return nil // this park has already had its one answer
	}
	body := mrOnlyUnparkRefusalComment(t)
	requestID := fmt.Sprintf("mr-only-unpark-refused-%s-%s", t.Name, latch)
	for i := range mrs {
		mr := &mrs[i]
		if mr.Status.State != "open" {
			continue // a closed merge request has no thread worth writing to
		}
		key := client.ObjectKeyFromObject(mr)
		if err := objbudget.FitMergeRequest(ctx, r.Client, r.spillerFor(proj), key,
			func(cur *tatarav1alpha1.MergeRequest) {
				for _, pc := range cur.Status.PendingComments {
					if pc.RequestID == requestID {
						return
					}
				}
				if len(cur.Status.PendingComments) >= mrOnlyRefusalPendingCap {
					return
				}
				cur.Status.PendingComments = append(cur.Status.PendingComments,
					tatarav1alpha1.PendingComment{RequestID: requestID, Action: "comment", Body: body})
			}); err != nil {
			return fmt.Errorf("resume: queue the mr-only refusal notice on %s: %w", key.Name, err)
		}
	}
	if err := r.annotateTask(ctx, t, tatarav1alpha1.AnnMROnlyUnparkRefused, latch); err != nil {
		return fmt.Errorf("resume: latch the mr-only refusal on %s: %w", t.Name, err)
	}
	if err := r.spendMaintainerComment(ctx, proj, t, now); err != nil {
		return err
	}
	r.Metrics.MROnlyUnpark(proj.Name, reason, obs.MROnlyUnparkRefused)
	log.FromContext(ctx).Info("refusing an mr-only un-park: the park was written at or after the merge",
		"action", "mr_only_unpark_refused", "resource_id", t.Name, "park_reason", reason,
		"kind", t.Spec.Kind, "state", t.Status.State, "project", proj.Name)
	return nil
}

// spendMaintainerComment stamps UnparkConsumedAt on every unspent non-bot
// pendingEvent WITHOUT releasing anything. It is what makes the refusal an
// ANSWER rather than a Task that re-evaluates the same comment every 60s
// forever - the loop consumeUnparkEvents' own doc was written to kill.
//
// Live-read and re-checked under the retry, like applyMROnlyUnpark: a stamp
// another writer just landed must be visible, or this spends nothing twice.
func (r *ProjectReconciler) spendMaintainerComment(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, now time.Time) error {

	getter := client.Reader(r.APIReader)
	if getter == nil {
		getter = r.Client
	}
	key := client.ObjectKeyFromObject(t)
	bot := botLoginOf(proj)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := getter.Get(ctx, key, fresh); err != nil {
			return err
		}
		if !hasNonBotPendingEvent(fresh, bot) {
			return nil
		}
		stage.ConsumeUnparkEvents(fresh, bot, now)
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*t = *fresh
		return nil
	}); err != nil {
		return fmt.Errorf("resume: spend the maintainer comment on %s: %w", key.Name, err)
	}
	return nil
}

// mrOnlyUnparkRefusalComment tells the maintainer the three things that separate
// this stop from every other one: the implementation is FINISHED, the merge
// request is still there and untouched, and a reply will not restart anything.
// It is mergeStageParkComment's sibling on the merge request instead of the
// issue, and it deliberately does NOT invite a reply: there is no Issue to
// re-mint from, so the only remedy is a human clearing the blocker.
func mrOnlyUnparkRefusalComment(t *tatarav1alpha1.Task) string {
	return fmt.Sprintf(
		"tatara read the comment and is NOT restarting task `%s`: the implementation finished and was "+
			"reviewed, and the task then stopped at the MERGE with `%s`.\n\n"+
			"That blocker is outside the code - a credential, a protected branch, a sibling repo that "+
			"already landed, or somebody else pushing to this branch - so re-running the agent cannot "+
			"clear it, and re-driving a merge that may have partially succeeded is how a double-merge "+
			"happens. This merge request is therefore left exactly as it is.\n\n"+
			"Clear the blocker and merge it by hand. Further comments here will not restart the task.",
		t.Name, t.Status.ParkReason)
}
