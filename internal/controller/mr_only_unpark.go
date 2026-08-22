// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"errors"
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
// a maintainer comment gets one notice on each OWNED merge request that still
// has a thread worth writing to, a latch so it stays one, and the comment
// SPENT - because one comment gets one answer, and an explanation is an
// answer.
//
// "STILL HAS A THREAD" MEANS state != closed, NOT state == open. deploy-blocked
// and deploy-timeout are merge-stage parks written AFTER a successful merge
// (stage.mergeStageParks' own doc), so their merge request is "merged" by
// construction, never "open" again - and a merged thread is exactly as
// readable and writable as an open one. Round-1 review caught this
// empirically: excluding "merged" reproduced the containers!1300 silence this
// whole change exists to remove, relocated into the arm meant to fix it.
//
// THE ORDER IS enqueue, then - ONLY IF SOMETHING WAS ACTUALLY QUEUED - latch
// and count, then ALWAYS spend. Latching or counting a refusal that reached
// nobody (every owned MR closed, or the cap already full) is the same
// swallowed-comment defect one level down, so posted==0 returns before either
// - the comment stays unspent and the next pass gets to try again, the way a
// closed MR reopening or a new owned MR arriving would resolve it. Spend is
// UNCONDITIONAL once past that gate, including on the already-latched path:
// see spendMaintainerComment's own doc for why a second comment on an answered
// park must still be spent.
//
// The enqueue is idempotent twice over (the requestId is already in the
// pending list, and DrainPendingComments dedups the same id on the forge
// thread), so a retried enqueue costs at worst a duplicate entry the mirror
// drops, never a lost notice - the direction AnnRetryExhaustedCommented picks,
// and the opposite of driveRetiredUnparks' latch, which guards a
// TOKEN-BURNING loop rather than a comment.
//
// IT DOES NOT CLOSE, LABEL OR TOUCH THE MERGE REQUEST. That is the whole point:
// the work is finished and reviewed, and the blocker is outside the code.
//
// IT LIVE-READS THE TASK FIRST, through the UNCACHED APIReader, exactly as
// applyMROnlyUnpark does before its release and for a strictly larger stake.
// The park this arm is about to describe OUT LOUD on a merge request came off
// resumeNoReentryParks' List snapshot, and stage.UnparkForMRTerminal clears a
// merge-stage park the moment the MR reaches a terminal forge state. Refusing
// off a stale snapshot posts a PUBLIC notice claiming the task "stopped at the
// MERGE with <reason>" on an MR that is no longer in that state, and spends the
// maintainer's comment against it. A forge comment is the most expensive thing
// this driver does, so it gets the freshest read available, not the stalest.
func (r *ProjectReconciler) refuseMROnlyUnpark(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest, now time.Time) error {

	getter := client.Reader(r.APIReader)
	if getter == nil {
		getter = r.Client
	}
	taskKey := client.ObjectKeyFromObject(t)
	want := t.Status.ParkReason
	fresh := &tatarav1alpha1.Task{}
	if err := getter.Get(ctx, taskKey, fresh); err != nil {
		return fmt.Errorf("resume: live-read %s before refusing the mr-only un-park: %w", taskKey.Name, err)
	}
	if !tatarav1alpha1.Parked(fresh) || fresh.Status.ParkReason != want ||
		!hasNonBotPendingEvent(fresh, botLoginOf(proj)) {
		// The Task drifted between the List and this read - the park was released
		// (UnparkForMRTerminal on an MR that just reached a terminal forge state
		// is the realistic writer), re-parked under a different reason, or the
		// comment was already spent by another writer. Nothing was posted and
		// nothing is owed: the next pass re-reads and routes the comment to
		// whichever arm is correct then. Symmetric with applyMROnlyUnpark's own
		// drifted-Task return, which is likewise silent and uncounted.
		return nil
	}
	// From here on the LIVE copy is the one every decision reads: the refusal
	// latch lives in metadata, and a cached annotation map that has not yet seen
	// this Task's own annotateTask write is how the same park gets answered twice.
	*t = *fresh

	reason := t.Status.ParkReason
	latch := parkedAt(t).UTC().Format(time.RFC3339)
	if t.Annotations[tatarav1alpha1.AnnMROnlyUnparkRefused] != latch {
		body := mrOnlyUnparkRefusalComment(t)
		requestID := fmt.Sprintf("mr-only-unpark-refused-%s-%s", t.Name, latch)
		posted := 0
		for i := range mrs {
			mr := &mrs[i]
			if mr.Status.State == "closed" {
				continue // no thread left to write to; "merged" still has one
			}
			key := client.ObjectKeyFromObject(mr)
			// present, not "newly appended": a retry that finds this exact
			// requestId already on the mirror (crash between enqueue and latch,
			// or the cache-lag race the metric comment below documents) still
			// counts as an answer that reached this MR, and must still satisfy
			// the posted==0 gate below - or a crashed retry would find the
			// notice already there, count it as nothing, and leave the comment
			// permanently unspent instead of finishing the latch it interrupted.
			present := false
			if err := objbudget.FitMergeRequest(ctx, r.Client, r.spillerFor(proj), key,
				func(cur *tatarav1alpha1.MergeRequest) {
					// RESET PER INVOCATION, for applyMROnlyUnpark's exact reason -
					// its own retry closure opens with released = false. This
					// closure is called TWICE by FitMergeRequest: once on the
					// candidate it sizes, once on the fresh copy inside its retry,
					// and only the second call is the one that persists. A present
					// left sticky from the first call survives a second call that
					// returned early at the cap, so posted counts a notice that was
					// never written: the latch is stamped and the maintainer's
					// comment is spent with nothing queued on this MR. That is
					// precisely the swallow the posted==0 gate below exists to stop.
					present = false
					for _, pc := range cur.Status.PendingComments {
						if pc.RequestID == requestID {
							present = true
							return
						}
					}
					if len(cur.Status.PendingComments) >= mrOnlyRefusalPendingCap {
						return
					}
					cur.Status.PendingComments = append(cur.Status.PendingComments,
						tatarav1alpha1.PendingComment{RequestID: requestID, Action: "comment", Body: body})
					present = true
				}); err != nil {
				if errors.Is(err, objbudget.ErrObjectTooLarge) {
					// PERMANENT, and dropped ON PURPOSE rather than propagated. A
					// mirror that cannot be fitted under the byte budget fails this
					// way on EVERY retry, and this driver runs from the project
					// reconcile AHEAD of driveStrandedParksPaced and
					// ReapTerminalPaced - an error returned here short-circuits both
					// of them for the whole Project, on every pass, forever. One fat
					// MR mirror must not take the reaper off the air: a refusal
					// notice is best-effort, the reaper is not.
					//
					// ONLY this class is dropped. Every other enqueue failure is
					// transient, so it still propagates and the posted==0 gate
					// leaves the comment unspent for the next pass to answer
					// properly - dropping those too would silently forfeit the one
					// notice on a blip. The release path (applyMROnlyUnpark) is
					// untouched and still propagates everything it fails on.
					log.FromContext(ctx).Error(err, "resume: the mr-only refusal notice does not fit on this merge request mirror",
						"action", "mr_only_unpark_refusal_dropped", "resource_id", t.Name,
						"merge_request", key.Name, "park_reason", reason, "project", proj.Name)
					continue
				}
				return fmt.Errorf("resume: queue the mr-only refusal notice on %s: %w", key.Name, err)
			}
			if present {
				posted++
			}
		}
		if posted == 0 {
			// Nothing was told: every owned MR was closed, already at the
			// pending-comments cap, or too large to fit the notice at all.
			// Latching or spending here would answer a comment that received no
			// answer anywhere - leave both alone so the next pass tries again.
			return nil
		}
		if err := r.annotateTask(ctx, t, tatarav1alpha1.AnnMROnlyUnparkRefused, latch); err != nil {
			return fmt.Errorf("resume: latch the mr-only refusal on %s: %w", t.Name, err)
		}
		// operator_mr_only_unpark_total{outcome=refused} can still overcount by
		// one, in a window the live read at the top of this function narrows but
		// does not close: a CRASH between the FitMergeRequest enqueue and the
		// annotateTask latch. The next pass live-reads a Task with no latch, finds
		// the notice already present on the mirror (posted counts presence,
		// deliberately, so exactly that interrupted retry can finish the latch
		// instead of finding posted==0 forever), and increments a second time for
		// the SAME park. The notice itself never double-posts - the requestId
		// dedup above is exact, and DrainPendingComments dedups again on the forge
		// thread - so the bound this series carries is "at least one refusal per
		// merge-stage park it answers", not "exactly one". The maintainer-facing
		// guarantee (one notice) is unaffected; only this counter's exactness is.
		r.Metrics.MROnlyUnpark(proj.Name, reason, obs.MROnlyUnparkRefused)
		log.FromContext(ctx).Info("refusing an mr-only un-park: the park was written at or after the merge",
			"action", "mr_only_unpark_refused", "resource_id", t.Name, "park_reason", reason,
			"kind", t.Spec.Kind, "state", t.Status.State, "project", proj.Name)
	}
	return r.spendMaintainerComment(ctx, proj, t, now)
}

// spendMaintainerComment stamps UnparkConsumedAt on every unspent non-bot
// pendingEvent WITHOUT releasing anything. It is what makes the refusal an
// ANSWER rather than a Task that re-evaluates the same comment every 60s
// forever - the loop consumeUnparkEvents' own doc was written to kill.
//
// refuseMROnlyUnpark calls this UNCONDITIONALLY, on the already-latched path
// too - round-1 review caught the alternative empirically: a second maintainer
// comment landing on a park that already had its one notice was never spent,
// because the latch's early return sat in front of the spend. The comment then
// stayed unspent forever, and applyMROnlyUnpark's own hasNonBotPendingEvent
// check would auto-release the NEXT park this Task takes under a non-merge-
// stage reason, using a comment that was never an answer to that park at all.
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
// request is left exactly as it stands, and a reply will not restart anything.
// It is mergeStageParkComment's sibling on the merge request instead of the
// issue, and it deliberately does NOT invite a reply: there is no Issue to
// re-mint from, so the only remedy is a human clearing the blocker.
//
// IT BRANCHES ON PRE-MERGE VERSUS POST-MERGE, and that split is not
// decoration: deploy-timeout and deploy-blocked (mergeStageParks' own doc)
// are written AFTER a successful merge, so a single sentence claiming the
// task "stopped at the merge" and inviting the maintainer to "merge it by
// hand" would be false for exactly the reasons finding-1's fix newly reaches -
// a merged merge request is not waiting to be merged, and there is nothing
// left to merge by hand.
//
// ReasonDeployTimeout IS INTENTIONALLY UNREACHABLE HERE and stays in the
// condition anyway: deploy-timeout is UnparkTimer, so resume.go's class filter
// drops it before this driver ever sees it. It is belt and braces against that
// classification moving - a post-merge reason that fell through to the
// pre-merge sentence would tell a maintainer to "merge it by hand" on a merge
// request that is already merged. Do not "simplify" it away.
func mrOnlyUnparkRefusalComment(t *tatarav1alpha1.Task) string {
	reason := t.Status.ParkReason
	if reason == stage.ReasonDeployTimeout || reason == stage.ReasonDeployBlocked {
		return fmt.Sprintf(
			"tatara read the comment and is NOT restarting task `%s`: the implementation finished, was "+
				"reviewed, and this merge request was ALREADY MERGED. The task then stopped AFTER THE "+
				"MERGE, at the deploy step, with `%s`.\n\n"+
				"That blocker is outside the code - most likely the deploy pipeline or the deployed "+
				"cluster itself - so re-running the agent cannot clear it, and re-driving a task whose "+
				"merge already happened is how a double-merge happens. This merge request is therefore "+
				"left exactly as it is: merged, and untouched from here.\n\n"+
				"Clear the deploy blocker by hand, outside this merge request. Further comments here "+
				"will not restart the task.",
			t.Name, reason)
	}
	return fmt.Sprintf(
		"tatara read the comment and is NOT restarting task `%s`: the implementation finished and was "+
			"reviewed, and the task then stopped at the MERGE with `%s`.\n\n"+
			"That blocker is outside the code - a credential, a protected branch, a sibling repo that "+
			"already landed, or somebody else pushing to this branch - so re-running the agent cannot "+
			"clear it, and re-driving a merge that may have partially succeeded is how a double-merge "+
			"happens. This merge request is therefore left exactly as it is.\n\n"+
			"Clear the blocker and merge it by hand. Further comments here will not restart the task.",
		t.Name, reason)
}
