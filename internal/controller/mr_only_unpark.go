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

// refuseMROnlyUnpark is implemented in the refusal arm.
func (r *ProjectReconciler) refuseMROnlyUnpark(_ context.Context, _ *tatarav1alpha1.Project,
	_ *tatarav1alpha1.Task, _ []tatarav1alpha1.MergeRequest, _ time.Time) error {
	return nil
}
