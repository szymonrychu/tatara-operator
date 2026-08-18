// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// parkedMergedFixture is THE MEASURED PRODUCTION SHAPE, 7 out of 7 on
// 2026-08-10: a kind=review Task that posted its verdict and parked, and whose
// PR a maintainer then merged one to twenty minutes later. The Task never
// notices the world moved.
func parkedMergedFixture(t *testing.T, name, kind, parkReason, mrState string) (
	*tatarav1alpha1.Project, *tatarav1alpha1.Task, *tatarav1alpha1.MergeRequest) {

	t.Helper()
	proj := tsProject(3)
	task := tsTask(name, kind, tatarav1alpha1.StateAwaitingReview, time.Now().Add(-time.Hour))
	parked := metav1.NewTime(time.Now().Add(-time.Hour))
	task.Status.ParkReason = parkReason
	task.Status.ParkedAt = &parked
	task.Status.ParkedFromState = tatarav1alpha1.StateAwaitingReview
	mr := mdMR(task, "tatara-operator", 582)
	mr.Status.State = mrState
	return proj, task, mr
}

// drive runs ONE full TaskReconciler.Reconcile. It has to be the whole
// reconcile, not reconcileClocks directly: the defect is that Reconcile RETURNS
// before reconcileClocks for a parked Task, so a test that called the inner
// function would pass against the broken code.
func drive(t *testing.T, r *TaskReconciler, name string) {
	t.Helper()
	_, err := reconcileTask(t, r, name)
	require.NoError(t, err)
}

// TestParkedReviewTaskFinalizesWhenItsMRMerged IS THE BUG, measured 7/7 in
// production: every review Task whose PR was merged sat parked awaiting-human
// and never reached terminal.
//
// The mechanism is one step earlier than "stage.Enter refuses a parked Task".
// TaskReconciler.Reconcile returns for ANY parked Task before reconcileClocks
// runs at all, so externalTerminalEdge - which has computed exactly the right
// answer since #33 - is never even evaluated. stage.Enter's StillParkedError is
// only the second line of defence behind that.
//
// A park stops the Task making stage PROGRESS while it waits on a human. An MR
// reaching a terminal forge state is not progress: it is the world moving on,
// and it makes the wait pointless.
func TestParkedReviewTaskFinalizesWhenItsMRMerged(t *testing.T) {
	proj, task, mr := parkedMergedFixture(t, "mt-r-582", "review", stage.ReasonAwaitingHuman, "merged")
	c := newMirrorClient(t, proj, mdSecret(), task, mr)

	drive(t, tsReconciler(c), "mt-r-582")

	got := mdGetTask(t, c, "mt-r-582")
	require.Equal(t, tatarav1alpha1.StateDone, got.Status.State,
		"a parked review task whose every MR merged must take its terminal edge, not wait out ParkRetention")
	require.Equal(t, stage.ReasonMRMergedExternally, got.Status.StateReason)
	require.False(t, tatarav1alpha1.Parked(got), "the park is cleared by the same write that goes terminal")
}

// It must hold for EVERY park reason, not just awaiting-human. handoff-stalled,
// no-outcome and the whole UnparkNever set land a review Task in the identical
// place, and a fix keyed on one reason would leave the others sitting.
func TestParkedReviewTaskFinalizesUnderEveryParkReason(t *testing.T) {
	for _, reason := range []string{
		stage.ReasonAwaitingHuman,  // UnparkHuman: the measured shape
		stage.ReasonHandoffStalled, // UnparkHuman
		stage.ReasonNoOutcome,      // UnparkTimer
		stage.ReasonOperatorError,  // UnparkNever
		stage.ReasonStageDeadline,  // UnparkNever
		stage.ReasonCIRed,          // UnparkNever
		stage.ReasonBacklogSweep,   // the zero-cost owner
	} {
		t.Run(reason, func(t *testing.T) {
			proj, task, mr := parkedMergedFixture(t, "mt-r-1", "review", reason, "merged")
			c := newMirrorClient(t, proj, mdSecret(), task, mr)

			drive(t, tsReconciler(c), "mt-r-1")

			got := mdGetTask(t, c, "mt-r-1")
			require.Equal(t, tatarav1alpha1.StateDone, got.Status.State, reason)
			require.False(t, tatarav1alpha1.Parked(got), reason)
		})
	}
}

// A CLOSED-unmerged MR is the other terminal forge state and takes the other
// terminal edge. Same argument: nothing is coming back to review.
func TestParkedReviewTaskFinalizesRejectedWhenItsMRClosed(t *testing.T) {
	proj, task, mr := parkedMergedFixture(t, "mt-r-583", "review", stage.ReasonAwaitingHuman, "closed")
	c := newMirrorClient(t, proj, mdSecret(), task, mr)

	drive(t, tsReconciler(c), "mt-r-583")

	got := mdGetTask(t, c, "mt-r-583")
	require.Equal(t, tatarav1alpha1.StateRejected, got.Status.State)
	require.Equal(t, stage.ReasonMRClosedExternally, got.Status.StateReason)
	require.False(t, tatarav1alpha1.Parked(got))
}

// THE SCOPE LINE, and it is the conservative one. A NON-review Task whose MRs
// all merged takes ownMRsShippedEdge to `merged`, which is NOT a terminal
// outcome - it still owes the merge cursor, the deploy ledger and the issue
// closes. Resuming a parked Task into that pipeline is real work restarted
// behind a human's back, which is exactly what a park exists to prevent. Going
// TERMINAL ends the Task and is the only thing this self-heal may do.
//
// Nothing is stranded by the exclusion: a non-review Task parked under an
// UnparkNever reason is C.3's, and one parked awaiting-human is genuinely
// waiting on a human who has not answered.
func TestParkedNonReviewTaskWithMergedMRStaysParked(t *testing.T) {
	proj, task, mr := parkedMergedFixture(t, "mt-i-44", "issue", stage.ReasonOperatorError, "merged")
	c := newMirrorClient(t, proj, mdSecret(), task, mr)

	drive(t, tsReconciler(c), "mt-i-44")

	got := mdGetTask(t, c, "mt-i-44")
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, got.Status.State,
		"`merged` is not a terminal outcome; a park must not be bypassed to restart a pipeline")
	require.True(t, tatarav1alpha1.Parked(got))
}

// ... but a non-review Task whose MR was CLOSED unmerged does go terminal: that
// edge IS rejected(mr-closed-externally), nothing restarts, and the Task is a
// corpse exactly as a review one would be.
func TestParkedNonReviewTaskFinalizesWhenItsMRClosed(t *testing.T) {
	proj, task, mr := parkedMergedFixture(t, "mt-i-45", "issue", stage.ReasonOperatorError, "closed")
	c := newMirrorClient(t, proj, mdSecret(), task, mr)

	drive(t, tsReconciler(c), "mt-i-45")

	got := mdGetTask(t, c, "mt-i-45")
	require.Equal(t, tatarav1alpha1.StateRejected, got.Status.State)
	require.Equal(t, stage.ReasonMRClosedExternally, got.Status.StateReason)
	require.False(t, tatarav1alpha1.Parked(got))
}

// The self-heal must NOT fire while any owned MR is still live: a half-merged
// multi-repo Task that finalized here would strand the unmerged repo, and a
// parked Task whose PR is still open is parked for a reason that still holds.
func TestParkedTaskWithALiveMRStaysParked(t *testing.T) {
	proj, task, mr := parkedMergedFixture(t, "mt-r-584", "review", stage.ReasonAwaitingHuman, "merged")
	open := mdMR(task, "charts", 12)
	open.Status.State = "open"
	c := newMirrorClient(t, proj, mdSecret(), task, mr, open)

	drive(t, tsReconciler(c), "mt-r-584")

	got := mdGetTask(t, c, "mt-r-584")
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, got.Status.State)
	require.True(t, tatarav1alpha1.Parked(got), "one live MR means the park still stands")
}

// A parked Task owning NO MergeRequest at all is untouched: the empty set is not
// terminal (AllMRsTerminal says so by design), and the takeover branch needs a
// non-empty status.mrRefs. This is the backlog-owner shape, which is most of the
// parked population.
func TestParkedTaskWithNoMRsIsUntouched(t *testing.T) {
	proj, task, _ := parkedMergedFixture(t, "mt-r-585", "review", stage.ReasonBacklogSweep, "merged")
	task.Status.MRRefs = nil
	c := newMirrorClient(t, proj, mdSecret(), task)

	drive(t, tsReconciler(c), "mt-r-585")

	got := mdGetTask(t, c, "mt-r-585")
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, got.Status.State)
	require.True(t, tatarav1alpha1.Parked(got))
}

// TestParkedFinalizeStillRunsTheC2IssueTreatment is the point of routing this
// through the choke point rather than writing status directly. A review Task
// going terminal is EXACTLY when its still-open issue must not be silently
// dropped: it gets the terminal notice and the tatara-parked label, so the next
// sweep re-mints it parked(backlog-sweep) at zero pods instead of ACTIVE.
func TestParkedFinalizeStillRunsTheC2IssueTreatment(t *testing.T) {
	proj, task, mr := parkedMergedFixture(t, "mt-r-586", "review", stage.ReasonAwaitingHuman, "merged")
	issName := tatarav1alpha1.IssueName("tatara-operator", 7)
	task.Status.IssueRefs = []string{issName}
	iss := ownedIssue(issName, 7, task, tatarav1alpha1.IssueStatus{State: "open", Author: "maintainer"})

	c := newMirrorClient(t, proj, mdSecret(), mdRepo("tatara-operator"), task, mr, iss)
	w := newStrandWriter()
	r := tsReconciler(c)
	r.SCMFor = scmForOf(w)

	drive(t, r, "mt-r-586")

	require.Equal(t, tatarav1alpha1.StateDone, mdGetTask(t, c, "mt-r-586").Status.State)
	require.True(t, w.labelled("szymonrychu/tatara-operator#7", TataraParkedLabel),
		"C.2 must still run on a park-clearing terminal transition")
	require.True(t, w.commentedWith("tatara has stopped working this issue"))
}

// TestParkedReviewTaskWithOnlyAnOpenMRStaysParked pins THE 5, and it is the
// test that matters most at this population's ratio.
//
// Of the 37 awaiting-review/awaiting-human Tasks measured on 2026-08-10, 32 own
// a MERGED MR and 5 own an OPEN one. Those 5 are parked CORRECTLY - a human
// genuinely has not replied - and at 32-versus-5 a predicate that is slightly
// too broad still looks like it works. The predicate is therefore "every owned
// MR reached a terminal forge state" (stage.AllMRsTerminal, via
// externalTerminalEdge) and never "the Task is parked awaiting-human".
func TestParkedReviewTaskWithOnlyAnOpenMRStaysParked(t *testing.T) {
	proj, task, mr := parkedMergedFixture(t, "mt-r-587", "review", stage.ReasonAwaitingHuman, "open")
	c := newMirrorClient(t, proj, mdSecret(), task, mr)

	drive(t, tsReconciler(c), "mt-r-587")

	got := mdGetTask(t, c, "mt-r-587")
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, got.Status.State,
		"an open MR means the human's reply is still genuinely owed; this must not fire")
	require.True(t, tatarav1alpha1.Parked(got))
	require.Equal(t, stage.ReasonAwaitingHuman, got.Status.ParkReason,
		"and the park reason is untouched")
}

// TestParkedRefinedTaskIsUntouched covers the refined/awaiting-human population
// (5 measured). A Task at `refined` has not implemented anything yet, so it owns
// no MergeRequest and there is no external forge fact to notice: it is waiting
// on a human who has genuinely not answered, which is a CORRECT park, not a
// strand. driveUnparks resumes it on a comment and reapParked collects it at
// ParkRetention, so it is owned either way.
func TestParkedRefinedTaskIsUntouched(t *testing.T) {
	proj := tsProject(3)
	task := tsTask("mt-i-refined", "issue", tatarav1alpha1.StateRefined, time.Now().Add(-time.Hour))
	parked := metav1.NewTime(time.Now().Add(-time.Hour))
	task.Status.ParkReason = stage.ReasonAwaitingHuman
	task.Status.ParkedAt = &parked
	c := newMirrorClient(t, proj, mdSecret(), task)

	drive(t, tsReconciler(c), "mt-i-refined")

	got := mdGetTask(t, c, "mt-i-refined")
	require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State)
	require.True(t, tatarav1alpha1.Parked(got))
}

// TestParkedStageDeadlineReviewTaskFinalizes covers awaiting-review/stage-deadline
// (3 measured): stage-deadline is UnparkNever, so before this the Task had no
// exit but ParkRetention. A review-kind one with a merged MR is claimed HERE,
// not by the C.3 driver - C.3 works from owned ISSUES and a review Task owns
// MergeRequests, so the two never contend for the same Task.
func TestParkedStageDeadlineReviewTaskFinalizes(t *testing.T) {
	proj, task, mr := parkedMergedFixture(t, "mt-r-588", "review", stage.ReasonStageDeadline, "merged")
	c := newMirrorClient(t, proj, mdSecret(), task, mr)

	drive(t, tsReconciler(c), "mt-r-588")

	require.Equal(t, tatarav1alpha1.StateDone, mdGetTask(t, c, "mt-r-588").Status.State)
}

// TestTerminalIssueReleaseIsPaced: a burst of terminal transitions must not turn
// into a burst of forge writes. When the pace limiter is spent the release is
// DEFERRED, not failed - the transition still lands, and the reap pass runs the
// identical sequence blocking with retries.
func TestTerminalIssueReleaseIsPaced(t *testing.T) {
	proj, task, mr := parkedMergedFixture(t, "mt-r-589", "review", stage.ReasonAwaitingHuman, "merged")
	issName := tatarav1alpha1.IssueName("tatara-operator", 9)
	task.Status.IssueRefs = []string{issName}
	iss := ownedIssue(issName, 9, task, tatarav1alpha1.IssueStatus{State: "open", Author: "maintainer"})

	c := newMirrorClient(t, proj, mdSecret(), mdRepo("tatara-operator"), task, mr, iss)
	w := newStrandWriter()

	spent := rate.NewLimiter(0, 0) // every Allow denies
	releaser := &TerminalReleaser{Client: c, SCMFor: scmForOf(w), Metrics: r2Metrics(), Limiter: spent}
	require.NoError(t, releaser.ReleaseTerminalIssues(context.Background(), task),
		"a paced deferral is not an error: the reaper still owes the release")
	require.Empty(t, w.comments, "no forge write may escape the limiter")
	require.Empty(t, w.labels)
}

// ---------------------------------------------------------------------------
// H2-E. THE STRAND THIS GUARD USED TO OWN, AND WHO OWNS IT NOW.
//
// The refusal above is deliberate and stays: `merged` is not a terminal
// outcome, so admitting it here would resume a live pipeline behind the back of
// the human the park was waiting for. What it does NOT answer is the shape it
// was measured against - an implement Task whose owned merge requests all
// landed, parked mid-corridor, its issues open forever (helmfile#27/#32,
// ansible!17 and !18, terraform!221). That Task is not waiting on a human at
// all; it is waiting on a red check or a dirty branch.
//
// H2-C moved exactly those parks into the retry lane, so the answer is now
// upstream of this file: the park RELEASES ITSELF, the Task re-enters the
// ordinary reconcile path at awaiting-review, and the merge corridor closes its
// issues through the path that already exists. Nothing here is widened.
// ---------------------------------------------------------------------------

// retryDueParkedMergedFixture is parkedMergedFixture in the retry lane, with
// its backoff already served - the state driveUnparks finds one backoff after
// the blocker was recorded.
func retryDueParkedMergedFixture(t *testing.T, name, kind, reason string) (
	*tatarav1alpha1.Project, *tatarav1alpha1.Task, *tatarav1alpha1.MergeRequest) {

	t.Helper()
	proj, task, mr := parkedMergedFixture(t, name, kind, reason, "merged")
	proj.Spec.MaxOpenTasks = 6
	// mrRefs is what stage.Unpark actually reads (loadTaskMRs), so without it
	// the "every owned MR merged" half of this fixture would be invisible to
	// the rule under test and the assertion would pass vacuously.
	task.Status.MRRefs = []string{mr.Name}
	task.Status.RetryAttempts = 1
	task.Status.RetryNextAt = &metav1.Time{Time: time.Now().Add(-time.Minute)}
	return proj, task, mr
}

// TestParkedTechnicalParkWithMergedMRsReleasesItself is the H2-E resolution.
// The merged merge requests are the point: every OTHER un-park arm refuses on
// anyMerged (DeclineMergedMR), and a retry arm that copied that refusal would
// have re-created the strand it exists to remove.
func TestParkedTechnicalParkWithMergedMRsReleasesItself(t *testing.T) {
	for _, reason := range []string{stage.ReasonCIFailed, stage.ReasonMergeConflictRetry} {
		t.Run(reason, func(t *testing.T) {
			proj, task, mr := retryDueParkedMergedFixture(t, "mt-i-27", "implement", reason)
			c := newMirrorClient(t, proj, mdSecret(), task, mr)
			r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics()}

			require.NoError(t, r.driveUnparks(context.Background(), proj, time.Now()))

			got := mdGetTask(t, c, "mt-i-27")
			require.False(t, tatarav1alpha1.Parked(got),
				"a technical park on a Task whose work has LANDED must not wait for a human; "+
					"the corridor it interrupted is what closes the issues")
			require.Equal(t, tatarav1alpha1.StateAwaitingReview, got.Status.State,
				"an un-park returns the Task to where it was, back in the ordinary reconcile path")
		})
	}
}

// THE UNCHANGED HALF, and it is non-negotiable. An awaiting-human park on the
// same Task is a genuine human wait: the retry lane must not touch it, and
// neither must the terminal self-heal.
func TestParkedAwaitingHumanTaskWithMergedMRsStillStaysParked(t *testing.T) {
	proj, task, mr := parkedMergedFixture(t, "mt-i-28", "implement", stage.ReasonAwaitingHuman, "merged")
	task.Status.MRRefs = []string{mr.Name}
	c := newMirrorClient(t, proj, mdSecret(), task, mr)
	r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics()}

	require.NoError(t, r.driveUnparks(context.Background(), proj, time.Now()))
	drive(t, tsReconciler(c), "mt-i-28")

	got := mdGetTask(t, c, "mt-i-28")
	require.True(t, tatarav1alpha1.Parked(got),
		"no non-bot comment has arrived; nothing may resume this Task")
	require.Equal(t, stage.ReasonAwaitingHuman, got.Status.ParkReason)
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, got.Status.State)
}
