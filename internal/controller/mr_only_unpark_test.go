// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// mrOnlyFixture is the measured population, exactly: an ADOPTED upgrade Task
// (kind=upgrade, LabelUpgradeOrigin=adopted) that owns ONE MergeRequest mirror
// and ZERO Issue mirrors, parked with no pod ever having run, carrying one
// unspent maintainer comment in pendingEvents. containers!1300, 2026-08-22.
func mrOnlyFixture(t *testing.T, reason, replyAuthor string) (
	*tatarav1alpha1.Project, *tatarav1alpha1.Repository,
	*tatarav1alpha1.Task, *tatarav1alpha1.MergeRequest, string, string) {

	t.Helper()
	proj := reapProject("resume")
	repo := reapRepo("resume", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	taskName := AdoptedUpgradeTaskName("resume", "tatara-operator", 1300)
	mrName := tatarav1alpha1.MergeRequestName("tatara-operator", 1300)

	task := reapTask("resume", taskName, "upgrade", tatarav1alpha1.StateAwaitingReview,
		reason, time.Now().Add(-26*time.Hour))
	parked := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	task.Status.ParkedAt = &parked
	task.Status.MRRefs = []string{mrName}
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{
		{At: metav1.Now(), Kind: "mr_comment", Author: replyAuthor, Body: "continue!"},
	}

	mr := botMR(mrName, taskName, "tatara-operator", 1300)
	return proj, repo, task, mr, taskName, mrName
}

// TestMROnlyUnpark_MaintainerCommentReleasesTheParkInPlace IS THE DEFECT. The
// event was present and unspent, resumeNoReentryParks ran every 60s, and
// resumeOne returned nil before its first side effect because the Task owns no
// Issue to sever. Nothing happened and nothing was logged.
//
// The assertions that matter are that it is the SAME Task (a true un-park, not
// the sever-and-re-mint the Issue path does) and that stateEnteredAt moved: a
// release with a stale stamp re-parks admission-starved on the very next pass.
func TestMROnlyUnpark_MaintainerCommentReleasesTheParkInPlace(t *testing.T) {
	ctx := context.Background()
	proj, repo, task, mr, taskName, mrName := mrOnlyFixture(t, stage.ReasonAdmissionStarved, "maintainer")
	c := newMirrorClient(t, proj, repo, reapSecret(), task, mr)
	w := &resumeWriter{}
	r := reapReconciler(c, w)
	before := testutil.ToFloat64(r.Metrics.MROnlyUnparkCounter(
		proj.Name, stage.ReasonAdmissionStarved, obs.MROnlyUnparkReleased))
	unparkedBefore := testutil.ToFloat64(r.Metrics.TaskUnparkedCounter(
		stage.ReasonAdmissionStarved, obs.UnparkClassMROnly))

	now := time.Now()
	require.NoError(t, r.resumeNoReentryParks(ctx, proj, now))

	fresh, ok := mustGetTask(t, c, taskName)
	require.True(t, ok, "the Task is released IN PLACE, never deleted")
	require.Equal(t, task.UID, fresh.UID, "a true un-park: the SAME Task, not a re-mint")
	require.Empty(t, fresh.Status.ParkReason, "the park must be released")
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, fresh.Status.State,
		"park is orthogonal to state: it resumes where it stopped")
	require.NotNil(t, fresh.Status.StateEnteredAt)
	require.False(t, fresh.Status.StateEnteredAt.Time.Before(now.Add(-time.Minute)),
		"stateEnteredAt must be re-armed, or clock 1 re-parks admission-starved next pass")
	require.Nil(t, fresh.Status.PodStartedAt)
	require.Nil(t, fresh.Status.StateWorkStartedAt)
	require.NotNil(t, fresh.Status.PendingEvents[0].UnparkConsumedAt,
		"one comment releases exactly one park")

	// NOTHING was destroyed: the merge request is untouched and still ours.
	require.Empty(t, w.closed, "an un-park closes no merge request")
	owner, owned := own.ControllerOwner(mustGetMR(t, c, mrName))
	require.True(t, owned)
	require.Equal(t, taskName, owner)

	require.Equal(t, before+1, testutil.ToFloat64(r.Metrics.MROnlyUnparkCounter(
		proj.Name, stage.ReasonAdmissionStarved, obs.MROnlyUnparkReleased)))

	// The release also feeds the shared "every park release" series, under
	// class=mr-only, exactly as driveRetiredUnparks/driveCIRecoveryUnparks feed
	// it under their own class. A release that never touched this series would
	// be an observability blind spot on the one metric that answers "how many
	// parks were released, and by what" - see ci_recovery_unpark_test.go's
	// equivalent assertion.
	require.Equal(t, unparkedBefore+1, testutil.ToFloat64(r.Metrics.TaskUnparkedCounter(
		stage.ReasonAdmissionStarved, obs.UnparkClassMROnly)))
}

// TestMROnlyUnpark_EveryEligibleReasonIsReleased walks the reasons the design's
// eligibility table names, through the REAL driver rather than the primitive, so
// a class filter that drifts in the controller is caught here and not only in
// internal/stage.
func TestMROnlyUnpark_EveryEligibleReasonIsReleased(t *testing.T) {
	for _, reason := range []string{
		stage.ReasonStageDeadline, stage.ReasonAdmissionStarved, stage.ReasonImplementDeclined,
		stage.ReasonOperatorError, stage.ReasonObjectTooLarge, stage.ReasonTriageStalled,
		stage.ReasonCIBlocked, stage.ReasonAgentContractMismatch, stage.ReasonOwnershipLost,
		stage.ReasonNameTooLong, stage.ReasonReviewPostRefused, stage.ReasonFoldAdoptionUnverified,
		stage.ReasonTurnBudgetExhausted, stage.ReasonReviewLoopExhausted, stage.ReasonPodRecreationExhausted,
	} {
		t.Run(reason, func(t *testing.T) {
			ctx := context.Background()
			proj, repo, task, mr, taskName, _ := mrOnlyFixture(t, reason, "maintainer")
			c := newMirrorClient(t, proj, repo, reapSecret(), task, mr)
			r := reapReconciler(c, &resumeWriter{})

			require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

			fresh, ok := mustGetTask(t, c, taskName)
			require.True(t, ok)
			require.Empty(t, fresh.Status.ParkReason)
		})
	}
}

// TestMROnlyUnpark_BotOnlyEventsAreUntouched: the operator's OWN park comment
// must never release the park it wrote. Authorship is the whole test.
func TestMROnlyUnpark_BotOnlyEventsAreUntouched(t *testing.T) {
	ctx := context.Background()
	proj, repo, task, mr, taskName, _ := mrOnlyFixture(t, stage.ReasonStageDeadline, "tatara-bot")
	c := newMirrorClient(t, proj, repo, reapSecret(), task, mr)
	r := reapReconciler(c, &resumeWriter{})

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	still, ok := mustGetTask(t, c, taskName)
	require.True(t, ok)
	require.Equal(t, stage.ReasonStageDeadline, still.Status.ParkReason, "still parked")
}

// TestMROnlyUnpark_AConsumedCommentDoesNotReleaseASecondPark: one comment, one
// lap. A comment already spent releasing an earlier park must not release the
// next one too, or the bound this whole change rests on is not a bound.
func TestMROnlyUnpark_AConsumedCommentDoesNotReleaseASecondPark(t *testing.T) {
	ctx := context.Background()
	proj, repo, task, mr, taskName, _ := mrOnlyFixture(t, stage.ReasonStageDeadline, "maintainer")
	spent := metav1.NewTime(time.Now().Add(-time.Hour))
	task.Status.PendingEvents[0].UnparkConsumedAt = &spent
	c := newMirrorClient(t, proj, repo, reapSecret(), task, mr)
	r := reapReconciler(c, &resumeWriter{})

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	still, ok := mustGetTask(t, c, taskName)
	require.True(t, ok)
	require.Equal(t, stage.ReasonStageDeadline, still.Status.ParkReason)
}

// TestMROnlyUnpark_IssueOwningTaskStillTakesResumeOne IS THE REGRESSION GUARD.
// Sever-and-re-mint is unchanged for a Task that owns an Issue: the new branch
// must be reached ONLY on the population resumeOne structurally cannot serve.
func TestMROnlyUnpark_IssueOwningTaskStillTakesResumeOne(t *testing.T) {
	ctx := context.Background()
	proj, repo, old, iss, oldName, issName := noReentryFixture(t, stage.ReasonStageDeadline, "maintainer")
	mrName := tatarav1alpha1.MergeRequestName("tatara-operator", 42)
	old.Status.MRRefs = []string{mrName}
	mr := botMR(mrName, oldName, "tatara-operator", 42)

	c := newMirrorClient(t, proj, repo, reapSecret(), old, iss, mr)
	w := &resumeWriter{}
	r := reapReconciler(c, w)

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	fresh, ok := mustGetTask(t, c, oldName)
	require.True(t, ok)
	require.NotEqual(t, old.UID, fresh.UID,
		"an Issue-owning Task still gets a FRESH Task; the MR-only arm must not have swallowed it")
	require.Contains(t, w.closed, 42, "and its bot PR is still closed before the re-mint")
	owner, owned := own.ControllerOwner(mustGetIssue(t, c, issName))
	require.True(t, owned)
	require.Equal(t, oldName, owner)
}

// TestMROnlyUnpark_TaskOwningNeitherIsLeftToTheReaper: the new branch requires
// AT LEAST ONE merge request. A Task owning nothing at all keeps resumeOne's
// existing behaviour, which is to leave it to ParkRetention.
func TestMROnlyUnpark_TaskOwningNeitherIsLeftToTheReaper(t *testing.T) {
	ctx := context.Background()
	proj, repo, task, _, taskName, _ := mrOnlyFixture(t, stage.ReasonStageDeadline, "maintainer")
	task.Status.MRRefs = nil
	c := newMirrorClient(t, proj, repo, reapSecret(), task)
	r := reapReconciler(c, &resumeWriter{})

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	still, ok := mustGetTask(t, c, taskName)
	require.True(t, ok)
	require.Equal(t, stage.ReasonStageDeadline, still.Status.ParkReason,
		"nothing to release against and nothing to tell; the reaper's clock still owns it")
}

// TestMROnlyUnpark_ResumeReleasingInFlightDefersToResumeOne is the fix for the
// AnnResumeReleasing race the round-1 review found. resumeOne step 3 severs the
// Task's OWNED Issues before step 4 collects it; a crash or an error between
// steps 3 and 4 leaves a Task that is, from this point on, indistinguishable
// from a Task that was MR-only from the start - zero owned Issues, one or more
// owned MRs, still parked, still carrying the unspent comment (resumeOne never
// consumes pendingEvents). Without the AnnResumeReleasing check that Task would
// be routed into driveMROnlyUnpark, which would spend the maintainer's comment
// reviving a Task that is already committed to collection, and closeTaskBotMRs
// had already run for it - the resumed agent would work against a merge
// request resumeOne is in the middle of closing.
//
// The marker means resumeOne owns finishing this release, so the assertion is
// that driveMROnlyUnpark's own signature - the MROnlyUnpark metric and the
// pending event's UnparkConsumedAt stamp - never fires, and resumeOne's own
// collection (deleteReapedTask) completes instead: the Task is gone, not
// released in place with a stale park.
func TestMROnlyUnpark_ResumeReleasingInFlightDefersToResumeOne(t *testing.T) {
	ctx := context.Background()
	proj, repo, task, mr, taskName, _ := mrOnlyFixture(t, stage.ReasonStageDeadline, "maintainer")
	task.Annotations = map[string]string{AnnResumeReleasing: "true"}
	c := newMirrorClient(t, proj, repo, reapSecret(), task, mr)
	w := &resumeWriter{}
	r := reapReconciler(c, w)
	before := testutil.ToFloat64(r.Metrics.MROnlyUnparkCounter(
		proj.Name, stage.ReasonStageDeadline, obs.MROnlyUnparkReleased))

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	// driveMROnlyUnpark never ran: no release-in-place metric, and the comment
	// that would have been spent releasing it was never touched.
	require.Equal(t, before, testutil.ToFloat64(r.Metrics.MROnlyUnparkCounter(
		proj.Name, stage.ReasonStageDeadline, obs.MROnlyUnparkReleased)))

	// resumeOne finished the interrupted release: the old Task, already
	// committed to collection, is gone rather than left released-in-place with
	// a live park that a resumed agent could act against.
	_, ok := mustGetTask(t, c, taskName)
	require.False(t, ok, "resumeOne must finish the collection it already committed to, not resurrect the Task in place")
}
