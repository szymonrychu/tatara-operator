package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ISSUE #478. The production incident, reproduced at both levels it can be
// fixed at.
//
// A kind=review Task in reviewing. The TASK controller finalizes it
// reviewing -> delivered (review_finalize_terminal_mr: every owned MR reached a
// terminal forge state). 1.6-2.1 seconds later the MERGEREQUEST controller,
// still holding the informer cache's copy that says reviewing, computes
// parked(awaiting-human) and applies it. stage.Enter re-reads the Task LIVE,
// sees delivered, and refuses delivered -> parked - correctly, because that edge
// is not in the F.3 table and must never be.
//
// Two things were wrong, and both are asserted here:
//
//  1. the losing reconcile ERRORED, on a race whose outcome was already correct;
//  2. the loss was charged to operator_illegal_stage_transition_total, whose
//     alert annotation reads "ANY non-zero value is a code bug in the F.3
//     transition table". A concurrency loss paged as a table bug.

// Both tests hand the losing path its stale Task copy directly, which is what
// the informer cache hands it in production: owningTask (reviewpost.go) resolves
// the owner through the CACHED client, and the frozen copy it returns is the
// only input the advance decision ever had.

// TestEnterStage_LostRaceIsDroppedNotCountedAsIllegal is the choke-point half.
// The caller pre-checked a stage the Task no longer has, so its edge is legal
// from where it looked and illegal from where the Task actually is. That is a
// lost race, not a table bug: no error, no illegal-transition count, and the
// winner's stage stands untouched.
func TestEnterStage_LostRaceIsDroppedNotCountedAsIllegal(t *testing.T) {
	const (
		liveFrom = tatarav1alpha1.StageDelivered
		to       = tatarav1alpha1.StageParked
	)
	beforeIllegal := illegalCount(t, obs.IllegalStageTransitionCounter(liveFrom, to))
	beforeRace := illegalCount(t, obs.StageRaceLostCounter(liveFrom, to))

	proj := tsProject(3)
	now := time.Unix(30000, 0)
	entered := metav1.NewTime(now.Add(-10 * time.Minute))

	// The mergerequest controller's IN-MEMORY copy: still reviewing. The edge it
	// computes off this, reviewing -> parked(awaiting-human), IS in the F.3
	// table - the pre-check inside EnterStage passes.
	staleCopy := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-race", Namespace: mdNS, UID: types.UID("uid-t-race")},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Kind: "review"},
		Status: tatarav1alpha1.TaskStatus{
			Stage:          tatarav1alpha1.StageReviewing,
			AgentKind:      stage.AgentReview,
			StageEnteredAt: &entered,
		},
	}
	require.True(t, stage.Legal(tatarav1alpha1.StageReviewing, to),
		"precondition: the loser's edge must be LEGAL from the stage it pre-checked, or this proves nothing")
	require.False(t, stage.Legal(liveFrom, to),
		"precondition: delivered -> parked must stay absent from F.3; the table is the detector, not the defect")

	// THE API SERVER: the task controller already finalized it.
	live := staleCopy.DeepCopy()
	live.Status.Stage = liveFrom
	live.Status.StageReason = stage.ReasonMRMergedExternally

	c := newMirrorClient(t, proj, mdSecret(), live)

	err := EnterStage(context.Background(), c, nil, obs.NewOperatorMetrics(prometheus.NewRegistry()),
		staleCopy, nil, to, stage.ReasonAwaitingHuman, now, nil)
	require.NoError(t, err,
		"losing a benign race to another controller must not fail the reconcile")

	require.Equal(t, beforeIllegal, illegalCount(t, obs.IllegalStageTransitionCounter(liveFrom, to)),
		"a lost race was charged to operator_illegal_stage_transition_total, whose alert calls any non-zero value a bug in the F.3 table")
	require.Equal(t, beforeRace+1, illegalCount(t, obs.StageRaceLostCounter(liveFrom, to)),
		"the lost race must still be VISIBLE, on its own counter - dropping it silently trades one blind spot for another")

	got := mdGetTask(t, c, "t-race")
	require.Equal(t, liveFrom, got.Status.Stage, "the winner's terminal stage was overwritten by the loser")
	require.Equal(t, stage.ReasonMRMergedExternally, got.Status.StageReason)
}

// The discrimination must not swallow the bug the counter exists for. When the
// caller's own from IS live and the edge is still absent from F.3, that is a
// genuine table bug: it errors, it counts, and it counts on the ILLEGAL
// counter, not the race one.
func TestEnterStage_GenuineIllegalEdgeStillErrorsAndCounts(t *testing.T) {
	const (
		from = tatarav1alpha1.StageDelivered
		to   = tatarav1alpha1.StageParked
	)
	beforeIllegal := illegalCount(t, obs.IllegalStageTransitionCounter(from, to))
	beforeRace := illegalCount(t, obs.StageRaceLostCounter(from, to))

	now := time.Unix(30000, 0)
	task := tsTask("t-bug", "implement", from, now)
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := tsReconciler(c)

	err := r.enter(context.Background(), proj, task, nil, to, stage.ReasonAwaitingHuman, now)
	require.Error(t, err, "an edge absent from F.3, computed off the LIVE stage, is a code bug and must fail loudly")
	require.Equal(t, beforeIllegal+1, illegalCount(t, obs.IllegalStageTransitionCounter(from, to)))
	require.Equal(t, beforeRace, illegalCount(t, obs.StageRaceLostCounter(from, to)),
		"a genuine table bug must not be laundered onto the race counter")
}

// TestAdvanceAfterReview_AdoptsTheLiveTaskBeforeGuarding is the caller half, and
// it is the one that keeps the log honest: without it the mergerequest
// controller logs "review: task advancing off reviewing" about a Task that left
// reviewing two seconds earlier, then attempts a write the choke point has to
// clean up after. The guard must test the same snapshot the write uses, so the
// advance is never computed at all.
func TestAdvanceAfterReview_AdoptsTheLiveTaskBeforeGuarding(t *testing.T) {
	const (
		liveFrom = tatarav1alpha1.StageDelivered
		to       = tatarav1alpha1.StageParked
	)
	beforeIllegal := illegalCount(t, obs.IllegalStageTransitionCounter(liveFrom, to))
	beforeRace := illegalCount(t, obs.StageRaceLostCounter(liveFrom, to))

	// THE API SERVER: the task controller finalized this review Task already.
	live := mdTask("t1", "review", liveFrom)
	live.Status.StageReason = stage.ReasonMRMergedExternally
	// An owned MR that is still open in the mirror, so reviewAdvanceEdge on a
	// kind=review Task yields parked(awaiting-human) - the exact edge the
	// incident tried to apply.
	mr := mdMR(live, "tatara-operator", 972)
	base := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), live, mr)

	f := newFakeForge(t)
	d := mdNewDriver(t, f, base)
	d.APIReader = base

	// The mergerequest controller's own copy, off the informer cache.
	stale := live.DeepCopy()
	stale.Status.Stage = tatarav1alpha1.StageReviewing
	stale.Status.StageReason = ""

	err := d.advanceAfterReview(context.Background(), mdProject(), stale, mdGetMR(t, base, mr.Name))
	require.NoError(t, err, "the advance must be skipped, not attempted and refused")

	got := mdGetTask(t, base, "t1")
	require.Equal(t, liveFrom, got.Status.Stage, "a terminal Task was dragged back to parked off a stale cached stage")
	require.Equal(t, beforeIllegal, illegalCount(t, obs.IllegalStageTransitionCounter(liveFrom, to)))
	require.Equal(t, beforeRace, illegalCount(t, obs.StageRaceLostCounter(liveFrom, to)),
		"the guard must adopt the live stage and never reach the write, so not even the race counter fires")
}
