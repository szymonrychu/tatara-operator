package stage_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// THE WEDGE MITIGATION. A future writer sets State and forgets to clear
// ParkReason, wedging the Task forever with a stale reason - the same silent
// drift genre as #521. Enter is the ONE place a state changes and it REFUSES a
// non-park edge on a parked Task.
func TestEnterRefusesANonParkEdgeWhileParked(t *testing.T) {
	tk := task(v1alpha1.StateRefined)
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))

	err := stage.Enter(tk, nil, v1alpha1.StateUnderImplementation, "", now)
	require.ErrorAs(t, err, new(*stage.StillParkedError))
	require.Equal(t, v1alpha1.StateRefined, tk.Status.State, "the Task must be untouched")
	require.Equal(t, stage.ReasonAwaitingHuman, tk.Status.ParkReason)
}

func TestUnparkIsTheOnlyThingThatClearsParkReason(t *testing.T) {
	tk := task(v1alpha1.StateRefined)
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
	tk.Status.PendingEvents = []v1alpha1.TaskEvent{{Author: "szymonrychu"}}
	tk.Status.IssueRefs = []string{"iss-1"}

	decline := stage.Unpark(stage.UnparkInput{
		Task: tk, BotLogin: "bot", LiveHasRoom: true, Now: now,
		Issues: []v1alpha1.Issue{{Status: v1alpha1.IssueStatus{State: "open"}}},
	})
	require.Equal(t, stage.DeclineNone, decline)
	require.Empty(t, tk.Status.ParkReason)
	require.Nil(t, tk.Status.ParkedAt)
	require.Equal(t, v1alpha1.StateRefined, tk.Status.State,
		"un-parking returns a Task to WHERE IT WAS; it never moves state")
}

// The ONE documented exception, and it is narrow: takeover_mint clears the flag
// AND moves state, because a re-taken MR resumes at merged, not at wherever the
// ownership flip happened to catch it.
func TestUnparkTakeoverIsTheOnlyStateMovingUnpark(t *testing.T) {
	tk := task(v1alpha1.StateUnderImplementation)
	require.NoError(t, stage.Park(tk, stage.ReasonOwnershipLost, now))

	require.NoError(t, stage.UnparkTakeover(tk, v1alpha1.StateMerged, now))
	require.Empty(t, tk.Status.ParkReason)
	require.Equal(t, v1alpha1.StateMerged, tk.Status.State)
}

func TestUnparkTakeoverRefusesAParkThatIsNotOwnershipLost(t *testing.T) {
	tk := task(v1alpha1.StateUnderImplementation)
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
	require.Error(t, stage.UnparkTakeover(tk, v1alpha1.StateMerged, now))
}

// GUARD 1 is structural and UnparkTakeover does not get to route around it.
func TestUnparkTakeoverStillRefusesMergedForAKindReviewTask(t *testing.T) {
	tk := taskOfKind(v1alpha1.StateAwaitingReview, "review")
	require.NoError(t, stage.Park(tk, stage.ReasonOwnershipLost, now))
	require.Error(t, stage.UnparkTakeover(tk, v1alpha1.StateMerged, now))
}

func TestUnparkTakeoverRefusesATargetOutsideItsOwnAllowList(t *testing.T) {
	tk := task(v1alpha1.StateUnderImplementation)
	require.NoError(t, stage.Park(tk, stage.ReasonOwnershipLost, now))
	require.Error(t, stage.UnparkTakeover(tk, v1alpha1.StateDone, now))
}

func TestParkStampsTheWholeTupleAtomically(t *testing.T) {
	tk := task(v1alpha1.StateAwaitingReview)
	require.NoError(t, stage.Park(tk, stage.ReasonReviewLoopExhausted, now))
	require.Equal(t, stage.ReasonReviewLoopExhausted, tk.Status.ParkReason)
	require.Equal(t, v1alpha1.StateAwaitingReview, tk.Status.ParkedFromState)
	require.NotNil(t, tk.Status.ParkedAt)
	require.Equal(t, v1alpha1.StateAwaitingReview, tk.Status.State,
		"a park does not move the Task; that non-atomicity IS the #521 bug shape")
}

func TestParkIsIdempotentAndFirstReasonWins(t *testing.T) {
	tk := task(v1alpha1.StateRefined)
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
	require.NoError(t, stage.Park(tk, stage.ReasonNoOutcome, now.Add(time.Hour)))
	require.Equal(t, stage.ReasonAwaitingHuman, tk.Status.ParkReason)
}

func TestParkRefusesAReasonOutsideTheClosedSet(t *testing.T) {
	require.Error(t, stage.Park(task(v1alpha1.StateNew), "declined", now),
		"declined is a RejectReason and must go through Enter(rejected), not Park")
	require.Error(t, stage.Park(task(v1alpha1.StateNew), "not-a-reason", now))
	require.Error(t, stage.Park(task(v1alpha1.StateNew), "", now))
}

func TestEveryParkReasonHasAnUnparkClass(t *testing.T) {
	for _, r := range stage.ParkReasons {
		_, ok := stage.UnparkClassFor(r)
		require.True(t, ok, "park reason %q has no UnparkClass; the axis must be total", r)
	}
	require.Len(t, stage.ParkReasons, 28)
	require.Len(t, stage.RejectReasons, 6)
	require.Len(t, stage.DoneReasons, 2)
	require.Len(t, stage.Reasons, 36, "the three sets must partition Reasons with no remainder")
}

func TestTheThreeReasonSetsPartitionReasonsWithNoOverlap(t *testing.T) {
	seen := map[string]int{}
	for _, set := range [][]string{stage.ParkReasons, stage.RejectReasons, stage.DoneReasons} {
		for _, r := range set {
			seen[r]++
		}
	}
	for _, r := range stage.Reasons {
		require.Equal(t, 1, seen[r], "reason %q must appear in EXACTLY one of the three sets", r)
	}
	require.Len(t, seen, len(stage.Reasons))
}

// The park clock replaces the deleted `parked` STAGE's ParkRetention row. A
// parked Task ages out from ParkedAt, whatever state it is sitting in.
func TestArmedClock_AParkedTaskRunsTheRetentionClockFromParkedAt(t *testing.T) {
	tk := task(v1alpha1.StateUnderImplementation)
	require.NoError(t, stage.Park(tk, stage.ReasonReviewLoopExhausted, now))

	clock, since, budget, edge := stage.ArmedClock(tk, false)
	require.Equal(t, stage.ClockWork, clock)
	require.Equal(t, now, since)
	require.Equal(t, v1alpha1.ParkRetention, budget)
	require.Equal(t, stage.Reap, edge.To)
}

// THE ONE EXEMPTION, carried over verbatim: parked(backlog-sweep) is not
// stalled work, it is the durable owner of an Issue CR at zero agent cost.
func TestArmedClock_BacklogSweepParkDisarmsEveryClock(t *testing.T) {
	tk := task(v1alpha1.StateNew)
	require.NoError(t, stage.Park(tk, stage.ReasonBacklogSweep, now))
	clock, _, _, _ := stage.ArmedClock(tk, false)
	require.Equal(t, stage.ClockNone, clock)
}

func TestParkAccumulatesTheElapsedCarryOnATimeoutPark(t *testing.T) {
	tk := task(v1alpha1.StateMerged)
	tk.Status.StateEnteredAt = ptrTime(now.Add(-4 * time.Hour))
	require.NoError(t, stage.Park(tk, stage.ReasonMergeTimeout, now))
	require.Equal(t, int((4 * time.Hour).Seconds()), tk.Status.StageElapsedCarrySeconds)
}
