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
	require.Len(t, stage.ParkReasons, 34)
	require.Len(t, stage.RejectReasons, 6)
	require.Len(t, stage.DoneReasons, 2)
	require.Len(t, stage.Reasons, 42, "the three sets must partition Reasons with no remainder")
}

// TestMergeStageParksAreParkReasonsAndExcludeTheImplementationOnes pins the
// SECOND axis. Every member must be a real park reason (a typo here would
// silently classify nothing), and the boundary case is ci-blocked: it is the one
// reason in the same neighbourhood that a re-implementation is the correct
// answer to, because stage.CIRed reaches it only when NOTHING has merged yet.
func TestMergeStageParksAreParkReasonsAndExcludeTheImplementationOnes(t *testing.T) {
	merge := []string{
		stage.ReasonMergeTimeout, stage.ReasonMergeBlocked, stage.ReasonMergeAuthRefused,
		stage.ReasonMergeOrderMissing, stage.ReasonHeadMoving, stage.ReasonCIRed,
		stage.ReasonMergeConflict, stage.ReasonDeployTimeout, stage.ReasonDeployBlocked,
	}
	for _, r := range merge {
		require.True(t, stage.IsParkReason(r), "%q must be a park reason", r)
		require.True(t, stage.IsMergeStagePark(r), "%q is written at or after the merge", r)
	}
	for _, r := range []string{
		stage.ReasonCIBlocked, stage.ReasonStageDeadline, stage.ReasonImplementDeclined,
		stage.ReasonReviewPostRefused, stage.ReasonOwnershipLost, stage.ReasonOperatorError,
		stage.ReasonBacklogSweep, stage.ReasonAwaitingHuman,
	} {
		require.False(t, stage.IsMergeStagePark(r), "%q is not a merge-stage park", r)
	}
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

// TestUnparkForMRTerminalIsNarrow pins the guards that keep the C.2 park
// override from becoming a general park bypass. The function is the third and
// last exception to "Unpark is the only way out of a park", so its refusals are
// the entire safety argument.
func TestUnparkForMRTerminalIsNarrow(t *testing.T) {
	parked := func() *v1alpha1.Task {
		t := &v1alpha1.Task{}
		t.Status.State = v1alpha1.StateAwaitingReview
		t.Status.ParkReason = stage.ReasonAwaitingHuman
		return t
	}

	tests := []struct {
		name       string
		to, reason string
		wantErr    bool
	}{
		{"merged externally clears the park", v1alpha1.StateDone, stage.ReasonMRMergedExternally, false},
		{"closed externally clears the park", v1alpha1.StateRejected, stage.ReasonMRClosedExternally, false},
		{"taken over clears the park", v1alpha1.StateRejected, stage.ReasonMRTakenOver, false},
		{
			// THE SCOPE LINE. `merged` is not a terminal outcome: it RESTARTS a
			// pipeline (merge cursor, deploy ledger, issue closes) behind the back
			// of the human the park was waiting for.
			"a non-terminal target is refused", v1alpha1.StateMerged, stage.ReasonMRMergedExternally, true,
		},
		{"an ordinary terminal reason is refused", v1alpha1.StateDone, stage.ReasonDeclined, true},
		{"an empty reason is refused", v1alpha1.StateDone, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := parked()
			err := stage.UnparkForMRTerminal(task, tc.to, tc.reason)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("stage.UnparkForMRTerminal(%q, %q) = nil, want an error", tc.to, tc.reason)
				}
				if task.Status.ParkReason == "" {
					t.Fatal("a REFUSED un-park must leave the park standing")
				}
				return
			}
			if err != nil {
				t.Fatalf("stage.UnparkForMRTerminal: %v", err)
			}
			if task.Status.ParkReason != "" {
				t.Fatal("an accepted un-park must clear the park flag")
			}
		})
	}

	// A Task that is not parked is a no-op success, so no caller has to ask first.
	unparked := &v1alpha1.Task{}
	if err := stage.UnparkForMRTerminal(unparked, v1alpha1.StateDone, stage.ReasonMRMergedExternally); err != nil {
		t.Fatalf("an unparked task must be a no-op success, got %v", err)
	}
}

// UpgradeDeclineToOwnershipLost is the ONE park reason the ownership flip is
// allowed to rewrite (#604 review). Both of its guards are load-bearing, so both
// are pinned: widening either one turns a genuine terminal into a Task a human
// push can resurrect.
func TestUpgradeDeclineToOwnershipLost(t *testing.T) {
	later := now.Add(72 * time.Hour)

	t.Run("upgrades a declined takeover", func(t *testing.T) {
		tk := taskOfKind(v1alpha1.StateUnderImplementation, "takeover")
		require.NoError(t, stage.Park(tk, stage.ReasonImplementDeclined, now))
		carry, entered := tk.Status.StageElapsedCarrySeconds, tk.Status.StateEnteredAt.Time

		changed, err := stage.UpgradeDeclineToOwnershipLost(tk, later)
		require.NoError(t, err)
		require.True(t, changed)
		require.Equal(t, stage.ReasonOwnershipLost, tk.Status.ParkReason)
		require.Equal(t, v1alpha1.StateUnderImplementation, tk.Status.State,
			"the upgrade must not move State")
		require.Equal(t, later, tk.Status.ParkedAt.Time,
			"the retention window restarts from the flip, so the maintainer gets a full ParkRetention "+
				"from when the branch actually came back")
		// The reason repark is wrong here: this is ONE park's reason being
		// corrected, so Park's park-event bookkeeping - the residency fold and
		// the StateEnteredAt re-arm - must not run a second time. NOT because a
		// re-fold would blow ResidencyExceeded: it could not, since every exit
		// from parked(ownership-lost) runs UnparkTakeover -> stampEnter, which
		// zeroes the carry anyway. See UpgradeDeclineToOwnershipLost.
		require.Equal(t, carry, tk.Status.StageElapsedCarrySeconds,
			"the upgrade must not fold the parked interval into the residency carry")
		require.Equal(t, entered, tk.Status.StateEnteredAt.Time,
			"the upgrade must not re-arm the state clock")
	})

	t.Run("refuses another kind", func(t *testing.T) {
		tk := taskOfKind(v1alpha1.StateUnderImplementation, "implement")
		require.NoError(t, stage.Park(tk, stage.ReasonImplementDeclined, now))
		_, err := stage.UpgradeDeclineToOwnershipLost(tk, later)
		require.Error(t, err,
			"on a non-takeover kind the branch is tatara's own, so a human push is not a hand-back")
		require.Equal(t, stage.ReasonImplementDeclined, tk.Status.ParkReason)
	})

	t.Run("refuses another park reason", func(t *testing.T) {
		tk := taskOfKind(v1alpha1.StateUnderImplementation, "takeover")
		require.NoError(t, stage.Park(tk, stage.ReasonStageDeadline, now))
		_, err := stage.UpgradeDeclineToOwnershipLost(tk, later)
		require.Error(t, err,
			"every other unresumable park on a takeover is a genuine terminal")
		require.Equal(t, stage.ReasonStageDeadline, tk.Status.ParkReason)
	})

	t.Run("refuses an unparked task", func(t *testing.T) {
		_, err := stage.UpgradeDeclineToOwnershipLost(
			taskOfKind(v1alpha1.StateUnderImplementation, "takeover"), later)
		require.Error(t, err)
	})

	// CONVERGED, NOT MISUSED. Every caller is a converge-by-retry path -
	// resumeFlipToExternal re-drives an interrupted flip off a CACHED read that
	// can still say implement-declined after the write landed - so erroring here
	// would block the retry and double-count the upgrade.
	t.Run("is a no-op success when already ownership-lost", func(t *testing.T) {
		tk := taskOfKind(v1alpha1.StateUnderImplementation, "takeover")
		require.NoError(t, stage.Park(tk, stage.ReasonOwnershipLost, now))
		stamped := tk.Status.ParkedAt.Time

		changed, err := stage.UpgradeDeclineToOwnershipLost(tk, later)
		require.NoError(t, err)
		require.False(t, changed, "a converged Task must not re-count as an upgrade")
		require.Equal(t, stamped, tk.Status.ParkedAt.Time,
			"a no-op must not restart the retention window either")
	})
}

// TestTheRetryReasonsAreUnparkRetry pins the MACHINE lane. These four name a
// technical blocker a machine is expected to clear on its own, so what releases
// them is a timer bounded by an attempt count - not a human comment, which is
// the whole difference between this class and UnparkHuman.
func TestTheRetryReasonsAreUnparkRetry(t *testing.T) {
	tests := []struct {
		name   string
		reason string
		want   stage.UnparkClass
	}{
		{"a pipeline that has not answered yet", stage.ReasonCIPending, stage.UnparkRetry},
		{"a check that is red", stage.ReasonCIFailed, stage.UnparkRetry},
		{"an owned merge request the forge reports dirty", stage.ReasonMergeConflictRetry, stage.UnparkRetry},
		{"work to ship and no merge request that can carry it", stage.ReasonMRSurfaceSpent, stage.UnparkRetry},
		{"the lane itself, spent", stage.ReasonRetryExhausted, stage.UnparkHuman},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, stage.IsParkReason(tc.reason), "%q must be a park reason", tc.reason)
			got, ok := stage.UnparkClassFor(tc.reason)
			require.True(t, ok, "%q has no unpark class", tc.reason)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestMergeAuthRefusedIsStillUnparkNever is the DELIBERATE exclusion. A refused
// merge credential does not fix itself, and retrying a merge that may have
// partially succeeded is how a double-merge happens - so it is the one
// technical-looking blocker that never joins the retry lane.
func TestMergeAuthRefusedIsStillUnparkNever(t *testing.T) {
	got, ok := stage.UnparkClassFor(stage.ReasonMergeAuthRefused)
	require.True(t, ok)
	require.Equal(t, stage.UnparkNever, got)
}

// TestUnparkRetryBackoffIsExponentialAndCapped pins the SCHEDULE. n is the
// attempts already spent, so the first lap of a fresh park waits
// UnparkRetryBackoffBase and each further lap doubles until the cap - which is
// CIWaitDeadline, past which this platform has already decided a pipeline that
// has not spoken is not going to.
func TestUnparkRetryBackoffIsExponentialAndCapped(t *testing.T) {
	tests := []struct {
		name     string
		attempts int
		want     time.Duration
	}{
		{"the first lap of a fresh park", 0, time.Minute},
		{"second", 1, 2 * time.Minute},
		{"third", 2, 4 * time.Minute},
		{"fourth", 3, 8 * time.Minute},
		{"fifth", 4, 16 * time.Minute},
		{"the doubling is ceilinged, not unbounded", 5, 30 * time.Minute},
		{"and stays there", 6, 30 * time.Minute},
		{"a nonsense negative attempt count still serves the base wait", -1, time.Minute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, stage.RetryWait(tc.attempts))

			tk := task(v1alpha1.StateAwaitingReview)
			require.NoError(t, stage.Park(tk, stage.ReasonCIFailed, now))
			// The laps have to be charged against THIS blocker, which is what
			// ArmRetry recorded on the lap that spent them: a counter with no
			// blocker attached is one this park has not spent anything on yet.
			tk.Status.RetryBlocker = stage.ReasonCIFailed
			tk.Status.RetryAttempts = tc.attempts
			require.NoError(t, stage.ArmRetry(tk, now))
			require.NotNil(t, tk.Status.RetryNextAt)
			require.Equal(t, now.Add(tc.want), tk.Status.RetryNextAt.Time)
			require.Equal(t, tc.attempts+1, tk.Status.RetryAttempts,
				"arming SPENDS the lap it schedules; nothing else charges the budget")
		})
	}
}

func TestArmRetryRefusesAParkThatIsNotInTheRetryLane(t *testing.T) {
	tk := task(v1alpha1.StateAwaitingReview)
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
	require.Error(t, stage.ArmRetry(tk, now))
	require.Zero(t, tk.Status.RetryAttempts)

	require.Error(t, stage.ArmRetry(task(v1alpha1.StateAwaitingReview), now),
		"an un-parked Task has no lane to arm")
}

// RescheduleRetry is the verdict that is neither "still standing" nor
// "release": the blocker has cleared and the project has no live room for the
// pod a release would mint. Without a schedule that answer is not recorded
// anywhere and the driver re-reads the forge on every 30s pass forever.
func TestRescheduleRetryPacesTheReReadWithoutChargingALap(t *testing.T) {
	tk := task(v1alpha1.StateAwaitingReview)
	require.NoError(t, stage.Park(tk, stage.ReasonMergeConflictRetry, now))
	tk.Status.RetryBlocker = stage.ReasonMergeConflictRetry
	tk.Status.RetryAttempts = v1alpha1.MaxUnparkRetries

	require.NoError(t, stage.RescheduleRetry(tk, now))
	require.NotNil(t, tk.Status.RetryNextAt)
	require.Equal(t, now.Add(stage.RetryWait(v1alpha1.MaxUnparkRetries-1)), tk.Status.RetryNextAt.Time,
		"the wait the current lap already paid for is re-served; no lap buys a longer one")
	require.Equal(t, v1alpha1.MaxUnparkRetries, tk.Status.RetryAttempts,
		"the operator's own ceiling is not the blocker's lap")
	require.Equal(t, stage.ReasonMergeConflictRetry, tk.Status.RetryBlocker)

	require.Error(t, stage.RescheduleRetry(task(v1alpha1.StateAwaitingReview), now),
		"an un-parked Task has no lane to reschedule")
	human := task(v1alpha1.StateAwaitingReview)
	require.NoError(t, stage.Park(human, stage.ReasonAwaitingHuman, now))
	require.Error(t, stage.RescheduleRetry(human, now), "a human park is not on a timer")
}

// TestARetryParkIsRefusedUntilItIsDue puts the backoff in the PURE package, not
// only in the driver: ApplyUnpark has three call sites and a webhook comment
// must not be able to short-circuit a schedule the lane is counting laps on.
func TestARetryParkIsRefusedUntilItIsDue(t *testing.T) {
	tests := []struct {
		name string
		arm  func(*v1alpha1.Task)
		want string
	}{
		{"never armed", func(*v1alpha1.Task) {}, stage.DeclineRetryNotDue},
		{"armed, still waiting", func(tk *v1alpha1.Task) {
			require.NoError(t, stage.ArmRetry(tk, now))
		}, stage.DeclineRetryNotDue},
		{"armed and due", func(tk *v1alpha1.Task) {
			require.NoError(t, stage.ArmRetry(tk, now.Add(-2*time.Hour)))
		}, stage.DeclineNone},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tk := task(v1alpha1.StateAwaitingReview)
			require.NoError(t, stage.Park(tk, stage.ReasonCIFailed, now))
			tc.arm(tk)
			got := stage.Unpark(stage.UnparkInput{Task: tk, BotLogin: "bot", LiveHasRoom: true, Now: now})
			require.Equal(t, tc.want, got)
		})
	}
}

// A RELEASED retry park keeps its attempt count. The release is what the lane
// is spending laps ON, so refunding it there makes the budget unreachable and
// MaxUnparkRetries dead code - the same unbounded-loop shape StageElapsedCarry
// is folded across a park round trip to avoid.
func TestReleasingARetryParkKeepsTheBudgetAndDropsTheSchedule(t *testing.T) {
	tk := task(v1alpha1.StateAwaitingReview)
	require.NoError(t, stage.Park(tk, stage.ReasonCIFailed, now))
	require.NoError(t, stage.ArmRetry(tk, now.Add(-time.Hour)))

	require.Equal(t, stage.DeclineNone,
		stage.Unpark(stage.UnparkInput{Task: tk, BotLogin: "bot", LiveHasRoom: true, Now: now}))
	require.Empty(t, tk.Status.ParkReason)
	require.Equal(t, 1, tk.Status.RetryAttempts, "the lap it just spent is not refunded")
	require.Nil(t, tk.Status.RetryNextAt, "the schedule belongs to the park that is over")
}

// The two doors that DO refund it: real progress, and a human answering.
func TestTheRetryBudgetIsResetByProgressAndByAHumanAnswer(t *testing.T) {
	t.Run("a genuine state transition", func(t *testing.T) {
		tk := task(v1alpha1.StateRefined)
		tk.Status.RetryAttempts = 4
		tk.Status.RetryNextAt = ptrTime(now)
		require.NoError(t, stage.Enter(tk, nil, v1alpha1.StateUnderImplementation, "", now))
		require.Zero(t, tk.Status.RetryAttempts)
		require.Nil(t, tk.Status.RetryNextAt)
	})

	t.Run("a human comment releasing a human park", func(t *testing.T) {
		tk := task(v1alpha1.StateRefined)
		require.NoError(t, stage.Park(tk, stage.ReasonRetryExhausted, now))
		tk.Status.RetryAttempts = v1alpha1.MaxUnparkRetries
		tk.Status.PendingEvents = []v1alpha1.TaskEvent{{Author: "szymonrychu"}}

		require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{
			Task: tk, BotLogin: "bot", LiveHasRoom: true, Now: now,
			Issues: []v1alpha1.Issue{{Status: v1alpha1.IssueStatus{State: "open"}}},
		}))
		require.Zero(t, tk.Status.RetryAttempts,
			"a human who answers buys the machine a fresh budget for the next blocker")
	})
}

// ExhaustRetry is the LOUD end of the lane and it never leaves the Task
// un-parked, exactly as repark's other caller does not.
func TestExhaustRetryReparksToRetryExhausted(t *testing.T) {
	tk := task(v1alpha1.StateAwaitingReview)
	require.NoError(t, stage.Park(tk, stage.ReasonCIFailed, now))
	require.NoError(t, stage.ArmRetry(tk, now))
	tk.Status.RetryAttempts = v1alpha1.MaxUnparkRetries

	from, err := stage.ExhaustRetry(tk, now)
	require.NoError(t, err)
	require.Equal(t, stage.ReasonCIFailed, from, "the caller has to name the blocker in its comment")
	require.Equal(t, stage.ReasonRetryExhausted, tk.Status.ParkReason)
	require.Equal(t, v1alpha1.StateAwaitingReview, tk.Status.State, "a repark never moves the Task")
	require.Nil(t, tk.Status.RetryNextAt, "nothing is scheduled any more")
	require.Equal(t, v1alpha1.MaxUnparkRetries, tk.Status.RetryAttempts,
		"the spent budget is the record the escalation comment is written from")

	_, err = stage.ExhaustRetry(tk, now)
	require.Error(t, err, "retry-exhausted is not itself in the lane")
}

// TestTheRetryBudgetIsScopedToOneBlocker is finding 4's pure half. The field
// doc says "the laps this Task has spent on its CURRENT blocker", and without
// status.retryBlocker the counter was per-TASK: a Task that cleared ci-failed
// and later hit merge-conflict-retry inherited the first blocker's spend and
// escalated early, with a comment naming laps that were never spent on the
// blocker it names.
func TestTheRetryBudgetIsScopedToOneBlocker(t *testing.T) {
	t.Run("the SAME blocker keeps its spend across the park round trip", func(t *testing.T) {
		tk := task(v1alpha1.StateAwaitingReview)
		require.NoError(t, stage.Park(tk, stage.ReasonCIFailed, now))
		require.NoError(t, stage.ArmRetry(tk, now.Add(-time.Hour)))
		require.Equal(t, stage.DeclineNone,
			stage.Unpark(stage.UnparkInput{Task: tk, BotLogin: "bot", LiveHasRoom: true, Now: now}))

		require.NoError(t, stage.Park(tk, stage.ReasonCIFailed, now))
		require.NoError(t, stage.ArmRetry(tk, now))
		require.Equal(t, 2, tk.Status.RetryAttempts, "the same blocker's laps accumulate")
		require.Equal(t, now.Add(2*time.Minute), tk.Status.RetryNextAt.Time,
			"and the backoff grows with them")
	})

	t.Run("a DIFFERENT blocker starts from a full budget", func(t *testing.T) {
		tk := task(v1alpha1.StateAwaitingReview)
		require.NoError(t, stage.Park(tk, stage.ReasonCIFailed, now))
		for i := 0; i < 4; i++ {
			require.NoError(t, stage.ArmRetry(tk, now.Add(-time.Hour)))
		}
		require.Equal(t, 4, tk.Status.RetryAttempts)
		require.Equal(t, stage.DeclineNone,
			stage.Unpark(stage.UnparkInput{Task: tk, BotLogin: "bot", LiveHasRoom: true, Now: now}))

		require.NoError(t, stage.Park(tk, stage.ReasonMergeConflictRetry, now))
		require.NoError(t, stage.ArmRetry(tk, now))
		require.Equal(t, 1, tk.Status.RetryAttempts,
			"a conflict must not inherit the red pipeline's spend")
		require.Equal(t, stage.ReasonMergeConflictRetry, tk.Status.RetryBlocker)
		require.Equal(t, now.Add(time.Minute), tk.Status.RetryNextAt.Time)
	})
}

// ResetRetryBudget is the merge corridor's door onto the same protection: a
// cursor advance ends a blocker without any state transition at all.
func TestResetRetryBudgetLaundersAllThreeFields(t *testing.T) {
	tk := task(v1alpha1.StateMerged)
	require.NoError(t, stage.Park(tk, stage.ReasonCIFailed, now))
	require.NoError(t, stage.ArmRetry(tk, now))

	stage.ResetRetryBudget(tk)
	require.Zero(t, tk.Status.RetryAttempts)
	require.Empty(t, tk.Status.RetryBlocker)
	require.Nil(t, tk.Status.RetryNextAt)
}

// Repark is the migration's door. It refuses the two things the unexported form
// never had to: an un-parked Task, and a reason that is not a park reason.
func TestReparkMovesAParkWithoutEverUnparkingIt(t *testing.T) {
	tk := task(v1alpha1.StateMerged)
	require.NoError(t, stage.Park(tk, stage.ReasonCIRed, now))

	require.NoError(t, stage.Repark(tk, stage.ReasonCIFailed, now))
	require.Equal(t, stage.ReasonCIFailed, tk.Status.ParkReason)
	require.Equal(t, v1alpha1.StateMerged, tk.Status.State)
	require.NotNil(t, tk.Status.ParkedAt)

	require.Error(t, stage.Repark(tk, "not-a-park-reason", now))
	require.Error(t, stage.Repark(task(v1alpha1.StateMerged), stage.ReasonCIFailed, now))
}

// laneStrandedTask is the state the agent-stop re-arm cap leaves behind on a
// Task the retry lane had released: parked no-outcome in the live state the
// release put it back into, still carrying the blocker and the laps spent on it,
// because clearPark preserves both and no genuine transition happened.
func laneStrandedTask(blocker string, attempts int) *v1alpha1.Task {
	tk := task(v1alpha1.StateAwaitingReview)
	tk.Status.RetryBlocker = blocker
	tk.Status.RetryAttempts = attempts
	if err := stage.Park(tk, stage.ReasonNoOutcome, now); err != nil {
		panic(err)
	}
	return tk
}

// TestStrandRetryLaneEndsALaneTheAgentStopCapTookOver: the lane's terminal is
// retry-exhausted, reached WITHOUT the Task ever being un-parked, and it returns
// the blocker so the escalation can name what the laps were spent on. Without
// it the cap's no-outcome park is owned by nobody: its own un-park arm declines
// merged-mr forever, driveStrandedParks skips its class, and the retry driver no
// longer recognises the reason.
func TestStrandRetryLaneEndsALaneTheAgentStopCapTookOver(t *testing.T) {
	tk := laneStrandedTask(stage.ReasonCIFailed, 2)

	blocker, err := stage.StrandRetryLane(tk, now)
	require.NoError(t, err)
	require.Equal(t, stage.ReasonCIFailed, blocker)
	require.Equal(t, stage.ReasonRetryExhausted, tk.Status.ParkReason)
	require.Equal(t, v1alpha1.StateAwaitingReview, tk.Status.State)
	require.Equal(t, 2, tk.Status.RetryAttempts, "the laps are the record the comment is written from")
}

// TestStrandRetryLaneRefusesEverythingItDoesNotOwn. no-outcome is written by
// several paths that never went near the lane, so the blocker fingerprint is the
// whole discriminator and it has to be checked, not assumed.
func TestStrandRetryLaneRefusesEverythingItDoesNotOwn(t *testing.T) {
	tests := []struct {
		name string
		tk   *v1alpha1.Task
	}{
		{"no blocker recorded: it never entered the lane", laneStrandedTask("", 0)},
		{"a blocker with no laps spent on it", laneStrandedTask(stage.ReasonCIFailed, 0)},
		{"laps against a reason outside the lane", laneStrandedTask(stage.ReasonAwaitingHuman, 3)},
		{"a park that is not no-outcome", func() *v1alpha1.Task {
			tk := task(v1alpha1.StateMerged)
			require.NoError(t, stage.Park(tk, stage.ReasonCIFailed, now))
			require.NoError(t, stage.ArmRetry(tk, now))
			return tk
		}()},
		{"not parked at all", task(v1alpha1.StateAwaitingReview)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := tc.tk.Status.ParkReason
			require.False(t, stage.LaneStranded(tc.tk))
			_, err := stage.StrandRetryLane(tc.tk, now)
			require.Error(t, err)
			require.Equal(t, before, tc.tk.Status.ParkReason, "a refusal must change nothing")
		})
	}
}

// mrOnlyEligibleReasons is the population the MR-only un-park exists for: every
// UnparkNever reason that is NOT merge-stage, plus the three UnparkRetired
// migration reasons. It is spelled out rather than derived from unparkClasses
// because a derived list would silently follow a future reclassification into
// releasing something this change never argued for.
var mrOnlyEligibleReasons = []string{
	stage.ReasonStageDeadline,
	stage.ReasonAdmissionStarved,
	stage.ReasonImplementDeclined,
	stage.ReasonOperatorError,
	stage.ReasonObjectTooLarge,
	stage.ReasonTriageStalled,
	stage.ReasonCIBlocked,
	stage.ReasonAgentContractMismatch,
	stage.ReasonOwnershipLost,
	stage.ReasonNameTooLong,
	stage.ReasonReviewPostRefused,
	stage.ReasonFoldAdoptionUnverified,
	stage.ReasonTurnBudgetExhausted,
	stage.ReasonReviewLoopExhausted,
	stage.ReasonPodRecreationExhausted,
}

// TestUnparkMaintainerCommentReleasesEveryEligibleReason pins the FIVE stamps
// that make this a release rather than a no-op that looks like one - or worse,
// a release that quietly re-parks itself on the very next reconcile.
//
// stateEnteredAt and stageElapsedCarrySeconds are BOTH load-bearing, and for
// the SAME clock: ResidencyExceeded reads StateElapsedSeconds, which is
// now - stateEnteredAt + stageElapsedCarrySeconds (liveness.go, stage.go).
// Re-stamping stateEnteredAt alone is not enough, because Park already folded
// a carry that is, for the two headline reasons, already PAST ResidencyCapAll
// (24h = 86400s) by construction before this release ever runs: stage-deadline
// is written BY the residency check itself, and admission-starved fires at
// AdmissionStarvedBudget (24h) measured from stateEnteredAt. A release that
// leaves that carry standing is void even with a fresh stateEnteredAt: the pod
// this release just admitted gets stateWorkStartedAt stamped at pod-Ready,
// ResidencyExceeded reads true on the very next reconcile, and the Task
// re-parks stage-deadline with the pod deleted mid-turn - one dead park spending
// an admission slot and a pod to become a different dead park.
func TestUnparkMaintainerCommentReleasesEveryEligibleReason(t *testing.T) {
	later := now.Add(30 * time.Hour)
	for _, reason := range mrOnlyEligibleReasons {
		t.Run(reason, func(t *testing.T) {
			tk := taskOfKind(v1alpha1.StateAwaitingReview, "upgrade")
			tk.Status.PodStartedAt = ptrTime(now)
			tk.Status.StateWorkStartedAt = ptrTime(now)
			tk.Status.StageElapsedCarrySeconds = 100000 // > ResidencyCapAll; must not survive the release
			tk.Status.PendingEvents = []v1alpha1.TaskEvent{
				{Kind: "mr_comment", Author: "maintainer", Body: "continue!"},
				{Kind: "mr_comment", Author: "tatara-bot", Body: "parked"},
			}
			require.NoError(t, stage.Park(tk, reason, now))

			require.NoError(t, stage.UnparkMaintainerComment(tk, "tatara-bot", later))

			require.Empty(t, tk.Status.ParkReason, "the park flag must be cleared")
			require.Nil(t, tk.Status.ParkedAt)
			require.Equal(t, v1alpha1.StateAwaitingReview, tk.Status.State,
				"park is orthogonal to state: the Task resumes WHERE IT STOPPED")
			require.NotNil(t, tk.Status.StateEnteredAt)
			require.Equal(t, later, tk.Status.StateEnteredAt.Time,
				"clock 1 measures from stateEnteredAt; a stale value re-parks admission-starved next pass")
			require.Zero(t, tk.Status.StageElapsedCarrySeconds,
				"a carry already past ResidencyCapAll would re-park stage-deadline on the very next reconcile, "+
					"voiding the release stateEnteredAt alone looked like it made")
			require.Nil(t, tk.Status.PodStartedAt, "clock 1 re-arms only with the pod clocks nil")
			require.Nil(t, tk.Status.StateWorkStartedAt)
			require.NotNil(t, tk.Status.PendingEvents[0].UnparkConsumedAt,
				"one comment releases exactly ONE park")
			require.Nil(t, tk.Status.PendingEvents[1].UnparkConsumedAt,
				"a bot event was never eligible, so stamping it would only make the field lie")
		})
	}
}

// TestUnparkMaintainerCommentRefusesEveryMergeStagePark is the SAFETY BOUNDARY,
// not tidiness. Issue #597 recorded a Task parked merge-auth-refused re-entering
// ReconcileMerging and issuing live forge merges, and park.go makes the same
// argument for the retry lane: retrying a merge that may have partially
// succeeded is how a double-merge happens.
//
// The four rows marked below are refused by the CLASS guard before the
// merge-stage guard is ever reached (merge-timeout and deploy-timeout are
// UnparkTimer; ci-failed and merge-conflict-retry are UnparkRetry). They are in
// the table anyway, for the reason mergeStageParks itself gives about its own
// inert entries: the day one of them is reclassified, the refusal is already
// correct instead of having to be remembered.
func TestUnparkMaintainerCommentRefusesEveryMergeStagePark(t *testing.T) {
	mergeStage := []string{
		stage.ReasonMergeBlocked,
		stage.ReasonMergeAuthRefused,
		stage.ReasonMergeOrderMissing,
		stage.ReasonHeadMoving,
		stage.ReasonCIRed,
		stage.ReasonMergeConflict,
		stage.ReasonDeployBlocked,
		stage.ReasonMergeTimeout,       // refused by class first
		stage.ReasonDeployTimeout,      // refused by class first
		stage.ReasonCIFailed,           // refused by class first
		stage.ReasonMergeConflictRetry, // refused by class first
	}
	for _, reason := range mergeStage {
		t.Run(reason, func(t *testing.T) {
			require.True(t, stage.IsMergeStagePark(reason), "the fixture must be a merge-stage park")
			tk := task(v1alpha1.StateMerged)
			tk.Status.PendingEvents = []v1alpha1.TaskEvent{{Kind: "mr_comment", Author: "maintainer"}}
			require.NoError(t, stage.Park(tk, reason, now))
			entered := tk.Status.StateEnteredAt.Time

			require.Error(t, stage.UnparkMaintainerComment(tk, "tatara-bot", now.Add(time.Hour)))

			require.Equal(t, reason, tk.Status.ParkReason, "a REFUSED un-park leaves the park standing")
			require.Equal(t, entered, tk.Status.StateEnteredAt.Time, "and touches no clock")
			require.Nil(t, tk.Status.PendingEvents[0].UnparkConsumedAt,
				"a refusal inside this package spends nothing; the CALLER decides what the comment bought")
		})
	}
}

// TestUnparkMaintainerCommentRefusalIsTheMergeStageGuardNotTheClassGuard pins
// which guard does the work for the seven reasons that would otherwise sail
// past. Without this, deleting the merge-stage guard leaves the table above
// green and the double-merge hazard wide open.
func TestUnparkMaintainerCommentRefusalIsTheMergeStageGuardNotTheClassGuard(t *testing.T) {
	for _, reason := range []string{
		stage.ReasonMergeBlocked, stage.ReasonMergeAuthRefused, stage.ReasonMergeOrderMissing,
		stage.ReasonHeadMoving, stage.ReasonCIRed, stage.ReasonMergeConflict, stage.ReasonDeployBlocked,
	} {
		t.Run(reason, func(t *testing.T) {
			class, ok := stage.UnparkClassFor(reason)
			require.True(t, ok)
			require.Equal(t, stage.UnparkNever, class,
				"this reason PASSES the class guard, so only the merge-stage guard can refuse it")
			tk := task(v1alpha1.StateMerged)
			require.NoError(t, stage.Park(tk, reason, now))
			require.Error(t, stage.UnparkMaintainerComment(tk, "tatara-bot", now))
		})
	}
}

// TestUnparkMaintainerCommentRefusesAParkThatIsSomebodyElsesLane: this driver
// answers ONLY the population nothing else owns. A human-, timer- or
// retry-class park already has a driver (driveUnparks), and releasing one here
// would double-drive it.
//
// Together with mrOnlyEligibleReasons (UnparkNever/UnparkRetired) and
// mergeStage (the merge-stage subset of UnparkNever), the reasons below cover
// every remaining ParkReasons member - UnparkHuman, UnparkTimer and
// UnparkRetry - so the three tables jointly account for the whole enum.
func TestUnparkMaintainerCommentRefusesAParkThatIsSomebodyElsesLane(t *testing.T) {
	tests := []struct {
		name   string
		reason string
	}{
		{"awaiting-human belongs to driveUnparks", stage.ReasonAwaitingHuman},
		{"backlog-sweep belongs to driveUnparks", stage.ReasonBacklogSweep},
		{"identity-unverified belongs to driveUnparks", stage.ReasonIdentityUnverified},
		{"handoff-stalled belongs to driveUnparks", stage.ReasonHandoffStalled},
		{"retry-exhausted belongs to driveUnparks", stage.ReasonRetryExhausted},
		{"no-outcome is a bounded timer lane", stage.ReasonNoOutcome},
		{"ci-pending is the backed-off retry lane", stage.ReasonCIPending},
		{"mr-surface-spent is the backed-off retry lane", stage.ReasonMRSurfaceSpent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tk := task(v1alpha1.StateAwaitingReview)
			require.NoError(t, stage.Park(tk, tc.reason, now))
			require.Error(t, stage.UnparkMaintainerComment(tk, "tatara-bot", now))
			require.Equal(t, tc.reason, tk.Status.ParkReason)
		})
	}

	// A Task that is not parked at all is a CALLER BUG, not a decline, and it
	// gets the same typed error every other un-park primitive returns for it.
	unparked := task(v1alpha1.StateAwaitingReview)
	require.ErrorAs(t, stage.UnparkMaintainerComment(unparked, "tatara-bot", now), new(*stage.NotParkedError))
}

// TestUnparkMaintainerCommentResetsTheRetryBudget: park.go's ResetRetryBudget
// already names "the UnparkHuman release (a human answered)" as an authorised
// caller, and this IS that release for a population that was misclassified as
// unreachable. Without it the one release that ends an escalation is also the
// one that guarantees the next technical park re-escalates instantly.
func TestUnparkMaintainerCommentResetsTheRetryBudget(t *testing.T) {
	tk := task(v1alpha1.StateAwaitingReview)
	tk.Status.RetryAttempts = 5
	tk.Status.RetryBlocker = stage.ReasonCIFailed
	tk.Status.RetryNextAt = ptrTime(now.Add(time.Hour))
	require.NoError(t, stage.Park(tk, stage.ReasonStageDeadline, now))

	require.NoError(t, stage.UnparkMaintainerComment(tk, "tatara-bot", now.Add(time.Hour)))
	require.Zero(t, tk.Status.RetryAttempts)
	require.Empty(t, tk.Status.RetryBlocker)
	require.Nil(t, tk.Status.RetryNextAt)
}

// TestConsumeUnparkEventsSpendsOnlyUnspentHumanEvents covers the exported
// wrapper the REFUSAL arm needs: the comment is answered, so it is spent, but
// nothing is released.
func TestConsumeUnparkEventsSpendsOnlyUnspentHumanEvents(t *testing.T) {
	already := ptrTime(now.Add(-time.Hour))
	tk := task(v1alpha1.StateMerged)
	tk.Status.PendingEvents = []v1alpha1.TaskEvent{
		{Kind: "mr_comment", Author: "maintainer"},
		{Kind: "mr_comment", Author: "tatara-bot"},
		{Kind: "mr_comment", Author: "maintainer", UnparkConsumedAt: already},
	}
	require.NoError(t, stage.Park(tk, stage.ReasonMergeBlocked, now))

	stage.ConsumeUnparkEvents(tk, "tatara-bot", now.Add(time.Hour))

	require.NotNil(t, tk.Status.PendingEvents[0].UnparkConsumedAt)
	require.Equal(t, now.Add(time.Hour), tk.Status.PendingEvents[0].UnparkConsumedAt.Time)
	require.Nil(t, tk.Status.PendingEvents[1].UnparkConsumedAt, "a bot event is never eligible")
	require.Equal(t, already.Time, tk.Status.PendingEvents[2].UnparkConsumedAt.Time,
		"an already-spent stamp is never rewritten")
	require.Equal(t, stage.ReasonMergeBlocked, tk.Status.ParkReason,
		"consuming spends the comment; it does NOT release the park")
}
