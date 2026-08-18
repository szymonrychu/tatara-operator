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
