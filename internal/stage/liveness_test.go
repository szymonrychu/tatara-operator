package stage_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

func TestLiveIsExactlyThreeStates(t *testing.T) {
	live := map[string]bool{}
	for _, s := range stage.AllStates() {
		if stage.Live(s) {
			live[s] = true
		}
	}
	require.Equal(t, map[string]bool{
		v1alpha1.StateRefined:             true,
		v1alpha1.StateUnderImplementation: true,
		v1alpha1.StateAwaitingReview:      true,
	}, live)
}

func TestLiveAndOperatorDrivenAreDisjointAndCoverTheWorkStates(t *testing.T) {
	for _, s := range stage.AllStates() {
		require.False(t, stage.Live(s) && stage.OperatorDriven(s),
			"%s cannot be both live and operator-driven", s)
	}
	require.True(t, stage.OperatorDriven(v1alpha1.StateMerged))
	require.True(t, stage.OperatorDriven(v1alpha1.StateDeployed))
	require.False(t, stage.OperatorDriven(v1alpha1.StateNew))
	require.False(t, stage.OperatorDriven(v1alpha1.StateDone))
}

func TestAgentKindForIsKeyedOnStateAndOriginKind(t *testing.T) {
	require.Equal(t, stage.AgentImplement,
		stage.AgentKindFor(v1alpha1.StateUnderImplementation, "implement"))
	require.Equal(t, stage.AgentBrainstorm,
		stage.AgentKindFor(v1alpha1.StateRefined, "brainstorm"),
		"a brainstorm Task in refined needs a brainstorm agent, not an implement one")
	require.Equal(t, stage.AgentReview,
		stage.AgentKindFor(v1alpha1.StateAwaitingReview, "implement"))
	require.Equal(t, "", stage.AgentKindFor(v1alpha1.StateMerged, "implement"))
	require.Equal(t, "", stage.AgentKindFor(v1alpha1.StateNew, "implement"),
		"new is operator triage and runs no pod")
	require.Equal(t, "", stage.AgentKindFor(v1alpha1.StateRefined, "unknown-kind"),
		"an unknown origin kind must fail CLOSED, not spawn a default agent")
}

func TestAgentKindForRoutesTakeoverToImplement(t *testing.T) {
	require.Equal(t, stage.AgentImplement,
		stage.AgentKindFor(v1alpha1.StateUnderImplementation, "takeover"))
}

func TestClarifyIsNotAnAgentKind(t *testing.T) {
	for _, s := range stage.AllStates() {
		for _, k := range []string{"implement", "review", "brainstorm", "incident", "refine", "documentation", "takeover"} {
			require.NotEqual(t, "clarify", stage.AgentKindFor(s, k))
		}
	}
}

func TestAllStatesIsTheEightMembersInLifecycleOrder(t *testing.T) {
	require.Equal(t, []string{
		v1alpha1.StateNew, v1alpha1.StateRefined, v1alpha1.StateUnderImplementation,
		v1alpha1.StateAwaitingReview, v1alpha1.StateMerged, v1alpha1.StateDeployed,
		v1alpha1.StateDone, v1alpha1.StateRejected,
	}, stage.AllStates())
}

// THE PREDICATE WIDENED. This is the change that silently gives `refined` and
// `awaiting-review` an idle clock they never had.
func TestArmedClock_ArmsTheIdleClockForEveryLiveState(t *testing.T) {
	for _, state := range []string{
		v1alpha1.StateRefined, v1alpha1.StateUnderImplementation, v1alpha1.StateAwaitingReview,
	} {
		tk := task(state)
		tk.Status.PodStartedAt = ptrTime(now.Add(-30 * time.Minute))
		tk.Status.StateWorkStartedAt = ptrTime(now.Add(-29 * time.Minute))
		tk.Status.ConversationLastEventAt = ptrTime(now.Add(-10 * time.Minute))
		// Turn 0 ran and finished: the conversation exists and nobody is in it.
		tk.Annotations = map[string]string{
			v1alpha1.AnnCurrentTurn:  "turn-1",
			v1alpha1.AnnTurnComplete: now.Add(-20 * time.Minute).UTC().Format(time.RFC3339),
		}
		clock, since, budget, _ := stage.ArmedClock(tk, false)
		require.Equal(t, stage.ClockWork, clock, "%s must run the idle clock", state)
		require.Equal(t, tk.Status.ConversationLastEventAt.Time, since,
			"the idle clock measures SILENCE, never pod age - a pod TTL rotation re-stamps stateWorkStartedAt on a conversation that is very much alive")
		require.Equal(t, v1alpha1.ConversationIdleDefault, budget)
	}
}

func TestArmedClock_ANonLiveStateStillRunsTheOrdinaryClocks(t *testing.T) {
	tk := task(v1alpha1.StateMerged)
	clock, since, budget, edge := stage.ArmedClock(tk, false)
	require.Equal(t, stage.ClockWork, clock)
	require.Equal(t, tk.Status.StateEnteredAt.Time, since, "operator-driven runs clock 3 from stateEnteredAt")
	require.Equal(t, 4*time.Hour, budget)
	require.Equal(t, stage.ReasonMergeTimeout, edge.Reason)
	require.Equal(t, stage.ParkTarget, edge.To)
}

func TestArmedClock_ALiveStateWithNoConversationStampArmsNothing(t *testing.T) {
	tk := task(v1alpha1.StateRefined)
	tk.Status.ConversationLastEventAt = nil
	tk.Status.PodStartedAt = ptrTime(now)
	tk.Status.StateWorkStartedAt = ptrTime(now)
	clock, _, _, _ := stage.ArmedClock(tk, false)
	require.Equal(t, stage.ClockNone, clock,
		"an unarmed idle clock is why Enter stamps ConversationLastEventAt on entry into a live state")
}

// A live state that has NOT yet been admitted still runs the ADMISSION clock:
// the idle clock only takes over once a pod exists to be idle.
func TestArmedClock_ALiveStateWithNoPodRunsTheAdmissionClock(t *testing.T) {
	tk := task(v1alpha1.StateUnderImplementation)
	tk.Status.ConversationLastEventAt = ptrTime(now)
	clock, since, budget, edge := stage.ArmedClock(tk, false)
	require.Equal(t, stage.ClockAdmission, clock)
	require.Equal(t, tk.Status.StateEnteredAt.Time, since)
	require.Equal(t, v1alpha1.AdmissionStarvedBudget, budget)
	require.Equal(t, stage.ReasonAdmissionStarved, edge.Reason)
}

func TestEnterStampsTheIdleClockOnEveryLiveState(t *testing.T) {
	for _, tc := range []struct{ from, to string }{
		{v1alpha1.StateNew, v1alpha1.StateRefined},
		{v1alpha1.StateRefined, v1alpha1.StateUnderImplementation},
		{v1alpha1.StateUnderImplementation, v1alpha1.StateAwaitingReview},
	} {
		tk := task(tc.from)
		require.NoError(t, stage.Enter(tk, nil, tc.to, "", now))
		require.NotNil(t, tk.Status.ConversationLastEventAt, "%s -> %s must arm the idle clock", tc.from, tc.to)
		require.Equal(t, now, tk.Status.ConversationLastEventAt.Time)
	}
}

func TestEnterDoesNotStampTheIdleClockOnANonLiveState(t *testing.T) {
	tk := task(v1alpha1.StateAwaitingReview)
	require.NoError(t, stage.Enter(tk, []v1alpha1.MergeRequest{{}}, v1alpha1.StateMerged, "", now))
	require.Nil(t, tk.Status.ConversationLastEventAt)
}

// THE MANDATORY BACKSTOP. Generalising liveness replaces a live state's WORK
// clock with an IDLE clock, so under-implementation lost the absolute residency
// bound `implementing` had.
func TestResidencyExceededBoundsEveryLiveState(t *testing.T) {
	for _, tc := range []struct {
		state string
		cap   time.Duration
	}{
		// O3: ALL THREE ARE 24h. The 6h/4h caps were the last two clocks that
		// killed working agents on a proxy for "stuck", and "no feature should
		// exceed 24h ever" is the platform invariant that replaced them.
		{v1alpha1.StateRefined, stage.ResidencyCapAll},
		{v1alpha1.StateUnderImplementation, stage.ResidencyCapAll},
		{v1alpha1.StateAwaitingReview, stage.ResidencyCapAll},
	} {
		require.Equal(t, 24*time.Hour, tc.cap, "the ONE surviving ceiling is 24h for %s", tc.state)
		under := task(tc.state)
		under.Status.StateEnteredAt = ptrTime(now.Add(-tc.cap + time.Minute))
		under.Status.StateWorkStartedAt = ptrTime(now.Add(-tc.cap + time.Minute))
		require.False(t, stage.ResidencyExceeded(under, now), "%s must not fire under its cap", tc.state)

		over := task(tc.state)
		over.Status.StateEnteredAt = ptrTime(now.Add(-tc.cap - time.Minute))
		over.Status.StateWorkStartedAt = ptrTime(now.Add(-tc.cap - time.Minute))
		require.True(t, stage.ResidencyExceeded(over, now), "%s must fire over its cap", tc.state)
	}
}

func TestResidencyExceededIsCumulativeAcrossAParkRoundTrip(t *testing.T) {
	tk := task(v1alpha1.StateUnderImplementation)
	tk.Status.StateEnteredAt = ptrTime(now.Add(-1 * time.Hour))
	tk.Status.StateWorkStartedAt = ptrTime(now.Add(-1 * time.Hour))
	tk.Status.StageElapsedCarrySeconds = int((23*time.Hour + 30*time.Minute).Seconds())
	require.True(t, stage.ResidencyExceeded(tk, now),
		"24h30m of cumulative residency exceeds the 24h cap")

	// The park round trip must not launder the carry into a fresh day.
	under := task(v1alpha1.StateUnderImplementation)
	under.Status.StateEnteredAt = ptrTime(now.Add(-1 * time.Hour))
	under.Status.StateWorkStartedAt = ptrTime(now.Add(-1 * time.Hour))
	under.Status.StageElapsedCarrySeconds = int((22 * time.Hour).Seconds())
	require.False(t, stage.ResidencyExceeded(under, now), "23h cumulative is still inside the cap")
}

// B3. RESIDENCY MUST NOT CHARGE ADMISSION-QUEUE TIME. A live-state Task that
// has never been admitted has StateWorkStartedAt == nil, and StateEnteredAt is
// stamped the instant its QueuedEvent is enqueued - not when a pod exists. A
// residency check that reads StateEnteredAt alone therefore parks a Task that
// never ran a pod at its live-state cap (6h/4h) instead of the 24h admission
// budget clock 1 owns, which is exactly what StateWorkStartedAt's own doc
// (api/v1alpha1/task_types.go) exists to prevent for clock 2.
func TestResidencyExceededDoesNotApplyBeforeWorkStarted(t *testing.T) {
	tk := task(v1alpha1.StateUnderImplementation)
	tk.Status.StateEnteredAt = ptrTime(now.Add(-25 * time.Hour))
	require.Nil(t, tk.Status.StateWorkStartedAt)
	require.False(t, stage.ResidencyExceeded(tk, now),
		"a Task with no pod and no work started is still queueing; residency must not fire before admission")
}

// The pair to the above: once work has actually started, residency is the
// non-negotiable mitigation again and must still fire past the cap.
func TestResidencyExceededAppliesOnceWorkStarted(t *testing.T) {
	tk := task(v1alpha1.StateUnderImplementation)
	tk.Status.StateEnteredAt = ptrTime(now.Add(-25 * time.Hour))
	tk.Status.StateWorkStartedAt = ptrTime(now.Add(-25 * time.Hour))
	require.True(t, stage.ResidencyExceeded(tk, now),
		"work started 25h ago is 19h past the 6h under-implementation cap: residency must still fire")
}

func TestResidencyExceededDoesNotApplyOutsideTheLiveStates(t *testing.T) {
	for _, state := range []string{
		v1alpha1.StateNew, v1alpha1.StateMerged, v1alpha1.StateDeployed,
		v1alpha1.StateDone, v1alpha1.StateRejected,
	} {
		tk := task(state)
		tk.Status.StateEnteredAt = ptrTime(now.Add(-1000 * time.Hour))
		require.False(t, stage.ResidencyExceeded(tk, now),
			"%s has its own work budget or none at all; residency must not shadow it", state)
	}
}

// ---------------------------------------------------------------------------
// THE IDLE CLOCK BOUNDS A CONVERSATION, NOT AGENT WORK.
//
// Promoting `conversing`'s 60m idle clock to all three live states deleted the
// WORK budget every one of them used to have (implementing 6h, reviewing 4h),
// and ConversationLastEventAt is re-stamped by exactly one thing: a HUMAN
// webhook comment. A silently-working implement agent therefore aged out at 60
// minutes and parked awaiting-human, where it needed a human comment or died at
// ParkRetention.
//
// The fix is that the idle clock is only armed while a conversation is actually
// OUTSTANDING - no turn in flight - and re-arms from the end of the last turn.
// A working agent is bounded by the turn stall timeout and, absolutely, by
// ResidencyExceeded.
// ---------------------------------------------------------------------------

// live builds a live-state Task entered `ago`, with a pod up, work started, and
// the conversation stamp equally old - so it is simultaneously `ago` of state
// residency and `ago` of silence, and a test can read either bound off it.
func live(state string, ago time.Duration) *v1alpha1.Task {
	tk := task(state)
	tk.Status.StateEnteredAt = ptrTime(now.Add(-ago))
	tk.Status.PodStartedAt = ptrTime(now.Add(-ago))
	tk.Status.StateWorkStartedAt = ptrTime(now.Add(-ago))
	tk.Status.ConversationLastEventAt = ptrTime(now.Add(-ago))
	return tk
}

func TestArmedClock_AnInFlightTurnDisarmsTheIdleClock(t *testing.T) {
	for _, state := range []string{
		v1alpha1.StateRefined, v1alpha1.StateUnderImplementation, v1alpha1.StateAwaitingReview,
	} {
		tk := live(state, 3*time.Hour)
		tk.Annotations = map[string]string{v1alpha1.AnnCurrentTurn: "turn-1"}
		clock, _, _, _ := stage.ArmedClock(tk, false)
		require.Equal(t, stage.ClockNone, clock,
			"%s: an agent three hours into a turn is WORKING, not idle; parking it is the #521 regression", state)
	}
}

func TestArmedClock_TheIdleClockReArmsFromTheEndOfTheLastTurn(t *testing.T) {
	tk := live(v1alpha1.StateUnderImplementation, 3*time.Hour)
	tk.Annotations = map[string]string{
		v1alpha1.AnnCurrentTurn:  "turn-1",
		v1alpha1.AnnTurnComplete: now.Add(-10 * time.Minute).UTC().Format(time.RFC3339),
	}
	clock, since, _, edge := stage.ArmedClock(tk, false)
	require.Equal(t, stage.ClockWork, clock, "the turn is over: the conversation is outstanding again")
	require.Equal(t, now.Add(-10*time.Minute), since,
		"the idle window starts when the agent stopped talking, not when the human last did")
	require.Equal(t, stage.ParkTarget, edge.To)
	require.Equal(t, stage.ReasonAwaitingHuman, edge.Reason)
}

// The human is the LATER of the two: a comment arriving mid-turn re-arms from
// the comment, not from the turn that was already running when it landed.
func TestArmedClock_TheIdleBaseIsTheLaterOfTheHumanEventAndTheTurnEnd(t *testing.T) {
	tk := live(v1alpha1.StateUnderImplementation, 3*time.Hour)
	tk.Status.ConversationLastEventAt = ptrTime(now.Add(-5 * time.Minute))
	tk.Annotations = map[string]string{
		v1alpha1.AnnCurrentTurn:  "turn-1",
		v1alpha1.AnnTurnComplete: now.Add(-40 * time.Minute).UTC().Format(time.RFC3339),
	}
	_, since, _, _ := stage.ArmedClock(tk, false)
	require.Equal(t, now.Add(-5*time.Minute), since)
}

// THE ADMISSION QUEUE IS NOT SILENCE. A Task that waited 3h for a slot enters
// its pod with a conversation stamp 3h old, so an idle clock armed the instant
// the pod goes Ready parks it BEFORE turn 0 is ever submitted - the agent never
// speaks and the human never gets anything to be silent about. The clock waits
// for a turn to exist; until then clock 1 and the residency cap own the Task.
func TestArmedClock_TheIdleClockDoesNotChargeTheAdmissionQueue(t *testing.T) {
	tk := live(v1alpha1.StateRefined, 3*time.Hour)
	// The Task entered refined 3h ago and queued; its pod went Ready a minute ago.
	tk.Status.PodStartedAt = ptrTime(now.Add(-2 * time.Minute))
	tk.Status.StateWorkStartedAt = ptrTime(now.Add(-time.Minute))

	clock, since, _, _ := stage.ArmedClock(tk, false)
	require.Equal(t, stage.ClockWork, clock, "the exit must stay armed: every live state needs a deadline")
	require.Equal(t, tk.Status.StateWorkStartedAt.Time, since,
		"three hours of queueing is not three hours of silence; measuring it from state entry parks the Task before turn 0 is ever submitted")
	_, fired := stage.Elapsed(tk, false, now)
	require.False(t, fired, "the agent has not been given a turn yet and is already out of time")
}

// The regression guard for the behaviour that was CORRECT all along: a
// conversation nobody is working on still parks.
func TestArmedClock_AnAbandonedConversationStillParksOnTheIdleClock(t *testing.T) {
	tk := live(v1alpha1.StateRefined, 3*time.Hour)
	tk.Annotations = map[string]string{
		v1alpha1.AnnCurrentTurn:  "turn-1",
		v1alpha1.AnnTurnComplete: now.Add(-3 * time.Hour).UTC().Format(time.RFC3339),
	}
	clock, since, budget, edge := stage.ArmedClock(tk, false)
	require.Equal(t, stage.ClockWork, clock)
	require.Equal(t, tk.Status.ConversationLastEventAt.Time, since)
	require.Equal(t, v1alpha1.ConversationIdleDefault, budget)
	require.Equal(t, stage.ReasonAwaitingHuman, edge.Reason)
	_, fired := stage.Elapsed(tk, false, now)
	require.True(t, fired, "three hours of silence with no turn running is an abandoned conversation")
}

// An unparseable or absent turn stamp must never widen the bound: it falls back
// to the conversation stamp rather than disarming the clock.
func TestArmedClock_AnUnparseableTurnCompleteStampFallsBackToTheConversation(t *testing.T) {
	tk := live(v1alpha1.StateUnderImplementation, 3*time.Hour)
	tk.Annotations = map[string]string{
		v1alpha1.AnnCurrentTurn:  "turn-1",
		v1alpha1.AnnTurnComplete: "not-a-timestamp",
	}
	clock, since, _, _ := stage.ArmedClock(tk, false)
	require.Equal(t, stage.ClockWork, clock)
	require.Equal(t, tk.Status.ConversationLastEventAt.Time, since)
}

// THE TWO BOUNDS COMPOSE, AND THEY DO NOT OVERLAP. The idle clock fires when the
// state is QUIET; residency fires when it has been LONG. An agent that keeps a
// turn in flight forever is invisible to the first and caught by the second, so
// disarming the idle clock cannot let an agent run without a deadline.
func TestASilentlyWorkingAgentIsBoundedByResidencyNotByTheIdleClock(t *testing.T) {
	tk := live(v1alpha1.StateUnderImplementation, 25*time.Hour)
	tk.Annotations = map[string]string{v1alpha1.AnnCurrentTurn: "turn-1"}

	clock, _, _, _ := stage.ArmedClock(tk, false)
	require.Equal(t, stage.ClockNone, clock, "the idle clock must not touch a working agent")
	require.True(t, stage.ResidencyExceeded(tk, now),
		"25h in under-implementation is past the 24h absolute cap: residency is the bound that fires")

	// A SEVEN-HOUR CODING RUN IS NOT A STALL. It used to park here at the old 6h
	// under-implementation cap, and that was the ceiling O3 exists to delete.
	sevenHours := live(v1alpha1.StateUnderImplementation, 7*time.Hour)
	sevenHours.Annotations = map[string]string{v1alpha1.AnnCurrentTurn: "turn-1"}
	require.False(t, stage.ResidencyExceeded(sevenHours, now),
		"a 7h implement run must survive: the 6h cap is deleted")

	// And BELOW the cap neither fires, which is what makes them a pair rather
	// than a duplicate: there is exactly one live deadline at any moment.
	fresh := live(v1alpha1.StateUnderImplementation, 2*time.Hour)
	fresh.Annotations = map[string]string{v1alpha1.AnnCurrentTurn: "turn-1"}
	clock, _, _, _ = stage.ArmedClock(fresh, false)
	require.Equal(t, stage.ClockNone, clock)
	require.False(t, stage.ResidencyExceeded(fresh, now))
}
