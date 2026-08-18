package stage_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// THE EDGE A DELIVERED TASK NEEDS, AND THE GUARD THAT KEEPS IT FROM BEING A
// DOOR AROUND REVIEW.
//
// `under-implementation -> merged` exists for exactly one fact: every merge
// request the Task controller-owns has ALREADY MERGED on the forge, so there is
// nothing left to open, nothing left to review and nothing left to merge. That
// is the same fact OwnMRsShippedEdge already finalizes from `awaiting-review`;
// the state it can be recognised FROM is the only thing widening here.
//
// The guard is what makes it safe. Without AllMRsMerged the edge would let any
// under-implementation Task skip review entirely, which is the hole GUARDS 3-6
// close for their own edges - and a guard that lives in the caller is not a
// guard (LegalFor's own doc comment).
func TestLegalFor_UnderImplementationToMerged(t *testing.T) {
	mr := func(state string) v1alpha1.MergeRequest {
		return v1alpha1.MergeRequest{Status: v1alpha1.MergeRequestStatus{State: state}}
	}
	cases := []struct {
		name string
		kind string
		mrs  []v1alpha1.MergeRequest
		want bool
	}{
		{"every owned MR merged", "upgrade", []v1alpha1.MergeRequest{mr("merged")}, true},
		{"multi-repo, all merged", "implement",
			[]v1alpha1.MergeRequest{mr("merged"), mr("merged")}, true},
		{"one still open", "implement",
			[]v1alpha1.MergeRequest{mr("merged"), mr("open")}, false},
		{"one closed unmerged", "implement",
			[]v1alpha1.MergeRequest{mr("merged"), mr("closed")}, false},
		{"no owned MR at all", "implement", nil, false},
		{"kind=review may never reach merged", "review", []v1alpha1.MergeRequest{mr("merged")}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: tc.kind}}
			got := stage.LegalFor(task, tc.mrs, v1alpha1.StateUnderImplementation, v1alpha1.StateMerged)
			require.Equal(t, tc.want, got)
		})
	}
}

// Enter is the choke point and must refuse the edge on the same predicate: a
// caller that reaches for it with an open merge request gets an
// IllegalTransitionError, never a silent skip past review.
func TestEnter_UnderImplementationToMergedRefusedWithAnOpenMR(t *testing.T) {
	now := time.Now()
	task := &v1alpha1.Task{
		Spec:   v1alpha1.TaskSpec{Kind: "upgrade"},
		Status: v1alpha1.TaskStatus{State: v1alpha1.StateUnderImplementation},
	}
	open := []v1alpha1.MergeRequest{{Status: v1alpha1.MergeRequestStatus{State: "open"}}}
	require.Error(t, stage.Enter(task, open, v1alpha1.StateMerged, "", now))

	merged := []v1alpha1.MergeRequest{{Status: v1alpha1.MergeRequestStatus{State: "merged"}}}
	require.NoError(t, stage.Enter(task, merged, v1alpha1.StateMerged, "", now))
	require.Equal(t, v1alpha1.StateMerged, task.Status.State)
}

// THE RE-ARM LATCH. An agent that ends its pod with a handoff note has said it
// is done; re-minting a pod is only justified while the platform has reason to
// believe the next turn differs from the one that just ended. The FIRST re-arms
// are the legitimate continuation budget (an agent that ran out of context
// mid-implementation hands over and the next pod carries on). Past the cap, with
// nothing new to deliver, the platform must stop asking.
func TestAgentStopReArmExhausted(t *testing.T) {
	cases := []struct {
		name    string
		stops   int
		pending []v1alpha1.TaskEvent
		want    bool
	}{
		{"first stop re-arms", 1, nil, false},
		{"inside the continuation budget", v1alpha1.AgentStopReArmCap - 1, nil, false},
		{"at the cap", v1alpha1.AgentStopReArmCap, nil, true},
		{"past the cap", v1alpha1.AgentStopReArmCap + 3, nil, true},
		{
			"a pending event is a change of circumstance and always re-arms",
			v1alpha1.AgentStopReArmCap + 3,
			[]v1alpha1.TaskEvent{{Kind: "comment", Author: "human"}},
			false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &v1alpha1.Task{Status: v1alpha1.TaskStatus{
				State:         v1alpha1.StateUnderImplementation,
				PendingEvents: tc.pending,
			}}
			task.Status.Stats.AgentStops = tc.stops
			require.Equal(t, tc.want, stage.AgentStopReArmExhausted(task))
		})
	}
}

// The streak belongs to ONE state occupancy. Every way out of the state - a
// transition or an un-park - resets it in the SAME status write as the state, so
// a re-driven Task never arrives at its replacement pod already over budget (the
// #513 shape: a re-entry that spends its whole budget in milliseconds).
func TestAgentStopStreakIsResetByEnterAndByUnpark(t *testing.T) {
	now := time.Now()

	entered := &v1alpha1.Task{
		Spec:   v1alpha1.TaskSpec{Kind: "upgrade"},
		Status: v1alpha1.TaskStatus{State: v1alpha1.StateUnderImplementation},
	}
	entered.Status.Stats.AgentStops = v1alpha1.AgentStopReArmCap
	require.NoError(t, stage.Enter(entered, nil, v1alpha1.StateAwaitingReview, "", now))
	require.Zero(t, entered.Status.Stats.AgentStops,
		"a state change is a change of circumstance: the streak starts again")

	stamp := metav1.NewTime(now.Add(-2 * time.Hour))
	unparked := &v1alpha1.Task{
		Spec: v1alpha1.TaskSpec{Kind: "upgrade"},
		Status: v1alpha1.TaskStatus{
			State: v1alpha1.StateUnderImplementation, ParkReason: stage.ReasonNoOutcome,
			ParkedFromState: v1alpha1.StateUnderImplementation, ParkedAt: &stamp,
			StateEnteredAt: &stamp,
		},
	}
	unparked.Status.Stats.AgentStops = v1alpha1.AgentStopReArmCap
	require.Equal(t, stage.DeclineNone,
		stage.Unpark(stage.UnparkInput{Task: unparked, Now: now, LiveHasRoom: true}))
	require.Zero(t, unparked.Status.Stats.AgentStops,
		"an un-park re-drives the state: it must not arrive already exhausted")
}

// RESIDENCY'S ARMING GATE MUST NOT BE THE THING THE LOOP KEEPS CLEARING.
//
// ResidencyExceeded requires evidence that work actually STARTED in this state
// (fix B3: charging admission-queue time would park a Task that never got a
// slot). StateWorkStartedAt is that evidence and it is NILLED on every
// agent-requested stop, so through the 80-second respawn loop the absolute bound
// was armed for roughly 55 seconds of each cycle and disarmed for the rest. An
// agent-requested stop is itself proof that a pod was admitted, became Ready and
// ran a turn in this state, so the streak arms it durably.
func TestResidencyExceededArmedByTheAgentStopStreak(t *testing.T) {
	now := time.Now()
	entered := metav1.NewTime(now.Add(-stage.ResidencyCapAll - time.Hour))

	queued := &v1alpha1.Task{Status: v1alpha1.TaskStatus{
		State: v1alpha1.StateUnderImplementation, StateEnteredAt: &entered,
	}}
	require.False(t, stage.ResidencyExceeded(queued, now),
		"a Task that never got a pod must not be charged for the admission queue")

	stopped := queued.DeepCopy()
	stopped.Status.Stats.AgentStops = 1
	require.True(t, stage.ResidencyExceeded(stopped, now),
		"an agent-requested stop proves a pod ran here; the absolute bound stays armed between pods")
}
