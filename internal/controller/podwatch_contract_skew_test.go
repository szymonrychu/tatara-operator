package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// TestPodWatchContractSkewWaitsInsteadOfDestroying is the #544 regression.
//
// On origin/main a wrapper one contract version ahead of the operator - the
// GUARANTEED state in the middle of a four-release train, because helmfile rolls
// the agent-image pin and the operator Deployment independently - routes
// straight to ParkTask(agent-contract-mismatch). That destroyed 5 Tasks at
// turn-0 across two Projects inside a 56-minute window on 2026-08-08. The skew
// resolved on its own the moment the operator rolled forward; the very same pod
// would then have passed the handshake.
//
// The Task must SURVIVE the window: requeue and re-check, no park, no state
// move, and still zero turns burned.
func TestPodWatchContractSkewWaitsInsteadOfDestroying(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  int
	}{
		{name: "wrapper ahead", got: agent.ContractVersion + 1},
		{name: "wrapper behind", got: agent.ContractVersion - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newFakeSession()
			sess.sessionInfo = agent.SessionInfo{State: agent.SessionStateReady, ContractVersion: ptrInt(tc.got)}
			name := "podclock-skew-" + sanitizeName(tc.name)
			r, _, pod := seedStagedTask(t, name, tatarav1alpha1.StateUnderImplementation, "implement", sess)
			setPodStatus(t, pod, 20*time.Second, true)

			res := reconcilePod(t, r, pod)

			got := getTask(t, name)
			require.False(t, tatarav1alpha1.Parked(got),
				"a release-train skew must not park the Task: the skew resolves on its own when the train finishes")
			require.Equal(t, tatarav1alpha1.StateUnderImplementation, got.Status.State)
			require.Nil(t, got.Status.StateWorkStartedAt, "a skewed pod never starts the work clock")
			require.Greater(t, res.RequeueAfter, time.Duration(0), "the skew must be re-checked, not dropped")
			require.Empty(t, sess.submits, "a skewed pod must submit ZERO turns while it waits")
		})
	}
}

// TestPodWatchContractSkewParksOnceItIsNoLongerARollout: the wait is BOUNDED. A
// skew that outlives any plausible release train is a genuinely wrong pin, and
// the pre-existing terminal park is the right answer for it.
func TestPodWatchContractSkewParksOnceItIsNoLongerARollout(t *testing.T) {
	sess := newFakeSession()
	sess.sessionInfo = agent.SessionInfo{State: agent.SessionStateReady, ContractVersion: ptrInt(agent.ContractVersion + 1)}
	r, _, pod := seedStagedTask(t, "podclock-skew-stuck", tatarav1alpha1.StateUnderImplementation, "implement", sess)
	// The pod has been up far longer than any release train takes to settle, and
	// the wrapper still speaks a version the operator does not. The literal is
	// deliberate: it must stay past agentContractSkewDeadline, and spelling it out
	// keeps this file compiling against origin/main so the sibling test above can
	// be shown to fail there.
	setPodStatus(t, pod, 150*time.Minute, true)

	reconcilePod(t, r, pod)

	got := getTask(t, "podclock-skew-stuck")
	require.True(t, tatarav1alpha1.Parked(got), "a skew that never resolves is a wrong pin, not a rollout")
	require.Equal(t, stage.ReasonAgentContractMismatch, got.Status.ParkReason)
	require.Empty(t, sess.submits, "still zero turns burned")
}
