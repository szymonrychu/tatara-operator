// Tests for issue #527: the G.7 stop sequence reports TWO independent facts and
// used to squash them into one label. `outcome` is how the POD was stopped;
// `handoff` is how the CONTINUATION STATE was captured. A pod whose agent wrote
// a perfect handoff and whose wrapper then failed to tear down cleanly is
// `force_deleted` on one axis and `agent` on the other, and the old single label
// could only say the first - which is why synthetic_handoff was never once
// recorded in production and why the alert on force_deleted asserted a mechanism
// that cannot happen.
package agent_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// agentWritesItsNote scripts the agent's side of the handoff turn.
func agentWritesItsNote(t *testing.T, h *ttlHarness) func() {
	return func() {
		fresh := &tatarav1alpha1.Task{}
		require.NoError(t, h.c.Get(context.Background(),
			types.NamespacedName{Namespace: ttlNS, Name: "task-ttl"}, fresh))
		fresh.Status.Notes = append(fresh.Status.Notes, tatarav1alpha1.Note{
			At: metav1.NewTime(h.now), Agent: "implement", Kind: "handoff",
			Body: "PR #12 is open; rebase onto main and re-run the merge gate.",
		})
		require.NoError(t, h.c.Status().Update(context.Background(), fresh))
	}
}

// TestTTLStop_ForceDeleteKeepsHandoffDimension is the label-conflation
// regression. The agent DID write its handoff; only the teardown failed. Before
// the split, finish() overwrote the capture dimension with force_deleted and the
// fact that continuity was intact was destroyed on the way to the metric.
func TestTTLStop_ForceDeleteKeepsHandoffDimension(t *testing.T) {
	sess := &stopSession{
		states:    []string{agent.SessionStateReady},
		deleteErr: &agent.UnreachableError{},
	}
	h := newTTLHarness(t, sess)
	sess.onHandoff = agentWritesItsNote(t, h)

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, h.input())
	require.NoError(t, err)
	require.Equal(t, agent.TTLOutcomeForceDeleted, res.Outcome, "the pod genuinely had to be force-deleted")
	require.Equal(t, agent.TTLHandoffAgent, res.Handoff,
		"the agent's own handoff note landed; a failed teardown must not erase that")
	require.Equal(t, agent.TTLOutcomeForceDeleted, h.outcome)
	require.Equal(t, agent.TTLHandoffAgent, h.handoff, "both dimensions must reach the metric")
}

// TestTTLStop_GracefulIsItsOwnOutcome: a clean stop is `graceful` on the stop
// dimension whether the handoff came from the agent or from the operator. The
// stop dimension must not encode which.
func TestTTLStop_GracefulIsItsOwnOutcome(t *testing.T) {
	cases := []struct {
		name        string
		handoffErr  error
		agentWrites bool
		wantHandoff string
	}{
		{name: "agent answered", agentWrites: true, wantHandoff: agent.TTLHandoffAgent},
		{
			name:        "wrapper 410s the handoff turn",
			handoffErr:  &agent.HTTPError{Status: http.StatusGone},
			wantHandoff: agent.TTLHandoffSynthetic,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sess := &stopSession{states: []string{agent.SessionStateReady}, handoffErr: tc.handoffErr}
			h := newTTLHarness(t, sess)
			if tc.agentWrites {
				sess.onHandoff = agentWritesItsNote(t, h)
			}
			res, err := h.stopper.StopWithHandoff(context.Background(), h.task, h.input())
			require.NoError(t, err)
			require.Equal(t, agent.TTLOutcomeGraceful, res.Outcome)
			require.Equal(t, tc.wantHandoff, res.Handoff)
			require.False(t, h.podExists(t))
		})
	}
}

// TestTTLStop_ContentFreeSyntheticNoteIsHandoffNone is the #527 root-cause
// regression on the reporting side. When the operator holds NO continuation
// payload the synthetic note is a placeholder, not a handoff: it must say so in
// its body and must NOT be counted as a captured handoff. The old code wrote
// "Last turn's final text: (none). Repos pushed: none." and reported the
// non-empty-notes invariant as satisfied - vacuously.
func TestTTLStop_ContentFreeSyntheticNoteIsHandoffNone(t *testing.T) {
	sess := &stopSession{
		states:     []string{agent.SessionStateReady},
		handoffErr: &agent.HTTPError{Status: http.StatusGone},
	}
	h := newTTLHarness(t, sess)
	in := h.input()
	in.LastFinalText = ""
	in.PushedRepos = nil

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
	require.NoError(t, err)
	require.Equal(t, agent.TTLHandoffNone, res.Handoff,
		"a note with nothing in it is not a captured handoff")
	require.Equal(t, agent.TTLHandoffNone, h.handoff)

	notes := h.notes(t)
	require.Len(t, notes, 1, "the invariant still holds: notes are never empty after a TTL stop")
	require.Contains(t, notes[0].Body, "NO CONTINUATION STATE WAS CAPTURED",
		"the next pod must be able to tell a placeholder from a handoff")
	require.NotContains(t, notes[0].Body, "(none)",
		"the placeholder must not read as a filled-in handoff whose fields happened to be empty")
}

// TestTTLStop_PartialPayloadStillCountsAsSynthetic: pushed repos with no final
// text (or the reverse) IS continuation state. Only a completely empty payload
// is handoff=none.
func TestTTLStop_PartialPayloadStillCountsAsSynthetic(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(*agent.TTLStopInput)
	}{
		{"final text only", func(in *agent.TTLStopInput) { in.PushedRepos = nil }},
		{"pushed repos only", func(in *agent.TTLStopInput) { in.LastFinalText = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := &stopSession{
				states:     []string{agent.SessionStateReady},
				handoffErr: &agent.HTTPError{Status: http.StatusGone},
			}
			h := newTTLHarness(t, sess)
			in := h.input()
			tc.apply(&in)
			res, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
			require.NoError(t, err)
			require.Equal(t, agent.TTLHandoffSynthetic, res.Handoff)
		})
	}
}

// TestTTLStop_WrapperAlreadyGone is the reaper race from the incident: the idle
// backstop deleted the pod, then the TTL stop ran against the corpse. It spent
// the whole handoff budget on an unreachable wrapper and reported force_deleted
// for a pod nobody force-deleted. There is nothing to hand off from a pod that
// does not exist - say so, cheaply, and do not claim a forced stop.
func TestTTLStop_WrapperAlreadyGone(t *testing.T) {
	sess := &stopSession{states: []string{agent.SessionStateReady}}
	h := newTTLHarness(t, sess)
	pod := &corev1.Pod{}
	pod.Name, pod.Namespace = agent.PodName(h.task), ttlNS
	require.NoError(t, h.c.Delete(context.Background(), pod))

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, h.input())
	require.NoError(t, err)
	require.Equal(t, agent.TTLOutcomeGraceful, res.Outcome,
		"nothing was force-deleted: the pod was already gone")
	require.Equal(t, agent.TTLHandoffSynthetic, res.Handoff)
	require.Zero(t, sess.handoffs, "no handoff turn is offered to a pod that does not exist")
	require.Zero(t, sess.getCalls, "no session polling against a corpse")
	require.Zero(t, sess.deleteSessions, "no session delete against a corpse")
	require.NotEmpty(t, h.notes(t), "the synthetic note still lands")
}

// TestTTLStop_MidSequencePodLossIsNotAForcedStop: the pod vanishes between the
// up-front probe and the teardown. DeleteSession fails, but force-deleting an
// absent pod is a no-op, so reporting force_deleted would be a fabrication.
func TestTTLStop_MidSequencePodLossIsNotAForcedStop(t *testing.T) {
	sess := &stopSession{
		states:     []string{agent.SessionStateReady},
		handoffErr: &agent.HTTPError{Status: http.StatusGone},
		deleteErr:  &agent.UnreachableError{},
	}
	h := newTTLHarness(t, sess)
	sess.onDeleteSession = func() {
		pod := &corev1.Pod{}
		pod.Name, pod.Namespace = agent.PodName(h.task), ttlNS
		require.NoError(t, h.c.Delete(context.Background(), pod))
	}

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, h.input())
	require.NoError(t, err)
	require.Equal(t, agent.TTLOutcomeGraceful, res.Outcome)
	require.Equal(t, agent.TTLHandoffSynthetic, res.Handoff)
}
