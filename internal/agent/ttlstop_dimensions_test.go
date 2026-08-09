package agent_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// agentWrites makes the fake wrapper's agent answer the handoff turn with a note
// of its own, which is what TTLHandoffAgent means.
func agentWrites(t *testing.T, h *ttlHarness) {
	t.Helper()
	h.sess.onHandoff = func() {
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

// THE TWO DIMENSIONS ARE INDEPENDENT, AND finish() OWNS ONLY ONE OF THEM.
//
// operator_agent_pod_ttl_expired_total carried a single `outcome` label until
// #527, and finish() overwrote it on any DeleteSession/DeleteWrapper error. So
// the question the metric was documented to answer - was continuation state
// captured? - was destroyed by the answer to a different one: did the pod come
// down cleanly? Two consequences, both measured live:
//
//   - synthetic_handoff was STRUCTURALLY unreachable. The synthetic path only ran
//     when the wrapper was already unreachable, and finish() then always
//     escalated. list_prometheus_label_values over 30 days returned exactly
//     [agent_handoff, force_deleted].
//   - force_deleted, which the alert fired on, could equally mean "the agent
//     handed off perfectly and the wrapper was slow to die".
//
// This is the full cross product. Every pairing must be reachable, and the
// notes-are-never-empty invariant must hold on all of them.
func TestTTLStop_StopAndHandoffAreIndependentDimensions(t *testing.T) {
	cases := []struct {
		name       string
		agentWrote bool
		handoffErr error
		deleteErr  error
		empty      bool // the operator holds no last-turn payload
		want       agent.TTLStopResult
	}{
		{
			name:       "agent handoff, graceful",
			agentWrote: true,
			want:       agent.TTLStopResult{Outcome: agent.TTLOutcomeGraceful, Handoff: agent.TTLHandoffAgent},
		},
		{
			name:       "agent handoff, force-deleted",
			agentWrote: true,
			deleteErr:  &agent.UnreachableError{},
			want:       agent.TTLStopResult{Outcome: agent.TTLOutcomeForceDeleted, Handoff: agent.TTLHandoffAgent},
		},
		{
			name:       "synthetic handoff, graceful",
			handoffErr: &agent.HTTPError{Status: http.StatusGone},
			want:       agent.TTLStopResult{Outcome: agent.TTLOutcomeGraceful, Handoff: agent.TTLHandoffSynthetic},
		},
		{
			name:       "synthetic handoff, force-deleted",
			handoffErr: &agent.HTTPError{Status: http.StatusGone},
			deleteErr:  &agent.UnreachableError{},
			want:       agent.TTLStopResult{Outcome: agent.TTLOutcomeForceDeleted, Handoff: agent.TTLHandoffSynthetic},
		},
		{
			name:       "no handoff, graceful",
			handoffErr: &agent.HTTPError{Status: http.StatusGone},
			empty:      true,
			want:       agent.TTLStopResult{Outcome: agent.TTLOutcomeGraceful, Handoff: agent.TTLHandoffNone},
		},
		{
			name:       "no handoff, force-deleted",
			handoffErr: &agent.HTTPError{Status: http.StatusGone},
			deleteErr:  &agent.UnreachableError{},
			empty:      true,
			want:       agent.TTLStopResult{Outcome: agent.TTLOutcomeForceDeleted, Handoff: agent.TTLHandoffNone},
		},
	}
	require.Len(t, cases, 6, "2 stop values x 3 handoff values: every pairing must be reachable")

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sess := &stopSession{
				states:     []string{agent.SessionStateReady},
				handoffErr: tc.handoffErr,
				deleteErr:  tc.deleteErr,
			}
			h := newTTLHarness(t, sess)
			if tc.agentWrote {
				agentWrites(t, h)
			}
			in := h.input()
			if tc.empty {
				in.LastFinalText, in.PushedRepos = "", nil
			}

			res, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
			require.NoError(t, err)
			require.Equal(t, tc.want, res)
			require.Equal(t, tc.want, h.res, "the metric must carry both dimensions, not one")

			require.NotEmpty(t, h.notes(t),
				"result %+v left Task.status.notes EMPTY: the continuation state is gone", res)
		})
	}
}

// A FORCE-DELETE MUST NOT ERASE A CAPTURED HANDOFF. This is the single most
// consequential pairing and it is what the alert was reading backwards for 19
// days: the agent wrote a perfect note, the wrapper was then slow to die, and
// the metric reported the Task as force_deleted with no way to tell it from
// total loss.
func TestTTLStop_ForceDeleteKeepsTheAgentHandoffFact(t *testing.T) {
	sess := &stopSession{
		states:    []string{agent.SessionStateBusy, agent.SessionStateReady},
		deleteErr: &agent.UnreachableError{},
	}
	h := newTTLHarness(t, sess)
	agentWrites(t, h)

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, h.input())
	require.NoError(t, err)
	require.Equal(t, agent.TTLOutcomeForceDeleted, res.Outcome, "the pod did have to be forced")
	require.Equal(t, agent.TTLHandoffAgent, res.Handoff,
		"the agent's own handoff note landed; a failed teardown does not un-write it")
}

// EITHER HALF OF THE PAYLOAD IS REAL CONTINUATION STATE. handoff="none" has to
// mean "nothing was captured", not "no closing message was captured": a push
// with no final text still tells the next pod where the work went, and counting
// it as loss would make the work-loss alert fire on Tasks that lost nothing.
func TestTTLStop_PartialPayloadIsStillASyntheticHandoff(t *testing.T) {
	for _, tc := range []struct {
		name  string
		final string
		repos []string
	}{
		{name: "final text only", final: "left the merge gate red on the mirror suite"},
		{name: "pushed repos only", repos: []string{"tatara-operator"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := &stopSession{
				states:     []string{agent.SessionStateReady},
				handoffErr: &agent.HTTPError{Status: http.StatusGone},
			}
			h := newTTLHarness(t, sess)
			in := h.input()
			in.LastFinalText, in.PushedRepos = tc.final, tc.repos

			res, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
			require.NoError(t, err)
			require.Equal(t, agent.TTLHandoffSynthetic, res.Handoff,
				"half a payload is degraded continuity, not lost continuity")
		})
	}
}
