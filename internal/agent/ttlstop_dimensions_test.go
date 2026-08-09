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

// A PLACEHOLDER MUST READ AS A PLACEHOLDER, TO THE AGENT AS WELL AS TO THE
// METRIC.
//
// #557 added operator_agent_synthetic_handoff_empty_total for this case and left
// the note itself alone, so the operator could see the loss while the next agent
// still could not: the body said "Last turn's final text: (none). Repos pushed:
// none.", which is indistinguishable from a turn that genuinely had nothing
// worth saying. The next pod read it as continuity, resumed from it, re-ran
// turn-0 and re-charged maxTurnsPerTask - the exact loop #527 documents.
func TestTTLStop_ContentFreeNoteTellsTheNextAgentTheStateWasLost(t *testing.T) {
	sess := &stopSession{
		states:     []string{agent.SessionStateReady},
		handoffErr: &agent.HTTPError{Status: http.StatusGone},
	}
	h := newTTLHarness(t, sess)
	in := h.input()
	in.LastFinalText, in.PushedRepos = "", nil

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
	require.NoError(t, err)
	require.Equal(t, agent.TTLHandoffNone, res.Handoff)

	notes := h.notes(t)
	require.Len(t, notes, 1, "the notes journal is never left empty, even here")
	body := notes[0].Body
	require.Contains(t, body, agent.SyntheticNoteLostMarker,
		"the note must NAME the loss; the runbook greps for this exact marker")
	require.Contains(t, body, "PLACEHOLDER, not a handoff")
	require.Contains(t, body, "do not read this note as continuity")
	require.NotContains(t, body, "Last turn's final text: (none)",
		"rendering empty fields invites the next agent to resume from nothing")
}

// The marker is for the EMPTY case only. A real payload keeps the note that
// carries it - a marker on a note that does have continuation state would train
// the next agent to distrust the ones that matter.
func TestTTLStop_RealPayloadNoteCarriesNoLostMarker(t *testing.T) {
	sess := &stopSession{
		states:     []string{agent.SessionStateReady},
		handoffErr: &agent.HTTPError{Status: http.StatusGone},
	}
	h := newTTLHarness(t, sess)

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, h.input())
	require.NoError(t, err)
	require.Equal(t, agent.TTLHandoffSynthetic, res.Handoff)
	require.NotContains(t, h.notes(t)[0].Body, agent.SyntheticNoteLostMarker)
	require.Contains(t, h.notes(t)[0].Body, "wired the reconciler, tests still red")
}

// AN ALREADY-GONE WRAPPER IS NOT A FORCE-DELETE, AND IT IS NOT WORTH WAITING
// FOR.
//
// This is #527's 12:37:58Z event: the idle reaper deleted the pod at 12:10:19Z,
// the TTL stop ran 27 minutes later, spent its entire handoff budget polling a
// corpse, and reported force_deleted for a pod nobody forced - which is what the
// alert fired on. Both halves are wrong. There is no turn to offer an absent
// wrapper, and force-deleting an absent pod is a no-op that must not be counted
// as a forced teardown on a Task where nothing went wrong.
func TestTTLStop_AbsentWrapperIsNotProbedAndIsNotAForceDelete(t *testing.T) {
	sess := &stopSession{
		states:    []string{agent.SessionStateBusy}, // hung forever, if anyone asked
		deleteErr: &agent.UnreachableError{},        // there is nothing to delete a session on
	}
	h := newTTLHarness(t, sess)
	pod := &corev1.Pod{}
	require.NoError(t, h.c.Get(context.Background(),
		types.NamespacedName{Namespace: ttlNS, Name: agent.PodName(h.task)}, pod))
	require.NoError(t, h.c.Delete(context.Background(), pod))
	start := h.now

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, h.input())
	require.NoError(t, err)

	require.Zero(t, sess.getCalls, "there is nothing to talk to: the wrapper pod is gone")
	require.Zero(t, sess.handoffs, "an absent wrapper cannot be offered the handoff turn")
	require.Equal(t, start, h.now, "the stop must not spend its wait budget on a corpse")
	require.Equal(t, agent.TTLOutcomeGraceful, res.Outcome,
		"force-deleting an absent pod is a no-op; labelling it force_deleted fires the alert on a healthy Task")
	require.Equal(t, agent.TTLHandoffSynthetic, res.Handoff,
		"the operator still writes what it holds, so the next pod is not left with nothing")
	require.NotEmpty(t, h.notes(t), "the notes journal is never left empty, gone wrapper or not")
}

// The same rule on the OTHER path into a force-delete: the wrapper was there
// when the sequence started and vanished before teardown. The escalation is
// still a no-op, so it is still not a force-delete.
func TestTTLStop_WrapperThatVanishesDuringTeardownIsNotAForceDelete(t *testing.T) {
	sess := &stopSession{
		states:     []string{agent.SessionStateReady},
		handoffErr: &agent.HTTPError{Status: http.StatusGone},
		deleteErr:  &agent.UnreachableError{},
	}
	h := newTTLHarness(t, sess)
	sess.onDeleteSession = func() {
		pod := &corev1.Pod{}
		require.NoError(t, h.c.Get(context.Background(),
			types.NamespacedName{Namespace: ttlNS, Name: agent.PodName(h.task)}, pod))
		require.NoError(t, h.c.Delete(context.Background(), pod))
	}

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, h.input())
	require.NoError(t, err)
	require.Equal(t, agent.TTLOutcomeGraceful, res.Outcome)
	require.Equal(t, agent.TTLHandoffSynthetic, res.Handoff)
}

// A read error is NOT a NotFound. "I could not tell" must run the full sequence:
// skipping the handoff turn because the API server was briefly unreachable would
// throw away the agent's one chance to say something.
func TestTTLStop_UnknownPodStateRunsTheFullSequence(t *testing.T) {
	sess := &stopSession{states: []string{agent.SessionStateBusy, agent.SessionStateReady}}
	h := newTTLHarness(t, sess)
	agentWrites(t, h)

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, h.input())
	require.NoError(t, err)
	require.Equal(t, 1, sess.handoffs, "a present wrapper is still offered its handoff turn")
	require.Equal(t, agent.TTLHandoffAgent, res.Handoff)
}
