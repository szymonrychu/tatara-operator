package stage_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// TestEveryTransitionClearsTheLastTurnContinuationState.
//
// status.lastTurn{FinalText,PushedRepos} describe THIS state occupancy's work,
// and they are read by exactly one consumer: the G.7 synthetic handoff note. A
// stale pair carried across a transition makes the next state's TTL stop write
// the PREVIOUS state's final text into a note framed as this pod's continuation
// state - a clarify agent's closing message handed to an implement pod as though
// it were the last thing that happened.
//
// That is worse than the empty note #527 was filed about: an empty note says
// nothing, a stale one says something false, and the next agent has no way to
// tell. So they clear alongside podStartedAt, on the same edge, for the same
// reason.
func TestEveryTransitionClearsTheLastTurnContinuationState(t *testing.T) {
	tk := task(v1alpha1.StateRefined)
	tk.Status.PodStartedAt = ptrTime(now.Add(-time.Hour))
	tk.Status.LastTurnFinalText = "clarified the scope with the maintainer; ready to implement"
	tk.Status.LastTurnPushedRepos = []string{"tatara-operator"}
	tk.Status.LastTurnFailedRepos = []string{"tatara-cli"}

	require.NoError(t, stage.Enter(tk, nil, v1alpha1.StateUnderImplementation, "", now))

	require.Empty(t, tk.Status.LastTurnFinalText,
		"a new state must not hand its TTL stop the previous state's final text")
	require.Empty(t, tk.Status.LastTurnPushedRepos,
		"nor the previous state's pushed repos")
	require.Empty(t, tk.Status.LastTurnFailedRepos,
		"nor the previous state's FAILED repos - and this one is the worst to leave behind, "+
			"because a non-empty failed set is what stops writeSyntheticNote calling a "+
			"content-free stop content-free")
}
