package agent_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// A REPO THAT FAILED TO PUSH IS THE ONE THING THE SYNTHETIC NOTE MUST NOT DROP.
//
// tatara-claude-code-wrapper#167: the wrapper's turn-end loop no longer aborts on
// the first failing repo, so a turn can end having pushed most repos and lost
// one. The pod's workspace is ephemeral, so that repo's commits exist nowhere but
// on a disk that is about to disappear. The synthetic handoff note is the only
// place the next pod can learn it - a note that lists only what DID push reads as
// a complete turn.
func TestTTLStop_SyntheticNoteNamesTheReposThatFailedToPush(t *testing.T) {
	sess := &stopSession{
		states:     []string{agent.SessionStateReady},
		handoffErr: &agent.HTTPError{Status: http.StatusGone},
	}
	h := newTTLHarness(t, sess)
	in := h.input()
	in.PushedRepos = []string{"tatara-operator"}
	in.FailedRepos = []string{"tatara-cli"}

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
	require.NoError(t, err)
	require.Equal(t, agent.TTLHandoffSynthetic, res.Handoff)

	body := h.notes(t)[0].Body
	require.Contains(t, body, "tatara-cli",
		"the note must name the repo whose work never reached origin")
	require.Contains(t, body, "failed to push",
		"naming the repo is not enough; the note must say what happened to it")
}

// A turn whose ONLY content is a failed push is not content-free. It is the most
// consequential thing a turn can report, and rendering it as the placeholder
// would throw away the one fact worth carrying.
func TestTTLStop_FailedReposAloneIsNotAContentFreeNote(t *testing.T) {
	sess := &stopSession{
		states:     []string{agent.SessionStateReady},
		handoffErr: &agent.HTTPError{Status: http.StatusGone},
	}
	h := newTTLHarness(t, sess)
	in := h.input()
	in.LastFinalText, in.PushedRepos = "", nil
	in.FailedRepos = []string{"tatara-cli"}

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
	require.NoError(t, err)
	require.Equal(t, agent.TTLHandoffSynthetic, res.Handoff,
		"a lost push is continuation state, not the absence of it")

	body := h.notes(t)[0].Body
	require.NotContains(t, body, agent.SyntheticNoteLostMarker,
		"the placeholder marker trains the next agent to distrust the note; this one has something to say")
	require.Contains(t, body, "tatara-cli")
}
