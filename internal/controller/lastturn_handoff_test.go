package controller

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTurnComplete_PersistsLastTurnContinuationState is the #527 root-cause
// regression.
//
// The G.7 synthetic handoff note is BUILT from the last turn's finalText and
// pushedRepos. Both arrive on the turn-complete callback and neither was ever
// persisted, so ttlStop had no source to read and every synthetic note in
// production read exactly "TTL stop. Last turn's final text: (none). Repos
// pushed: none." - the non-empty-notes invariant satisfied vacuously while the
// Task's continuation state was reset. Dozens of those were confirmed live.
func TestTurnComplete_PersistsLastTurnContinuationState(t *testing.T) {
	mkTaskProject(t, "p-lastturn", 3)
	mkTaskRepository(t, "r-lastturn", "p-lastturn")
	mkTask(t, "t-lastturn", "p-lastturn", "r-lastturn")
	annotate(t, "t-lastturn", map[string]string{annCurrentTurn: "turn-lt-1"})

	cb := newCallbackServer()
	body, _ := json.Marshal(map[string]any{
		"turnId": "turn-lt-1", "state": "completed", "stopReason": "end_turn",
		"finalText":   "rebased onto main, CI green, opened PR #77",
		"pushedRepos": []string{"tatara-operator", "tatara-cli"},
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)
	require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())

	got := getTask(t, "t-lastturn")
	require.Equal(t, "rebased onto main, CI green, opened PR #77", got.Status.LastTurnFinalText,
		"the last turn's final text IS the continuation state the synthetic handoff note is built from")
	require.Equal(t, []string{"tatara-operator", "tatara-cli"}, got.Status.LastTurnPushedRepos,
		"pushedRepos is retained on the wire (G.2) precisely so the synthetic note can carry it")
}

// TestTurnComplete_EmptyPushedReposMeansNothingPushed: "the agent pushed
// nothing" and "we never learned" must stay distinguishable, so an explicit
// empty list clears a previous turn's repos rather than being ignored.
func TestTurnComplete_EmptyPushedReposMeansNothingPushed(t *testing.T) {
	mkTaskProject(t, "p-lastturn2", 3)
	mkTaskRepository(t, "r-lastturn2", "p-lastturn2")
	mkTask(t, "t-lastturn2", "p-lastturn2", "r-lastturn2")

	post := func(turn string, payload map[string]any) {
		annotate(t, "t-lastturn2", map[string]string{annCurrentTurn: turn, annTurnComplete: ""})
		payload["turnId"] = turn
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
		w := httptest.NewRecorder()
		newCallbackServer().Handler().ServeHTTP(w, req)
		require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
	}

	post("turn-lt-2", map[string]any{
		"state": "completed", "finalText": "pushed", "pushedRepos": []string{"tatara-operator"},
	})
	require.Equal(t, []string{"tatara-operator"}, getTask(t, "t-lastturn2").Status.LastTurnPushedRepos)

	post("turn-lt-3", map[string]any{
		"state": "completed", "finalText": "no diff to push", "pushedRepos": []string{},
	})
	require.Empty(t, getTask(t, "t-lastturn2").Status.LastTurnPushedRepos,
		"a turn that pushed nothing must not inherit the previous turn's repos")
	require.Equal(t, "no diff to push", getTask(t, "t-lastturn2").Status.LastTurnFinalText)
}

// TestTurnComplete_FailedTurnDoesNotBlankLastTurnState is the #557 regression,
// and it is why #557 does not actually close #527.
//
// stampLastTurn wrote status.lastTurnFinalText UNCONDITIONALLY, so the next
// turn to finish with nothing to say - state="failed" is a real wrapper state
// and reaches the callback with an empty finalText and no pushedRepos - erased
// the continuation state the previous turn had captured. The Task then TTL-stops
// onto the placeholder note, which is the exact outcome #557 exists to prevent,
// one turn later.
//
// The field is therefore the newest NON-EMPTY payload: "produced anything", not
// "completed". A turn with finalText and no repos still clears the repos (see
// TestTurnComplete_EmptyPushedReposMeansNothingPushed) - only a wholly empty
// payload is skipped.
func TestTurnComplete_FailedTurnDoesNotBlankLastTurnState(t *testing.T) {
	mkTaskProject(t, "p-lastturn3", 3)
	mkTaskRepository(t, "r-lastturn3", "p-lastturn3")
	mkTask(t, "t-lastturn3", "p-lastturn3", "r-lastturn3")

	post := func(turn string, payload map[string]any) {
		annotate(t, "t-lastturn3", map[string]string{annCurrentTurn: turn, annTurnComplete: ""})
		payload["turnId"] = turn
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
		w := httptest.NewRecorder()
		newCallbackServer().Handler().ServeHTTP(w, req)
		require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
	}

	post("turn-lt-4", map[string]any{
		"state":     "completed",
		"finalText": "opened PR #91; the merge gate is red on the mirror suite",
		// The repos the agent pushed are the other half of the continuation state.
		"pushedRepos": []string{"tatara-operator"},
	})

	// The wrapper's next turn dies without producing anything at all.
	post("turn-lt-5", map[string]any{"state": "failed", "error": "session crashed"})

	got := getTask(t, "t-lastturn3")
	require.Equal(t, "opened PR #91; the merge gate is red on the mirror suite", got.Status.LastTurnFinalText,
		"a turn that produced NOTHING must not erase the newest turn that produced something - "+
			"that write is what makes the G.7 synthetic note a placeholder one turn later (#557 regression)")
	require.Equal(t, []string{"tatara-operator"}, got.Status.LastTurnPushedRepos,
		"the same guard covers pushedRepos: an empty payload is not evidence that nothing was pushed")
}
