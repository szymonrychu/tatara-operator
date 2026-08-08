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
