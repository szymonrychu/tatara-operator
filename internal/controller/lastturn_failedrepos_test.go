package controller

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTurnComplete_PersistsLastTurnFailedRepos covers the receiving half of
// tatara-claude-code-wrapper#167.
//
// The wrapper's turn-end commit/push loop no longer aborts on the first repo
// that fails, so a turn can now finish having pushed eight repos and lost the
// ninth. pushedRepos alone cannot express that: a short list is
// indistinguishable from a turn that only touched eight repos. failedRepos is
// the field that says the difference out loud.
func TestTurnComplete_PersistsLastTurnFailedRepos(t *testing.T) {
	mkTaskProject(t, "p-failedrepos", 3)
	mkTaskRepository(t, "r-failedrepos", "p-failedrepos")
	mkTask(t, "t-failedrepos", "p-failedrepos", "r-failedrepos")
	annotate(t, "t-failedrepos", map[string]string{annCurrentTurn: "turn-fr-1"})

	body, _ := json.Marshal(map[string]any{
		"turnId": "turn-fr-1", "state": "completed", "stopReason": "end_turn",
		"finalText":   "pushed what I could; tatara-cli was rejected",
		"pushedRepos": []string{"tatara-operator"},
		"failedRepos": []string{"tatara-cli"},
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	newCallbackServer().Handler().ServeHTTP(w, req)
	require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())

	got := getTask(t, "t-failedrepos")
	require.Equal(t, []string{"tatara-operator"}, got.Status.LastTurnPushedRepos)
	require.Equal(t, []string{"tatara-cli"}, got.Status.LastTurnFailedRepos,
		"a repo whose work never reached origin is the one thing the next pod most needs told")
}

// TestTurnComplete_FailedReposAloneIsNotAnEmptyPayload: the empty-payload guard
// exists to stop a content-free turn blanking the newest turn that carried
// something. A turn that pushed nothing but FAILED on a repo is not
// content-free - it is the most consequential thing a turn can report - so it
// must pass the guard and be stamped.
func TestTurnComplete_FailedReposAloneIsNotAnEmptyPayload(t *testing.T) {
	mkTaskProject(t, "p-failedrepos2", 3)
	mkTaskRepository(t, "r-failedrepos2", "p-failedrepos2")
	mkTask(t, "t-failedrepos2", "p-failedrepos2", "r-failedrepos2")
	annotate(t, "t-failedrepos2", map[string]string{annCurrentTurn: "turn-fr-2"})

	body, _ := json.Marshal(map[string]any{
		"turnId": "turn-fr-2", "state": "failed",
		"failedRepos": []string{"tatara-operator", "tatara-cli"},
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	newCallbackServer().Handler().ServeHTTP(w, req)
	require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())

	require.Equal(t, []string{"tatara-operator", "tatara-cli"},
		getTask(t, "t-failedrepos2").Status.LastTurnFailedRepos,
		"the wholly-empty guard must not swallow a payload whose only content is a lost push")
}

// TestTurnComplete_EmptyFailedReposClearsThePreviousTurn mirrors
// TestTurnComplete_EmptyPushedReposMeansNothingPushed: the field is the
// finishing turn's state, not a high-water mark. A turn that pushed everything
// must not inherit the previous turn's failures.
func TestTurnComplete_EmptyFailedReposClearsThePreviousTurn(t *testing.T) {
	mkTaskProject(t, "p-failedrepos3", 3)
	mkTaskRepository(t, "r-failedrepos3", "p-failedrepos3")
	mkTask(t, "t-failedrepos3", "p-failedrepos3", "r-failedrepos3")

	post := func(turn string, payload map[string]any) {
		annotate(t, "t-failedrepos3", map[string]string{annCurrentTurn: turn, annTurnComplete: ""})
		payload["turnId"] = turn
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
		w := httptest.NewRecorder()
		newCallbackServer().Handler().ServeHTTP(w, req)
		require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
	}

	post("turn-fr-3", map[string]any{
		"state": "completed", "finalText": "one repo lost", "failedRepos": []string{"tatara-cli"},
	})
	require.Equal(t, []string{"tatara-cli"}, getTask(t, "t-failedrepos3").Status.LastTurnFailedRepos)

	post("turn-fr-4", map[string]any{
		"state": "completed", "finalText": "retried, everything landed",
		"pushedRepos": []string{"tatara-cli"},
	})
	require.Empty(t, getTask(t, "t-failedrepos3").Status.LastTurnFailedRepos,
		"a turn that lost nothing must not inherit the previous turn's losses")
}
