package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// postTurnComplete drives the real callback path for one turn.
func postTurnComplete(t *testing.T, task, turn string, payload map[string]any) {
	t.Helper()
	annotate(t, task, map[string]string{annCurrentTurn: turn, annTurnComplete: ""})
	payload["turnId"] = turn
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	newCallbackServer().Handler().ServeHTTP(w, req)
	require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
}

// A STALE FAILED SET IS AN INSTRUCTION TO REDO WORK THAT IS ON ORIGIN.
//
// pushedRepos and failedRepos arrive together and are both absent from the poll
// backstop's TurnResult, which is why one known-flag gates both. The symmetry
// holds for ARRIVAL and not for the cost of being wrong: a stale pushedRepos is
// optimistic and inert, a stale failedRepos tells the next agent to redo work
// that landed. When the backstop fills in for a NEWER turn, the final text
// becomes that turn's and the failure claim must not follow it.
func TestPollBackstop_DoesNotCarryAnOlderTurnsFailuresOntoANewerTurn(t *testing.T) {
	mkTaskProject(t, "p-fr-backstop", 3)
	mkTaskRepository(t, "r-fr-backstop", "p-fr-backstop")
	mkTask(t, "t-fr-backstop", "p-fr-backstop", "r-fr-backstop")

	postTurnComplete(t, "t-fr-backstop", "turn-b-1", map[string]any{
		"state": "completed", "finalText": "tatara-cli was rejected",
		"pushedRepos": []string{"tatara-operator"},
		"failedRepos": []string{"tatara-cli"},
	})
	require.Equal(t, []string{"tatara-cli"}, getTask(t, "t-fr-backstop").Status.LastTurnFailedRepos)

	// Turn 2 completes, its callback is never delivered, and the backstop fills
	// in from GET /v1/messages/{turnId} - which carries no repo lists at all.
	annotate(t, "t-fr-backstop", map[string]string{annCurrentTurn: "turn-b-2", annTurnComplete: ""})
	require.NoError(t, newCallbackServer().stampLastTurn(context.Background(),
		getTask(t, "t-fr-backstop"), "turn-b-2", "retried, everything landed", nil, nil, false))

	got := getTask(t, "t-fr-backstop")
	require.Equal(t, "retried, everything landed", got.Status.LastTurnFinalText)
	require.Empty(t, got.Status.LastTurnFailedRepos,
		"a newer turn's final text must not be stamped with an older turn's losses")
	require.Equal(t, []string{"tatara-operator"}, got.Status.LastTurnPushedRepos,
		"a stale pushed list is optimistic and inert: the #527 rule that the backstop must not clear it stands")
}

// The unknown path must only drop a failure claim that belongs to an EARLIER
// turn. The backstop polls the same turn the callback already reported - both
// run on every turn - so clearing on "unknown" alone would wipe the field on the
// very turn that reported it.
func TestPollBackstop_KeepsTheFailuresItsOwnTurnReported(t *testing.T) {
	mkTaskProject(t, "p-fr-backstop2", 3)
	mkTaskRepository(t, "r-fr-backstop2", "p-fr-backstop2")
	mkTask(t, "t-fr-backstop2", "p-fr-backstop2", "r-fr-backstop2")

	postTurnComplete(t, "t-fr-backstop2", "turn-b-3", map[string]any{
		"state": "completed", "finalText": "tatara-cli was rejected",
		"failedRepos": []string{"tatara-cli"},
	})
	require.NoError(t, newCallbackServer().stampLastTurn(context.Background(),
		getTask(t, "t-fr-backstop2"), "turn-b-3", "tatara-cli was rejected", nil, nil, false))

	require.Equal(t, []string{"tatara-cli"}, getTask(t, "t-fr-backstop2").Status.LastTurnFailedRepos,
		"the backstop knows less than the callback about its own turn; it must not overwrite it with that")
}
