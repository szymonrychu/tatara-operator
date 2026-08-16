package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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

// AN ABSENT lastTurnReposTurnId MEANS UNKNOWN, NEVER "OLDER".
//
// A Task already carrying failures when the field shipped has no id, and so does
// one whose callback was served by a not-yet-upgraded replica: the callback
// runnable is not leader-elected while the poll backstop is, so the two halves
// genuinely run different binaries during a rollout. Reading "" as stale would
// make the backstop delete a failure report belonging to its own turn - the exact
// loss the gate exists to prevent, arrived at from the other side.
func TestPollBackstop_TreatsAnAbsentReposTurnIDAsUnknown(t *testing.T) {
	mkTaskProject(t, "p-fr-backstop3", 3)
	mkTaskRepository(t, "r-fr-backstop3", "p-fr-backstop3")
	mkTask(t, "t-fr-backstop3", "p-fr-backstop3", "r-fr-backstop3")
	annotate(t, "t-fr-backstop3", map[string]string{annCurrentTurn: "turn-b-4"})

	// The state an older binary left behind: lists written, no id.
	task := getTask(t, "t-fr-backstop3")
	task.Status.LastTurnFailedRepos = []string{"tatara-cli"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	require.NoError(t, newCallbackServer().stampLastTurn(context.Background(),
		getTask(t, "t-fr-backstop3"), "turn-b-4", "tatara-cli was rejected", nil, nil, false))

	require.Equal(t, []string{"tatara-cli"}, getTask(t, "t-fr-backstop3").Status.LastTurnFailedRepos,
		"an unknown owner is not evidence that the failures belong to an older turn")
}

// A BLANK REPO NAME IS REJECTED ON ARRIVAL, NOT ONLY AT THE SENDER.
//
// The wrapper stopped producing them, but version skew is a designed-for state
// here: every pre-fix image keeps sending [""] until the train rolls. One blank
// element is len()==1 downstream, which suppresses the placeholder note and
// RecordEmptySynthetic (#527) and renders a handoff note that asserts lost work
// and names no repo.
func TestTurnComplete_DropsBlankRepoNames(t *testing.T) {
	mkTaskProject(t, "p-fr-blank", 3)
	mkTaskRepository(t, "r-fr-blank", "p-fr-blank")
	mkTask(t, "t-fr-blank", "p-fr-blank", "r-fr-blank")

	postTurnComplete(t, "t-fr-blank", "turn-b-5", map[string]any{
		"state": "completed", "finalText": "one landed, one is unnameable",
		"pushedRepos": []string{"", "tatara-operator"},
		"failedRepos": []string{""},
	})

	got := getTask(t, "t-fr-blank")
	require.Equal(t, []string{"tatara-operator"}, got.Status.LastTurnPushedRepos)
	require.Empty(t, got.Status.LastTurnFailedRepos,
		"a list whose only element names nothing carries nothing")
}

// THE GUARD AND THE CLAMP HAVE TO AGREE ON WHAT COUNTS AS CONTENT.
//
// The empty-payload guard (#527) used to run on the RAW lists while
// clampPushedRepos dropped blanks twenty lines later, so a payload whose only
// content was a blank name passed the guard, clamped to nothing, and then wrote
// that nothing over the previous turn's continuation state. Reachable on exactly
// the skew the blank-dropping is written for: a pre-fix wrapper image reporting
// a failed turn emits {finalText:"", pushedRepos:nil, failedRepos:[""]}.
func TestTurnComplete_ABlankOnlyPayloadDoesNotBlankThePreviousTurn(t *testing.T) {
	mkTaskProject(t, "p-fr-blankguard", 3)
	mkTaskRepository(t, "r-fr-blankguard", "p-fr-blankguard")
	mkTask(t, "t-fr-blankguard", "p-fr-blankguard", "r-fr-blankguard")

	postTurnComplete(t, "t-fr-blankguard", "turn-b-6", map[string]any{
		"state": "completed", "finalText": "opened PR #91",
		"pushedRepos": []string{"tatara-operator"},
	})

	postTurnComplete(t, "t-fr-blankguard", "turn-b-7", map[string]any{
		"state": "failed", "failedRepos": []string{""},
	})

	got := getTask(t, "t-fr-blankguard")
	require.Equal(t, "opened PR #91", got.Status.LastTurnFinalText,
		"a payload that clamps to nothing is content-free and must not overwrite a turn that said something")
	require.Equal(t, []string{"tatara-operator"}, got.Status.LastTurnPushedRepos)
	require.Equal(t, "turn-b-6", got.Status.LastTurnReposTurnID,
		"the repos still describe turn-b-6; a skipped payload must not claim ownership of them")
}

// AN OVER-LONG TURN ID COSTS THE FIELD IT DOES NOT FIT IN, NOT THE WHOLE WRITE.
//
// The turn id is the wrapper's, unvalidated operator-side, and annotation values
// are not length-limited - so an id can exceed lastTurnReposTurnId's MaxLength
// while still matching annCurrentTurn. An uncapped write is rejected by the
// apiserver in full, taking the final text and both repo lists with it. Clamping
// once, at both the write and the comparison, keeps the ownership check honest.
func TestTurnComplete_AnOverLongTurnIDDoesNotFailTheWholeStatusWrite(t *testing.T) {
	mkTaskProject(t, "p-fr-longid", 3)
	mkTaskRepository(t, "r-fr-longid", "p-fr-longid")
	mkTask(t, "t-fr-longid", "p-fr-longid", "r-fr-longid")

	long := strings.Repeat("t", 400)
	postTurnComplete(t, "t-fr-longid", long, map[string]any{
		"state": "completed", "finalText": "opened PR #91",
		"pushedRepos": []string{"tatara-operator"},
		"failedRepos": []string{"tatara-cli"},
	})

	got := getTask(t, "t-fr-longid")
	require.Equal(t, "opened PR #91", got.Status.LastTurnFinalText,
		"an id that does not fit must not take the continuation state down with it")
	require.Equal(t, []string{"tatara-cli"}, got.Status.LastTurnFailedRepos)
	require.Len(t, got.Status.LastTurnReposTurnID, maxLastTurnReposTurnID)

	// The backstop polling that SAME turn must still recognise it as its own.
	require.NoError(t, newCallbackServer().stampLastTurn(context.Background(),
		getTask(t, "t-fr-longid"), long, "opened PR #91", nil, nil, false))
	require.Equal(t, []string{"tatara-cli"}, getTask(t, "t-fr-longid").Status.LastTurnFailedRepos,
		"a clamped id must compare equal to the clamped form of the id that wrote it")
}
