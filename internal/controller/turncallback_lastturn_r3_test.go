// Review round 3 on #527. One theme: status.lastTurn must be written by EVERY
// path that finalises a turn, and must never be replaced by a payload that says
// less than the one it overwrites. Both defects surface as the same false
// positive - handoff="none" and the placeholder note on a Task whose finalText
// the operator was holding.
package controller

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// submitNextTurn advances the Task to a fresh turn the way task_stage.go does on
// every turn submit: stamp the new annCurrentTurn and DELETE annTurnComplete.
// annotate() can only merge, and the deletion is the half that matters here -
// it is what re-opens recordLastTurn's already-processed guard.
func submitNextTurn(t *testing.T, name, turnID string) {
	t.Helper()
	tk := getTask(t, name)
	if tk.Annotations == nil {
		tk.Annotations = map[string]string{}
	}
	tk.Annotations[annCurrentTurn] = turnID
	delete(tk.Annotations, annTurnComplete)
	if err := k8sClient.Update(context.Background(), tk); err != nil {
		t.Fatalf("submit next turn on %s: %v", name, err)
	}
}

// TestPollOnce_PersistsLastTurn is finding 1. The poll backstop is the recovery
// path for turns whose callback never landed - an unreachable, wedged or
// crashed wrapper - which is precisely the wrapper state that then leads to a
// G.7 TTL stop the agent cannot answer. It called recordResult and not
// recordLastTurn, so the population most likely to TTL-stop was the one
// guaranteed to report handoff="none" with the finalText in hand.
func TestPollOnce_PersistsLastTurn(t *testing.T) {
	mkTaskProject(t, "p-pb1", 3)
	mkTaskRepository(t, "r-pb1", "p-pb1")
	mkTask(t, "t-pb1", "p-pb1", "r-pb1")
	annotate(t, "t-pb1", map[string]string{
		annCurrentTurn:   "turn-pb1",
		annTurnStartedAt: time.Now().UTC().Format(time.RFC3339),
	})

	fs := newFakeSession()
	fs.getResult["turn-pb1"] = agent.TurnResult{
		State:     "complete",
		FinalText: "recovered by the backstop, callback never arrived",
	}
	cb := newCallbackServer()
	cb.Session = fs
	cb.PollOnce(context.Background())

	tk := getTask(t, "t-pb1")
	if tk.Annotations[annTurnComplete] == "" {
		t.Fatal("poll backstop did not record the result at all; test setup is wrong")
	}
	lt := tk.Status.LastTurn
	if lt == nil {
		t.Fatal("status.lastTurn is nil: the poll backstop finalised the turn without persisting its continuation payload, so a subsequent TTL stop writes the placeholder note and counts handoff=none")
	}
	if lt.FinalText != "recovered by the backstop, callback never arrived" {
		t.Errorf("lastTurn.finalText = %q", lt.FinalText)
	}
	if lt.At.IsZero() {
		t.Error("lastTurn.at is zero")
	}
}

// TestPollOnce_ContentFreeTurnLeavesLastTurnNil pins the other half of finding
// 1: agent.TurnResult carries no PushedRepos (that field is callback-wire only),
// so a backstop-recovered turn with no finalText has genuinely nothing to
// persist. handoff="none" is correct there and must stay reachable.
func TestPollOnce_ContentFreeTurnLeavesLastTurnNil(t *testing.T) {
	mkTaskProject(t, "p-pb2", 3)
	mkTaskRepository(t, "r-pb2", "p-pb2")
	mkTask(t, "t-pb2", "p-pb2", "r-pb2")
	annotate(t, "t-pb2", map[string]string{
		annCurrentTurn:   "turn-pb2",
		annTurnStartedAt: time.Now().UTC().Format(time.RFC3339),
	})

	fs := newFakeSession()
	fs.getResult["turn-pb2"] = agent.TurnResult{State: "failed", FinalText: ""}
	cb := newCallbackServer()
	cb.Session = fs
	cb.PollOnce(context.Background())

	if lt := getTask(t, "t-pb2").Status.LastTurn; lt != nil {
		t.Errorf("status.lastTurn = %+v, want nil: a turn that produced nothing must not be recorded as a continuation payload", lt)
	}
}

// TestTurnComplete_ContentFreeTurnDoesNotClobberLastTurn is finding 2.
// state="failed" is a real wrapper state and reaches the handler with no
// finalText and no pushedRepos. The write was unconditional, so it replaced a
// good payload with an empty one - and the next TTL stop then emitted the
// placeholder note ("do not read this note as continuity") and fired the new
// handoff=none alert on a Task whose turn-N payload the operator had been
// holding one turn earlier.
func TestTurnComplete_ContentFreeTurnDoesNotClobberLastTurn(t *testing.T) {
	mkTaskProject(t, "p-cf1", 3)
	mkTaskRepository(t, "r-cf1", "p-cf1")
	mkTask(t, "t-cf1", "p-cf1", "r-cf1")
	annotate(t, "t-cf1", map[string]string{annCurrentTurn: "turn-cf1a"})

	cb := newCallbackServer()
	if w := postTurnComplete(t, cb, map[string]any{
		"turnId":      "turn-cf1a",
		"taskName":    "t-cf1",
		"state":       "complete",
		"finalText":   "pushed the reaper guard",
		"pushedRepos": []string{"tatara-operator"},
	}); w.Code != http.StatusNoContent {
		t.Fatalf("turn A status = %d; body = %s", w.Code, w.Body.String())
	}
	before := getTask(t, "t-cf1").Status.LastTurn
	if before == nil {
		t.Fatal("turn A did not persist a payload; test setup is wrong")
	}

	submitNextTurn(t, "t-cf1", "turn-cf1b")
	if w := postTurnComplete(t, cb, map[string]any{
		"turnId":   "turn-cf1b",
		"taskName": "t-cf1",
		"state":    "failed",
	}); w.Code != http.StatusNoContent {
		t.Fatalf("turn B status = %d; body = %s", w.Code, w.Body.String())
	}

	after := getTask(t, "t-cf1").Status.LastTurn
	if after == nil {
		t.Fatal("status.lastTurn was cleared by a content-free turn")
	}
	if after.FinalText != "pushed the reaper guard" {
		t.Errorf("lastTurn.finalText = %q, want turn A's payload preserved: a turn that produced nothing must not overwrite one that did", after.FinalText)
	}
	if len(after.PushedRepos) != 1 || after.PushedRepos[0] != "tatara-operator" {
		t.Errorf("lastTurn.pushedRepos = %v, want turn A's", after.PushedRepos)
	}
	if !after.At.Equal(&before.At) {
		t.Errorf("lastTurn.at moved to %v (was %v): the timestamp must keep dating the turn that actually produced the text, because the synthetic note renders it", after.At, before.At)
	}
}

// TestTurnComplete_PartialPayloadStillPersists guards the guard: only BOTH
// fields being empty means nothing was produced. pushedRepos alone is real
// continuation state (syntheticNoteBody counts either field alone as a
// synthetic handoff), so it must not be swallowed by the emptiness check.
func TestTurnComplete_PartialPayloadStillPersists(t *testing.T) {
	mkTaskProject(t, "p-cf2", 3)
	mkTaskRepository(t, "r-cf2", "p-cf2")
	mkTask(t, "t-cf2", "p-cf2", "r-cf2")
	annotate(t, "t-cf2", map[string]string{annCurrentTurn: "turn-cf2"})

	if w := postTurnComplete(t, newCallbackServer(), map[string]any{
		"turnId":      "turn-cf2",
		"taskName":    "t-cf2",
		"state":       "complete",
		"pushedRepos": []string{"tatara-docs"},
	}); w.Code != http.StatusNoContent {
		t.Fatalf("status = %d; body = %s", w.Code, w.Body.String())
	}

	lt := getTask(t, "t-cf2").Status.LastTurn
	if lt == nil {
		t.Fatal("status.lastTurn is nil: pushedRepos with no finalText is still continuation state")
	}
	if len(lt.PushedRepos) != 1 || lt.PushedRepos[0] != "tatara-docs" {
		t.Errorf("lastTurn.pushedRepos = %v", lt.PushedRepos)
	}
}
