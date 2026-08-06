// Tests for issue #527: the turn-complete callback must PERSIST the turn's
// continuation payload. finalText and pushedRepos arrive on every callback and
// were thrown away, so G.7's synthetic handoff note - the only continuation
// state a TTL-stopped Task carries to its next pod - had nothing to say.
package controller

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func postTurnComplete(t *testing.T, cb *CallbackServer, payload map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)
	return w
}

// TestTurnComplete_PersistsLastTurn is the root-cause regression for #527.
func TestTurnComplete_PersistsLastTurn(t *testing.T) {
	mkTaskProject(t, "p-lt1", 3)
	mkTaskRepository(t, "r-lt1", "p-lt1")
	mkTask(t, "t-lt1", "p-lt1", "r-lt1")
	annotate(t, "t-lt1", map[string]string{annCurrentTurn: "turn-lt1"})

	w := postTurnComplete(t, newCallbackServer(), map[string]any{
		"turnId":      "turn-lt1",
		"taskName":    "t-lt1",
		"state":       "completed",
		"finalText":   "wired the reconciler, tests still red",
		"pushedRepos": []string{"tatara-operator", "tatara-cli"},
	})
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body = %s", w.Code, w.Body.String())
	}

	lt := getTask(t, "t-lt1").Status.LastTurn
	if lt == nil {
		t.Fatal("status.lastTurn is nil: the turn's continuation payload was thrown away (#527)")
	}
	if lt.FinalText != "wired the reconciler, tests still red" {
		t.Errorf("lastTurn.finalText = %q", lt.FinalText)
	}
	if strings.Join(lt.PushedRepos, ",") != "tatara-operator,tatara-cli" {
		t.Errorf("lastTurn.pushedRepos = %v", lt.PushedRepos)
	}
	if lt.At.IsZero() {
		t.Error("lastTurn.at is zero")
	}
}

// TestTurnComplete_LastTurnClampedToCRDLimit: an over-long finalText must be cut
// to the field's MaxLength, not 422 the whole status write. A rejected status
// update here loses the continuation payload AND the annTurnComplete requeue.
func TestTurnComplete_LastTurnClampedToCRDLimit(t *testing.T) {
	mkTaskProject(t, "p-lt2", 3)
	mkTaskRepository(t, "r-lt2", "p-lt2")
	mkTask(t, "t-lt2", "p-lt2", "r-lt2")
	annotate(t, "t-lt2", map[string]string{annCurrentTurn: "turn-lt2"})

	w := postTurnComplete(t, newCallbackServer(), map[string]any{
		"turnId":    "turn-lt2",
		"taskName":  "t-lt2",
		"state":     "completed",
		"finalText": strings.Repeat("x", 9000),
	})
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body = %s", w.Code, w.Body.String())
	}
	lt := getTask(t, "t-lt2").Status.LastTurn
	if lt == nil {
		t.Fatal("status.lastTurn is nil")
	}
	if len(lt.FinalText) > tatarav1alpha1.LastTurnFinalTextMaxBytes {
		t.Errorf("lastTurn.finalText = %d bytes, want <= %d",
			len(lt.FinalText), tatarav1alpha1.LastTurnFinalTextMaxBytes)
	}
}

// TestTurnComplete_StaleCallbackDoesNotOverwriteLastTurn: a duplicate or late
// callback for a turn the Task has already moved past must not replace the
// continuation payload of the turn that is actually current.
func TestTurnComplete_StaleCallbackDoesNotOverwriteLastTurn(t *testing.T) {
	mkTaskProject(t, "p-lt3", 3)
	mkTaskRepository(t, "r-lt3", "p-lt3")
	mkTask(t, "t-lt3", "p-lt3", "r-lt3")
	annotate(t, "t-lt3", map[string]string{annCurrentTurn: "turn-lt3"})

	cb := newCallbackServer()
	postTurnComplete(t, cb, map[string]any{
		"turnId": "turn-lt3", "taskName": "t-lt3", "state": "completed",
		"finalText": "the real last turn",
	})
	annotate(t, "t-lt3", map[string]string{annCurrentTurn: "turn-lt3-next"})
	postTurnComplete(t, cb, map[string]any{
		"turnId": "turn-lt3", "taskName": "t-lt3", "state": "completed",
		"finalText": "a replayed older turn",
	})

	lt := getTask(t, "t-lt3").Status.LastTurn
	if lt == nil || lt.FinalText != "the real last turn" {
		t.Fatalf("lastTurn = %+v, want the live turn's payload", lt)
	}
}

// TestTurnComplete_ReplayAfterTTLStopDoesNotResurrectLastTurn covers the
// duplicate-callback guard recordUsage has and recordLastTurn was missing.
//
// ttlStop clears status.lastTurn (it has just been spent on the synthetic note)
// but does not touch annCurrentTurn, so a replayed turn-complete callback for
// the SAME turn still matches. Without the annTurnComplete guard it resurrects
// the stopped pod's payload onto a Task that now belongs to a fresh pod - and if
// that pod TTL-stops before completing a turn of its own, its synthetic note
// carries the dead pod's final text and counts handoff=synthetic. That is the
// runbook's benign "stopped before its first turn" case reporting as a captured
// handoff.
func TestTurnComplete_ReplayAfterTTLStopDoesNotResurrectLastTurn(t *testing.T) {
	mkTaskProject(t, "p-lt4", 3)
	mkTaskRepository(t, "r-lt4", "p-lt4")
	mkTask(t, "t-lt4", "p-lt4", "r-lt4")
	annotate(t, "t-lt4", map[string]string{annCurrentTurn: "turn-lt4"})

	cb := newCallbackServer()
	postTurnComplete(t, cb, map[string]any{
		"turnId": "turn-lt4", "taskName": "t-lt4", "state": "completed",
		"finalText": "the stopped pod's work",
	})

	// The TTL stop spends lastTurn on the synthetic note and re-arms the Task.
	// annCurrentTurn is deliberately left alone, exactly as ttlStop leaves it.
	tk := getTask(t, "t-lt4")
	tk.Status.LastTurn = nil
	if err := k8sClient.Status().Update(t.Context(), tk); err != nil {
		t.Fatalf("clear lastTurn: %v", err)
	}

	postTurnComplete(t, cb, map[string]any{
		"turnId": "turn-lt4", "taskName": "t-lt4", "state": "completed",
		"finalText": "the stopped pod's work",
	})

	if lt := getTask(t, "t-lt4").Status.LastTurn; lt != nil {
		t.Errorf("lastTurn = %+v after a replayed callback: the stopped pod's payload was resurrected", lt)
	}
}
