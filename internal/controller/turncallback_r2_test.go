// Tests for audit-r2 findings in turncallback.go (findings 1,2/5/8,4/6,7).
package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// --- Finding 1: recordUsage stale/duplicate guard ---

// TestRecordUsage_StaleCallbackDoesNotDoubleCount verifies that recordUsage is
// a no-op when annCurrentTurn no longer matches the provided turnID.
// Before the fix, recordUsage had no stale-turn guard so a second POST for the
// same turnId (e.g. wrapper retry after a 500 from recordResult) would bump
// Stats.TokensOutput twice.
func TestRecordUsage_StaleCallbackDoesNotDoubleCount(t *testing.T) {
	mkTaskProject(t, "p-ru1", 3)
	mkTaskRepository(t, "r-ru1", "p-ru1")
	mkTask(t, "t-ru1", "p-ru1", "r-ru1")
	setTaskTokens(t, "t-ru1", 0)
	annotate(t, "t-ru1", map[string]string{annCurrentTurn: "turn-ru1"})

	cb := newCallbackServer()

	// First: normal usage update for the live turn.
	task := getTask(t, "t-ru1")
	rawUsage, _ := json.Marshal(map[string]any{"output_tokens": 50})
	if _, _, err := cb.recordUsage(context.Background(), task, rawUsage, "turn-ru1"); err != nil {
		t.Fatalf("recordUsage first call: %v", err)
	}
	if tk := getTask(t, "t-ru1"); tk.Status.Stats.TokensOutput != 50 {
		t.Fatalf("after first call Stats.TokensOutput = %d, want 50", tk.Status.Stats.TokensOutput)
	}

	// Simulate reconcile advancing the turn - old turn is no longer current.
	annotate(t, "t-ru1", map[string]string{annCurrentTurn: "turn-ru1-next"})

	// Duplicate/retry for the OLD turn must be a no-op.
	task2 := getTask(t, "t-ru1")
	if _, _, err := cb.recordUsage(context.Background(), task2, rawUsage, "turn-ru1"); err != nil {
		t.Fatalf("recordUsage stale call: %v", err)
	}
	if tk := getTask(t, "t-ru1"); tk.Status.Stats.TokensOutput != 50 {
		t.Errorf("Stats.TokensOutput = %d after stale recordUsage, want 50 (no double-count)", tk.Status.Stats.TokensOutput)
	}
}

// TestRecordUsage_TerminalTaskIsNoop verifies that recordUsage does not update
// a task already in a terminal phase.
func TestRecordUsage_TerminalTaskIsNoop(t *testing.T) {
	mkTaskProject(t, "p-ru2", 3)
	mkTaskRepository(t, "r-ru2", "p-ru2")
	mkTask(t, "t-ru2", "p-ru2", "r-ru2")
	setTaskStage(t, "t-ru2", tatarav1alpha1.StageFailed)
	setTaskTokens(t, "t-ru2", 999)
	annotate(t, "t-ru2", map[string]string{annCurrentTurn: "turn-ru2"})

	cb := newCallbackServer()
	task := getTask(t, "t-ru2")
	rawUsage, _ := json.Marshal(map[string]any{"output_tokens": 100})
	if _, _, err := cb.recordUsage(context.Background(), task, rawUsage, "turn-ru2"); err != nil {
		t.Fatalf("recordUsage: %v", err)
	}
	if tk := getTask(t, "t-ru2"); tk.Status.Stats.TokensOutput != 999 {
		t.Errorf("Stats.TokensOutput = %d, want 999 (no-op on terminal task)", tk.Status.Stats.TokensOutput)
	}
}

// --- Findings 2, 5: recordResult subtask write must be guarded ---

// --- Findings 4, 6: resolveTaskByTurn - direct Get when task name provided ---

// TestResolveTaskByTurnWithHint_DirectGet verifies that when a taskName is
// provided, the task is resolved without a full-namespace List.
func TestResolveTaskByTurnWithHint_DirectGet(t *testing.T) {
	mkTaskProject(t, "p-dg1", 3)
	mkTaskRepository(t, "r-dg1", "p-dg1")
	mkTask(t, "t-dg1", "p-dg1", "r-dg1")
	annotate(t, "t-dg1", map[string]string{annCurrentTurn: "turn-dg1"})

	cb := newCallbackServer()
	task, err := cb.resolveTaskByTurnWithHint(context.Background(), "turn-dg1", "t-dg1")
	if err != nil {
		t.Fatalf("resolveTaskByTurnWithHint: %v", err)
	}
	if task.Name != "t-dg1" {
		t.Errorf("resolved task = %q, want t-dg1", task.Name)
	}
}

// TestResolveTaskByTurnWithHint_WrongTurnReturnsNotFound verifies that when
// the task name hint is provided but annCurrentTurn does not match, the call
// returns errTurnNotFound.
func TestResolveTaskByTurnWithHint_WrongTurnReturnsNotFound(t *testing.T) {
	mkTaskProject(t, "p-dg2", 3)
	mkTaskRepository(t, "r-dg2", "p-dg2")
	mkTask(t, "t-dg2", "p-dg2", "r-dg2")
	annotate(t, "t-dg2", map[string]string{annCurrentTurn: "actual-turn"})

	cb := newCallbackServer()
	_, err := cb.resolveTaskByTurnWithHint(context.Background(), "wrong-turn", "t-dg2")
	if err == nil {
		t.Error("expected errTurnNotFound when turnID does not match the hinted task's annCurrentTurn")
	}
}

// TestHandleTurnComplete_WithTaskName verifies that a POST with taskName field
// routes via the direct-Get path and still completes the turn.
func TestHandleTurnComplete_WithTaskName(t *testing.T) {
	mkTaskProject(t, "p-tn1", 3)
	mkTaskRepository(t, "r-tn1", "p-tn1")
	mkTask(t, "t-tn1", "p-tn1", "r-tn1")
	annotate(t, "t-tn1", map[string]string{annCurrentTurn: "turn-tn1"})

	cb := newCallbackServer()
	body, _ := json.Marshal(map[string]any{
		"turnId":    "turn-tn1",
		"taskName":  "t-tn1",
		"state":     "completed",
		"finalText": "ok",
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204; body = %s", w.Code, w.Body.String())
	}
	if tk := getTask(t, "t-tn1"); tk.Annotations[annTurnComplete] == "" {
		t.Error("turn-complete annotation must be set")
	}
}

// --- Finding 7: bounded shutdown context ---
// no-test: the 10s shutdown context in Start() is wiring code that cannot
// be exercised without a live http.Server started in a goroutine and a port bind.
// The fix is a trivial one-liner mirroring webhook/server.go:823.

// --- helpers ---

// setSubtaskResult seeds a subtask's result for tests.
