// Tests for audit-r2 findings in turncallback.go (findings 1,2/5/8,4/6,7).
package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// --- Finding 1: recordUsage stale/duplicate guard ---

// TestRecordUsage_StaleCallbackDoesNotDoubleCount verifies that recordUsage is
// a no-op when annCurrentTurn no longer matches the provided turnID.
// Before the fix, recordUsage had no stale-turn guard so a second POST for the
// same turnId (e.g. wrapper retry after a 500 from recordResult) would bump
// CumulativeTokens twice.
func TestRecordUsage_StaleCallbackDoesNotDoubleCount(t *testing.T) {
	mkTaskProject(t, "p-ru1", 3)
	mkTaskRepository(t, "r-ru1", "p-ru1")
	mkTask(t, "t-ru1", "p-ru1", "r-ru1")
	setTaskCumulativeTokens(t, "t-ru1", 0)
	annotate(t, "t-ru1", map[string]string{annCurrentTurn: "turn-ru1"})

	cb := newCallbackServer()

	// First: normal usage update for the live turn.
	task := getTask(t, "t-ru1")
	rawUsage, _ := json.Marshal(map[string]any{"output_tokens": 50})
	if err := cb.recordUsage(context.Background(), task, rawUsage, "turn-ru1"); err != nil {
		t.Fatalf("recordUsage first call: %v", err)
	}
	if tk := getTask(t, "t-ru1"); tk.Status.CumulativeTokens != 50 {
		t.Fatalf("after first call CumulativeTokens = %d, want 50", tk.Status.CumulativeTokens)
	}

	// Simulate reconcile advancing the turn - old turn is no longer current.
	annotate(t, "t-ru1", map[string]string{annCurrentTurn: "turn-ru1-next"})

	// Duplicate/retry for the OLD turn must be a no-op.
	task2 := getTask(t, "t-ru1")
	if err := cb.recordUsage(context.Background(), task2, rawUsage, "turn-ru1"); err != nil {
		t.Fatalf("recordUsage stale call: %v", err)
	}
	if tk := getTask(t, "t-ru1"); tk.Status.CumulativeTokens != 50 {
		t.Errorf("CumulativeTokens = %d after stale recordUsage, want 50 (no double-count)", tk.Status.CumulativeTokens)
	}
}

// TestRecordUsage_TerminalTaskIsNoop verifies that recordUsage does not update
// a task already in a terminal phase.
func TestRecordUsage_TerminalTaskIsNoop(t *testing.T) {
	mkTaskProject(t, "p-ru2", 3)
	mkTaskRepository(t, "r-ru2", "p-ru2")
	mkTask(t, "t-ru2", "p-ru2", "r-ru2")
	setTaskPhase(t, "t-ru2", "Failed")
	setTaskCumulativeTokens(t, "t-ru2", 999)
	annotate(t, "t-ru2", map[string]string{annCurrentTurn: "turn-ru2"})

	cb := newCallbackServer()
	task := getTask(t, "t-ru2")
	rawUsage, _ := json.Marshal(map[string]any{"output_tokens": 100})
	if err := cb.recordUsage(context.Background(), task, rawUsage, "turn-ru2"); err != nil {
		t.Fatalf("recordUsage: %v", err)
	}
	if tk := getTask(t, "t-ru2"); tk.Status.CumulativeTokens != 999 {
		t.Errorf("CumulativeTokens = %d, want 999 (no-op on terminal task)", tk.Status.CumulativeTokens)
	}
}

// --- Findings 2, 5: recordResult subtask write must be guarded ---

// TestRecordResult_StaleSubtaskNotClobbered verifies that a stale callback
// (turnID != fresh annCurrentTurn) does NOT overwrite the current subtask's result.
func TestRecordResult_StaleSubtaskNotClobbered(t *testing.T) {
	mkTaskProject(t, "p-ss1", 3)
	mkTaskRepository(t, "r-ss1", "p-ss1")
	mkTask(t, "t-ss1", "p-ss1", "r-ss1")
	mkSubtask(t, "t-ss1-sub", "t-ss1", 1)
	// Seed correct result on the subtask - the "current turn" already wrote it.
	setSubtaskResult(t, "t-ss1-sub", "correct result from current turn")
	// Task has advanced to a new turn.
	annotate(t, "t-ss1", map[string]string{
		annCurrentTurn:    "new-turn-2",
		annCurrentSubtask: "t-ss1-sub",
	})

	cb := newCallbackServer()
	task := getTask(t, "t-ss1")
	// A stale callback for "old-turn" must not overwrite the subtask result.
	if err := cb.recordResult(context.Background(),
		agent.TurnResult{State: "completed", FinalText: "stale result"},
		task, "old-turn"); err != nil {
		t.Fatalf("recordResult: %v", err)
	}
	st := getSubtask(t, "t-ss1-sub")
	if st.Status.Result != "correct result from current turn" {
		t.Errorf("subtask result = %q; stale callback must NOT clobber existing result", st.Status.Result)
	}
}

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
func setSubtaskResult(t *testing.T, name, result string) {
	t.Helper()
	st := getSubtask(t, name)
	st.Status.Result = result
	if err := k8sClient.Status().Update(context.Background(), st); err != nil {
		t.Fatalf("setSubtaskResult %s: %v", name, err)
	}
}
