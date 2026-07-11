// audit-fixes tests for turncallback.go findings 1,2,3,4,7.
package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// --- Finding 1: stale-turn TOCTOU guard ---

// TestRecordResult_StaleCallbackIsNoop verifies that a callback for an old
// turnID is ignored after the task's annCurrentTurn has been replaced with a
// newer turn. Before the fix, recordResult stamped annTurnComplete even when
// the fresh Get showed a different current turn.
func TestRecordResult_StaleCallbackIsNoop(t *testing.T) {
	mkTaskProject(t, "p-stale1", 3)
	mkTaskRepository(t, "r-stale1", "p-stale1")
	mkTask(t, "t-stale1", "p-stale1", "r-stale1")
	// Task was on old-turn but has since advanced to new-turn.
	annotate(t, "t-stale1", map[string]string{
		annCurrentTurn: "new-turn",
	})

	cb := newCallbackServer()
	// Resolve by new-turn to get the task object.
	task, err := cb.resolveTaskByTurn(context.Background(), "new-turn")
	if err != nil {
		t.Fatalf("resolveTaskByTurn: %v", err)
	}
	// Pass "old-turn" as the turnID - stale callback scenario.
	// The task's annCurrentTurn is "new-turn", so this must be a no-op.
	if err := cb.recordResult(context.Background(), agent.TurnResult{State: "completed", FinalText: "stale"}, task, "old-turn"); err != nil {
		t.Fatalf("recordResult returned error: %v", err)
	}
	tk := getTask(t, "t-stale1")
	if tk.Annotations[annTurnComplete] != "" {
		t.Error("stale callback must NOT stamp annTurnComplete when annCurrentTurn != turnID")
	}
}

// TestRecordResult_CurrentTurnIsStamped verifies the happy path still works
// after the guard is added.
func TestRecordResult_CurrentTurnIsStamped(t *testing.T) {
	mkTaskProject(t, "p-stamp1", 3)
	mkTaskRepository(t, "r-stamp1", "p-stamp1")
	mkTask(t, "t-stamp1", "p-stamp1", "r-stamp1")
	annotate(t, "t-stamp1", map[string]string{annCurrentTurn: "turn-cur"})

	cb := newCallbackServer()
	task, err := cb.resolveTaskByTurn(context.Background(), "turn-cur")
	if err != nil {
		t.Fatalf("resolveTaskByTurn: %v", err)
	}
	if err := cb.recordResult(context.Background(), agent.TurnResult{State: "completed", FinalText: "done"}, task, "turn-cur"); err != nil {
		t.Fatalf("recordResult: %v", err)
	}
	tk := getTask(t, "t-stamp1")
	if tk.Annotations[annTurnComplete] == "" {
		t.Error("current-turn callback must stamp annTurnComplete")
	}
}

// TestRecordResult_PlanTurn_CapturesSyntheticOrderZeroEntry verifies item 8:
// the plan turn (turn 0, no annCurrentSubtask) writes its FinalText onto a
// synthetic order-0 "Planning" rollup entry instead of discarding it.
func TestRecordResult_PlanTurn_CapturesSyntheticOrderZeroEntry(t *testing.T) {
	mkTaskProject(t, "p-planturn", 3)
	mkTaskRepository(t, "r-planturn", "p-planturn")
	mkTask(t, "t-planturn", "p-planturn", "r-planturn")
	annotate(t, "t-planturn", map[string]string{annCurrentTurn: "turn-0"})

	cb := newCallbackServer()
	task, err := cb.resolveTaskByTurn(context.Background(), "turn-0")
	if err != nil {
		t.Fatalf("resolveTaskByTurn: %v", err)
	}
	if err := cb.recordResult(context.Background(), agent.TurnResult{State: "completed", FinalText: "the plan text"}, task, "turn-0"); err != nil {
		t.Fatalf("recordResult: %v", err)
	}
	tk := getTask(t, "t-planturn")
	if len(tk.Status.Subtasks) != 1 {
		t.Fatalf("Status.Subtasks = %+v, want 1 synthetic planning entry", tk.Status.Subtasks)
	}
	got := tk.Status.Subtasks[0]
	if got.Name != "" || got.Order != 0 || got.Title != "Planning" || got.Phase != "Done" || got.Result != "the plan text" {
		t.Errorf("Subtasks[0] = %+v, want {Name:\"\" Order:0 Title:Planning Phase:Done Result:\"the plan text\"}", got)
	}
}

// --- Finding 3: expireTimedOutTurn must clear annCurrentTurn ---

// TestExpireTimedOutTurn_ClearsCurrentTurnAnnotation verifies that after
// expireTimedOutTurn runs, annCurrentTurn and annTurnStartedAt are gone, so a
// late callback cannot resolve the task by the stale turn ID.
func TestExpireTimedOutTurn_ClearsCurrentTurnAnnotation(t *testing.T) {
	mkTaskProject(t, "p-exp1", 3)
	mkTaskRepository(t, "r-exp1", "p-exp1")
	mkTask(t, "t-exp1", "p-exp1", "r-exp1")
	setTaskPhase(t, "t-exp1", "Running")
	annotate(t, "t-exp1", map[string]string{
		annCurrentTurn:   "turn-expired",
		annTurnStartedAt: "2000-01-01T00:00:00Z",
	})

	cb := newCallbackServer()
	task := getTask(t, "t-exp1")
	if err := cb.expireTimedOutTurn(context.Background(), task, "turn-expired"); err != nil {
		t.Fatalf("expireTimedOutTurn: %v", err)
	}
	tk := getTask(t, "t-exp1")
	if tk.Annotations[annCurrentTurn] != "" {
		t.Error("expireTimedOutTurn must clear annCurrentTurn to prevent stale callback resolution")
	}
	if tk.Annotations[annTurnStartedAt] != "" {
		t.Error("expireTimedOutTurn must clear annTurnStartedAt")
	}
	if tk.Status.Phase != "Failed" {
		t.Errorf("phase = %q, want Failed", tk.Status.Phase)
	}
}

// TestExpireTimedOutTurn_LateCallbackIsNoop verifies that after expiry, a
// late callback cannot resolve the task (because annCurrentTurn is gone).
func TestExpireTimedOutTurn_LateCallbackIsNoop(t *testing.T) {
	mkTaskProject(t, "p-exp2", 3)
	mkTaskRepository(t, "r-exp2", "p-exp2")
	mkTask(t, "t-exp2", "p-exp2", "r-exp2")
	setTaskPhase(t, "t-exp2", "Running")
	annotate(t, "t-exp2", map[string]string{
		annCurrentTurn:   "turn-late",
		annTurnStartedAt: "2000-01-01T00:00:00Z",
	})

	cb := newCallbackServer()
	task := getTask(t, "t-exp2")
	if err := cb.expireTimedOutTurn(context.Background(), task, "turn-late"); err != nil {
		t.Fatalf("expireTimedOutTurn: %v", err)
	}

	// A late callback for the old turnID must now get errTurnNotFound.
	_, err := cb.resolveTaskByTurn(context.Background(), "turn-late")
	if err == nil {
		t.Error("expected errTurnNotFound after expiry cleared annCurrentTurn, got nil")
	}
}

// --- Finding 4: terminal-phase guard in expireTimedOutTurn ---

// TestExpireTimedOutTurn_AlreadyTerminalIsNoop verifies that calling
// expireTimedOutTurn on a task already in a terminal phase is a no-op
// (no second Status().Update that would conflict with the reconcile).
func TestExpireTimedOutTurn_AlreadyTerminalIsNoop(t *testing.T) {
	mkTaskProject(t, "p-term1", 3)
	mkTaskRepository(t, "r-term1", "p-term1")
	mkTask(t, "t-term1", "p-term1", "r-term1")
	setTaskPhase(t, "t-term1", "Failed") // already terminal
	annotate(t, "t-term1", map[string]string{
		annCurrentTurn:   "turn-term",
		annTurnStartedAt: "2000-01-01T00:00:00Z",
	})

	cb := newCallbackServer()
	task := getTask(t, "t-term1")
	if err := cb.expireTimedOutTurn(context.Background(), task, "turn-term"); err != nil {
		t.Fatalf("expireTimedOutTurn on terminal task should not error: %v", err)
	}
}

// --- Finding 7: nil-Metrics guard ---

// TestHandleTurnComplete_NilMetricsNoPanic verifies that handleTurnComplete
// does not panic when s.Metrics is nil, matching the LifecycleMetrics convention.
func TestHandleTurnComplete_NilMetricsNoPanic(t *testing.T) {
	mkTaskProject(t, "p-nilm1", 3)
	mkTaskRepository(t, "r-nilm1", "p-nilm1")
	mkTask(t, "t-nilm1", "p-nilm1", "r-nilm1")
	annotate(t, "t-nilm1", map[string]string{annCurrentTurn: "turn-nilm"})

	cb := &CallbackServer{
		Client:    k8sClient,
		Metrics:   nil, // intentionally nil
		Namespace: testNS,
	}
	body, _ := json.Marshal(map[string]any{
		"turnId": "turn-nilm", "state": "completed", "finalText": "ok",
		"durationSeconds": 1.5,
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	// Must not panic.
	cb.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204; body=%s", w.Code, w.Body.String())
	}
}

// TestPollOnce_NilMetricsNoPanic verifies that PollOnce does not panic when
// s.Metrics is nil, even when a timed-out turn is present.
func TestPollOnce_NilMetricsNoPanic(t *testing.T) {
	mkTaskProject(t, "p-nilm2", 3)
	mkTaskRepository(t, "r-nilm2", "p-nilm2")
	mkTask(t, "t-nilm2", "p-nilm2", "r-nilm2")
	setTaskPhase(t, "t-nilm2", "Running")
	annotate(t, "t-nilm2", map[string]string{
		annCurrentTurn:   "turn-nilm2",
		annTurnStartedAt: "2000-01-01T00:00:00Z",
	})

	cb := &CallbackServer{
		Client:    k8sClient,
		Metrics:   nil, // intentionally nil
		Namespace: testNS,
	}
	// Must not panic.
	cb.PollOnce(context.Background())
}

// --- Finding 2: single-resolve efficiency (regression check via handleTurnComplete) ---

// TestHandleTurnComplete_SingleResolveWithUsage verifies that handleTurnComplete
// with usage in the payload records both usage AND result correctly (regression
// check for the single-resolve refactor).
func TestHandleTurnComplete_SingleResolveWithUsage(t *testing.T) {
	mkTaskProject(t, "p-sr1", 3)
	mkTaskRepository(t, "r-sr1", "p-sr1")
	mkTask(t, "t-sr1", "p-sr1", "r-sr1")
	mkSubtask(t, "t-sr1-s1", "t-sr1", 1)
	annotate(t, "t-sr1", map[string]string{
		annCurrentTurn:    "turn-sr1",
		annCurrentSubtask: "t-sr1-s1",
	})

	cb := &CallbackServer{
		Client:    k8sClient,
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Namespace: testNS,
	}
	body, _ := json.Marshal(map[string]any{
		"turnId": "turn-sr1", "state": "completed", "finalText": "sr result",
		"usage": map[string]any{
			"input_tokens":            100,
			"output_tokens":           50,
			"cache_read_input_tokens": 10,
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", w.Code, w.Body.String())
	}
	tk := getTask(t, "t-sr1")
	if tk.Annotations[annTurnComplete] == "" {
		t.Error("turn-complete annotation must be set")
	}
	// LastTurnInputTokens = input_tokens + cache_read_input_tokens = 100 + 10 = 110
	if tk.Status.LastTurnInputTokens != 110 {
		t.Errorf("LastTurnInputTokens = %d, want 110", tk.Status.LastTurnInputTokens)
	}
	if tk.Status.CumulativeTokens != 50 {
		t.Errorf("CumulativeTokens = %d, want 50", tk.Status.CumulativeTokens)
	}
	st := getSubtask(t, "t-sr1-s1")
	if st.Status.Result != "sr result" {
		t.Errorf("subtask result = %q, want %q", st.Status.Result, "sr result")
	}
}
