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
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
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

// --- Finding 7: nil-Metrics guard ---

// TestHandleTurnComplete_NilMetricsNoPanic verifies that handleTurnComplete
// does not panic when s.Metrics is nil.
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
	setTaskStage(t, "t-nilm2", tatarav1alpha1.StateUnderImplementation)
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
	annotate(t, "t-sr1", map[string]string{annCurrentTurn: "turn-sr1"})

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
	if tk.Status.Stats.TokensInput != 100 {
		t.Errorf("stats.tokensInput = %d, want 100", tk.Status.Stats.TokensInput)
	}
	if tk.Status.Stats.TokensCacheRead != 10 {
		t.Errorf("stats.tokensCacheRead = %d, want 10", tk.Status.Stats.TokensCacheRead)
	}
	if tk.Status.Stats.TokensOutput != 50 {
		t.Errorf("stats.tokensOutput = %d, want 50", tk.Status.Stats.TokensOutput)
	}
}
