// Copyright 2026 tatara authors.

package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"k8s.io/apimachinery/pkg/types"
)

// TestTurnComplete_WithUsage verifies that when a callback payload includes a
// usage block, LastTurnInputTokens and CumulativeTokens are updated on the Task.
func TestTurnComplete_WithUsage(t *testing.T) {
	mkTaskProject(t, "p-usage1", 3)
	mkTaskRepository(t, "r-usage1", "p-usage1")
	mkTask(t, "t-usage1", "p-usage1", "r-usage1")
	annotate(t, "t-usage1", map[string]string{annCurrentTurn: "turn-usage-1"})

	cb := newCallbackServer()
	body, _ := json.Marshal(map[string]any{
		"turnId":    "turn-usage-1",
		"state":     "completed",
		"finalText": "done",
		"usage": map[string]any{
			"input_tokens":                1000,
			"output_tokens":               200,
			"cache_read_input_tokens":     500,
			"cache_creation_input_tokens": 0,
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", w.Code, w.Body.String())
	}

	tk := getTask(t, "t-usage1")
	// LastTurnInputTokens = input_tokens + cache_read_input_tokens = 1000 + 500 = 1500
	if tk.Status.LastTurnInputTokens != 1500 {
		t.Errorf("LastTurnInputTokens = %d, want 1500", tk.Status.LastTurnInputTokens)
	}
	// CumulativeTokens += output_tokens = 200
	if tk.Status.CumulativeTokens != 200 {
		t.Errorf("CumulativeTokens = %d, want 200", tk.Status.CumulativeTokens)
	}
}

// TestTurnComplete_WithUsage_Accumulates verifies that CumulativeTokens is
// additive across multiple turn callbacks.
func TestTurnComplete_WithUsage_Accumulates(t *testing.T) {
	mkTaskProject(t, "p-usage2", 3)
	mkTaskRepository(t, "r-usage2", "p-usage2")
	mkTask(t, "t-usage2", "p-usage2", "r-usage2")

	// Seed with existing cumulative tokens.
	setTaskCumulativeTokens(t, "t-usage2", 1000)
	annotate(t, "t-usage2", map[string]string{annCurrentTurn: "turn-usage-2"})

	cb := newCallbackServer()
	body, _ := json.Marshal(map[string]any{
		"turnId":    "turn-usage-2",
		"state":     "completed",
		"finalText": "done",
		"usage": map[string]any{
			"input_tokens":            300,
			"output_tokens":           100,
			"cache_read_input_tokens": 200,
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", w.Code, w.Body.String())
	}

	tk := getTask(t, "t-usage2")
	// LastTurnInputTokens = 300 + 200 = 500
	if tk.Status.LastTurnInputTokens != 500 {
		t.Errorf("LastTurnInputTokens = %d, want 500", tk.Status.LastTurnInputTokens)
	}
	// CumulativeTokens = 1000 (existing) + 100 (output) = 1100
	if tk.Status.CumulativeTokens != 1100 {
		t.Errorf("CumulativeTokens = %d, want 1100", tk.Status.CumulativeTokens)
	}
}

// TestTurnComplete_WithoutUsage_LeavesTokensUnchanged verifies that a callback
// without a usage block does not modify token fields.
func TestTurnComplete_WithoutUsage_LeavesTokensUnchanged(t *testing.T) {
	mkTaskProject(t, "p-usage3", 3)
	mkTaskRepository(t, "r-usage3", "p-usage3")
	mkTask(t, "t-usage3", "p-usage3", "r-usage3")
	setTaskCumulativeTokens(t, "t-usage3", 500)
	annotate(t, "t-usage3", map[string]string{annCurrentTurn: "turn-usage-3"})

	cb := newCallbackServer()
	body, _ := json.Marshal(map[string]any{
		"turnId":    "turn-usage-3",
		"state":     "completed",
		"finalText": "done",
		// no "usage" field
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", w.Code, w.Body.String())
	}

	tk := getTask(t, "t-usage3")
	if tk.Status.LastTurnInputTokens != 0 {
		t.Errorf("LastTurnInputTokens = %d, want 0 (unchanged)", tk.Status.LastTurnInputTokens)
	}
	if tk.Status.CumulativeTokens != 500 {
		t.Errorf("CumulativeTokens = %d, want 500 (unchanged)", tk.Status.CumulativeTokens)
	}
}

// TestRecordUsage_EmitsTurn verifies that a successful turn callback (usage
// recorded) increments operator_task_turns_total by 1.
func TestRecordUsage_EmitsTurn(t *testing.T) {
	mkTaskProject(t, "p-turnc1", 3)
	mkTaskRepository(t, "r-turnc1", "p-turnc1")
	mkTask(t, "t-turnc1", "p-turnc1", "r-turnc1")
	annotate(t, "t-turnc1", map[string]string{annCurrentTurn: "turn-tc1"})

	reg := prometheus.NewRegistry()
	cb := &CallbackServer{
		Client:    k8sClient,
		Metrics:   obs.NewOperatorMetrics(reg),
		Namespace: testNS,
	}
	body, _ := json.Marshal(map[string]any{
		"turnId": "turn-tc1", "state": "completed", "finalText": "done",
		"usage": map[string]any{"input_tokens": 100, "output_tokens": 50},
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", w.Code, w.Body.String())
	}

	// mkTask sets Spec.ProjectRef="p-turnc1", RepositoryRef="r-turnc1", Source=nil and
	// leaves Kind unset, so the CRD default (+kubebuilder:default="implement") applies.
	// taskTokenLabels then yields project="p-turnc1", repo="r-turnc1", kind="implement", issue="".
	got := testutil.ToFloat64(cb.Metrics.TaskTurnsCounter("p-turnc1", "r-turnc1", "implement", ""))
	if got != 1 {
		t.Fatalf("operator_task_turns_total after successful callback = %v, want 1", got)
	}
}

// TestRecordUsage_StaleCallback_NoTurn verifies that the stale-turn guard
// prevents AddTaskTurn when the task's annCurrentTurn does not match the
// callback's turnID (simulating a duplicate/stale callback).
func TestRecordUsage_StaleCallback_NoTurn(t *testing.T) {
	mkTaskProject(t, "p-turnc2", 3)
	mkTaskRepository(t, "r-turnc2", "p-turnc2")
	mkTask(t, "t-turnc2", "p-turnc2", "r-turnc2")
	// The task is on "actual-turn"; the callback will claim "wrong-turn".
	annotate(t, "t-turnc2", map[string]string{annCurrentTurn: "actual-turn"})

	reg := prometheus.NewRegistry()
	cb := &CallbackServer{
		Client:    k8sClient,
		Metrics:   obs.NewOperatorMetrics(reg),
		Namespace: testNS,
	}
	// Resolve via "actual-turn" so we have the Task object.
	task, err := cb.resolveTaskByTurn(context.Background(), "actual-turn")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	// recordUsage with wrong turnID: the inner guard will see annCurrentTurn != "wrong-turn"
	// and leave recorded=false, so AddTaskTurn must not be called.
	usage, _ := json.Marshal(map[string]any{"input_tokens": 200, "output_tokens": 80})
	if _, _, err := cb.recordUsage(context.Background(), task, json.RawMessage(usage), "wrong-turn"); err != nil {
		t.Fatalf("recordUsage: %v", err)
	}

	got := testutil.ToFloat64(cb.Metrics.TaskTurnsCounter("p-turnc2", "r-turnc2", "implement", ""))
	if got != 0 {
		t.Fatalf("operator_task_turns_total with stale turnID = %v, want 0", got)
	}
}

// setTaskCumulativeTokens is a test helper that seeds CumulativeTokens on a task.
func setTaskCumulativeTokens(t *testing.T, name string, n int64) {
	t.Helper()
	tk := getTask(t, name)
	tk.Status.CumulativeTokens = n
	if err := k8sClient.Status().Update(context.Background(), tk); err != nil {
		t.Fatalf("setTaskCumulativeTokens %s: %v", name, err)
	}
}

// ensure types import is used.
var _ = types.NamespacedName{}
