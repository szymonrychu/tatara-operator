package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/types"

	"github.com/szymonrychu/tatara-operator/internal/obs"
)

func newCallbackServer() *CallbackServer {
	return &CallbackServer{
		Client:    k8sClient,
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Namespace: testNS,
	}
}

func TestTurnComplete_RecordsResultAndRequeues(t *testing.T) {
	mkTaskProject(t, "p-cb", 3)
	mkTaskRepository(t, "r-cb", "p-cb")
	mkTask(t, "t-cb", "p-cb", "r-cb")
	mkSubtask(t, "t-cb-s1", "t-cb", 1)
	annotate(t, "t-cb", map[string]string{
		annCurrentTurn:    "turn-42",
		annCurrentSubtask: "t-cb-s1",
	})

	cb := newCallbackServer()
	body, _ := json.Marshal(map[string]any{
		"turnId": "turn-42", "state": "completed",
		"finalText": "subtask done well", "stopReason": "end_turn",
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", w.Code, w.Body.String())
	}
	st := getSubtask(t, "t-cb-s1")
	if st.Status.Result != "subtask done well" {
		t.Errorf("subtask result = %q, want recorded", st.Status.Result)
	}
	tk := getTask(t, "t-cb")
	if tk.Annotations[annTurnComplete] == "" {
		t.Error("expected turn-complete annotation set to requeue the task")
	}
}

func TestTurnComplete_UnknownTurn404(t *testing.T) {
	cb := newCallbackServer()
	body, _ := json.Marshal(map[string]any{"turnId": "nope", "state": "completed"})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestTurnComplete_PlanTurnNoSubtask(t *testing.T) {
	mkTaskProject(t, "p-cb2", 3)
	mkTaskRepository(t, "r-cb2", "p-cb2")
	mkTask(t, "t-cb2", "p-cb2", "r-cb2")
	annotate(t, "t-cb2", map[string]string{annCurrentTurn: "turn-plan"})

	cb := newCallbackServer()
	body, _ := json.Marshal(map[string]any{"turnId": "turn-plan", "state": "completed", "finalText": "planned"})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", w.Code, w.Body.String())
	}
	tk := getTask(t, "t-cb2")
	if tk.Annotations[annTurnComplete] == "" {
		t.Error("plan-turn callback must still requeue the task")
	}
}

func TestResolveTaskByTurn(t *testing.T) {
	mkTaskProject(t, "p-res", 3)
	mkTaskRepository(t, "r-res", "p-res")
	mkTask(t, "t-res", "p-res", "r-res")
	annotate(t, "t-res", map[string]string{annCurrentTurn: "turn-find-me"})

	cb := newCallbackServer()
	tk, err := cb.resolveTaskByTurn(context.Background(), "turn-find-me")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if tk.Name != "t-res" {
		t.Errorf("resolved = %q, want t-res", tk.Name)
	}
	if _, err := cb.resolveTaskByTurn(context.Background(), "missing"); err == nil {
		t.Error("expected error for unknown turn")
	}
	_ = types.NamespacedName{}
	_ = time.Now
}

// ----- Fix 3: empty turnId -> 400, no Task mutated -----

func TestTurnComplete_EmptyTurnID_Returns400(t *testing.T) {
	cb := newCallbackServer()
	body, _ := json.Marshal(map[string]any{"turnId": "", "state": "completed"})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for empty turnId", w.Code)
	}
}

func TestTurnComplete_MissingTurnID_Returns400(t *testing.T) {
	cb := newCallbackServer()
	// Omit turnId entirely from the body.
	body, _ := json.Marshal(map[string]any{"state": "completed"})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for missing turnId", w.Code)
	}
}

func TestResolveTaskByTurn_SkipsEmptyAnnotation(t *testing.T) {
	// Create a Task with no annCurrentTurn annotation (empty string).
	mkTaskProject(t, "p-skip", 3)
	mkTaskRepository(t, "r-skip", "p-skip")
	mkTask(t, "t-skip-empty", "p-skip", "r-skip")
	// t-skip-empty has no annCurrentTurn -> resolveTaskByTurn must NOT match it.

	cb := newCallbackServer()
	// Searching for empty string must return errTurnNotFound (not t-skip-empty).
	_, err := cb.resolveTaskByTurn(context.Background(), "")
	if err == nil {
		t.Error("expected error resolving empty turnId: Tasks with empty annCurrentTurn must be skipped")
	}
}

// ----- Fix 2: per-turn timeout in poll backstop -----

// ----- Spawn watchdog: Planning-without-turn past the stall deadline -----

func TestPollOnce_FailsStalledPlanning(t *testing.T) {
	mkTaskProject(t, "p-stall", 3)
	mkTaskRepository(t, "r-stall", "p-stall")
	mkTask(t, "t-stall", "p-stall", "r-stall")
	setTaskPhase(t, "t-stall", "Planning")
	// Entered Planning long ago, no current-turn ever acquired.
	annotate(t, "t-stall", map[string]string{annPlanningSince: "2000-01-01T00:00:00Z"})

	cb := newCallbackServer()
	cb.PollOnce(context.Background())

	tk := getTask(t, "t-stall")
	if tk.Status.Phase != "Failed" {
		t.Errorf("phase = %q, want Failed after planning stall", tk.Status.Phase)
	}
	cond := findCond(tk.Status.Conditions, "Ready")
	if cond == nil || cond.Reason != "PlanningStalled" {
		t.Errorf("expected Ready/PlanningStalled condition, got %+v", cond)
	}
}

func TestPollOnce_LeavesFreshPlanning(t *testing.T) {
	mkTaskProject(t, "p-fresh", 3)
	mkTaskRepository(t, "r-fresh", "p-fresh")
	mkTask(t, "t-fresh", "p-fresh", "r-fresh")
	setTaskPhase(t, "t-fresh", "Planning")
	// Just entered Planning: well within the stall deadline.
	annotate(t, "t-fresh", map[string]string{annPlanningSince: time.Now().UTC().Format(time.RFC3339)})

	cb := newCallbackServer()
	cb.PollOnce(context.Background())

	tk := getTask(t, "t-fresh")
	if tk.Status.Phase != "Planning" {
		t.Errorf("phase = %q, want Planning unchanged for a fresh spawn", tk.Status.Phase)
	}
}

func TestPollOnce_ExpiresTurnTimeout(t *testing.T) {
	mkTaskProject(t, "p-timeout", 3)
	mkTaskRepository(t, "r-timeout", "p-timeout")
	mkTask(t, "t-timeout", "p-timeout", "r-timeout")
	setTaskPhase(t, "t-timeout", "Running")
	// Seed turn-started-at far in the past to simulate a timed-out turn.
	annotate(t, "t-timeout", map[string]string{
		annCurrentTurn:   "turn-stale",
		annTurnStartedAt: "2000-01-01T00:00:00Z",
	})

	cb := newCallbackServer()
	cb.PollOnce(context.Background())

	tk := getTask(t, "t-timeout")
	if tk.Status.Phase != "Failed" {
		t.Errorf("phase = %q, want Failed after turn timeout", tk.Status.Phase)
	}
	cond := findCond(tk.Status.Conditions, "Ready")
	if cond == nil || cond.Reason != "TurnTimeout" {
		t.Errorf("expected Ready/TurnTimeout condition, got %+v", cond)
	}
}
