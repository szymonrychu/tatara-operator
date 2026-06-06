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
