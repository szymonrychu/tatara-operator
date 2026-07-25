package controller

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// TestTurnComplete_CountsInternalIssues: the alert for agent-reported platform
// problems moves off Loki onto agent_internal_issue_total (tatara-observability
// #63), so every internalIssues entry on a turn-complete callback must increment
// the counter for its (category, severity) pair, once per entry.
//
// Fixture names use an "-ii2" suffix (not "-ii" from turncallback_test.go's
// TestTurnComplete_LogsAgentInternalIssue) because both tests share the same
// envtest API server within this package and objects are not reset between
// tests: reusing "p-ii"/"r-ii"/"t-ii" here would collide with that test's
// Project/Repository/Task and fail with "already exists".
func TestTurnComplete_CountsInternalIssues(t *testing.T) {
	mkTaskProject(t, "p-ii2", 3)
	mkTaskRepository(t, "r-ii2", "p-ii2")
	mkTask(t, "t-ii2", "p-ii2", "r-ii2")
	annotate(t, "t-ii2", map[string]string{annCurrentTurn: "turn-ii2"})

	before := testutil.ToFloat64(obs.AgentInternalIssueTotal.WithLabelValues("tooling", "error"))

	cb := newCallbackServer()
	body, _ := json.Marshal(map[string]any{
		"turnId": "turn-ii2", "state": "completed",
		"internalIssues": []map[string]any{
			{"category": "tooling", "severity": "error", "description": "mcp tool exploded", "resource_id": "t-ii2"},
			{"category": "tooling", "severity": "error", "description": "and again", "resource_id": "t-ii2"},
			{"category": "prompt", "severity": "warn", "description": "ambiguous goal", "resource_id": "t-ii2"},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", w.Code, w.Body.String())
	}
	if got := testutil.ToFloat64(obs.AgentInternalIssueTotal.WithLabelValues("tooling", "error")); got != before+2 {
		t.Fatalf("agent_internal_issue_total{tooling,error} = %v, want %v (one per reported issue)", got, before+2)
	}
	if got := testutil.ToFloat64(obs.AgentInternalIssueTotal.WithLabelValues("prompt", "warn")); got < 1 {
		t.Fatalf("agent_internal_issue_total{prompt,warn} = %v, want >=1", got)
	}
}
