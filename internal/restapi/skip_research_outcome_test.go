package restapi_test

// Tests for the skip_research -> brainstorm-outcome contract.
// The cli's skip_research MCP tool POSTs {"action":"none","reason":...} to
// /tasks/{t}/brainstorm-outcome. These tests lock the handler semantics so any
// future refactor that breaks the contract is caught immediately.
//
// The existing TestBrainstormOutcome* tests in handlers_test.go cover the same
// handler paths; these tests name the skip_research tool explicitly to make the
// cross-repo contract (operator <-> tatara-cli) visible and traceable.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	restapi "github.com/szymonrychu/tatara-operator/internal/restapi"
)

// TestBrainstormOutcome_SkipResearchRecordsReason verifies that the operator
// records the outcome when the cli skip_research tool calls
// POST /tasks/{t}/brainstorm-outcome with {"action":"none","reason":"..."}.
func TestBrainstormOutcome_SkipResearchRecordsReason(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "brainstorm"))
	body := strings.NewReader(`{"action":"none","reason":"nothing novel cleared the bar"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/brainstorm-outcome", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.NotNil(t, out.Status.BrainstormOutcome)
	require.Equal(t, "none", out.Status.BrainstormOutcome.Action)
	require.Equal(t, "nothing novel cleared the bar", out.Status.BrainstormOutcome.Reason)
}

// TestBrainstormOutcome_RejectsBlankReason verifies that a blank reason is
// rejected with 400. The skip_research tool must always supply a reason.
func TestBrainstormOutcome_RejectsBlankReason(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "brainstorm"))
	body := strings.NewReader(`{"action":"none","reason":"  "}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/brainstorm-outcome", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// TestBrainstormOutcome_RejectsNonBrainstormKind verifies that posting against
// a non-brainstorm task returns 409. The skip_research tool is only valid on a
// brainstorm task.
func TestBrainstormOutcome_RejectsNonBrainstormKind(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "issueLifecycle"))
	body := strings.NewReader(`{"action":"none","reason":"nothing novel cleared the bar"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/brainstorm-outcome", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
}
