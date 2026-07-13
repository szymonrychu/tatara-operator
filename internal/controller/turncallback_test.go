package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
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
	annotate(t, "t-cb", map[string]string{annCurrentTurn: "turn-42"})

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
	tk := getTask(t, "t-cb")
	if tk.Annotations[annTurnComplete] == "" {
		t.Error("expected turn-complete annotation set to requeue the task")
	}
}

// TestTurnComplete_IgnoresRateLimitField asserts that a turn-complete payload
// carrying the now-retired wrapper `rateLimit` snapshot (issue #189) decodes
// without error and leaves Status.TokenBudget subscription fields untouched;
// subscription state now lives only in the fleet-wide account-usage store
// (Task A8, superseded by the poller in Task A9).
func TestTurnComplete_IgnoresRateLimitField(t *testing.T) {
	task := mkBudgetProject(t, "p-tb-rl", tatarav1alpha1.TokenBudgetSpec{
		Enabled: true,
		Mode:    "claudeSubscription",
	})
	annotate(t, task.Name, map[string]string{annCurrentTurn: "turn-rl1"})

	cb := newCallbackServer()
	body, _ := json.Marshal(map[string]any{
		"turnId": "turn-rl1", "state": "completed",
		"rateLimit": map[string]any{
			"fiveHourPercent": 61, "fiveHourResetUnix": time.Now().Add(time.Hour).Unix(),
			"weeklyPercent": 40, "weeklyResetUnix": time.Now().Add(72 * time.Hour).Unix(),
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", w.Code, w.Body.String())
	}
	p := getProject(t, "p-tb-rl")
	if p.Status.TokenBudget != nil && p.Status.TokenBudget.FiveHourPercent != 0 {
		t.Fatalf("Status.TokenBudget.FiveHourPercent = %d, want 0 (rateLimit must be ignored)",
			p.Status.TokenBudget.FiveHourPercent)
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

func TestTurnComplete_RequeuesWithNoFinalText(t *testing.T) {
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
		t.Error("callback must requeue the task")
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

// TestPollOnce_RefreshesLastActivity verifies the backstop stamps the
// turn-last-activity-at annotation from the wrapper's GetTurn report.
func TestPollOnce_RefreshesLastActivity(t *testing.T) {
	mkTaskProject(t, "p-act", 3)
	mkTaskRepository(t, "r-act", "p-act")
	mkTask(t, "t-act", "p-act", "r-act")
	annotate(t, "t-act", map[string]string{
		annCurrentTurn:   "turn-act",
		annTurnStartedAt: time.Now().UTC().Format(time.RFC3339), // recent: not timed out
	})

	activity := time.Now().Add(-3 * time.Second).UTC().Truncate(time.Second)
	fs := newFakeSession()
	fs.getResult["turn-act"] = agent.TurnResult{State: "running", LastActivityAt: activity}

	cb := newCallbackServer()
	cb.Session = fs
	cb.PollOnce(context.Background())

	tk := getTask(t, "t-act")
	want := activity.Format(time.RFC3339)
	if got := tk.Annotations[annTurnLastActivity]; got != want {
		t.Errorf("turn-last-activity-at = %q, want %q", got, want)
	}
}

// TestPollOnce_StallAwareKeepsActiveTurnAlive verifies the stall-aware backstop
// does NOT expire a turn whose start is well past the window but whose last
// activity is recent - the productive turns the wrapper inactivity-timer protects.
func TestPollOnce_StallAwareKeepsActiveTurnAlive(t *testing.T) {
	mkTaskProject(t, "p-keep", 3)
	mkTaskRepository(t, "r-keep", "p-keep")
	mkTask(t, "t-keep", "p-keep", "r-keep")
	annotate(t, "t-keep", map[string]string{
		annCurrentTurn:      "turn-keep",
		annTurnStartedAt:    time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339),
		annTurnLastActivity: time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339),
	})

	fs := newFakeSession()
	fs.getResult["turn-keep"] = agent.TurnResult{State: "running"}
	cb := newCallbackServer()
	cb.Session = fs
	cb.PollOnce(context.Background())

	tk := getTask(t, "t-keep")
	if tk.Annotations[annCurrentTurn] != "turn-keep" {
		t.Errorf("annCurrentTurn = %q, want turn-keep (turn must remain in flight)", tk.Annotations[annCurrentTurn])
	}
}

// TestPollOnce_ExpiresStalledTurn verifies a turn that has gone silent past the
// stall window is still failed by the backstop (hung-agent recovery unchanged).
func TestPollOnce_ExpiresStalledTurn(t *testing.T) {
	mkTaskProject(t, "p-stale", 3)
	mkTaskRepository(t, "r-stale", "p-stale")
	mkTask(t, "t-stale", "p-stale", "r-stale")
	stale := time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339)
	annotate(t, "t-stale", map[string]string{
		annCurrentTurn:      "turn-stale",
		annTurnStartedAt:    stale,
		annTurnLastActivity: stale,
	})

	cb := newCallbackServer()
	cb.Session = newFakeSession()
	cb.PollOnce(context.Background())

	tk := getTask(t, "t-stale")
	if tk.Annotations[annCurrentTurn] != "" {
		t.Errorf("annCurrentTurn = %q, want cleared: a stalled turn is expired and its pod torn down",
			tk.Annotations[annCurrentTurn])
	}
	if tatarav1alpha1.TaskDone(tk) {
		t.Error("a stalled TURN must not terminate the TASK: the stage machine respawns its pod")
	}
}

// TestPollOnce_RecordsResult_StateVariants verifies the poll backstop's
// success/failure gate on tr.State (bug I6): the wrapper reports a finished
// turn's terminal state as "complete" (see tatara-claude-code-wrapper
// internal/turn/turn.go's turn.Complete State = "complete"), not "completed".
// A turn stuck at "completed" never matches and the backstop never records
// the result, so the Task hangs until a clock fires.
func TestPollOnce_RecordsResult_StateVariants(t *testing.T) {
	tests := []struct {
		name       string
		state      string
		wantRecord bool
	}{
		{name: "complete state is recorded", state: "complete", wantRecord: true},
		{name: "failed state is recorded", state: "failed", wantRecord: true},
		{name: "in-progress state is not recorded", state: "running", wantRecord: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			taskName := "t-state-" + tt.state
			projName := "p-state-" + tt.state
			repoName := "r-state-" + tt.state
			turnID := "turn-state-" + tt.state
			mkTaskProject(t, projName, 3)
			mkTaskRepository(t, repoName, projName)
			mkTask(t, taskName, projName, repoName)
			annotate(t, taskName, map[string]string{
				annCurrentTurn:   turnID,
				annTurnStartedAt: time.Now().UTC().Format(time.RFC3339), // recent: not timed out
			})

			fs := newFakeSession()
			fs.getResult[turnID] = agent.TurnResult{State: tt.state}
			cb := newCallbackServer()
			cb.Session = fs
			cb.PollOnce(context.Background())

			tk := getTask(t, taskName)
			got := tk.Annotations[annTurnComplete] != ""
			if got != tt.wantRecord {
				t.Errorf("state=%q: turn-complete annotation set = %v, want %v", tt.state, got, tt.wantRecord)
			}
		})
	}
}

func TestTurnComplete_EmitsTaskTokens(t *testing.T) {
	mkTaskProject(t, "p-tok", 3)
	mkTaskRepository(t, "r-tok", "p-tok")
	mkTask(t, "t-tok", "p-tok", "r-tok")
	// Set a Kind + issue source so the emitted series carries real labels.
	tk := getTask(t, "t-tok")
	tk.Spec.Kind = "clarify"
	tk.Spec.Source = &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "szymonrychu/tatara-operator#7"}
	if err := k8sClient.Update(context.Background(), tk); err != nil {
		t.Fatalf("set kind/source: %v", err)
	}
	annotate(t, "t-tok", map[string]string{annCurrentTurn: "turn-emit-tok"})

	reg := prometheus.NewRegistry()
	cb := &CallbackServer{Client: k8sClient, Metrics: obs.NewOperatorMetrics(reg), Namespace: testNS}
	body, _ := json.Marshal(map[string]any{
		"turnId": "turn-emit-tok", "state": "completed",
		"finalText": "done", "stopReason": "end_turn",
		"usage": map[string]any{"input_tokens": 1200, "output_tokens": 300},
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", w.Code, w.Body.String())
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var input float64
	found := false
	for _, mf := range mfs {
		if mf.GetName() != "operator_task_tokens_total" {
			continue
		}
		for _, mc := range mf.GetMetric() {
			lbl := map[string]string{}
			for _, lp := range mc.GetLabel() {
				lbl[lp.GetName()] = lp.GetValue()
			}
			if lbl["kind"] == "clarify" && lbl["type"] == "input" && lbl["repo"] == "r-tok" {
				input = mc.GetCounter().GetValue()
				found = true
			}
		}
	}
	if !found {
		t.Fatal("operator_task_tokens_total{kind=clarify,type=input,repo=r-tok} not emitted")
	}
	if input != 1200 {
		t.Errorf("input tokens = %v, want 1200", input)
	}
}

func TestTurnComplete_LogsAgentInternalIssue(t *testing.T) {
	mkTaskProject(t, "p-ii", 3)
	mkTaskRepository(t, "r-ii", "p-ii")
	mkTask(t, "t-ii", "p-ii", "r-ii")
	annotate(t, "t-ii", map[string]string{annCurrentTurn: "turn-ii-1"})

	var buf bytes.Buffer
	ctx := captureLogger(&buf)

	cb := newCallbackServer()
	body, _ := json.Marshal(map[string]any{
		"turnId": "turn-ii-1", "state": "completed",
		"internalIssues": []map[string]string{
			{
				"category": "tool_error", "severity": "error",
				"description": "the tool blew up", "offending_tool": "Bash", "resource_id": "res-1",
			},
			{
				"category": "auth", "severity": "warn",
				"description": "auth flaked",
			},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body)).WithContext(ctx)
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", w.Code, w.Body.String())
	}

	out := buf.String()
	if !strings.Contains(out, `"action": "agent_internal_issue"`) {
		t.Fatalf("expected an agent_internal_issue log line, got: %s", out)
	}
	if !strings.Contains(out, `"category": "tool_error"`) || !strings.Contains(out, `"description": "the tool blew up"`) {
		t.Errorf("first issue fields missing from log output: %s", out)
	}
	if !strings.Contains(out, `"category": "auth"`) || !strings.Contains(out, `"description": "auth flaked"`) {
		t.Errorf("second issue fields missing from log output: %s", out)
	}
	if !strings.Contains(out, `"turn_id": "turn-ii-1"`) {
		t.Errorf("turn_id field missing from log output: %s", out)
	}
}

// captureLogger returns a context carrying a logger that writes into buf, so a
// test can assert on the operator's own structured log lines.
func captureLogger(buf *bytes.Buffer) context.Context {
	logger := zap.New(zap.WriteTo(buf), zap.UseDevMode(true))
	return log.IntoContext(context.Background(), logger)
}
