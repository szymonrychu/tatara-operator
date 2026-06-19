// Tests for audit-r3 findings in turncallback.go.
package controller

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// --- Finding 2: duplicate callback double-counts CumulativeTokens ---

// TestRecordUsage_SameTurnDuplicateDoesNotDoubleCount verifies that two
// callbacks for the same in-flight turnID (e.g. wrapper retry before the
// reconcile advances annCurrentTurn) do not double-count CumulativeTokens.
// Before the fix, annCurrentTurn stayed equal to turnID between the first
// callback and the reconcile, so both callbacks passed the guard and
// CumulativeTokens was incremented twice.
func TestRecordUsage_SameTurnDuplicateDoesNotDoubleCount(t *testing.T) {
	mkTaskProject(t, "p-dup1", 3)
	mkTaskRepository(t, "r-dup1", "p-dup1")
	mkTask(t, "t-dup1", "p-dup1", "r-dup1")
	setTaskCumulativeTokens(t, "t-dup1", 0)
	annotate(t, "t-dup1", map[string]string{annCurrentTurn: "turn-dup1"})

	cb := newCallbackServer()

	// First callback - lands normally via handleTurnComplete (stamps annTurnComplete).
	rawUsage := json.RawMessage(`{"output_tokens":50}`)
	body, _ := json.Marshal(map[string]any{
		"turnId":    "turn-dup1",
		"state":     "completed",
		"finalText": "first",
		"usage":     rawUsage,
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("first callback status = %d, want 204; body=%s", w.Code, w.Body.String())
	}
	// annTurnComplete is now set but annCurrentTurn still equals "turn-dup1"
	// (reconcile hasn't run yet). A second callback must be a no-op for usage.
	tk := getTask(t, "t-dup1")
	if tk.Annotations[annTurnComplete] == "" {
		t.Fatal("annTurnComplete must be set after first callback")
	}

	// Second callback (wrapper retry) - same turnId, should not increment tokens again.
	body2, _ := json.Marshal(map[string]any{
		"turnId":    "turn-dup1",
		"state":     "completed",
		"finalText": "duplicate",
		"usage":     rawUsage,
	})
	req2 := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body2))
	w2 := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w2, req2)
	// Duplicate callback may succeed (204) or get 404 (stale) - both OK.
	// The key invariant is CumulativeTokens stays at 50, not 100.

	tk2 := getTask(t, "t-dup1")
	if tk2.Status.CumulativeTokens != 50 {
		t.Errorf("CumulativeTokens = %d after duplicate callback, want 50 (no double-count)", tk2.Status.CumulativeTokens)
	}
}

// --- Finding 3: turnTimedOut free function deduplication ---

// TestTurnTimedOut_ExpiredReturnsTrue verifies that the free function
// turnTimedOut returns true when started > timeout+grace ago and there is no
// later activity.
func TestTurnTimedOut_ExpiredReturnsTrue(t *testing.T) {
	startedAt := time.Now().Add(-2 * time.Hour)
	if !turnTimedOut(startedAt.Format(time.RFC3339), "", 1800) {
		t.Error("turnTimedOut must return true for a turn started 2h ago with 1800s timeout")
	}
}

// TestTurnTimedOut_RecentReturnsFalse verifies that turnTimedOut returns false
// for a turn started less than timeout+grace ago.
func TestTurnTimedOut_RecentReturnsFalse(t *testing.T) {
	startedAt := time.Now().Add(-10 * time.Second)
	if turnTimedOut(startedAt.Format(time.RFC3339), "", 1800) {
		t.Error("turnTimedOut must return false for a turn started 10s ago with 1800s timeout")
	}
}

// TestTurnTimedOut_EmptyStartedAtReturnsFalse verifies safe default when the
// annotation is missing.
func TestTurnTimedOut_EmptyStartedAtReturnsFalse(t *testing.T) {
	if turnTimedOut("", "", 1800) {
		t.Error("turnTimedOut must return false (safe default) for empty startedAt")
	}
}

// TestTurnTimedOut_BadFormatReturnsFalse verifies safe default for unparseable
// annotation values.
func TestTurnTimedOut_BadFormatReturnsFalse(t *testing.T) {
	if turnTimedOut("not-a-timestamp", "", 1800) {
		t.Error("turnTimedOut must return false (safe default) for unparseable startedAt")
	}
}

// TestTurnTimedOut_RecentActivityKeepsAlive verifies the stall semantics: a turn
// started well past the window stays alive while recent activity keeps arriving.
func TestTurnTimedOut_RecentActivityKeepsAlive(t *testing.T) {
	startedAt := time.Now().Add(-2 * time.Hour)
	lastActivity := time.Now().Add(-5 * time.Second)
	if turnTimedOut(startedAt.Format(time.RFC3339), lastActivity.Format(time.RFC3339), 1800) {
		t.Error("turnTimedOut must return false when last activity is recent, even if started long ago")
	}
}

// TestTurnTimedOut_StaleActivityExpires verifies a turn fails once activity has
// been silent past the window. The anchor is max(started, activity), so a recent
// start with stale activity does NOT fire, but once both are stale it does.
func TestTurnTimedOut_StaleActivityExpires(t *testing.T) {
	recentStart := time.Now().Add(-10 * time.Second).Format(time.RFC3339)
	stale := time.Now().Add(-2 * time.Hour).Format(time.RFC3339)
	if turnTimedOut(recentStart, stale, 1800) {
		t.Error("turnTimedOut must return false: anchor is max(started, activity) = recent start")
	}
	if !turnTimedOut(stale, stale, 1800) {
		t.Error("turnTimedOut must return true when both start and last activity are past the window")
	}
}

// TestTurnTimedOut_FallsBackToStartedAt verifies that an empty/unparseable
// last-activity value falls back to the start anchor so the bound is never lost.
func TestTurnTimedOut_FallsBackToStartedAt(t *testing.T) {
	startedAt := time.Now().Add(-2 * time.Hour).Format(time.RFC3339)
	if !turnTimedOut(startedAt, "", 1800) {
		t.Error("empty last-activity must fall back to startedAt (expired -> true)")
	}
	if !turnTimedOut(startedAt, "not-a-timestamp", 1800) {
		t.Error("unparseable last-activity must fall back to startedAt (expired -> true)")
	}
}

// TestSetDeadlineMinutes_SetsAndResets verifies that setDeadlineMinutes
// replaces an existing deadline in a single RetryOnConflict (not two writes).
func TestSetDeadlineMinutes_SetsAndResets(t *testing.T) {
	mkTaskProject(t, "p-sdm1", 3)
	mkTaskRepository(t, "r-sdm1", "p-sdm1")
	mkTask(t, "t-sdm1", "p-sdm1", "r-sdm1")

	r := newTaskReconciler(nil)

	// Set initial deadline.
	tk := getTask(t, "t-sdm1")
	if err := r.setDeadlineMinutes(context.Background(), tk, 60); err != nil {
		t.Fatalf("setDeadlineMinutes set: %v", err)
	}
	tk1 := getTask(t, "t-sdm1")
	if tk1.Status.DeadlineAt == nil {
		t.Fatal("DeadlineAt must be set after setDeadlineMinutes")
	}
	first := *tk1.Status.DeadlineAt

	// Override with a shorter deadline - setDeadlineMinutes must replace it.
	if err := r.setDeadlineMinutes(context.Background(), tk1, 5); err != nil {
		t.Fatalf("setDeadlineMinutes reset: %v", err)
	}
	tk2 := getTask(t, "t-sdm1")
	if tk2.Status.DeadlineAt == nil {
		t.Fatal("DeadlineAt must still be set after reset")
	}
	if !tk2.Status.DeadlineAt.Time.Before(first.Time) {
		t.Errorf("reset deadline %v must be before first deadline %v", tk2.Status.DeadlineAt.Time, first.Time)
	}
}

// --- Finding 4: per-GetTurn context deadline in PollOnce ---

// TestPollOnce_GetTurnContextDeadline verifies that PollOnce passes a bounded
// context to GetTurn so a slow/unreachable wrapper cannot stall the whole cycle.
func TestPollOnce_GetTurnContextDeadline(t *testing.T) {
	mkTaskProject(t, "p-ctxdl1", 3)
	mkTaskRepository(t, "r-ctxdl1", "p-ctxdl1")
	mkTask(t, "t-ctxdl1", "p-ctxdl1", "r-ctxdl1")
	setTaskPhase(t, "t-ctxdl1", "Running")
	startedAt := time.Now().Add(-30 * time.Second) // not yet timed out
	annotate(t, "t-ctxdl1", map[string]string{
		annCurrentTurn:   "turn-ctxdl1",
		annTurnStartedAt: startedAt.UTC().Format(time.RFC3339),
	})

	session := &deadlineCapturingSession{}
	cb := &CallbackServer{
		Client:    k8sClient,
		Namespace: testNS,
		Session:   session,
	}
	cb.PollOnce(context.Background())

	session.mu.Lock()
	dl := session.deadline
	ok := session.deadlineOk
	session.mu.Unlock()

	if !ok {
		t.Error("PollOnce must pass a context with a deadline to GetTurn")
	}
	// The deadline should be within a reasonable future window (not unbounded).
	maxExpectedDeadline := time.Now().Add(30 * time.Second)
	if dl.After(maxExpectedDeadline) {
		t.Errorf("GetTurn context deadline %v is too far in the future (> 30s); PollOnce should use a short per-call timeout", dl)
	}
}

// --- Finding 1: HMAC authn on /internal/turn-complete ---

// TestHandleTurnComplete_RejectsRequestWithoutSecret verifies that when a
// CallbackSecret is configured, an unsigned POST is rejected with 401.
func TestHandleTurnComplete_RejectsRequestWithoutSecret(t *testing.T) {
	mkTaskProject(t, "p-hmac1", 3)
	mkTaskRepository(t, "r-hmac1", "p-hmac1")
	mkTask(t, "t-hmac1", "p-hmac1", "r-hmac1")
	annotate(t, "t-hmac1", map[string]string{annCurrentTurn: "turn-hmac1"})

	cb := &CallbackServer{
		Client:         k8sClient,
		Namespace:      testNS,
		CallbackSecret: "test-secret-abc",
	}
	body, _ := json.Marshal(map[string]any{
		"turnId": "turn-hmac1",
		"state":  "completed",
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	// No X-Tatara-Signature header.
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 (no auth header when secret configured)", w.Code)
	}
}

// TestHandleTurnComplete_RejectsRequestWithWrongSecret verifies that a request
// with a wrong HMAC signature is rejected with 401.
func TestHandleTurnComplete_RejectsRequestWithWrongSecret(t *testing.T) {
	mkTaskProject(t, "p-hmac2", 3)
	mkTaskRepository(t, "r-hmac2", "p-hmac2")
	mkTask(t, "t-hmac2", "p-hmac2", "r-hmac2")
	annotate(t, "t-hmac2", map[string]string{annCurrentTurn: "turn-hmac2"})

	cb := &CallbackServer{
		Client:         k8sClient,
		Namespace:      testNS,
		CallbackSecret: "correct-secret",
	}
	body, _ := json.Marshal(map[string]any{
		"turnId": "turn-hmac2",
		"state":  "completed",
	})
	// Sign with the wrong secret.
	mac := hmac.New(sha256.New, []byte("wrong-secret"))
	mac.Write(body)
	sig := hex.EncodeToString(mac.Sum(nil))

	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	req.Header.Set("X-Tatara-Signature", "sha256="+sig)
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 (wrong HMAC signature)", w.Code)
	}
}

// TestHandleTurnComplete_AcceptsRequestWithCorrectSecret verifies that a
// correctly-signed POST succeeds end-to-end.
func TestHandleTurnComplete_AcceptsRequestWithCorrectSecret(t *testing.T) {
	mkTaskProject(t, "p-hmac3", 3)
	mkTaskRepository(t, "r-hmac3", "p-hmac3")
	mkTask(t, "t-hmac3", "p-hmac3", "r-hmac3")
	annotate(t, "t-hmac3", map[string]string{annCurrentTurn: "turn-hmac3"})

	secret := "correct-secret-xyz"
	cb := &CallbackServer{
		Client:         k8sClient,
		Namespace:      testNS,
		CallbackSecret: secret,
	}
	body, _ := json.Marshal(map[string]any{
		"turnId": "turn-hmac3",
		"state":  "completed",
	})
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	sig := hex.EncodeToString(mac.Sum(nil))

	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	req.Header.Set("X-Tatara-Signature", "sha256="+sig)
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204 (correct HMAC signature); body=%s", w.Code, w.Body.String())
	}
}

// TestHandleTurnComplete_NoSecretConfigured_NoAuthRequired verifies that when
// CallbackSecret is empty (not configured), the endpoint behaves as before -
// no auth check, backward-compatible with existing deployments.
func TestHandleTurnComplete_NoSecretConfigured_NoAuthRequired(t *testing.T) {
	mkTaskProject(t, "p-hmac4", 3)
	mkTaskRepository(t, "r-hmac4", "p-hmac4")
	mkTask(t, "t-hmac4", "p-hmac4", "r-hmac4")
	annotate(t, "t-hmac4", map[string]string{annCurrentTurn: "turn-hmac4"})

	cb := &CallbackServer{
		Client:    k8sClient,
		Namespace: testNS,
		// CallbackSecret intentionally empty = backward-compatible, no auth.
	}
	body, _ := json.Marshal(map[string]any{
		"turnId": "turn-hmac4",
		"state":  "completed",
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/turn-complete", bytes.NewReader(body))
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204 (no secret configured = no auth required); body=%s", w.Code, w.Body.String())
	}
}

// --- helpers ---

// deadlineCapturingSession is a fake agent.Session that captures the context
// deadline passed to GetTurn so tests can verify per-call timeouts.
type deadlineCapturingSession struct {
	mu         sync.Mutex
	deadline   time.Time
	deadlineOk bool
}

func (d *deadlineCapturingSession) GetTurn(ctx context.Context, _ string, _ string) (agent.TurnResult, error) {
	dl, ok := ctx.Deadline()
	d.mu.Lock()
	d.deadline = dl
	d.deadlineOk = ok
	d.mu.Unlock()
	return agent.TurnResult{}, nil
}

func (d *deadlineCapturingSession) SubmitTurn(_ context.Context, _, _, _ string) (string, error) {
	return "", nil
}

func (d *deadlineCapturingSession) Interject(_ context.Context, _, _ string) error {
	return nil
}

func (d *deadlineCapturingSession) DeleteSession(_ context.Context, _ string) error {
	return nil
}
