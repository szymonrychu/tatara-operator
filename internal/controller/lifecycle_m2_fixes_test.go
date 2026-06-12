package controller

// Tests for code-review findings on the issue-lifecycle M2 (FIX 1-4).
// Written RED-first per TDD: each test must fail before the fix is applied.

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// ============================================================
// FIX 1 - stale Conversation idle deadline leaks into MRCI
// ============================================================

// TestFinishImplement_ClearsDeadlineBeforeMRCI verifies that when a task
// travels discuss->Conversation (deadline set) then triggerLabel->Implement,
// running finishImplement clears DeadlineAt before transitioning to MRCI.
// Without the fix, MRCI inherits the near-expired idle deadline and parks
// the PR immediately via the deadline check in handleMRCI.
func TestFinishImplement_ClearsDeadlineBeforeMRCI(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-fi-cleardl"
	proj := "lc-fi-cleardl-proj"
	repo := "lc-fi-cleardl-repo"
	sec := "lc-fi-cleardl-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#50", URL: "https://github.com/o/r/issues/50",
		Number: 50,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	// Simulate: task went Conversation (deadline set near-future),
	// then triggerLabel jumped it to Implement. Implement run completed.
	conversationDeadline := metav1.NewTime(time.Now().Add(5 * time.Minute))
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.DeadlineAt = &conversationDeadline // stale conversation deadline
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{openPRURL: "https://github.com/o/r/pull/99"}
	r := newLifecycleReconciler(t, fw)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle (finishImplement): %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.LifecycleState != "MRCI" {
		t.Errorf("LifecycleState = %q, want MRCI", got.Status.LifecycleState)
	}
	// FIX: DeadlineAt must be nil when entering MRCI so ensureDeadline sets a fresh one.
	if got.Status.DeadlineAt != nil {
		t.Errorf("DeadlineAt = %v after finishImplement->MRCI, want nil (stale conversation deadline must be cleared)", got.Status.DeadlineAt)
	}
}

// TestMRCI_AfterConversationDeadlineCleared_GetsFreshDeadline verifies that
// after finishImplement clears the stale deadline, the subsequent MRCI entry
// via ensureDeadline sets a fresh far-future babysit deadline.
func TestMRCI_AfterConversationDeadlineCleared_GetsFreshDeadline(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	// Seed MRCI task with nil deadline (as finishImplement should leave it).
	r, _, name := seedMRCITask(t, "fresh-conv", scm.PRState{Author: "bot", CIStatus: "pending"}, 0)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle MRCI: %v", err)
	}

	got := fetchTask(t, name)
	// Must NOT park (deadline just set should be far in the future).
	if got.Status.LifecycleState == "Parked" {
		t.Error("MRCI must NOT park immediately when it just received a fresh deadline from ensureDeadline")
	}
	// ensureDeadline must have set a deadline.
	if got.Status.DeadlineAt == nil {
		t.Error("MRCI must have set DeadlineAt via ensureDeadline on first entry with nil deadline")
	}
	// Fresh deadline must be in the future (not near-expired).
	if got.Status.DeadlineAt != nil && !time.Now().Before(got.Status.DeadlineAt.Time) {
		t.Errorf("fresh MRCI deadline = %v is not in the future", got.Status.DeadlineAt.Time)
	}
}

// FIX 2 tests live in internal/webhook/issue_comment_create_test.go.

// FIX 3 tests live in internal/webhook/issue_comment_create_test.go.
// RetryOnConflict wrapping is structural and verified via code review;
// the fake client cannot inject conflicts.

// ============================================================
// FIX 4 - handleConversation nil-deadline safety
// ============================================================

// TestConversation_NilDeadline_SetsDeadlineAndRequeues verifies that a task
// in Conversation state with nil DeadlineAt does NOT transition to Stopped
// and does NOT loop forever. Instead it sets a deadline and requeues.
func TestConversation_NilDeadline_SetsDeadlineAndRequeues(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-conv-nildeadline"
	proj := "lc-cndl-proj"
	repo := "lc-cndl-repo"
	sec := "lc-cndl-sec"
	src := &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#22", Number: 22}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	// Explicitly nil DeadlineAt in Conversation (e.g. state set without deadline).
	task.Status.LifecycleState = "Conversation"
	task.Status.DeadlineAt = nil // intentionally nil
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	r := newLifecycleReconciler(t, nil)
	res, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle with nil DeadlineAt in Conversation: %v", err)
	}

	// Must requeue (not finish immediately).
	if res.RequeueAfter == 0 {
		t.Error("Conversation with nil DeadlineAt must requeue, not finish immediately")
	}

	got := fetchTask(t, name)
	// Must NOT have transitioned to Stopped (deadline not yet passed).
	if got.Status.LifecycleState == "Stopped" {
		t.Error("Conversation with nil DeadlineAt must not immediately transition to Stopped")
	}
	// FIX: DeadlineAt must be set now (the safety net populated it).
	if got.Status.DeadlineAt == nil {
		t.Error("Conversation with nil DeadlineAt must set a deadline after the first reconcile")
	}
	// The newly-set deadline must be in the future.
	if got.Status.DeadlineAt != nil && !time.Now().Before(got.Status.DeadlineAt.Time) {
		t.Errorf("newly set DeadlineAt = %v is not in the future", got.Status.DeadlineAt.Time)
	}
}
