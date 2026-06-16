// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// seedDeclaredDeclineTask creates a lifecycle task in Implement/Succeeded state
// with ImplementOutcome set to a declared decline. The fake SCM writer returns
// 422 for OpenChange (no PR). The task has no prior ImplementEmptyRetries.
func seedDeclaredDeclineTask(t *testing.T, name, proj, repo, sec, reason string) (*tatarav1alpha1.Task, *noChangeRecordingSCMWriter) {
	t.Helper()
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#300",
		URL: "https://github.com/o/r/issues/300", Number: 300,
		IsPR: false,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.ImplementOutcome = &tatarav1alpha1.ImplementOutcome{
		Action: "declined",
		Reason: reason,
	}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	fw := &noChangeRecordingSCMWriter{}
	return task, fw
}

// reconcileDeclaredDeclineTask reconciles the named task using fw as the SCM writer.
func reconcileDeclaredDeclineTask(t *testing.T, name string, fw *noChangeRecordingSCMWriter) *tatarav1alpha1.Task {
	t.Helper()
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r := newLifecycleReconciler(t, nil)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	_, err := r.reconcileLifecycle(ctx, func() *tatarav1alpha1.Task {
		tk := &tatarav1alpha1.Task{}
		if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); e != nil {
			t.Fatalf("get task: %v", e)
		}
		return tk
	}())
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}
	return got
}

// TestFinishImplement_DeclaredDecline_Parks verifies the codified refusal path:
// no PR + ImplementOutcome.Action=="declined" -> Parked reason "refused",
// reason posted as issue comment, declined label applied, ImplementOutcome cleared,
// ImplementEmptyRetries reset.
func TestFinishImplement_DeclaredDecline_Parks(t *testing.T) {
	name := "lc-refusal-parks"
	reason := "this feature is already implemented in PR #123"
	_, fw := seedDeclaredDeclineTask(t, name, "lc-rp-proj", "lc-rp-repo", "lc-rp-sec", reason)

	got := reconcileDeclaredDeclineTask(t, name, fw)

	if got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked (refused)", got.Status.LifecycleState)
	}
	if got.Status.ImplementOutcome != nil {
		t.Errorf("ImplementOutcome should be nil after consuming; got %+v", got.Status.ImplementOutcome)
	}
	if got.Status.ImplementEmptyRetries != 0 {
		t.Errorf("ImplementEmptyRetries = %d, want 0 (reset on codified refusal)", got.Status.ImplementEmptyRetries)
	}

	// Reason must be posted as an issue comment.
	bodies := fw.commentBodies("o/r#300")
	if len(bodies) == 0 {
		t.Fatal("expected a comment to be posted with the refusal reason; got none")
	}
	combined := strings.Join(bodies, " ")
	if !strings.Contains(combined, reason) {
		t.Errorf("comment body %q does not contain the refusal reason %q", combined, reason)
	}
}

// TestFinishImplement_DeclaredDecline_ResetsRetries verifies ImplementEmptyRetries
// is reset even when retries > 0 before the decline.
func TestFinishImplement_DeclaredDecline_ResetsRetries(t *testing.T) {
	name := "lc-refusal-reset-retries"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#301",
		URL: "https://github.com/o/r/issues/301", Number: 301,
	}
	task := seedLifecycleTask(t, name, "lc-rrr-proj", "lc-rrr-repo", "lc-rrr-sec", src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.ImplementEmptyRetries = 1 // was already retried once silently
	task.Status.ImplementOutcome = &tatarav1alpha1.ImplementOutcome{
		Action: "declined",
		Reason: "not feasible: would break public API",
	}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	fw := &noChangeRecordingSCMWriter{}

	got := reconcileDeclaredDeclineTask(t, name, fw)

	if got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked", got.Status.LifecycleState)
	}
	if got.Status.ImplementEmptyRetries != 0 {
		t.Errorf("ImplementEmptyRetries = %d, want 0", got.Status.ImplementEmptyRetries)
	}
}

// TestFinishImplement_NoDecline_FirstRetryWithPrompt verifies the no-decline
// re-prompt path: no PR + no ImplementOutcome -> increment counter + set
// ImplementContext containing decline instruction, re-enter Implement.
func TestFinishImplement_NoDecline_FirstRetryWithPrompt(t *testing.T) {
	name := "lc-refusal-nod-retry1"
	_, fw := seedEmptyImplementTask(t, name, "lc-ndr1-proj", "lc-ndr1-repo", "lc-ndr1-sec", 0)

	got := reconcileEmptyImplementTask(t, name, fw)

	if got.Status.LifecycleState != "Implement" {
		t.Errorf("LifecycleState = %q, want Implement (should retry)", got.Status.LifecycleState)
	}
	if got.Status.ImplementEmptyRetries != 1 {
		t.Errorf("ImplementEmptyRetries = %d, want 1", got.Status.ImplementEmptyRetries)
	}
	// Re-entry prompt must mention decline_implementation.
	if !strings.Contains(got.Status.ImplementContext, "decline_implementation") {
		t.Errorf("ImplementContext = %q; want it to mention 'decline_implementation'", got.Status.ImplementContext)
	}
}

// TestFinishImplement_NoDecline_ParksAtCapWithComment verifies that at the retry
// cap with no ImplementOutcome the task parks with reason "refused-no-explanation"
// and posts a comment.
func TestFinishImplement_NoDecline_ParksAtCapWithComment(t *testing.T) {
	name := "lc-refusal-nod-cap"
	_, fw := seedEmptyImplementTask(t, name, "lc-ndc-proj", "lc-ndc-repo", "lc-ndc-sec", 2)

	got := reconcileEmptyImplementTask(t, name, fw)

	if got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked (at cap, no explanation)", got.Status.LifecycleState)
	}

	// A comment must be posted noting the agent did not explain.
	bodies := fw.commentBodies("o/r#200")
	if len(bodies) == 0 {
		t.Fatal("expected a comment at cap with no explanation; got none")
	}
}

// TestFinishImplement_PROpened_IgnoresImplementOutcome verifies the PR-opened path
// is unchanged by the new refusal logic (ImplementOutcome ignored when PR exists).
func TestFinishImplement_PROpened_IgnoresImplementOutcome(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-refusal-pr-opened"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#302",
		URL: "https://github.com/o/r/issues/302", Number: 302,
	}
	task := seedLifecycleTask(t, name, "lc-rpo-proj", "lc-rpo-repo", "lc-rpo-sec", src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	// ImplementOutcome set but a PR was also opened - PR wins.
	task.Status.ImplementOutcome = &tatarav1alpha1.ImplementOutcome{
		Action: "declined",
		Reason: "should not be used",
	}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed status: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{openPRURL: "https://github.com/o/r/pull/303"}
	r := newLifecycleReconciler(t, fw)

	_, err := r.reconcileLifecycle(ctx, func() *tatarav1alpha1.Task {
		tk := &tatarav1alpha1.Task{}
		if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); e != nil {
			t.Fatalf("get task: %v", e)
		}
		return tk
	}())
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}

	// Should have moved to MRCI, not Parked.
	if got.Status.LifecycleState != "MRCI" {
		t.Errorf("LifecycleState = %q, want MRCI (PR opened overrides refusal)", got.Status.LifecycleState)
	}
	if got.Status.PrURL == "" {
		t.Error("PrURL should be set when PR was opened")
	}
}

// TestSetLifecycleState_EntersImplement_ClearsImplementOutcome verifies that a
// triage-initiated Implement entry clears ImplementOutcome (fresh triage-implement
// transition). CI-failure re-entries do NOT clear it so a stale refusal is not
// silently discarded (finding 8).
func TestSetLifecycleState_EntersImplement_ClearsImplementOutcome(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-refusal-entry-clear"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#303",
		URL: "https://github.com/o/r/issues/303", Number: 303,
	}
	task := seedLifecycleTask(t, name, "lc-rec-proj", "lc-rec-repo", "lc-rec-sec", src)
	task.Status.LifecycleState = "Triage"
	task.Status.ImplementOutcome = &tatarav1alpha1.ImplementOutcome{
		Action: "declined",
		Reason: "stale from prior run",
	}
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	r := newLifecycleReconciler(t, nil)
	// Use "triage-implement" reason: this is a fresh triage-initiated entry and must
	// clear ImplementOutcome. CI-failure re-entries ("mrci-failure", "mainci-failure")
	// must NOT clear it.
	if err := r.setLifecycleState(ctx, task, "Implement", "triage-implement"); err != nil {
		t.Fatalf("setLifecycleState: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.ImplementOutcome != nil {
		t.Errorf("ImplementOutcome should be nil after triage-implement entry; got %+v", got.Status.ImplementOutcome)
	}
}

// TestSetLifecycleState_CIFailureReentry_PreservesImplementOutcome verifies that
// CI-failure re-entries do NOT clear ImplementOutcome (finding 8: stale refusal
// must not be discarded silently on a re-implement).
func TestSetLifecycleState_CIFailureReentry_PreservesImplementOutcome(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-refusal-ci-reentry"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#304",
		URL: "https://github.com/o/r/issues/304", Number: 304,
	}
	task := seedLifecycleTask(t, name, "lc-rci-proj", "lc-rci-repo", "lc-rci-sec", src)
	task.Status.LifecycleState = "MRCI"
	task.Status.ImplementOutcome = &tatarav1alpha1.ImplementOutcome{
		Action: "declined",
		Reason: "stale refusal from a failed clearImplementOutcome",
	}
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	r := newLifecycleReconciler(t, nil)
	if err := r.setLifecycleState(ctx, task, "Implement", "mrci-failure"); err != nil {
		t.Fatalf("setLifecycleState: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.ImplementOutcome == nil {
		t.Error("ImplementOutcome must NOT be cleared on CI-failure re-entry (finding 8)")
	}
}
