// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// ============================================================
// M4 Task 3 - MR body uses delivered scope
// ============================================================

// TestLifecycleImplement_ChangeSummarySetUsesPRTitleAndBody verifies that when
// Status.ChangeSummary is set the opened MR uses ChangeSummary.PRTitle as the
// title and a body that includes PRBody + "## Delivered" block + "Closes #N".
func TestLifecycleImplement_ChangeSummarySetUsesPRTitleAndBody(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-m4-cs-set"
	proj := "lc-m4-csp"
	repo := "lc-m4-csr"
	sec := "lc-m4-css"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#80",
		URL: "https://github.com/o/r/issues/80", Number: 80,
		IsPR: false,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.DeployState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.LifecycleIterations = 1
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{
		PRTitle:        "feat: new login flow",
		PRBody:         "Implements the redesigned login flow with OAuth2.",
		DeliveredScope: "OAuth2 login, session management",
		RemainingScope: "",
	}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{openPRURL: "https://github.com/o/r/pull/81"}
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

	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.openCalls) == 0 {
		t.Fatal("expected OpenChange to be called")
	}
	call := fw.openCalls[0]
	if call.title != "feat: new login flow" {
		t.Errorf("MR title = %q, want %q", call.title, "feat: new login flow")
	}
	if !strings.Contains(call.body, "Implements the redesigned login flow with OAuth2.") {
		t.Errorf("MR body missing PRBody, got: %q", call.body)
	}
	if !strings.Contains(call.body, "## Delivered") {
		t.Errorf("MR body missing '## Delivered' block, got: %q", call.body)
	}
	if !strings.Contains(call.body, "OAuth2 login, session management") {
		t.Errorf("MR body missing DeliveredScope, got: %q", call.body)
	}
	if !strings.Contains(call.body, "Closes #80") {
		t.Errorf("MR body missing 'Closes #80', got: %q", call.body)
	}
}

// TestLifecycleImplement_ChangeSummaryUnsetFallsBack verifies that when
// Status.ChangeSummary is nil the MR falls back to the M1 behavior
// (firstLine(Goal) as title, writeBackBody as body).
func TestLifecycleImplement_ChangeSummaryUnsetFallsBack(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-m4-cs-nil"
	proj := "lc-m4-cnp"
	repo := "lc-m4-cnr"
	sec := "lc-m4-cns"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#82",
		URL: "https://github.com/o/r/issues/82", Number: 82,
		IsPR: false,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	// Goal is a multi-line string; firstLine should use only the first line.
	task.Spec.Goal = "Fix the login bug\nMore detail here"
	if err := k8sClient.Update(context.Background(), task); err != nil {
		t.Fatalf("update task spec goal: %v", err)
	}
	task.Status.DeployState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.LifecycleIterations = 1
	// ChangeSummary is intentionally nil (not set)
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{openPRURL: "https://github.com/o/r/pull/83"}
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

	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.openCalls) == 0 {
		t.Fatal("expected OpenChange to be called")
	}
	call := fw.openCalls[0]
	// Derived fallback: no Source.Title, no strong ChangeSummary -> fix(<repo>): <firstLine(Goal)>
	want := "fix(lc-m4-cnr): Fix the login bug"
	if call.title != want {
		t.Errorf("MR title = %q, want %q (derived fallback)", call.title, want)
	}
	// M1 fallback: body must NOT contain "## Delivered"
	if strings.Contains(call.body, "## Delivered") {
		t.Errorf("MR body should not contain '## Delivered' when ChangeSummary is nil, got: %q", call.body)
	}
}

// ============================================================
// Request C - RemainingScope hard-fails instead of opening a follow-up issue
// ============================================================

// TestLifecycleImplement_RemainingScope_FailsIncomplete verifies that when
// ChangeSummary.RemainingScope is non-empty the Task hard-fails
// (Phase=Failed, reason=IncompleteImplementation) instead of opening a
// follow-up issue.
func TestLifecycleImplement_RemainingScope_FailsIncomplete(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-m4-rem"
	proj := "lc-m4-rp"
	repo := "lc-m4-rr"
	sec := "lc-m4-rs"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#90",
		URL: "https://github.com/o/r/issues/90", Number: 90,
		IsPR: false,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Spec.Goal = "Implement login system"
	if err := k8sClient.Update(context.Background(), task); err != nil {
		t.Fatalf("update task spec: %v", err)
	}
	task.Status.DeployState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.LifecycleIterations = 1
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{
		PRTitle:        "feat: login",
		PRBody:         "Adds login.",
		DeliveredScope: "login endpoint",
		RemainingScope: "logout endpoint, password reset",
	}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{openPRURL: "https://github.com/o/r/pull/91"}
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

	fw.mu.Lock()
	if len(fw.createIssues) != 0 {
		t.Errorf("CreateIssue called %d times; want 0 - no follow-up issues are ever filed", len(fw.createIssues))
	}
	fw.mu.Unlock()

	got := &tatarav1alpha1.Task{}
	if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); e != nil {
		t.Fatalf("get task after: %v", e)
	}
	if got.Status.Phase != "Failed" {
		t.Errorf("Phase = %q, want Failed", got.Status.Phase)
	}
	cond := findCond(got.Status.Conditions, "Ready")
	if cond == nil || cond.Reason != "IncompleteImplementation" {
		t.Fatalf("Ready condition reason = %v, want IncompleteImplementation", cond)
	}
}

// TestLifecycleImplement_RemainingScope_NeverOpensPROrAutoMerges verifies F1:
// the RemainingScope hard-fail must run BEFORE writeBackOpenChange, so an
// incomplete change never gets its PR opened, never gets the semver label
// stamped, and never has auto-merge enabled. Before the fix the hard-fail ran
// AFTER writeBackOpenChange had already opened the PR, labeled it, and turned
// on auto-merge - CI going green would then auto-merge the incomplete work
// despite the Task terminating Failed.
func TestLifecycleImplement_RemainingScope_NeverOpensPROrAutoMerges(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-m4-rem-noship"
	proj := "lc-m4-rns-p"
	repo := "lc-m4-rns-r"
	sec := "lc-m4-rns-s"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#95",
		URL: "https://github.com/o/r/issues/95", Number: 95,
		IsPR: false,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.DeployState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.LifecycleIterations = 1
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{
		PRTitle:        "feat: login",
		PRBody:         "Adds login.",
		DeliveredScope: "login endpoint",
		RemainingScope: "logout endpoint, password reset",
		Significance:   "minor", // set so a buggy ordering WOULD stamp the label + auto-merge
	}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{openPRURL: "https://github.com/o/r/pull/96"}
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

	fw.mu.Lock()
	if len(fw.openCalls) != 0 {
		t.Errorf("OpenChange called %d times; want 0 - no PR may open for an incomplete change", len(fw.openCalls))
	}
	if len(fw.labelCalls) != 0 {
		t.Errorf("AddLabel called %d times; want 0 - no semver label for an incomplete change", len(fw.labelCalls))
	}
	if fw.autoMergeCalls != 0 {
		t.Errorf("EnableAutoMerge called %d times; want 0 - auto-merge must never be enabled for an incomplete change", fw.autoMergeCalls)
	}
	fw.mu.Unlock()

	got := &tatarav1alpha1.Task{}
	if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); e != nil {
		t.Fatalf("get task after: %v", e)
	}
	if got.Status.Phase != "Failed" {
		t.Errorf("Phase = %q, want Failed", got.Status.Phase)
	}
	if got.Status.PrURL != "" {
		t.Errorf("PrURL = %q, want empty - no PR must ever be recorded for an incomplete change", got.Status.PrURL)
	}
}

// TestLifecycleImplement_EmptyRemainingScope_Succeeds verifies that when
// RemainingScope is empty the Task proceeds normally (no follow-up issue,
// no hard-fail; transitions to MRCI).
func TestLifecycleImplement_EmptyRemainingScope_Succeeds(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-m4-empty-rem"
	proj := "lc-m4-ep"
	repo := "lc-m4-er"
	sec := "lc-m4-es"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#110",
		URL: "https://github.com/o/r/issues/110", Number: 110,
		IsPR: false,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.DeployState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.LifecycleIterations = 1
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{
		PRTitle:        "feat: all done",
		PRBody:         "Complete.",
		DeliveredScope: "everything",
		RemainingScope: "", // empty - proceeds normally
	}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{openPRURL: "https://github.com/o/r/pull/111"}
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

	fw.mu.Lock()
	if len(fw.createIssues) != 0 {
		t.Errorf("CreateIssue called %d times; want 0", len(fw.createIssues))
	}
	fw.mu.Unlock()

	got := &tatarav1alpha1.Task{}
	if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); e != nil {
		t.Fatalf("get task after: %v", e)
	}
	if got.Status.Phase == "Failed" {
		t.Errorf("Phase = Failed, want non-Failed (empty RemainingScope must not hard-fail)")
	}
	if got.Status.DeployState != "MRCI" {
		t.Errorf("DeployState = %q, want MRCI", got.Status.DeployState)
	}
}
