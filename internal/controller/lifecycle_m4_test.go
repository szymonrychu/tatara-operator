// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	task.Status.LifecycleState = "Implement"
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
	task.Status.LifecycleState = "Implement"
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
	// M1 fallback: title must be first line of Goal only
	if call.title != "Fix the login bug" {
		t.Errorf("MR title = %q, want %q (M1 fallback)", call.title, "Fix the login bug")
	}
	// M1 fallback: body must NOT contain "## Delivered"
	if strings.Contains(call.body, "## Delivered") {
		t.Errorf("MR body should not contain '## Delivered' when ChangeSummary is nil, got: %q", call.body)
	}
}

// ============================================================
// M4 Task 4 - follow-up issue on remaining scope
// ============================================================

// TestLifecycleImplement_RemainingScope_OpensFollowupIssue verifies that when
// ChangeSummary.RemainingScope is non-empty a follow-up issue is created and
// the URL is appended to Status.DiscoveredIssues.
func TestLifecycleImplement_RemainingScope_OpensFollowupIssue(t *testing.T) {
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
	task.Status.LifecycleState = "Implement"
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

	fw := &lifecycleFakeSCMWriter{
		openPRURL:      "https://github.com/o/r/pull/91",
		createIssueURL: "https://github.com/o/r/issues/99",
	}
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
	if len(fw.createIssues) == 0 {
		t.Fatal("expected CreateIssue to be called for remaining scope")
	}
	issue := fw.createIssues[0]
	if !strings.Contains(issue.title, "Follow-up:") {
		t.Errorf("follow-up issue title = %q, want to contain 'Follow-up:'", issue.title)
	}
	if !strings.Contains(issue.title, "(remaining scope)") {
		t.Errorf("follow-up issue title = %q, want to contain '(remaining scope)'", issue.title)
	}
	if !strings.Contains(issue.body, "logout endpoint, password reset") {
		t.Errorf("follow-up issue body = %q, want remaining scope content", issue.body)
	}
	if !strings.Contains(issue.body, "https://github.com/o/r/pull/91") {
		t.Errorf("follow-up issue body = %q, want PR URL linked", issue.body)
	}

	// Verify DiscoveredIssues is populated.
	got := &tatarav1alpha1.Task{}
	if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); e != nil {
		t.Fatalf("get task after: %v", e)
	}
	found := false
	for _, u := range got.Status.DiscoveredIssues {
		if u == "https://github.com/o/r/issues/99" {
			found = true
		}
	}
	if !found {
		t.Errorf("follow-up issue URL not in DiscoveredIssues: %v", got.Status.DiscoveredIssues)
	}
}

// TestLifecycleImplement_RemainingScope_Idempotent verifies that a second
// reconcile with DiscoveredIssues already containing the follow-up URL does NOT
// open a second follow-up issue.
func TestLifecycleImplement_RemainingScope_Idempotent(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-m4-idem"
	proj := "lc-m4-ip"
	repo := "lc-m4-ir"
	sec := "lc-m4-is"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#100",
		URL: "https://github.com/o/r/issues/100", Number: 100,
		IsPR: false,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.LifecycleIterations = 1
	// PrURL already set simulates idempotent retry (PR already open).
	task.Status.PrURL = "https://github.com/o/r/pull/101"
	// DiscoveredIssues already contains the follow-up URL from a prior reconcile.
	task.Status.DiscoveredIssues = []string{"https://github.com/o/r/issues/102"}
	task.Status.FollowupIssueURL = "https://github.com/o/r/issues/102"
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{
		PRTitle:        "feat: x",
		PRBody:         "y",
		DeliveredScope: "part A",
		RemainingScope: "part B",
	}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{
		openPRURL:      "https://github.com/o/r/pull/101",
		createIssueURL: "https://github.com/o/r/issues/103",
	}
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
	if len(fw.createIssues) != 0 {
		t.Errorf("CreateIssue called %d times on idempotent retry; want 0", len(fw.createIssues))
	}
}

// TestLifecycleImplement_EmptyRemainingScope_NoFollowup verifies that when
// RemainingScope is empty no follow-up issue is opened.
func TestLifecycleImplement_EmptyRemainingScope_NoFollowup(t *testing.T) {
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
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.LifecycleIterations = 1
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{
		PRTitle:        "feat: all done",
		PRBody:         "Complete.",
		DeliveredScope: "everything",
		RemainingScope: "", // empty - no follow-up
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
	defer fw.mu.Unlock()
	if len(fw.createIssues) != 0 {
		t.Errorf("CreateIssue called %d times; want 0 for empty RemainingScope", len(fw.createIssues))
	}
}

// seedLifecycleTaskWithGoal creates a lifecycle task with custom Goal.
// This is a thin convenience wrapper around seedLifecycleTask.
func seedLifecycleTaskWithGoal(t *testing.T, name, proj, repo, sec, goal string, source *tatarav1alpha1.TaskSource) *tatarav1alpha1.Task {
	t.Helper()
	task := seedLifecycleTask(t, name, proj, repo, sec, source)
	task.Spec.Goal = goal
	if err := k8sClient.Update(context.Background(), task); err != nil {
		t.Fatalf("update goal: %v", err)
	}
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       task.Spec,
	}
}
