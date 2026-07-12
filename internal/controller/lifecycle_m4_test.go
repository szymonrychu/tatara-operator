// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
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

// TestLifecycleImplement_RemainingScope_CommentsAndRemovesLabel is the m9
// regression: checkRemainingScopeHardFail used to post NO issue comment and
// leave the tatara-implementation label in place, unlike every other terminal
// path (parkWithComment / the codifiedTerminal declined path) - a human saw an
// implementation-labelled issue with no PR and no explanation. It must post an
// explanatory comment mentioning the declared remaining scope, and remove the
// implementation label (setLifecycleLabel's declined swap), matching those
// other terminal paths.
func TestLifecycleImplement_RemainingScope_CommentsAndRemovesLabel(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-m9-rem"
	proj := "lc-m9-rp"
	repo := "lc-m9-rr"
	sec := "lc-m9-rs"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#91",
		URL: "https://github.com/o/r/issues/91", Number: 91,
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

	fw := &lifecycleFakeSCMWriter{openPRURL: "https://github.com/o/r/pull/92"}
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
	if len(fw.commentCalls) != 1 {
		t.Fatalf("commentCalls = %d, want 1 (explanatory comment for the human)", len(fw.commentCalls))
	}
	if !strings.Contains(fw.commentCalls[0].body, "logout endpoint, password reset") {
		t.Errorf("comment body = %q, want it to mention the declared remaining scope", fw.commentCalls[0].body)
	}
	if fw.commentCalls[0].issueRef != "o/r#91" {
		t.Errorf("comment issueRef = %q, want o/r#91", fw.commentCalls[0].issueRef)
	}
	found := false
	for _, lb := range fw.removeLabelCalls {
		if lb == "tatara-implementation" {
			found = true
		}
	}
	if !found {
		t.Errorf("removeLabelCalls = %v, want tatara-implementation removed", fw.removeLabelCalls)
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
	// m9: checkRemainingScopeHardFail now swaps the phase label to
	// tatara-declined (labelCalls carries "tatara-declined"), a legitimate,
	// intentional label write distinct from the semver label this test
	// actually guards against - that one would come from applySemverAutoMerge
	// downstream of writeBackOpenChange, which never runs here.
	for _, lb := range fw.labelCalls {
		if strings.HasPrefix(lb, "semver:") {
			t.Errorf("AddLabel(%q) called; want no semver label for an incomplete change", lb)
		}
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

// TestLifecycleImplement_RemainingScope_WithOpenPR_DisarmsAutoMerge is the D1
// regression. Auto-merge is armed on the PR at OPEN time (applySemverAutoMerge)
// and was never disarmed, while the lifecycle re-enters Implement with that PR
// still open on four paths (mrci-failure, merge-conflict, mainci-failure,
// deploy-failure). So: turn 1 posts a clean change_summary -> PR opens, semver
// label stamped, native auto-merge ARMED -> CI red -> re-enter Implement ->
// turn 2 posts change_summary{remainingScope} -> the hard-fail terminated the
// Task Failed/IncompleteImplementation but LEFT the armed PR open, and the
// forge merged the incomplete change as soon as CI went green. The hard-fail
// must now disarm the already-open PR: disable auto-merge, strip the semver
// label, and close the PR.
func TestLifecycleImplement_RemainingScope_WithOpenPR_DisarmsAutoMerge(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-d1-rem-openpr"
	proj := "lc-d1-p"
	repo := "lc-d1-r"
	sec := "lc-d1-s"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#95",
		URL: "https://github.com/o/r/issues/95", Number: 95,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.DeployState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.LifecycleIterations = 2
	// Turn 1 already opened the PR and armed auto-merge on it.
	task.Status.PrURL = "https://github.com/o/r/pull/96"
	task.Status.PRNumber = 96
	// Turn 2's change_summary declares a gap.
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{
		PRTitle:        "feat: login",
		PRBody:         "Adds login.",
		DeliveredScope: "login endpoint",
		RemainingScope: "logout endpoint, password reset",
		Significance:   "minor",
	}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)

	cur := &tatarav1alpha1.Task{}
	if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, cur); e != nil {
		t.Fatalf("get task: %v", e)
	}
	if _, err := r.reconcileLifecycle(ctx, cur); err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	fw.mu.Lock()
	if fw.disableAutoMergeCalls != 1 {
		t.Errorf("DisableAutoMerge called %d times; want 1 - an armed PR must be disarmed when the change is declared incomplete", fw.disableAutoMergeCalls)
	}
	if len(fw.closePRCalls) != 1 || fw.closePRCalls[0].number != 96 {
		t.Errorf("ClosePR calls = %+v; want exactly one close of PR #96", fw.closePRCalls)
	}
	if !containsStr(fw.removeLabelCalls, "semver:minor") {
		t.Errorf("removeLabelCalls = %v; want the semver:minor label stripped from the incomplete PR", fw.removeLabelCalls)
	}
	fw.mu.Unlock()

	got := &tatarav1alpha1.Task{}
	if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); e != nil {
		t.Fatalf("get task after: %v", e)
	}
	if got.Status.Phase != "Failed" {
		t.Errorf("Phase = %q, want Failed", got.Status.Phase)
	}
	cond := apimeta.FindStatusCondition(got.Status.Conditions, "Ready")
	if cond == nil || cond.Reason != "IncompleteImplementation" {
		t.Errorf("Ready condition = %+v, want reason IncompleteImplementation", cond)
	}
}
