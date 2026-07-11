package controller

import (
	"context"
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// ----- Task 6: Implement state handler -----

func TestLifecycleImplement_SucceededOpensMRAndEntersMRCI(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-impl-ok"
	proj := "lc-ip-ok"
	repo := "lc-ir-ok"
	sec := "lc-is-ok"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#10", URL: "https://github.com/o/r/issues/10",
		Number: 10,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	// Seed: Implement agent run completed successfully.
	// LifecycleIterations=1: spawn already incremented it when Phase was "".
	task.Status.DeployState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.LifecycleIterations = 1
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed implement succeeded: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{openPRURL: "https://github.com/o/r/pull/42"}
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
		t.Fatal("OpenChange must be called for Implement succeeded")
	}
	wantBranch := taskBranch(&tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS}, Spec: tatarav1alpha1.TaskSpec{Kind: "issueLifecycle", Source: src}})
	if fw.openCalls[0].sourceBranch != wantBranch {
		t.Errorf("OpenChange sourceBranch = %q, want %q", fw.openCalls[0].sourceBranch, wantBranch)
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}
	if got.Status.DeployState != "MRCI" {
		t.Errorf("DeployState = %q, want MRCI", got.Status.DeployState)
	}
	if got.Status.PrURL != "https://github.com/o/r/pull/42" {
		t.Errorf("PrURL = %q, want https://github.com/o/r/pull/42", got.Status.PrURL)
	}
	if got.Status.PRNumber != 42 {
		t.Errorf("PRNumber = %d, want 42", got.Status.PRNumber)
	}
	if got.Status.HeadBranch == "" {
		t.Error("HeadBranch must be set")
	}
	if got.Status.LifecycleIterations != 1 {
		t.Errorf("LifecycleIterations = %d, want 1", got.Status.LifecycleIterations)
	}
}

// noChangeSCMWriter returns 422 for OpenChange, simulating no-diff / branch absent.
type noChangeSCMWriter struct{ scm.SCMWriter }

func (n *noChangeSCMWriter) OpenChange(_ context.Context, _, _, _, _, _, _ string) (string, error) {
	return "", &scm.HTTPError{Status: 422, Body: "no diff", Path: "/pulls"}
}

func (n *noChangeSCMWriter) Comment(_ context.Context, _, _, _ string) error { return nil }

// TestLifecycleImplement_NoPRFirstEmptyRetries verifies that a first empty run
// (no PR, counter==0) triggers a retry rather than immediately parking.
// Parking now only happens after the retry cap (2) is exhausted.
func TestLifecycleImplement_NoPRFirstEmptyRetries(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-impl-nopr"
	proj := "lc-ip-nopr"
	repo := "lc-ir-nopr"
	sec := "lc-is-nopr"
	src := &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#11", Number: 11}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.DeployState = "Implement"
	task.Status.Phase = "Succeeded"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)
	// Override SCMFor to return a writer that returns 422.
	r.SCMFor = func(string) (scm.SCMWriter, error) {
		return &noChangeSCMWriter{}, nil
	}

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
	// First empty run retries (counter 0 -> 1), stays Implement.
	if got.Status.DeployState != "Implement" {
		t.Errorf("DeployState = %q, want Implement (first empty run should retry, not park)", got.Status.DeployState)
	}
	if got.Status.ImplementEmptyRetries != 1 {
		t.Errorf("ImplementEmptyRetries = %d, want 1", got.Status.ImplementEmptyRetries)
	}
}

func TestLifecycleImplement_FailedTransitionsToParked(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-impl-fail"
	proj := "lc-ip-fail"
	repo := "lc-ir-fail"
	sec := "lc-is-fail"
	src := &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#12", Number: 12}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.DeployState = "Implement"
	task.Status.Phase = "Failed"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	r := newLifecycleReconciler(t, nil)
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
	if got.Status.DeployState != "Parked" {
		t.Errorf("DeployState = %q, want Parked (implement-failed)", got.Status.DeployState)
	}
}

// ----- Task 2: Implement re-entry context prompt + field clear -----

// TestLifecycleImplementPlanText_PlainWhenContextEmpty verifies that when
// ImplementContext is empty the prompt contains the base planTurnText and the
// hard decline_implementation instruction but no re-entry block.
func TestLifecycleImplementPlanText_PlainWhenContextEmpty(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-plain", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "proj", RepositoryRef: "repo",
			Goal: "fix the bug", Kind: "issueLifecycle",
		},
		Status: tatarav1alpha1.TaskStatus{ImplementContext: ""},
	}
	got := implementPrompt(task)
	base := planTurnText(task.Spec.Goal, taskBranch(task), task.Spec.ProjectRef, task.Name)
	if !strings.Contains(got, base) {
		t.Errorf("implementPrompt must contain base planTurnText; got: %q", got)
	}
	if !strings.Contains(got, "decline_implementation") {
		t.Errorf("implementPrompt must contain decline_implementation instruction; got: %q", got)
	}
	if strings.Contains(got, "## Re-entry context") {
		t.Errorf("implementPrompt with empty context must not contain re-entry block; got: %q", got)
	}
}

// TestLifecycleImplementPlanText_IncludesContextBlockWhenSet verifies that
// when ImplementContext is non-empty the prompt contains both the base plan
// text and a "## Re-entry context" block with the context value.
func TestLifecycleImplementPlanText_IncludesContextBlockWhenSet(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-reentry", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "proj", RepositoryRef: "repo",
			Goal: "fix the bug", Kind: "issueLifecycle",
		},
		Status: tatarav1alpha1.TaskStatus{ImplementContext: "CI failed: test_login timed out"},
	}
	got := implementPrompt(task)
	if !strings.Contains(got, planTurnText(task.Spec.Goal, taskBranch(task), task.Spec.ProjectRef, task.Name)) {
		t.Error("implementPrompt with context must include the base plan text")
	}
	if !strings.Contains(got, "## Re-entry context") {
		t.Errorf("implementPrompt with context must contain '## Re-entry context'; got: %q", got)
	}
	if !strings.Contains(got, "CI failed: test_login timed out") {
		t.Errorf("implementPrompt with context must contain the context detail; got: %q", got)
	}
}

// TestLifecycleImplement_ContextClearedAfterRunStarts verifies that ImplementContext
// is preserved through the spawn reconcile (Phase="" -> Planning) and is only
// cleared in finishImplement once the run completes. The old assertion (cleared on
// spawn) tested the bug; this replacement tests the correct behaviour.
func TestLifecycleImplement_ContextClearedAfterRunStarts(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-impl-ctx-clear"
	proj := "lc-icc-proj"
	repo := "lc-icc-repo"
	sec := "lc-icc-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#30", URL: "https://github.com/o/r/issues/30",
		Number: 30,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	// State: ready to spawn a fresh implement run; ImplementContext is set (re-entry).
	task.Status.DeployState = "Implement"
	task.Status.Phase = ""
	task.Status.ImplementContext = "CI failed: test_auth timed out"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed implement re-entry: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)

	// Spawn reconcile: pod created, Phase -> Planning.
	_, err := r.reconcileLifecycle(ctx, func() *tatarav1alpha1.Task {
		tk := &tatarav1alpha1.Task{}
		if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); e != nil {
			t.Fatalf("get task: %v", e)
		}
		return tk
	}())
	if err != nil {
		t.Fatalf("reconcileLifecycle (spawn): %v", err)
	}

	// After spawn, ImplementContext must still be set (not cleared yet).
	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after spawn: %v", err)
	}
	if got.Status.ImplementContext == "" {
		t.Errorf("ImplementContext cleared on spawn (Phase=Planning); must persist until finishImplement")
	}
	if got.Status.Phase != "Planning" {
		t.Errorf("Phase = %q, want Planning after spawn", got.Status.Phase)
	}
}

// TestLifecycleImplement_ContextInPromptWhenPodReady verifies that when a
// task with ImplementContext set reaches the driveTurns step (pod ready, no
// current turn), the submitted turn text contains the re-entry context block.
func TestLifecycleImplement_ContextInPromptWhenPodReady(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-impl-ctx-prompt"
	proj := "lc-icp-proj"
	repo := "lc-icp-repo"
	sec := "lc-icp-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#31", URL: "https://github.com/o/r/issues/31",
		Number: 31,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	// State: pod exists and is ready, Phase=Planning, ImplementContext set.
	task.Status.DeployState = "Implement"
	task.Status.Phase = "Planning"
	task.Status.PodName = agent.PodName(task)
	task.Status.ImplementContext = "CI failed: build timed out"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Create a ready pod so podReady returns true.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      agent.PodName(task),
			Namespace: testNS,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "wrapper", Image: "wrapper:1"}},
		},
	}
	if err := k8sClient.Create(ctx, pod); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	pod.Status.Conditions = []corev1.PodCondition{{
		Type: corev1.PodReady, Status: corev1.ConditionTrue,
	}}
	if err := k8sClient.Status().Update(ctx, pod); err != nil {
		t.Fatalf("set pod ready: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	sess := newFakeSession()
	r := newLifecycleReconciler(t, fw)
	r.Session = sess

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

	sub, ok := sess.lastSubmit()
	if !ok {
		t.Fatal("expected a SubmitTurn call; none recorded")
	}
	if !strings.Contains(sub.Text, "## Re-entry context") {
		t.Errorf("submitted turn text missing re-entry context block; text=%q", sub.Text)
	}
	if !strings.Contains(sub.Text, "CI failed: build timed out") {
		t.Errorf("submitted turn text missing context detail; text=%q", sub.Text)
	}
}

// ----- Task 7: Closes #N on lifecycle MR body (primary repo only) -----

// seedLifecycleTaskWithSecondaryRepo creates the same objects as seedLifecycleTask
// plus a second Repository in the same project. Returns the task and secondary repo name.
func seedLifecycleTaskWithSecondaryRepo(t *testing.T, name, proj, primaryRepo, secondaryRepo, scmSecret string, source *tatarav1alpha1.TaskSource) *tatarav1alpha1.Task {
	t.Helper()
	task := seedLifecycleTask(t, name, proj, primaryRepo, scmSecret, source)
	// Add a second repo to the same project.
	r2 := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: secondaryRepo, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: proj, URL: "https://github.com/o/r2.git",
			DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(context.Background(), r2); err != nil {
		t.Fatalf("create secondary repo %s: %v", secondaryRepo, err)
	}
	return task
}

// TestLifecycleImplement_ClosesIssueInPrimaryRepoMRBody verifies that an
// issue-linked lifecycle task's MR body for the PRIMARY repo contains
// "Closes #<issueNumber>".
func TestLifecycleImplement_ClosesIssueInPrimaryRepoMRBody(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-closes-primary"
	proj := "lc-cp-proj"
	primaryRepo := "lc-cp-repo"
	sec := "lc-cp-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#50",
		URL: "https://github.com/o/r/issues/50", Number: 50,
		IsPR: false,
	}
	task := seedLifecycleTask(t, name, proj, primaryRepo, sec, src)
	task.Status.DeployState = "Implement"
	task.Status.Phase = "Succeeded"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
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
	primaryBody := fw.openCalls[0].body
	wantCloses := "Closes #50"
	if !strings.Contains(primaryBody, wantCloses) {
		t.Errorf("primary repo MR body = %q, want to contain %q", primaryBody, wantCloses)
	}
}

// TestLifecycleImplement_PushCDEligibleSuppressesCloses verifies that a push-CD
// eligible task (declared change significance) does NOT append "Closes #N" to its
// primary-repo MR body: native auto-merge would otherwise close the issue at
// MERGE time, defeating D9's close-on-confirmed-apply intent and leaving the issue
// wrongly closed on an apply-failure reroll. deploy-supervision owns the close.
func TestLifecycleImplement_PushCDEligibleSuppressesCloses(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-closes-pushcd"
	proj := "lc-cpc-proj"
	primaryRepo := "lc-cpc-repo"
	sec := "lc-cpc-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#52",
		URL: "https://github.com/o/r/issues/52", Number: 52,
		IsPR: false,
	}
	task := seedLifecycleTask(t, name, proj, primaryRepo, sec, src)
	task.Status.DeployState = "Implement"
	task.Status.Phase = "Succeeded"
	// Declared significance => pushCDEligible => deploy-supervision owns the close.
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{PRTitle: "feat: x", Significance: "minor"}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
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
	if strings.Contains(fw.openCalls[0].body, "Closes #52") {
		t.Errorf("push-CD eligible MR body must NOT contain 'Closes #52', got: %q", fw.openCalls[0].body)
	}
}

// TestLifecycleImplement_ClosesIssueNotInSecondaryRepoMRBody verifies that the
// "Closes #N" line does NOT appear in secondary-repo MR bodies.
func TestLifecycleImplement_ClosesIssueNotInSecondaryRepoMRBody(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-closes-secondary"
	proj := "lc-cs-proj"
	primaryRepo := "lc-cs-repo1"
	secondaryRepo := "lc-cs-repo2"
	sec := "lc-cs-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#51",
		URL: "https://github.com/o/r/issues/51", Number: 51,
		IsPR: false,
	}
	task := seedLifecycleTaskWithSecondaryRepo(t, name, proj, primaryRepo, secondaryRepo, sec, src)
	task.Status.DeployState = "Implement"
	task.Status.Phase = "Succeeded"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
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
	if len(fw.openCalls) < 2 {
		t.Fatalf("expected 2 OpenChange calls (primary+secondary), got %d", len(fw.openCalls))
	}
	// Primary repo must have Closes #51.
	if !strings.Contains(fw.openCalls[0].body, "Closes #51") {
		t.Errorf("primary repo MR body = %q, must contain 'Closes #51'", fw.openCalls[0].body)
	}
	// Secondary repo must NOT have Closes #51.
	if strings.Contains(fw.openCalls[1].body, "Closes #51") {
		t.Errorf("secondary repo MR body = %q, must NOT contain 'Closes #51'", fw.openCalls[1].body)
	}
}

// TestLifecycleImplement_NoPREntryLifecycleTaskDoesNotClose verifies that a
// lifecycle Task entered from a PR (Source.IsPR=true) does NOT emit Closes #N.
func TestLifecycleImplement_NoPREntryLifecycleTaskDoesNotClose(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-closes-pr-entry"
	proj := "lc-cpe-proj"
	repo := "lc-cpe-repo"
	sec := "lc-cpe-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#52",
		URL: "https://github.com/o/r/pull/52", Number: 52,
		IsPR: true,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.DeployState = "Implement"
	task.Status.Phase = "Succeeded"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
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
	if strings.Contains(fw.openCalls[0].body, "Closes #") {
		t.Errorf("PR-entry lifecycle task MR body = %q, must NOT contain 'Closes #'", fw.openCalls[0].body)
	}
}

// TestLifecycleImplement_LegacyImplementTaskDoesNotClose verifies that a
// generic (non-lifecycle) implement Task's MR body does NOT contain Closes #N.
func TestLifecycleImplement_LegacyImplementTaskDoesNotClose(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	// Use the existing writeBackOpenChange test infrastructure.
	mkSecret(t, "lc-legacy-sec", map[string][]byte{"token": []byte("tok"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-legacy-proj", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "lc-legacy-sec",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"},
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
				MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
			},
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	proj.Status.Memory = stableMemStatus("http://mem.svc:8080")
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("set memory ready: %v", err)
	}
	r2 := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-legacy-repo", Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:    "lc-legacy-proj",
			URL:           "https://github.com/o/r.git",
			DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, r2); err != nil {
		t.Fatalf("create repo: %v", err)
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-legacy-task", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    "lc-legacy-proj",
			RepositoryRef: "lc-legacy-repo",
			Goal:          "improve the login flow",
			Kind:          "implement", // NOT issueLifecycle
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#53",
				URL: "https://github.com/o/r/issues/53", Number: 53, IsPR: false,
			},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	// Seed WritebackPending=True to trigger doWriteBack -> writeBackOpenChange.
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type: "WritebackPending", Status: metav1.ConditionTrue,
		Reason: "AgentDone", ObservedGeneration: task.Generation,
	})
	task.Status.Phase = "Succeeded"
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed legacy task: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "lc-legacy-task"}}
	_, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.openCalls) == 0 {
		t.Fatal("expected OpenChange to be called for legacy implement task")
	}
	if strings.Contains(fw.openCalls[0].body, "Closes #") {
		t.Errorf("legacy implement MR body = %q, must NOT contain 'Closes #'", fw.openCalls[0].body)
	}
}

// ----- FIX 2: idempotent writeBackOpenChange + atomic implement-finish -----

// TestLifecycleImplement_IdempotentOnRetry verifies that calling the implement-finish
// path twice (Phase Succeeded, PrURL already set from a previous reconcile) opens
// the PR exactly once and still ends in DeployState=MRCI.
func TestLifecycleImplement_IdempotentOnRetry(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-impl-idem"
	proj := "lc-ip-idem"
	repo := "lc-ir-idem"
	sec := "lc-is-idem"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#20", URL: "https://github.com/o/r/issues/20",
		Number: 20,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	// Simulate: first reconcile opened the PR and set PrURL, but then errored
	// before finishing the state transition. So: Phase=Succeeded, PrURL already set,
	// DeployState still Implement.
	task.Status.DeployState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.PrURL = "https://github.com/o/r/pull/77"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{openPRURL: "https://github.com/o/r/pull/77"}
	r := newLifecycleReconciler(t, fw)

	_, err := r.reconcileLifecycle(ctx, func() *tatarav1alpha1.Task {
		tk := &tatarav1alpha1.Task{}
		if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); e != nil {
			t.Fatalf("get task: %v", e)
		}
		return tk
	}())
	if err != nil {
		t.Fatalf("reconcileLifecycle (retry): %v", err)
	}

	fw.mu.Lock()
	openCount := len(fw.openCalls)
	fw.mu.Unlock()
	if openCount != 0 {
		t.Errorf("OpenChange called %d times on retry; want 0 (PR already open)", openCount)
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}
	if got.Status.DeployState != "MRCI" {
		t.Errorf("DeployState = %q, want MRCI after idempotent retry", got.Status.DeployState)
	}
	if got.Status.PrURL != "https://github.com/o/r/pull/77" {
		t.Errorf("PrURL = %q, want unchanged", got.Status.PrURL)
	}
}

// ============================================================
// Task 6 - Iteration backstop
// ============================================================

// seedImplementReadyTask seeds a task in DeployState=Implement, Phase=""
// (ready to spawn a fresh run). Extra status fields set via the returned task
// pointer before calling reconcileLifecycle.
func seedImplementReadyTask(t *testing.T, suffix string, iterations int) (*TaskReconciler, *lifecycleFakeSCMWriter, *tatarav1alpha1.Task) {
	t.Helper()
	ctx := context.Background()
	name := "lc-backstop-" + suffix
	proj := "lc-bsp-" + suffix
	repo := "lc-bsr-" + suffix
	sec := "lc-bss-" + suffix
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#7", URL: "https://github.com/o/r/issues/7",
		Number: 7,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	// Set MaxLifecycleIterations on the project to 3 for deterministic tests.
	projObj := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: proj}, projObj); err != nil {
		t.Fatalf("get project: %v", err)
	}
	projObj.Spec.Agent.MaxLifecycleIterations = 3
	if err := k8sClient.Update(ctx, projObj); err != nil {
		t.Fatalf("update project MaxLifecycleIterations: %v", err)
	}

	task.Status.DeployState = "Implement"
	task.Status.Phase = ""
	task.Status.LifecycleIterations = iterations
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed implement ready: %v", err)
	}
	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)
	// Wire the reader so GetPRState works (not needed for backstop but keeps
	// the reconciler consistent).
	return r, fw, task
}

func fetchTask(t *testing.T, name string) *tatarav1alpha1.Task {
	t.Helper()
	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task %s: %v", name, err)
	}
	return got
}

// TestLifecycleImplement_BackstopParksWhenMaxIterationsReached verifies that
// entering Implement with LifecycleIterations >= MaxLifecycleIterations parks the
// task without spawning a pod, increments giveup metric, and posts a comment.
func TestLifecycleImplement_BackstopParksWhenMaxIterationsReached(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, fw, task := seedImplementReadyTask(t, "max", 3) // 3 >= max(3) -> park

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, task.Name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, task.Name)
	if got.Status.DeployState != "Parked" {
		t.Errorf("DeployState = %q, want Parked (backstop)", got.Status.DeployState)
	}
	// No pod spawned.
	pods := &corev1.PodList{}
	if err := k8sClient.List(ctx, pods, client.InNamespace(testNS), client.MatchingFields{"metadata.name": agent.PodName(task)}); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	// Pod count check via comment - backstop must post comment.
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.commentCalls) == 0 {
		t.Error("backstop must post a comment on the issue/PR")
	}
	found := false
	for _, c := range fw.commentCalls {
		if strings.Contains(c.body, "max lifecycle iterations") || strings.Contains(c.body, "human") {
			found = true
		}
	}
	if !found {
		t.Errorf("backstop comment must mention max iterations; got %+v", fw.commentCalls)
	}
}

// TestLifecycleImplement_BackstopAllowsSpawnBelowMax verifies that with iterations
// below max, Implement still spawns (transitions away from Implement).
func TestLifecycleImplement_BackstopAllowsSpawnBelowMax(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, task := seedImplementReadyTask(t, "below", 2) // 2 < max(3) -> spawn

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, task.Name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, task.Name)
	// Phase should advance (Planning) meaning spawn occurred, not Parked.
	if got.Status.DeployState == "Parked" {
		t.Error("DeployState must not be Parked below max iterations")
	}
	// LifecycleIterations must be incremented.
	if got.Status.LifecycleIterations != 3 {
		t.Errorf("LifecycleIterations = %d, want 3 (incremented on spawn)", got.Status.LifecycleIterations)
	}
}

func TestImplementPrompt_SystemicGroup(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t"},
		Spec: tatarav1alpha1.TaskSpec{
			Goal: "Triage issue o/r1#12", ProjectRef: "p",
			SystemicGroup: &tatarav1alpha1.SystemicGroup{
				SystemicID: "abc", SameRepoSiblings: []int{15},
				CrossRepo: []string{"o/r2#9 - B"},
			},
		},
	}
	got := implementPrompt(task)
	if !strings.Contains(got, "Closes #15") {
		t.Fatalf("prompt must instruct closing same-repo sibling: %s", got)
	}
	if !strings.Contains(got, "o/r2#9") {
		t.Fatalf("prompt must reference cross-repo sibling: %s", got)
	}
}

// TestImplementPrompt_GuidanceAppearsExactlyOnce guards the token-conservation
// double-append fix: the Implement turn-0 prompt must carry platformProblemGuidance
// and toolingConsumeGuidance exactly once each (planTurnText already appends both;
// lifecyclePhaseGuidance and the old explicit re-append duplicated them).
func TestImplementPrompt_GuidanceAppearsExactlyOnce(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-dedupe", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "proj", RepositoryRef: "repo",
			Goal: "fix the bug", Kind: "issueLifecycle",
		},
	}
	got := implementPrompt(task)
	if n := strings.Count(got, "## Platform problems"); n != 1 {
		t.Errorf("platformProblemGuidance appears %d times, want 1:\n%s", n, got)
	}
	if n := strings.Count(got, toolingConsumeSubstr); n != 1 {
		t.Errorf("toolingConsumeGuidance (%q) appears %d times, want 1:\n%s", toolingConsumeSubstr, n, got)
	}
	// The phase block must still be present (fix must not delete it).
	if !strings.Contains(got, "## Lifecycle phase: Implement") {
		t.Errorf("implementPrompt missing lifecycle phase block:\n%s", got)
	}
}
