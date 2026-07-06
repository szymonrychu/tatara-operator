package controller

// Tests for code-review findings on the issue-lifecycle M1 (FIX 1-8).
// Written RED-first: each test documents expected correct behaviour.

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// ============================================================
// FIX 1 - ImplementContext must survive through the Planning
//          requeue and reach the actual turn-0 submission.
//          Replaces TestLifecycleImplement_ContextClearedAfterRunStarts.
// ============================================================

// TestLifecycleImplement_ContextNotClearedOnSpawn verifies that when Phase
// transitions from "" to Planning (spawn), ImplementContext is NOT cleared.
// It must persist so the pod-ready reconcile can include it in the turn-0 prompt.
func TestLifecycleImplement_ContextNotClearedOnSpawn(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-impl-ctx-spawn"
	proj := "lc-icsp-proj"
	repo := "lc-icsp-repo"
	sec := "lc-icsp-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#40", URL: "https://github.com/o/r/issues/40",
		Number: 40,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = ""
	task.Status.ImplementContext = "CI failed: test_auth timed out"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed implement re-entry: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)

	// First reconcile: spawn path (Phase="").  Pod created, Phase -> Planning.
	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle (spawn): %v", err)
	}

	// ImplementContext must still be set - NOT cleared on spawn.
	got := fetchTask(t, name)
	if got.Status.ImplementContext == "" {
		t.Errorf("ImplementContext was cleared on spawn (Phase=Planning); want it preserved until finishImplement")
	}
	if got.Status.Phase != "Planning" {
		t.Errorf("Phase = %q after spawn reconcile, want Planning", got.Status.Phase)
	}
}

// TestLifecycleImplement_ContextInSubmittedTurnAcrossPlanningRequeue verifies the
// full re-entry path: Phase=Planning (pod already ready), ImplementContext set ->
// driveTurns submits turn whose text contains the context block.
func TestLifecycleImplement_ContextInSubmittedTurnAcrossPlanningRequeue(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-impl-ctx-full"
	proj := "lc-icf-proj"
	repo := "lc-icf-repo"
	sec := "lc-icf-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#41", URL: "https://github.com/o/r/issues/41",
		Number: 41,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	// State: pod already in Planning, ImplementContext still set (was NOT cleared on spawn).
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Planning"
	task.Status.PodName = agent.PodName(task)
	task.Status.ImplementContext = "MainCI failed after merge (SHA abc). Re-implement the fix."
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Create a ready pod.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: agent.PodName(task), Namespace: testNS},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "wrapper", Image: "wrapper:1"}}},
	}
	if err := k8sClient.Create(ctx, pod); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if err := k8sClient.Status().Update(ctx, pod); err != nil {
		t.Fatalf("set pod ready: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	sess := newFakeSession()
	r := newLifecycleReconciler(t, fw)
	r.Session = sess

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
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
	if !strings.Contains(sub.Text, "MainCI failed") {
		t.Errorf("submitted turn text missing context detail; text=%q", sub.Text)
	}
}

// TestLifecycleImplement_ContextClearedInFinishImplement verifies that
// finishImplement (Phase=Succeeded) clears ImplementContext.
func TestLifecycleImplement_ContextClearedInFinishImplement(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-impl-ctx-finish"
	proj := "lc-icfi-proj"
	repo := "lc-icfi-repo"
	sec := "lc-icfi-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#42", URL: "https://github.com/o/r/issues/42",
		Number: 42,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.ImplementContext = "some re-entry context"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{openPRURL: "https://github.com/o/r/pull/88"}
	r := newLifecycleReconciler(t, fw)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle (finish): %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.ImplementContext != "" {
		t.Errorf("ImplementContext = %q after finishImplement, want empty", got.Status.ImplementContext)
	}
}

// ============================================================
// FIX 2 - MainCI failure must clear DeadlineAt so the next
//          MRCI entry gets a fresh deadline and does not park.
// ============================================================

// TestLifecycleMainCI_FailureClearsDeadline verifies that when MainCI transitions
// to Implement on failure, DeadlineAt is cleared on the task.
func TestLifecycleMainCI_FailureClearsDeadline(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMainCI{ciStatus: "failure"}
	r, name := seedMainCITask(t, "fail-dl", fw, time.Hour)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Implement" {
		t.Errorf("LifecycleState = %q, want Implement", got.Status.LifecycleState)
	}
	if got.Status.DeadlineAt != nil {
		t.Errorf("DeadlineAt must be cleared on MainCI->Implement transition, got %v", got.Status.DeadlineAt)
	}
}

// TestLifecycleMRCI_AfterMainCIFailureGetsNewDeadline verifies that a subsequent
// MRCI entry after MainCI failure (DeadlineAt=nil) sets a fresh DeadlineAt and
// does NOT park immediately.
func TestLifecycleMRCI_AfterMainCIFailureGetsNewDeadline(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	// Seed: no deadline (cleared by MainCI failure fix).
	r, _, name := seedMRCITask(t, "fresh-dl", scm.PRState{Author: "bot", CIStatus: "pending"}, 0)

	res, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("MRCI with pending CI must requeue, not park")
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState == "Parked" {
		t.Error("MRCI must not park immediately on first entry with nil DeadlineAt")
	}
	if got.Status.DeadlineAt == nil {
		t.Error("MRCI must set DeadlineAt on first entry (ensureDeadline)")
	}
}

// ============================================================
// FIX 3 - GitLab MainCI must use GitLabProjectPath, not OwnerRepo.
// ============================================================

// fakeReaderCapture records the owner/repo/sha args of GetCommitCIStatus.
type fakeReaderCapture struct {
	owner, repo, sha string
	ciStatus         string
}

func (f *fakeReaderCapture) ListOpenPRs(_ context.Context, _, _ string) ([]scm.PRRef, error) {
	return nil, nil
}
func (f *fakeReaderCapture) ListOpenIssues(_ context.Context, _, _ string) ([]scm.IssueRef, error) {
	return nil, nil
}
func (f *fakeReaderCapture) ListBoardItems(_ context.Context, _ scm.BoardRef) ([]scm.BoardItem, error) {
	return nil, nil
}
func (f *fakeReaderCapture) GetCommitCIStatus(_ context.Context, owner, repo, sha string) (string, error) {
	f.owner = owner
	f.repo = repo
	f.sha = sha
	return f.ciStatus, nil
}
func (f *fakeReaderCapture) ListIssueComments(_ context.Context, _, _ string, _ int) ([]scm.IssueComment, error) {
	return nil, nil
}
func (f *fakeReaderCapture) GetIssue(_ context.Context, _, _ string, _ int) (scm.IssueContent, error) {
	return scm.IssueContent{}, nil
}
func (f *fakeReaderCapture) GetDefaultBranchHeadSHA(_ context.Context, _, _ string) (string, error) {
	return "", nil
}
func (f *fakeReaderCapture) ListClosedIssues(_ context.Context, _, _ string, _ time.Time) ([]scm.IssueRef, error) {
	return nil, nil
}
func (f *fakeReaderCapture) ListCommits(_ context.Context, _, _ string, _ time.Time) ([]scm.CommitRef, error) {
	return nil, nil
}

// seedGitLabMainCITask seeds a MainCI task backed by a GitLab nested-path repo.
func seedGitLabMainCITask(t *testing.T, suffix string, reader *fakeReaderCapture) (*TaskReconciler, string) {
	t.Helper()
	ctx := context.Background()
	name := "lc-glmainci-" + suffix
	proj := "lc-glmcp-" + suffix
	repo := "lc-glmcr-" + suffix
	sec := "lc-glmcs-" + suffix

	mkSecret(t, sec, map[string][]byte{"token": []byte("tok"), "webhookSecret": []byte("wh")})
	projObj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: proj, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: sec,
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "gitlab", Owner: "group", BotLogin: "bot"},
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
				MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
			},
		},
	}
	if err := k8sClient.Create(ctx, projObj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	projObj.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Ready", Endpoint: "http://mem.svc:8080"}
	if err := k8sClient.Status().Update(ctx, projObj); err != nil {
		t.Fatalf("set memory ready: %v", err)
	}
	// GitLab nested URL: group/sub/project
	repoObj := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: repo, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:       proj,
			URL:              "https://gitlab.example.com/group/sub/project.git",
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, repoObj); err != nil {
		t.Fatalf("create repo: %v", err)
	}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj,
			RepositoryRef: repo,
			Goal:          "fix it",
			Kind:          "issueLifecycle",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "gitlab",
				IssueRef: "group/sub/project#5",
				URL:      "https://gitlab.example.com/group/sub/project/-/issues/5",
				Number:   5,
			},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Status.LifecycleState = "MainCI"
	task.Status.MergeCommitSHA = "cafebabe"
	dl := metav1.NewTime(time.Now().Add(time.Hour))
	task.Status.DeadlineAt = &dl
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed status: %v", err)
	}

	fw := &lifecycleFakeSCMWriterMainCI{ciStatus: reader.ciStatus}
	rec := newLifecycleReconciler(t, &fw.lifecycleFakeSCMWriter)
	rec.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	rec.ReaderFor = func(_, _ string) (scm.SCMReader, error) { return reader, nil }
	return rec, name
}

// TestLifecycleMainCI_GitLabUsesProjectPath verifies GetCommitCIStatus receives
// the full GitLab project path (group/sub/project), not just the owner.
func TestLifecycleMainCI_GitLabUsesProjectPath(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	reader := &fakeReaderCapture{ciStatus: "pending"}
	r, name := seedGitLabMainCITask(t, "glpath", reader)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	// For https://gitlab.example.com/group/sub/project.git the project path is
	// "group/sub/project".  OwnerRepo would return ("group", "sub") which is wrong.
	if reader.owner != "group/sub/project" {
		t.Errorf("GetCommitCIStatus owner = %q, want %q (full GitLab project path)", reader.owner, "group/sub/project")
	}
}

// ============================================================
// FIX 5 - parkWithComment must comment on the PR when there is
//          no IssueRef (bot-PR-entry task).
// ============================================================

// fakeWriterPark is a simple fake that records Comment calls.
type fakeWriterPark struct {
	lifecycleFakeSCMWriter
	prState scm.PRState
	prErr   error
}

func (f *fakeWriterPark) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	return f.prState, f.prErr
}

func (f *fakeWriterPark) Merge(_ context.Context, _, _ string, _ int, _ string) (string, error) {
	return "", nil
}

// TestParkWithComment_PREntryTaskCommentsOnPR verifies that a task with no IssueRef
// (entered from a bot PR) gets a park comment on the PR resource.
func TestParkWithComment_PREntryTaskCommentsOnPR(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	// Use the MRCI deadline path to trigger parkWithComment.
	// Bot-PR-entry task: Source.IsPR=true, IssueRef="" (no linked issue).
	name := "lc-mrci-prpark"
	proj := "lc-mrcipp-proj"
	repo := "lc-mrcipp-repo"
	sec := "lc-mrcipp-sec"

	// PR-entry: IsPR=true, no IssueRef.
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "", IsPR: true,
		URL: "https://github.com/o/r/pull/77", Number: 77,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "MRCI"
	task.Status.PRNumber = 77
	task.Status.PrURL = "https://github.com/o/r/pull/77"
	// Set an already-expired deadline so parkWithComment fires.
	past := metav1.NewTime(time.Now().Add(-time.Minute))
	task.Status.DeadlineAt = &past
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &fakeWriterPark{prState: scm.PRState{Author: "bot", CIStatus: "pending"}}
	r := newLifecycleReconciler(t, &fw.lifecycleFakeSCMWriter)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked", got.Status.LifecycleState)
	}

	// A comment must have been posted somewhere - either the issue ref or the PR.
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.commentCalls) == 0 {
		t.Error("parkWithComment must post a comment for PR-entry tasks with no IssueRef")
	}
}

// ============================================================
// FIX 7 - handleMerge on an already-merged PR skips Merge call.
// ============================================================

// lifecycleFakeSCMWriterAlreadyMerged simulates a PR that is already merged:
// GetPRState reports merged/closed, Merge must NOT be called.
type lifecycleFakeSCMWriterAlreadyMerged struct {
	lifecycleFakeSCMWriter
	mergeCalled bool
}

func (f *lifecycleFakeSCMWriterAlreadyMerged) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	return scm.PRState{
		Author:   "bot",
		CIStatus: "success",
		// HeadSHA is set; in a real merged PR the branch may be deleted.
	}, nil
}

func (f *lifecycleFakeSCMWriterAlreadyMerged) Merge(_ context.Context, _, _ string, _ int, _ string) (string, error) {
	f.mu.Lock()
	f.mergeCalled = true
	f.mu.Unlock()
	return "", scm.ErrMergeConflict
}

// TestLifecycleMerge_AlreadyMergedSkipsMergeTransitionsToMainCI verifies that
// handleMerge on a PR that is already merged detects this via GetPRState and
// skips the Merge call, transitioning directly to MainCI.
func TestLifecycleMerge_AlreadyMergedSkipsMergeTransitionsToMainCI(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	// Use a variant of seedMergeTask with the already-merged writer.
	name := "lc-merge-already"
	proj := "lc-mergep-already"
	repo := "lc-merger-already"
	sec := "lc-merges-already"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#9", URL: "https://github.com/o/r/issues/9",
		Number: 9,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Merge"
	task.Status.PRNumber = 42
	task.Status.PrURL = "https://github.com/o/r/pull/42"
	task.Status.HeadBranch = "tatara/task-" + name
	// MergeCommitSHA set: PR was previously merged, SHA is known.
	task.Status.MergeCommitSHA = "already-merged-sha"
	dl := metav1.NewTime(time.Now().Add(time.Hour))
	task.Status.DeadlineAt = &dl
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriterAlreadyMerged{}
	r := newLifecycleReconciler(t, &fw.lifecycleFakeSCMWriter)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.LifecycleState != "MainCI" {
		t.Errorf("LifecycleState = %q, want MainCI (already-merged PR)", got.Status.LifecycleState)
	}

	fw.mu.Lock()
	mergedCalled := fw.mergeCalled
	fw.mu.Unlock()
	if mergedCalled {
		t.Error("Merge must NOT be called when PR is already merged")
	}
}

// ============================================================
// FIX 8 - Guards: PR number 0 parks; empty MergeCommitSHA requeues.
// ============================================================

// TestLifecycleMRCI_ZeroPRNumberParks verifies that MRCI with a PR number of 0
// parks the task rather than calling GetPRState(0).
func TestLifecycleMRCI_ZeroPRNumberParks(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "lc-mrci-zero"
	proj := "lc-mrcizerp-proj"
	repo := "lc-mrcizero-repo"
	sec := "lc-mrcizero-sec"
	// No PR number set; IsPR=false, no PRNumber in status.
	src := &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "MRCI"
	task.Status.PRNumber = 0 // explicit zero
	dl := metav1.NewTime(time.Now().Add(time.Hour))
	task.Status.DeadlineAt = &dl
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Writer that panics or errors on GetPRState(0) to confirm it is not called.
	fw := &fakeWriterPark{prState: scm.PRState{Author: "bot"}}
	r := newLifecycleReconciler(t, &fw.lifecycleFakeSCMWriter)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked when PR number is 0", got.Status.LifecycleState)
	}
}

// TestLifecycleMainCI_EmptyMergeCommitSHARequeues verifies that MainCI with an
// empty MergeCommitSHA requeues (re-fetch) rather than polling "" until the
// deadline parks.
func TestLifecycleMainCI_EmptyMergeCommitSHARequeues(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMainCI{ciStatus: ""}
	// Override seedMainCITask: set MergeCommitSHA="" before seeding.
	name := "lc-mainci-emptysha"
	proj := "lc-mcemptyp-proj"
	repo := "lc-mcemptyr-repo"
	sec := "lc-mcemptys-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#12", URL: "https://github.com/o/r/issues/12",
		Number: 12,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "MainCI"
	task.Status.MergeCommitSHA = "" // empty - the bug scenario
	dl := metav1.NewTime(time.Now().Add(time.Hour))
	task.Status.DeadlineAt = &dl
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	r := newLifecycleReconciler(t, &fw.lifecycleFakeSCMWriter)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &fakeReaderMainCI{ciStatus: "", ciErr: nil}, nil
	}

	res, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("MainCI with empty MergeCommitSHA must requeue, not park immediately")
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState == "Parked" {
		t.Error("MainCI with empty MergeCommitSHA must not park immediately")
	}
}

// ============================================================
// FIX 9 - MainCI failure must reset the merged-change state so
//          the re-implement opens and merges a NEW MR instead of
//          looping forever on the stale merged PR / failing SHA.
//          Repro: szymonrychu/tatara-chat#19 / PR #20 (chart job
//          skipped on the PR, failed on main post-merge).
// ============================================================

// TestLifecycleMainCI_FailureClearsMergedChangeState verifies that when MainCI
// re-enters Implement on a post-merge failure, MergeCommitSHA, PrURL and PRNumber
// are cleared. Without this, writeBackOpenChange short-circuits on the stale PrURL
// ("AlreadyWritten") so no new MR is opened, and handleMerge's "already-merged"
// guard (MergeCommitSHA set) skips the merge - bouncing the task between MRCI,
// Merge and MainCI on the stale failing SHA until maxLifecycleIterations parks it.
func TestLifecycleMainCI_FailureClearsMergedChangeState(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMainCI{ciStatus: "failure"}
	r, name := seedMainCITask(t, "fail-reset", fw, time.Hour)

	// seedMainCITask sets MergeCommitSHA="deadbeef", PRNumber=55,
	// PrURL=".../pull/55" - the merged-change state that must be cleared.
	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Implement" {
		t.Fatalf("LifecycleState = %q, want Implement on MainCI failure", got.Status.LifecycleState)
	}
	if got.Status.MergeCommitSHA != "" {
		t.Errorf("MergeCommitSHA = %q, want cleared so the next Merge actually merges the new MR", got.Status.MergeCommitSHA)
	}
	if got.Status.PrURL != "" {
		t.Errorf("PrURL = %q, want cleared so writeBackOpenChange opens a NEW MR", got.Status.PrURL)
	}
	if got.Status.PRNumber != 0 {
		t.Errorf("PRNumber = %d, want 0 so MRCI polls the new MR, not the merged one", got.Status.PRNumber)
	}
}

// ============================================================
// FIX 10 - MRCI must not nurse a PR that re-proposes the
//          already-merged change. After a post-merge MainCI failure
//          clearMergedChangeState clears PrURL/PRNumber so a fresh MR
//          opens, but the deterministic task branch (tatara/task-<name>)
//          is reused; if the re-implement does not advance it, the new
//          PR re-proposes the SAME already-merged commits. Observed
//          in-repo: tatara-operator PR #50 duplicated merged PR #46 with
//          an identical head SHA. Nursing it re-merges identical code and
//          fails MainCI again, bouncing to maxLifecycleIterations. The
//          guard detects head SHA == MergedHeadSHA, closes the duplicate
//          and parks for a human.
// ============================================================

type fakeWriterDupMerged struct {
	lifecycleFakeSCMWriter
	headSHA       string
	closePRCalled bool
	closePRNumber int
}

func (f *fakeWriterDupMerged) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	return scm.PRState{Author: "bot", HeadSHA: f.headSHA, CIStatus: "success"}, nil
}

func (f *fakeWriterDupMerged) Merge(_ context.Context, _, _ string, _ int, _ string) (string, error) {
	return "", nil
}

func (f *fakeWriterDupMerged) ClosePR(_ context.Context, _, _ string, number int, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closePRCalled = true
	f.closePRNumber = number
	return nil
}

// TestLifecycleMRCI_DuplicateOfMergedHeadParks verifies that a re-opened PR whose
// head equals the last merged head is closed and parked, not nursed into a loop.
func TestLifecycleMRCI_DuplicateOfMergedHeadParks(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "lc-mrci-dupmerged"
	proj := "lc-dupm-proj"
	repo := "lc-dupm-repo"
	sec := "lc-dupm-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#21", URL: "https://github.com/o/r/issues/21",
		Number: 21,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "MRCI"
	task.Status.PRNumber = 50
	task.Status.PrURL = "https://github.com/o/r/pull/50"
	task.Status.HeadBranch = "tatara/task-" + name
	// The branch was already merged at this head; the re-opened PR points at the
	// SAME head -> re-proposes the merged change with no new fix.
	task.Status.MergedHeadSHA = "82073dbd7d"
	dl := metav1.NewTime(time.Now().Add(time.Hour))
	task.Status.DeadlineAt = &dl
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &fakeWriterDupMerged{headSHA: "82073dbd7d"}
	r := newLifecycleReconciler(t, &fw.lifecycleFakeSCMWriter)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked (duplicate of merged head)", got.Status.LifecycleState)
	}
	fw.mu.Lock()
	closed := fw.closePRCalled
	closedNum := fw.closePRNumber
	fw.mu.Unlock()
	if !closed {
		t.Error("ClosePR must be called to close the duplicate PR")
	}
	if closedNum != 50 {
		t.Errorf("ClosePR number = %d, want 50", closedNum)
	}
}

// TestLifecycleMRCI_AdvancedHeadAfterMergeProceeds verifies the guard does NOT
// fire when the re-implement genuinely advanced the branch (new head SHA): MRCI
// proceeds to Merge as normal and the PR is not closed.
func TestLifecycleMRCI_AdvancedHeadAfterMergeProceeds(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "lc-mrci-advhead"
	proj := "lc-advh-proj"
	repo := "lc-advh-repo"
	sec := "lc-advh-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#22", URL: "https://github.com/o/r/issues/22",
		Number: 22,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "MRCI"
	task.Status.PRNumber = 60
	task.Status.PrURL = "https://github.com/o/r/pull/60"
	task.Status.HeadBranch = "tatara/task-" + name
	task.Status.MergedHeadSHA = "old-merged-head"
	dl := metav1.NewTime(time.Now().Add(time.Hour))
	task.Status.DeadlineAt = &dl
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Head differs from MergedHeadSHA -> a real new commit landed.
	fw := &fakeWriterDupMerged{headSHA: "new-head-with-fix"}
	r := newLifecycleReconciler(t, &fw.lifecycleFakeSCMWriter)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Merge" {
		t.Errorf("LifecycleState = %q, want Merge (advanced head, green CI)", got.Status.LifecycleState)
	}
	fw.mu.Lock()
	closed := fw.closePRCalled
	fw.mu.Unlock()
	if closed {
		t.Error("ClosePR must NOT be called when the branch advanced past the merged head")
	}
}
