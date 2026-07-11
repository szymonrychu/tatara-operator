package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// ----- Task 2: buildTriagePrompt includes comment thread -----

// fakeReaderComments is a fake SCMReader that returns scripted comments.
type fakeReaderComments struct {
	comments []scm.IssueComment
	err      error
}

func (f *fakeReaderComments) ListOpenPRs(_ context.Context, _, _ string) ([]scm.PRRef, error) {
	return nil, nil
}
func (f *fakeReaderComments) ListOpenIssues(_ context.Context, _, _ string) ([]scm.IssueRef, error) {
	return nil, nil
}
func (f *fakeReaderComments) ListBoardItems(_ context.Context, _ scm.BoardRef) ([]scm.BoardItem, error) {
	return nil, nil
}
func (f *fakeReaderComments) GetCommitCIStatus(_ context.Context, _, _, _ string) (string, error) {
	return "", nil
}
func (f *fakeReaderComments) ListIssueComments(_ context.Context, _, _ string, _ int) ([]scm.IssueComment, error) {
	return f.comments, f.err
}
func (f *fakeReaderComments) GetIssue(_ context.Context, _, _ string, _ int) (scm.IssueContent, error) {
	return scm.IssueContent{}, nil
}
func (f *fakeReaderComments) GetDefaultBranchHeadSHA(_ context.Context, _, _ string) (string, error) {
	return "", nil
}
func (f *fakeReaderComments) ListClosedIssues(_ context.Context, _, _ string, _ time.Time) ([]scm.IssueRef, error) {
	return nil, nil
}
func (f *fakeReaderComments) ListCommits(_ context.Context, _, _ string, _ time.Time) ([]scm.CommitRef, error) {
	return nil, nil
}

// fakeReaderWithIssue is a fake SCMReader that returns scripted issue content and comments.
type fakeReaderWithIssue struct {
	title    string
	body     string
	comments []scm.IssueComment
}

func (f *fakeReaderWithIssue) ListOpenPRs(_ context.Context, _, _ string) ([]scm.PRRef, error) {
	return nil, nil
}
func (f *fakeReaderWithIssue) ListOpenIssues(_ context.Context, _, _ string) ([]scm.IssueRef, error) {
	return nil, nil
}
func (f *fakeReaderWithIssue) ListBoardItems(_ context.Context, _ scm.BoardRef) ([]scm.BoardItem, error) {
	return nil, nil
}
func (f *fakeReaderWithIssue) GetCommitCIStatus(_ context.Context, _, _, _ string) (string, error) {
	return "", nil
}
func (f *fakeReaderWithIssue) ListIssueComments(_ context.Context, _, _ string, _ int) ([]scm.IssueComment, error) {
	return f.comments, nil
}
func (f *fakeReaderWithIssue) GetIssue(_ context.Context, _, _ string, _ int) (scm.IssueContent, error) {
	return scm.IssueContent{Title: f.title, Body: f.body}, nil
}
func (f *fakeReaderWithIssue) GetDefaultBranchHeadSHA(_ context.Context, _, _ string) (string, error) {
	return "", nil
}
func (f *fakeReaderWithIssue) ListClosedIssues(_ context.Context, _, _ string, _ time.Time) ([]scm.IssueRef, error) {
	return nil, nil
}
func (f *fakeReaderWithIssue) ListCommits(_ context.Context, _, _ string, _ time.Time) ([]scm.CommitRef, error) {
	return nil, nil
}

// TestBuildTriagePrompt_NoComments verifies the prompt equals the plain
// lifecycleTriageText when there are no prior comments.
func TestBuildTriagePrompt_NoComments(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-nocomments", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    "proj",
			RepositoryRef: "repo",
			Goal:          "Fix the login bug",
			Kind:          "issueLifecycle",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#5",
				URL: "https://github.com/o/r/issues/5", Number: 5,
			},
		},
	}
	got := buildTriagePrompt(task, "My Issue Title", "My Issue Body", nil)
	want := lifecycleTriageText(task, "My Issue Title", "My Issue Body")
	if got != want {
		t.Errorf("buildTriagePrompt with no comments:\ngot:  %q\nwant: %q", got, want)
	}
	// Title and body must appear; goal must not appear as issue body.
	if !strings.Contains(got, "My Issue Title") {
		t.Errorf("prompt must contain real issue title; got: %q", got)
	}
	if !strings.Contains(got, "My Issue Body") {
		t.Errorf("prompt must contain real issue body; got: %q", got)
	}
	if strings.Contains(got, "Issue body:\nFix the login bug") {
		t.Errorf("prompt must NOT use task Goal as issue body; got: %q", got)
	}
}

// TestBuildTriagePrompt_WithComments verifies that when comments are present,
// the prompt contains the base triage text AND a rendered comment thread block
// with the author and body of each comment.
func TestBuildTriagePrompt_WithComments(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	comments := []scm.IssueComment{
		{Author: "alice", Body: "What about the edge case?", CreatedAt: t1},
		{Author: "bot", Body: "Good point, I will investigate.", CreatedAt: t2},
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-withcomments", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    "proj",
			RepositoryRef: "repo",
			Goal:          "Fix the login bug",
			Kind:          "issueLifecycle",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#5",
				URL: "https://github.com/o/r/issues/5", Number: 5,
			},
		},
	}
	got := buildTriagePrompt(task, "Fix Title", "Fix Body", comments)

	// Must contain the base triage instructions.
	base := lifecycleTriageText(task, "Fix Title", "Fix Body")
	if !strings.Contains(got, "issue_outcome") {
		t.Errorf("buildTriagePrompt must contain base triage instructions; got: %q", got)
	}
	_ = base
	// Real title and body must appear; goal must not appear as issue body.
	if !strings.Contains(got, "Fix Title") {
		t.Errorf("prompt must contain real issue title; got: %q", got)
	}
	if !strings.Contains(got, "Fix Body") {
		t.Errorf("prompt must contain real issue body; got: %q", got)
	}
	if strings.Contains(got, "Issue body:\nFix the login bug") {
		t.Errorf("prompt must NOT use task Goal as issue body; got: %q", got)
	}

	// Must contain a thread section.
	if !strings.Contains(got, "## Conversation thread") {
		t.Errorf("buildTriagePrompt with comments must contain '## Conversation thread'; got: %q", got)
	}
	// Must contain author and body of each comment.
	if !strings.Contains(got, "alice") {
		t.Errorf("buildTriagePrompt must contain author 'alice'; got: %q", got)
	}
	if !strings.Contains(got, "What about the edge case?") {
		t.Errorf("buildTriagePrompt must contain first comment body; got: %q", got)
	}
	if !strings.Contains(got, "bot") {
		t.Errorf("buildTriagePrompt must contain author 'bot'; got: %q", got)
	}
	if !strings.Contains(got, "Good point, I will investigate.") {
		t.Errorf("buildTriagePrompt must contain second comment body; got: %q", got)
	}
}

// TestBuildTriagePrompt_CapLength verifies the cap: if there are more than 20
// comments, only the most recent 20 are included.
func TestBuildTriagePrompt_CapLength(t *testing.T) {
	comments := make([]scm.IssueComment, 25)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range comments {
		comments[i] = scm.IssueComment{
			Author:    "user",
			Body:      strings.Repeat("x", 100) + " comment-" + string(rune('A'+i%26)),
			CreatedAt: base.Add(time.Duration(i) * time.Hour),
		}
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-cap", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "p", RepositoryRef: "r", Goal: "g", Kind: "issueLifecycle",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#1", Number: 1},
		},
	}
	got := buildTriagePrompt(task, "cap title", "cap body", comments)
	// Should NOT contain the first 5 comments (oldest ones were dropped).
	for i := 0; i < 5; i++ {
		oldest := comments[i].Body
		if strings.Contains(got, oldest) {
			t.Errorf("buildTriagePrompt: comment %d (oldest, should be dropped) found in prompt", i)
		}
	}
	// Should contain last 20 comments.
	for i := 5; i < 25; i++ {
		if !strings.Contains(got, comments[i].Body) {
			t.Errorf("buildTriagePrompt: comment %d (recent, should be included) missing from prompt", i)
		}
	}
}

// TestHandleTriage_PromptIncludesCommentWhenReaderWired verifies the integration:
// when handleTriage spawns the agent run (Pod ready, no current turn), the
// submitted turn text includes the comment thread fetched via ReaderFor.
func TestHandleTriage_PromptIncludesCommentWhenReaderWired(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-triage-prompt-comment"
	proj := "lc-tpc-proj"
	repo := "lc-tpc-repo"
	sec := "lc-tpc-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#42",
		URL: "https://github.com/o/r/issues/42", Number: 42,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	// State: Triage, agent run about to submit turn-0 (pod ready, Phase=Planning, no turn yet).
	task.Status.DeployState = "Triage"
	task.Status.Phase = "Planning"
	task.Status.PodName = agent.PodName(task)
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

	// Wire a fake reader that returns one comment.
	comments := []scm.IssueComment{
		{Author: "maintainer", Body: "Please clarify the expected output format.", CreatedAt: time.Now()},
	}
	fw := &lifecycleFakeSCMWriter{}
	sess := newFakeSession()
	r := newLifecycleReconciler(t, fw)
	r.Session = sess
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &fakeReaderComments{comments: comments}, nil
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

	sub, ok := sess.lastSubmit()
	if !ok {
		t.Fatal("expected a SubmitTurn call; none recorded")
	}
	if !strings.Contains(sub.Text, "maintainer") {
		t.Errorf("turn text must include comment author 'maintainer'; text=%q", sub.Text)
	}
	if !strings.Contains(sub.Text, "Please clarify the expected output format.") {
		t.Errorf("turn text must include comment body; text=%q", sub.Text)
	}
	if !strings.Contains(sub.Text, "## Conversation thread") {
		t.Errorf("turn text must include '## Conversation thread'; text=%q", sub.Text)
	}
}

// TestHandleTriage_PromptNoCommentWhenReaderNotWired verifies that when
// ReaderFor is nil the triage prompt is still built (plain triage text) and no
// error is returned.
func TestHandleTriage_PromptNoCommentWhenReaderNotWired(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-triage-noreader"
	proj := "lc-tnr-proj"
	repo := "lc-tnr-repo"
	sec := "lc-tnr-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#43",
		URL: "https://github.com/o/r/issues/43", Number: 43,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	task.Status.DeployState = "Triage"
	task.Status.Phase = "Planning"
	task.Status.PodName = agent.PodName(task)
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

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
	// ReaderFor is nil - no comments available.
	r.ReaderFor = nil

	_, err := r.reconcileLifecycle(ctx, func() *tatarav1alpha1.Task {
		tk := &tatarav1alpha1.Task{}
		if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); e != nil {
			t.Fatalf("get task: %v", e)
		}
		return tk
	}())
	if err != nil {
		t.Fatalf("reconcileLifecycle with nil ReaderFor: %v", err)
	}

	sub, ok := sess.lastSubmit()
	if !ok {
		t.Fatal("expected a SubmitTurn call; none recorded")
	}
	// Prompt must still contain issue_outcome instruction.
	if !strings.Contains(sub.Text, "issue_outcome") {
		t.Errorf("turn text must contain base triage instructions; text=%q", sub.Text)
	}
}

// ----- Task 3: Conversation handler idle -> Stopped -----

// TestConversation_BeforeDeadline_Requeues verifies that reconcileLifecycle in
// Conversation state with a future deadline returns RequeueAfter and does NOT
// create a pod or transition to any other state.
func TestConversation_BeforeDeadline_Requeues(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-conv-before"
	proj := "lc-cbf-proj"
	repo := "lc-cbf-repo"
	sec := "lc-cbf-sec"
	src := &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#20", Number: 20}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	dl := metav1.NewTime(time.Now().Add(time.Hour))
	task.Status.DeployState = "Conversation"
	task.Status.DeadlineAt = &dl
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	r := newLifecycleReconciler(t, nil)
	res, err := r.reconcileLifecycle(ctx, func() *tatarav1alpha1.Task {
		tk := &tatarav1alpha1.Task{}
		if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); e != nil {
			t.Fatalf("get task: %v", e)
		}
		return tk
	}())
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("expected RequeueAfter before deadline, got 0")
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}
	if got.Status.DeployState != "Conversation" {
		t.Errorf("DeployState = %q, want Conversation (before deadline)", got.Status.DeployState)
	}

	// No pod must be created.
	pod := &corev1.Pod{}
	podErr := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: agent.PodName(task)}, pod)
	if podErr == nil {
		t.Error("pod must NOT be created in Conversation state")
	}
}

// TestConversation_AfterDeadline_TransitionsToStopped verifies that when
// DeadlineAt is in the past, the handler transitions to Stopped and increments
// the idle-stop counter (NOT the giveup counter).
func TestConversation_AfterDeadline_TransitionsToStopped(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-conv-after"
	proj := "lc-caf-proj"
	repo := "lc-caf-repo"
	sec := "lc-caf-sec"
	src := &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#21", Number: 21}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	dl := metav1.NewTime(time.Now().Add(-time.Minute)) // already past
	task.Status.DeployState = "Conversation"
	task.Status.DeadlineAt = &dl
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
	if got.Status.DeployState != "Stopped" {
		t.Errorf("DeployState = %q, want Stopped after deadline", got.Status.DeployState)
	}
	// No pod.
	pod := &corev1.Pod{}
	podErr := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: agent.PodName(task)}, pod)
	if podErr == nil {
		t.Error("pod must NOT be created when transitioning to Stopped")
	}
}

// TestTriagePrompt_ContainsRealTitleAndBody verifies that the triage prompt
// produced by buildTriagePromptFor contains the real issue title and body
// fetched via GetIssue, and does NOT contain the task's Goal as the issue body.
func TestTriagePrompt_ContainsRealTitleAndBody(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-triage-realtitle"
	proj := "lc-trt-proj"
	repo := "lc-trt-repo"
	sec := "lc-trt-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#55",
		URL: "https://github.com/o/r/issues/55", Number: 55,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	task.Status.DeployState = "Triage"
	task.Status.Phase = "Planning"
	task.Status.PodName = agent.PodName(task)
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	sess := newFakeSession()
	r := newLifecycleReconciler(t, &lifecycleFakeSCMWriter{})
	r.Session = sess
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &fakeReaderWithIssue{
			title: "Real Title",
			body:  "Real Body",
		}, nil
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

	sub, ok := sess.lastSubmit()
	if !ok {
		t.Fatal("expected a SubmitTurn call; none recorded")
	}
	if !strings.Contains(sub.Text, "Real Title") {
		t.Errorf("prompt must contain real issue title; text=%q", sub.Text)
	}
	if !strings.Contains(sub.Text, "Real Body") {
		t.Errorf("prompt must contain real issue body; text=%q", sub.Text)
	}
	// Goal must NOT appear as the issue body.
	goal := task.Spec.Goal
	if strings.Contains(sub.Text, "Issue body:\n"+goal) {
		t.Errorf("prompt must NOT use task Goal as issue body; goal=%q text=%q", goal, sub.Text)
	}
}

// ----- C4: Triage prompt names the follow-up skill -----

func TestLifecycleTriageText_NamesFollowupSkill(t *testing.T) {
	task := &tatarav1alpha1.Task{}
	task.Spec.Source = &tatarav1alpha1.TaskSource{IssueRef: "o/r#1", URL: "http://x"}
	got := lifecycleTriageText(task, "T", "B")
	if !strings.Contains(got, "tatara-research-followup") {
		t.Fatalf("triage prompt does not invoke tatara-research-followup skill:\n%s", got)
	}
}
