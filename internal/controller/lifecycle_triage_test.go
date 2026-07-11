package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// ----- Task 5: Triage state handler -----

// seedTriageSucceeded seeds a task in DeployState=Triage/Phase=Succeeded
// with the given IssueOutcome, then returns the reconciler and task name.
func seedTriageSucceeded(t *testing.T, nameSuffix string, outcome *tatarav1alpha1.IssueOutcome) (r *TaskReconciler, fw *lifecycleFakeSCMWriter, taskName string) {
	t.Helper()
	ctx := context.Background()
	name := "lc-triage-" + nameSuffix
	proj := "lc-tp-" + nameSuffix
	repo := "lc-tr-" + nameSuffix
	sec := "lc-ts-" + nameSuffix
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#5", URL: "https://github.com/o/r/issues/5",
		Number: 5,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	// Seed the task as if a Triage agent run completed: DeployState=Triage, Phase=Succeeded.
	task.Status.DeployState = "Triage"
	task.Status.Phase = "Succeeded"
	task.Status.IssueOutcome = outcome
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed triage succeeded status: %v", err)
	}
	fw = &lifecycleFakeSCMWriter{}
	r = newLifecycleReconciler(t, fw)
	return r, fw, name
}

func TestLifecycleTriage_Close(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, fw, name := seedTriageSucceeded(t, "close", &tatarav1alpha1.IssueOutcome{
		Action: "close", Comment: "out of scope",
	})

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
	if len(fw.closeCalls) != 1 {
		t.Fatalf("CloseIssue call count = %d, want 1; closeCalls=%+v", len(fw.closeCalls), fw.closeCalls)
	}
	if fw.closeCalls[0].comment != "out of scope" {
		t.Errorf("CloseIssue comment = %q, want %q", fw.closeCalls[0].comment, "out of scope")
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}
	if got.Status.DeployState != "Done" {
		t.Errorf("DeployState = %q, want Done", got.Status.DeployState)
	}
	if got.Status.IssueOutcome != nil {
		t.Error("IssueOutcome must be cleared after consuming")
	}
}

func TestLifecycleTriage_Discuss(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, fw, name := seedTriageSucceeded(t, "discuss", &tatarav1alpha1.IssueOutcome{
		Action: "discuss", Comment: "I have two design questions...",
	})

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
		t.Fatalf("Comment call count = %d, want 1", len(fw.commentCalls))
	}
	if !strings.Contains(fw.commentCalls[0].body, "design questions") {
		t.Errorf("Comment body = %q, want to contain %q", fw.commentCalls[0].body, "design questions")
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}
	if got.Status.DeployState != "Conversation" {
		t.Errorf("DeployState = %q, want Conversation", got.Status.DeployState)
	}
	if got.Status.DeadlineAt == nil {
		t.Error("DeadlineAt must be set after discuss transition")
	}
	if got.Status.IssueOutcome != nil {
		t.Error("IssueOutcome must be cleared after consuming")
	}
}

func TestLifecycleTriage_Implement(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, fw, name := seedTriageSucceeded(t, "impl", &tatarav1alpha1.IssueOutcome{
		Action: "implement",
	})
	recordApproval(t, name, "szymon") // verified maintainer approval is the release signal

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
	if len(fw.closeCalls) != 0 {
		t.Error("CloseIssue must NOT be called for implement outcome")
	}
	if len(fw.commentCalls) != 0 {
		t.Error("Comment must NOT be called for implement outcome")
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}
	if got.Status.DeployState != "Implement" {
		t.Errorf("DeployState = %q, want Implement", got.Status.DeployState)
	}
	if got.Status.IssueOutcome != nil {
		t.Error("IssueOutcome must be cleared after consuming")
	}
}

// TestLifecycleTriage_NilOutcomeDefaultsToDiscuss verifies that a triage run that
// finishes with no IssueOutcome set (inconclusive run: turn cap, no subtasks, etc.)
// enters Conversation (discuss) rather than Implement. Defaulting to Implement
// silently converts an inconclusive triage into work (finding 2).
func TestLifecycleTriage_NilOutcomeDefaultsToDiscuss(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, name := seedTriageSucceeded(t, "nilout", nil)

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
	// After the fix: nil outcome -> Conversation (inconclusive, await human input).
	if got.Status.DeployState == "Implement" {
		t.Error("nil IssueOutcome must NOT enter Implement; inconclusive run should enter Conversation")
	}
	if got.Status.DeployState != "Conversation" {
		t.Errorf("DeployState = %q, want Conversation (nil outcome is inconclusive, not implement)", got.Status.DeployState)
	}
}

func TestLifecycleTriage_FailedTransitionsToParked(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-triage-failed"
	proj := "lc-tp-failed"
	repo := "lc-tr-failed"
	sec := "lc-ts-failed"
	src := &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.DeployState = "Triage"
	task.Status.Phase = "Failed"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed failed status: %v", err)
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
		t.Errorf("DeployState = %q, want Parked", got.Status.DeployState)
	}
}

// TestLifecycleTriage_ConcurrencyCapDoesNotBlockFinishTriage asserts that a Triage
// task with Phase=Succeeded still runs finishTriage (consumes outcome, transitions
// to Implement). The concurrency gate no longer exists; this test verifies the
// triage completion path still works unconditionally.
func TestLifecycleTriage_ConcurrencyCapDoesNotBlockFinishTriage(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	// Suffix to keep resources unique in the shared envtest namespace.
	const suffix = "capgate"
	name := "lc-triage-" + suffix
	projName := "lc-tp-" + suffix
	repoName := "lc-tr-" + suffix
	sec := "lc-ts-" + suffix

	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#99", URL: "https://github.com/o/r/issues/99",
		Number: 99,
	}
	task := seedLifecycleTask(t, name, projName, repoName, sec, src)

	// Put the task into Triage / Succeeded with an implement outcome.
	task.Status.DeployState = "Triage"
	task.Status.Phase = "Succeeded"
	task.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: "implement"}
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed triage succeeded: %v", err)
	}
	recordApproval(t, name, "szymon") // verified maintainer approval is the release signal

	r := newLifecycleReconciler(t, &lifecycleFakeSCMWriter{})

	// Reconcile: the terminal-phase Triage task must finish (transition to Implement).
	_, err := r.reconcileLifecycle(ctx, func() *tatarav1alpha1.Task {
		tk := &tatarav1alpha1.Task{}
		if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); e != nil {
			t.Fatalf("get task: %v", e)
		}
		return tk
	}())
	if err != nil {
		t.Fatalf("reconcileLifecycle at cap: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}
	if got.Status.DeployState != "Implement" {
		t.Errorf("DeployState = %q, want Implement; concurrency cap must not block finishTriage", got.Status.DeployState)
	}
}

// errListReader embeds commentReader but fails ListIssueComments, to prove the
// approval scan fails closed on an SCM read error.
type errListReader struct {
	commentReader
}

func (r *errListReader) ListIssueComments(context.Context, string, string, int) ([]scm.IssueComment, error) {
	return nil, errors.New("boom")
}

func TestApprovingMaintainer(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		approvers []string
		comments  []scm.IssueComment
		want      string
	}{
		{"maintainer after non-maintainer", []string{"szymon"},
			[]scm.IssueComment{{Author: "rando", Body: "x"}, {Author: "szymon", Body: "go"}}, "szymon"},
		{"most-recent maintainer wins", []string{"szymon", "alice"},
			[]scm.IssueComment{{Author: "szymon", Body: "a"}, {Author: "alice", Body: "b"}}, "alice"},
		{"non-maintainer only", []string{"szymon"},
			[]scm.IssueComment{{Author: "rando", Body: "do it"}}, ""},
		{"bot comment ignored", []string{"szymon"},
			[]scm.IssueComment{{Author: "bot", Body: "hi"}}, ""},
		{"empty approver list never releases", nil,
			[]scm.IssueComment{{Author: "szymon", Body: "go"}}, ""},
		{"no comments", []string{"szymon"}, nil, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr := triageReader{
				reader:    &commentReader{comments: tc.comments},
				botLogin:  "bot",
				approvers: tc.approvers,
				resolved:  true,
			}
			got, err := tr.approvingMaintainer(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestApprovingMaintainer_ReadErrorFailsClosed(t *testing.T) {
	tr := triageReader{reader: &errListReader{}, botLogin: "bot", approvers: []string{"szymon"}, resolved: true}
	got, err := tr.approvingMaintainer(context.Background())
	require.Error(t, err)
	require.Equal(t, "", got)
}
