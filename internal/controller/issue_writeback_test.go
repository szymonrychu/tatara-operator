package controller

import (
	"context"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type fakeIssueWriter struct {
	scm.SCMWriter
	mu         sync.Mutex
	closeCalls []string // repo|comment
	closeErr   error    // returned by CloseIssue when set
}

func (f *fakeIssueWriter) CloseIssue(_ context.Context, _, repo string, _ int, comment string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls = append(f.closeCalls, repo+"|"+comment)
	return f.closeErr
}

func TestWriteBackIssueClose(t *testing.T) {
	ctx := context.Background()
	fw := &fakeIssueWriter{}
	r := newWriteBackReconciler(t, &fakeWriter{}) // reuse harness for client/metrics
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "iw-scm", Namespace: testNS}, Data: map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")}}
	_ = k8sClient.Create(ctx, sec)
	proj := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "iw-proj", Namespace: testNS}, Spec: tatarav1alpha1.ProjectSpec{ScmSecretRef: "iw-scm", Scm: &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "bot"}}}
	_ = k8sClient.Create(ctx, proj)
	repo := &tatarav1alpha1.Repository{ObjectMeta: metav1.ObjectMeta{Name: "iw-repo", Namespace: testNS}, Spec: tatarav1alpha1.RepositorySpec{ProjectRef: "iw-proj", URL: "https://github.com/o/r.git", DefaultBranch: "main", ReingestSchedule: "0 6 * * *"}}
	_ = k8sClient.Create(ctx, repo)

	task := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "iw-task", Namespace: testNS}}
	task.Spec = tatarav1alpha1.TaskSpec{ProjectRef: "iw-proj", RepositoryRef: "iw-repo", Goal: "g", Kind: "triageIssue",
		Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#7", Number: 7}}
	_ = k8sClient.Create(ctx, task)
	task.Status.Phase = "Succeeded"
	task.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: "close", Comment: "out of scope"}
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{Type: "WritebackPending", Status: metav1.ConditionTrue, Reason: "x", Message: "x"})
	_ = k8sClient.Status().Update(ctx, task)

	if _, err := reconcileWriteback(t, r, "iw-task"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.closeCalls) != 1 || fw.closeCalls[0] != "o/r|out of scope" {
		t.Fatalf("CloseIssue calls = %+v", fw.closeCalls)
	}
}

// TestWriteBackIssueClose_TargetGone: a writeback close against a permanently
// gone issue (410 deleted) must not return an error (no requeue), must clear the
// WritebackPending gate, and must count the write as result="gone" not "error"
// (issue #268).
func TestWriteBackIssueClose_TargetGone(t *testing.T) {
	ctx := context.Background()
	fw := &fakeIssueWriter{closeErr: &scm.HTTPError{Status: 410, Path: "/repos/o/r/issues/8/comments", Body: `{"message":"This issue was deleted"}`}}
	r := newWriteBackReconciler(t, &fakeWriter{})
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "iwg-scm", Namespace: testNS}, Data: map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")}}
	_ = k8sClient.Create(ctx, sec)
	proj := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "iwg-proj", Namespace: testNS}, Spec: tatarav1alpha1.ProjectSpec{ScmSecretRef: "iwg-scm", Scm: &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "bot"}}}
	_ = k8sClient.Create(ctx, proj)
	repo := &tatarav1alpha1.Repository{ObjectMeta: metav1.ObjectMeta{Name: "iwg-repo", Namespace: testNS}, Spec: tatarav1alpha1.RepositorySpec{ProjectRef: "iwg-proj", URL: "https://github.com/o/r.git", DefaultBranch: "main", ReingestSchedule: "0 6 * * *"}}
	_ = k8sClient.Create(ctx, repo)

	task := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "iwg-task", Namespace: testNS}}
	task.Spec = tatarav1alpha1.TaskSpec{ProjectRef: "iwg-proj", RepositoryRef: "iwg-repo", Goal: "g", Kind: "triageIssue",
		Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#8", Number: 8}}
	_ = k8sClient.Create(ctx, task)
	task.Status.Phase = "Succeeded"
	task.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: "close", Comment: "out of scope"}
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{Type: "WritebackPending", Status: metav1.ConditionTrue, Reason: "x", Message: "x"})
	_ = k8sClient.Status().Update(ctx, task)

	if _, err := reconcileWriteback(t, r, "iwg-task"); err != nil {
		t.Fatalf("reconcile must not error (no requeue) on a gone target: %v", err)
	}

	// WritebackPending must be cleared (not left True to retry-loop).
	got := fetchTask(t, "iwg-task")
	if c := apimeta.FindStatusCondition(got.Status.Conditions, "WritebackPending"); c != nil && c.Status == metav1.ConditionTrue {
		t.Errorf("WritebackPending still True after gone close; want cleared")
	}

	if v := testutil.ToFloat64(r.Metrics.SCMWriteCounter("github", "close_issue", "gone")); v != 1 {
		t.Errorf("close_issue{result=gone} = %v, want 1", v)
	}
	if v := testutil.ToFloat64(r.Metrics.SCMWriteCounter("github", "close_issue", "error")); v != 0 {
		t.Errorf("close_issue{result=error} = %v, want 0 for a gone target", v)
	}
}
