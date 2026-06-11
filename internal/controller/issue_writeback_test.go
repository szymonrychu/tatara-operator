package controller

import (
	"context"
	"sync"
	"testing"

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
}

func (f *fakeIssueWriter) CloseIssue(_ context.Context, _, repo string, _ int, comment string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls = append(f.closeCalls, repo+"|"+comment)
	return nil
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
