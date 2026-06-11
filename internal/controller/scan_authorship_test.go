package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type humanAuthorWriter struct {
	scm.SCMWriter
}

func (humanAuthorWriter) GetPRState(context.Context, string, string, int) (scm.PRState, error) {
	return scm.PRState{Author: "human-dev"}, nil // NOT the bot
}

func TestCronSelfImproveOnHumanPRTerminates(t *testing.T) {
	ctx := context.Background()
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "auth-scm", Namespace: testNS}, Data: map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")}}
	_ = k8sClient.Create(ctx, sec)
	proj := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "auth-proj", Namespace: testNS}, Spec: tatarav1alpha1.ProjectSpec{ScmSecretRef: "auth-scm", Scm: &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"}}}
	_ = k8sClient.Create(ctx, proj)
	// memory Ready so the task is not gated for that reason
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Ready", Endpoint: "http://mem"}
	_ = k8sClient.Status().Update(ctx, proj)
	repo := &tatarav1alpha1.Repository{ObjectMeta: metav1.ObjectMeta{Name: "auth-repo", Namespace: testNS}, Spec: tatarav1alpha1.RepositorySpec{ProjectRef: "auth-proj", URL: "https://github.com/o/r.git", DefaultBranch: "main", ReingestSchedule: "0 6 * * *"}}
	_ = k8sClient.Create(ctx, repo)

	task := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "auth-task", Namespace: testNS,
		Labels: scanTaskLabels(candidate{repo: "o/r", number: 9, headSHA: "abc", isPR: true}, "mrScan", "selfImprove")}}
	task.Spec = tatarav1alpha1.TaskSpec{ProjectRef: "auth-proj", RepositoryRef: "auth-repo", Goal: "g", Kind: "selfImprove",
		Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#9", Number: 9, IsPR: true, AuthorLogin: "tatara-bot"}}
	_ = k8sClient.Create(ctx, task)

	r := newWriteBackReconciler(t, &fakeWriter{})
	r.SCMFor = func(string) (scm.SCMWriter, error) { return humanAuthorWriter{}, nil }
	if _, err := reconcileWriteback(t, r, "auth-task"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := &tatarav1alpha1.Task{}
	_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "auth-task"}, got)
	if got.Status.Phase != "Failed" {
		t.Fatalf("cron selfImprove on human PR must terminate Failed, got phase=%q", got.Status.Phase)
	}
}
