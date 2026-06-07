package controller

import (
	"context"
	"testing"
	"time"

	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func markJob(t *testing.T, name string, cond batchv1.JobConditionType) {
	t.Helper()
	job := &batchv1.Job{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: name}, job); err != nil {
		t.Fatalf("get job %s: %v", name, err)
	}
	now := metav1.Now()
	// K8s 1.33 requires prerequisite conditions before terminal ones can be set:
	// Complete requires SuccessCriteriaMet=True first; Failed requires FailureTarget=True.
	var conditions []batchv1.JobCondition
	switch cond {
	case batchv1.JobComplete:
		conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now},
		}
	case batchv1.JobFailed:
		conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, LastTransitionTime: now},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: now},
		}
	default:
		conditions = []batchv1.JobCondition{
			{Type: cond, Status: corev1.ConditionTrue, LastTransitionTime: now},
		}
	}
	job.Status.Conditions = conditions
	job.Status.StartTime = &metav1.Time{Time: now.Add(-30 * time.Second)}
	if cond == batchv1.JobComplete {
		job.Status.CompletionTime = &now
	}
	if err := k8sClient.Status().Update(context.Background(), job); err != nil {
		t.Fatalf("update job status %s: %v", name, err)
	}
}

func setResultSHA(t *testing.T, repoName, sha string) {
	t.Helper()
	cm := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: repoName + "-ingest-result"}, cm); err != nil {
		t.Fatalf("get result cm: %v", err)
	}
	cm.Data = map[string]string{"sha": sha}
	if err := k8sClient.Update(context.Background(), cm); err != nil {
		t.Fatalf("update result cm: %v", err)
	}
}

func TestRepoReconcile_JobSuccessAppliesSHA(t *testing.T) {
	mkProject(t, "rp-ok", "rp-ok-scm")
	mkSecret(t, "rp-ok-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "okrepo", "rp-ok")
	setProjectMemoryReady(t, "rp-ok", "http://mem-rp-ok.tatara.svc:8080")

	if _, err := reconcileRepo(t, "okrepo"); err != nil {
		t.Fatalf("launch reconcile: %v", err)
	}
	jobName := waitRepoJob(t, "okrepo")

	setResultSHA(t, "okrepo", "deadbeef")
	markJob(t, jobName, batchv1.JobComplete)

	if _, err := reconcileRepo(t, "okrepo"); err != nil {
		t.Fatalf("post-completion reconcile: %v", err)
	}

	got := getRepo(t, "okrepo")
	if got.Status.LastIngestedCommit != "deadbeef" {
		t.Errorf("lastIngestedCommit = %q, want deadbeef", got.Status.LastIngestedCommit)
	}
	if got.Status.Phase != "Ingested" {
		t.Errorf("phase = %q, want Ingested", got.Status.Phase)
	}
	if got.Status.JobName != "" {
		t.Errorf("jobName = %q, want cleared", got.Status.JobName)
	}
	if got.Status.LastIngestTime == nil || got.Status.LastIngestTime.IsZero() {
		t.Error("lastIngestTime not set")
	}
	c := apimeta.FindStatusCondition(got.Status.Conditions, "Ingested")
	if c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("Ingested condition = %+v, want True", c)
	}
}

func TestRepoReconcile_JobFailureSetsFailed(t *testing.T) {
	mkProject(t, "rp-bad", "rp-bad-scm")
	mkSecret(t, "rp-bad-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "badrepo", "rp-bad")
	setProjectMemoryReady(t, "rp-bad", "http://mem-rp-bad.tatara.svc:8080")

	if _, err := reconcileRepo(t, "badrepo"); err != nil {
		t.Fatalf("launch reconcile: %v", err)
	}
	jobName := waitRepoJob(t, "badrepo")
	markJob(t, jobName, batchv1.JobFailed)

	if _, err := reconcileRepo(t, "badrepo"); err != nil {
		t.Fatalf("post-failure reconcile: %v", err)
	}

	got := getRepo(t, "badrepo")
	if got.Status.Phase != "Failed" {
		t.Errorf("phase = %q, want Failed", got.Status.Phase)
	}
	if got.Status.JobName != "" {
		t.Errorf("jobName = %q, want cleared", got.Status.JobName)
	}
	c := apimeta.FindStatusCondition(got.Status.Conditions, "Ingested")
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != "IngestFailed" {
		t.Errorf("Ingested condition = %+v, want False/IngestFailed", c)
	}
}

var _ = tataradevv1alpha1.Project{}
