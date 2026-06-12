package controller

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestIngestBackoff verifies the exponential schedule:
// 1 failure -> 30s, 2 -> 1m, 3 -> 2m, 4 -> 4m, large -> capped 30m.
func TestIngestBackoff(t *testing.T) {
	cases := []struct {
		failures int
		want     time.Duration
	}{
		{1, 30 * time.Second},
		{2, 1 * time.Minute},
		{3, 2 * time.Minute},
		{4, 4 * time.Minute},
		{100, 30 * time.Minute},
	}
	for _, tc := range cases {
		t.Run("", func(t *testing.T) {
			got := ingestBackoff(tc.failures)
			if got != tc.want {
				t.Errorf("ingestBackoff(%d) = %v, want %v", tc.failures, got, tc.want)
			}
		})
	}
}

// TestRepoReconcile_HandleFinishedJob_FailedIncrementsCounter verifies that
// handleFinishedJob on a failed Job increments IngestFailureCount and stamps
// LastIngestFailureTime.
func TestRepoReconcile_HandleFinishedJob_FailedIncrementsCounter(t *testing.T) {
	mkProject(t, "rp-bf1", "rp-bf1-scm")
	mkSecret(t, "rp-bf1-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "bfrepo1", "rp-bf1")
	setProjectMemoryReady(t, "rp-bf1", "http://mem-rp-bf1.tatara.svc:8080")

	if _, err := reconcileRepo(t, "bfrepo1"); err != nil {
		t.Fatalf("launch reconcile: %v", err)
	}
	jobName := waitRepoJob(t, "bfrepo1")
	markJob(t, jobName, batchv1.JobFailed)

	if _, err := reconcileRepo(t, "bfrepo1"); err != nil {
		t.Fatalf("post-failure reconcile: %v", err)
	}

	got := getRepo(t, "bfrepo1")
	if got.Status.IngestFailureCount != 1 {
		t.Errorf("IngestFailureCount = %d, want 1", got.Status.IngestFailureCount)
	}
	if got.Status.LastIngestFailureTime == nil {
		t.Error("LastIngestFailureTime must be set after failure")
	}
}

// TestRepoReconcile_HandleFinishedJob_SuccessResetsCounter verifies that
// handleFinishedJob on a succeeded Job resets IngestFailureCount to 0 and
// clears LastIngestFailureTime.
func TestRepoReconcile_HandleFinishedJob_SuccessResetsCounter(t *testing.T) {
	mkProject(t, "rp-bf2", "rp-bf2-scm")
	mkSecret(t, "rp-bf2-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "bfrepo2", "rp-bf2")
	setProjectMemoryReady(t, "rp-bf2", "http://mem-rp-bf2.tatara.svc:8080")

	if _, err := reconcileRepo(t, "bfrepo2"); err != nil {
		t.Fatalf("launch reconcile: %v", err)
	}
	jobName := waitRepoJob(t, "bfrepo2")

	// Seed a prior failure count so we can verify the reset.
	r := getRepo(t, "bfrepo2")
	r.Status.IngestFailureCount = 3
	failTime := metav1.NewTime(time.Now().Add(-5 * time.Minute))
	r.Status.LastIngestFailureTime = &failTime
	if err := k8sClient.Status().Update(context.Background(), r); err != nil {
		t.Fatalf("seed failure status: %v", err)
	}

	setResultSHA(t, "bfrepo2", "abc123")
	markJob(t, jobName, batchv1.JobComplete)

	if _, err := reconcileRepo(t, "bfrepo2"); err != nil {
		t.Fatalf("post-success reconcile: %v", err)
	}

	got := getRepo(t, "bfrepo2")
	if got.Status.IngestFailureCount != 0 {
		t.Errorf("IngestFailureCount = %d, want 0 after success", got.Status.IngestFailureCount)
	}
	if got.Status.LastIngestFailureTime != nil {
		t.Errorf("LastIngestFailureTime = %v, want nil after success", got.Status.LastIngestFailureTime)
	}
}

// TestRepoReconcile_BackoffBlocksNewJob verifies that when IngestFailureCount>0
// and LastIngestFailureTime is within the backoff window, no new Job is created,
// RequeueAfter>0, and an IngestBackoff condition is set.
func TestRepoReconcile_BackoffBlocksNewJob(t *testing.T) {
	mkProject(t, "rp-bf3", "rp-bf3-scm")
	mkSecret(t, "rp-bf3-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "bfrepo3", "rp-bf3")
	setProjectMemoryReady(t, "rp-bf3", "http://mem-rp-bf3.tatara.svc:8080")

	// Repo has no prior ingest (ingestDecision returns want=true for first ingest),
	// so seed a failure state to make the backoff fire before a Job is created.
	r := getRepo(t, "bfrepo3")
	r.Status.IngestFailureCount = 1
	failTime := metav1.NewTime(time.Now().Add(-5 * time.Second)) // 5s ago, backoff=30s
	r.Status.LastIngestFailureTime = &failTime
	if err := k8sClient.Status().Update(context.Background(), r); err != nil {
		t.Fatalf("seed failure: %v", err)
	}

	res, err := reconcileRepo(t, "bfrepo3")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if jobs := listIngestJobs(t, "bfrepo3"); len(jobs) != 0 {
		t.Fatalf("backoff must block Job creation, got %d jobs", len(jobs))
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("backoff must set RequeueAfter, got %v", res.RequeueAfter)
	}

	got := getRepo(t, "bfrepo3")
	c := apimeta.FindStatusCondition(got.Status.Conditions, "IngestBackoff")
	if c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("IngestBackoff condition = %+v, want True", c)
	}
}

// TestRepoReconcile_BackoffElapsedAllowsJob verifies that once the backoff
// window has passed (LastIngestFailureTime far in the past), a new Job IS
// created and the IngestBackoff condition is cleared.
func TestRepoReconcile_BackoffElapsedAllowsJob(t *testing.T) {
	mkProject(t, "rp-bf4", "rp-bf4-scm")
	mkSecret(t, "rp-bf4-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "bfrepo4", "rp-bf4")
	setProjectMemoryReady(t, "rp-bf4", "http://mem-rp-bf4.tatara.svc:8080")

	// Seed failure state but with LastIngestFailureTime well outside the window.
	r := getRepo(t, "bfrepo4")
	r.Status.IngestFailureCount = 1
	failTime := metav1.NewTime(time.Now().Add(-10 * time.Minute)) // far past the 30s backoff
	r.Status.LastIngestFailureTime = &failTime
	if err := k8sClient.Status().Update(context.Background(), r); err != nil {
		t.Fatalf("seed failure: %v", err)
	}

	res, err := reconcileRepo(t, "bfrepo4")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if jobs := listIngestJobs(t, "bfrepo4"); len(jobs) != 1 {
		t.Fatalf("elapsed backoff must allow Job creation, got %d jobs", len(jobs))
	}
	if res.RequeueAfter != 0 {
		t.Errorf("elapsed backoff must not block (RequeueAfter=0), got %v", res.RequeueAfter)
	}

	got := getRepo(t, "bfrepo4")
	c := apimeta.FindStatusCondition(got.Status.Conditions, "IngestBackoff")
	if c != nil && c.Status == metav1.ConditionTrue {
		t.Errorf("IngestBackoff condition must be cleared/False after successful launch, got %+v", c)
	}
}
