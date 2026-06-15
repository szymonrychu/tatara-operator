package controller

// Tests covering audit findings for repository_controller.go.
// Each test name references the finding number from the spec.

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Finding 1: handleFinishedJob failure path returns ctrl.Result{} (no
// RequeueAfter), so the backoff retry never fires at the computed time.
// After a job failure the reconcile result must carry RequeueAfter >0.
func TestRepoReconcile_FailedJobRequeuesAtBackoff(t *testing.T) {
	mkProject(t, "rp-audit1", "rp-audit1-scm")
	mkSecret(t, "rp-audit1-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "audit1repo", "rp-audit1")
	setProjectMemoryReady(t, "rp-audit1", "http://mem-rp-audit1.tatara.svc:8080")

	if _, err := reconcileRepo(t, "audit1repo"); err != nil {
		t.Fatalf("launch reconcile: %v", err)
	}
	jobName := waitRepoJob(t, "audit1repo")
	markJob(t, jobName, batchv1.JobFailed)

	res, err := reconcileRepo(t, "audit1repo")
	if err != nil {
		t.Fatalf("post-failure reconcile: %v", err)
	}
	// The backoff for failure #1 is 30s; result must carry that as RequeueAfter
	// so the reconciler wakes at the computed time instead of waiting for the
	// 600s Job TTL to fire the owned-object watch.
	if res.RequeueAfter <= 0 {
		t.Errorf("handleFinishedJob failure must return RequeueAfter>0 (backoff duration), got %v", res.RequeueAfter)
	}
	// Sanity: the requeue must not exceed maxIngestBackoff.
	if res.RequeueAfter > maxIngestBackoff {
		t.Errorf("RequeueAfter = %v, must be <= maxIngestBackoff (%v)", res.RequeueAfter, maxIngestBackoff)
	}
}

// Finding 2: The reingest-requested annotation is never deleted after a
// successful ingest; it relies solely on timestamp ordering. After success the
// annotation must be removed.
func TestRepoReconcile_SuccessDeletesReingestAnnotation(t *testing.T) {
	mkProject(t, "rp-audit2", "rp-audit2-scm")
	mkSecret(t, "rp-audit2-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "audit2repo", "rp-audit2")
	setProjectMemoryReady(t, "rp-audit2", "http://mem-rp-audit2.tatara.svc:8080")

	// Seed a prior successful ingest so incremental path is taken.
	r := getRepo(t, "audit2repo")
	r.Status.LastIngestedCommit = "prevsha"
	lastTime := metav1.NewTime(time.Now().Add(-1 * time.Hour))
	r.Status.LastIngestTime = &lastTime
	r.Status.Phase = "Ingested"
	if err := k8sClient.Status().Update(context.Background(), r); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	// Set reingest annotation newer than lastIngestTime.
	r = getRepo(t, "audit2repo")
	if r.Annotations == nil {
		r.Annotations = map[string]string{}
	}
	r.Annotations[ReingestAnnotation] = time.Now().Format(time.RFC3339)
	if err := k8sClient.Update(context.Background(), r); err != nil {
		t.Fatalf("set annotation: %v", err)
	}

	// First reconcile launches the incremental job.
	if _, err := reconcileRepo(t, "audit2repo"); err != nil {
		t.Fatalf("launch reconcile: %v", err)
	}
	jobName := waitRepoJob(t, "audit2repo")

	// Simulate successful job completion.
	setResultSHA(t, "audit2repo", "newsha42")
	markJob(t, jobName, batchv1.JobComplete)

	if _, err := reconcileRepo(t, "audit2repo"); err != nil {
		t.Fatalf("post-success reconcile: %v", err)
	}

	got := getRepo(t, "audit2repo")
	if v, ok := got.Annotations[ReingestAnnotation]; ok && v != "" {
		t.Errorf("reingest annotation must be cleared after successful ingest, got %q", v)
	}
}

// Finding 3: scheduleNextReingest writes the annotation (spec Update) before
// persisting LastScheduledReingest (status Update), making the dedup key
// non-atomic. After the stamp pass, LastScheduledReingest must be set before
// the annotation change triggers a new reconcile.
// This test verifies the status is persisted atomically in the same pass.
func TestRepoReconcile_ScheduleWritesStatusBeforeAnnotation(t *testing.T) {
	mkProject(t, "rp-audit3", "rp-audit3-scm")
	mkSecret(t, "rp-audit3-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "audit3repo", "rp-audit3")
	setProjectMemoryReady(t, "rp-audit3", "http://mem-rp-audit3.tatara.svc:8080")

	r := getRepo(t, "audit3repo")
	r.Spec.ReingestSchedule = "* * * * *" // every minute - will be due
	if err := k8sClient.Update(context.Background(), r); err != nil {
		t.Fatalf("set schedule: %v", err)
	}
	setRepoIngested(t, "audit3repo", "shaAudit3", time.Now().Add(-1*time.Hour))

	if _, err := reconcileRepo(t, "audit3repo"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getRepo(t, "audit3repo")
	// The annotation must be stamped (schedule was due).
	if got.Annotations[ReingestAnnotation] == "" {
		t.Fatal("due schedule must stamp the reingest-requested annotation")
	}
	// LastScheduledReingest must be persisted in the same pass so a concurrent
	// reconcile sees the dedup key before the annotation watch triggers again.
	if got.Status.LastScheduledReingest == nil {
		t.Fatal("LastScheduledReingest must be persisted in the same pass as the annotation stamp")
	}
}

// Finding 4: Incremental ingest passes LastIngestedCommit as --since; after
// a force-push that SHA may be absent from history, wedging the repo in
// perpetual incremental failure. After failing IngestFailureCount past the
// threshold, the next attempt must fall back to a full ingest (since="").
func TestRepoReconcile_IncrementalFallsBackToFullAfterRepeatedFailures(t *testing.T) {
	mkProject(t, "rp-audit4", "rp-audit4-scm")
	mkSecret(t, "rp-audit4-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "audit4repo", "rp-audit4")
	setProjectMemoryReady(t, "rp-audit4", "http://mem-rp-audit4.tatara.svc:8080")

	// Seed state: prior successful ingest + a pending reingest annotation
	// + IngestFailureCount at the fallback threshold.
	r := getRepo(t, "audit4repo")
	r.Status.LastIngestedCommit = "goneSHAabc"
	lastTime := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	r.Status.LastIngestTime = &lastTime
	r.Status.Phase = "Failed"
	r.Status.IngestFailureCount = incrementalFallbackThreshold
	failTime := metav1.NewTime(time.Now().Add(-31 * time.Minute)) // past maxIngestBackoff
	r.Status.LastIngestFailureTime = &failTime
	if err := k8sClient.Status().Update(context.Background(), r); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	r = getRepo(t, "audit4repo")
	if r.Annotations == nil {
		r.Annotations = map[string]string{}
	}
	r.Annotations[ReingestAnnotation] = time.Now().Format(time.RFC3339)
	if err := k8sClient.Update(context.Background(), r); err != nil {
		t.Fatalf("set reingest annotation: %v", err)
	}

	if _, err := reconcileRepo(t, "audit4repo"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	waitRepoJob(t, "audit4repo")
	jobs := listIngestJobs(t, "audit4repo")
	if len(jobs) != 1 {
		t.Fatalf("jobs = %d, want 1", len(jobs))
	}
	script := jobs[0].Spec.Template.Spec.Containers[0].Args[0]
	if contains(script, "--since") {
		t.Errorf("after %d failures, ingest must fall back to full (no --since), got: %q",
			incrementalFallbackThreshold, script)
	}
}

// Finding 5: When ingestDecision returns want=false a stale IngestBackoff
// condition is never cleared. After failures followed by a want=false pass,
// IngestBackoff must be cleared when IngestFailureCount==0.
func TestRepoReconcile_WantFalseClearsStaleBBackoffCondition(t *testing.T) {
	mkProject(t, "rp-audit5", "rp-audit5-scm")
	mkSecret(t, "rp-audit5-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "audit5repo", "rp-audit5")
	setProjectMemoryReady(t, "rp-audit5", "http://mem-rp-audit5.tatara.svc:8080")

	// Seed a repo that has IngestFailureCount=0 but a lingering IngestBackoff=True
	// condition (as if a success reset the counter but the condition was not
	// cleared via the want=false path).
	r := getRepo(t, "audit5repo")
	r.Status.LastIngestedCommit = "shaAudit5"
	nowTime := metav1.NewTime(time.Now())
	r.Status.LastIngestTime = &nowTime
	r.Status.Phase = "Ingested"
	r.Status.IngestFailureCount = 0
	r.Status.LastIngestFailureTime = nil
	r.Status.Conditions = []metav1.Condition{
		{
			Type:               "IngestBackoff",
			Status:             metav1.ConditionTrue,
			Reason:             "IngestFailing",
			Message:            "stale",
			LastTransitionTime: metav1.Now(),
		},
	}
	if err := k8sClient.Status().Update(context.Background(), r); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	// No reingest annotation -> ingestDecision returns want=false.

	if _, err := reconcileRepo(t, "audit5repo"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getRepo(t, "audit5repo")
	c := apimeta.FindStatusCondition(got.Status.Conditions, "IngestBackoff")
	if c != nil && c.Status == metav1.ConditionTrue {
		t.Errorf("stale IngestBackoff=True must be cleared on want=false + IngestFailureCount==0, got %+v", c)
	}
}
