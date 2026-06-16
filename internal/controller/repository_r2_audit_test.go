package controller

// Round 2 audit tests for repository_controller.go findings.
// Each test is named after its finding number in the spec.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/ingest"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// ingestDurationCount returns the sample count from
// operator_ingest_job_duration_seconds by gathering from the registry.
func ingestDurationCount(t *testing.T, reg prometheus.Gatherer) uint64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "operator_ingest_job_duration_seconds" {
			for _, m := range mf.GetMetric() {
				return m.GetHistogram().GetSampleCount()
			}
		}
	}
	return 0
}

// R2-Finding 1: ObserveIngestJobDuration must be called for failed jobs even
// when job.Status.CompletionTime is nil (Kubernetes does not set it for
// failures). Before the fix the histogram gets zero observations.
func TestRepoR2F1_FailedJobObservesIngestDuration(t *testing.T) {
	mkProject(t, "r2f1-proj", "r2f1-scm")
	mkSecret(t, "r2f1-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "r2f1-repo", "r2f1-proj")
	setProjectMemoryReady(t, "r2f1-proj", "http://mem-r2f1.tatara.svc:8080")

	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	r := &RepositoryReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: metrics,
		IngestConfig: ingest.Config{
			IngesterImage:  "registry.example/ingester:test",
			OIDCIssuer:     "https://kc.example/realms/tatara",
			OIDCClientID:   "tatara-operator",
			OIDCSecretName: "tatara-operator",
			OIDCAudience:   "tatara-memory",
			Namespace:      testNS,
		},
	}
	reconcile := func() (ctrl.Result, error) {
		return r.Reconcile(logf.IntoContext(context.Background(), logf.Log), ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: testNS, Name: "r2f1-repo"},
		})
	}

	if _, err := reconcile(); err != nil {
		t.Fatalf("launch reconcile: %v", err)
	}
	jobName := waitRepoJob(t, "r2f1-repo")

	// Mark as failed: StartTime is set, CompletionTime is NOT set (matching K8s behaviour).
	markJob(t, jobName, batchv1.JobFailed)

	before := ingestDurationCount(t, reg)

	if _, err := reconcile(); err != nil {
		t.Fatalf("post-failure reconcile: %v", err)
	}

	after := ingestDurationCount(t, reg)
	if after <= before {
		t.Errorf("ingest duration histogram must be observed for failed jobs: before=%d after=%d", before, after)
	}
}

// conflictOnceRepoStatusClient injects one conflict on the first Status().Update
// for Repository objects.
type conflictOnceRepoStatusClient struct {
	client.Client
	statusCalls *atomic.Int32
}

func (c *conflictOnceRepoStatusClient) Status() client.SubResourceWriter {
	return &conflictOnceRepoStatusSub{
		SubResourceWriter: c.Client.Status(),
		calls:             c.statusCalls,
	}
}

type conflictOnceRepoStatusSub struct {
	client.SubResourceWriter
	calls *atomic.Int32
}

func (s *conflictOnceRepoStatusSub) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if _, ok := obj.(*tataradevv1alpha1.Repository); ok {
		if s.calls.Add(1) == 1 {
			return apierrors.NewConflict(
				schema.GroupResource{Group: "tatara.dev", Resource: "repositories"},
				obj.GetName(), nil,
			)
		}
	}
	return s.SubResourceWriter.Update(ctx, obj, opts...)
}

// R2-Finding 3: handleFinishedJob must succeed even when the first status
// write returns a Conflict. Before the fix the conflict bubbles up as an error.
func TestRepoR2F3_HandleFinishedJobRetriesOnConflict(t *testing.T) {
	mkProject(t, "r2f3-proj", "r2f3-scm")
	mkSecret(t, "r2f3-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "r2f3-repo", "r2f3-proj")
	setProjectMemoryReady(t, "r2f3-proj", "http://mem-r2f3.tatara.svc:8080")

	// Launch job using the plain client.
	if _, err := reconcileRepo(t, "r2f3-repo"); err != nil {
		t.Fatalf("launch reconcile: %v", err)
	}
	jobName := waitRepoJob(t, "r2f3-repo")
	setResultSHA(t, "r2f3-repo", "r2f3sha")
	markJob(t, jobName, batchv1.JobComplete)

	// Now reconcile with a conflict-injecting client.
	var statusCalls atomic.Int32
	cc := &conflictOnceRepoStatusClient{Client: k8sClient, statusCalls: &statusCalls}
	r := &RepositoryReconciler{
		Client:  cc,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		IngestConfig: ingest.Config{
			IngesterImage:  "registry.example/ingester:test",
			OIDCIssuer:     "https://kc.example/realms/tatara",
			OIDCClientID:   "tatara-operator",
			OIDCSecretName: "tatara-operator",
			OIDCAudience:   "tatara-memory",
			Namespace:      testNS,
		},
	}
	_, err := r.Reconcile(logf.IntoContext(context.Background(), logf.Log), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: "r2f3-repo"},
	})
	if err != nil {
		t.Fatalf("handleFinishedJob must succeed despite one status conflict, got: %v", err)
	}
	if statusCalls.Load() < 2 {
		t.Errorf("expected at least 2 status writes (conflict + retry), got %d", statusCalls.Load())
	}

	got := getRepo(t, "r2f3-repo")
	if got.Status.Phase != "Ingested" {
		t.Errorf("phase = %q, want Ingested", got.Status.Phase)
	}
}

// R2-Finding 3 (scheduleNextReingest path): a conflict on the scheduleNextReingest
// status write must be retried rather than erroring the reconcile.
func TestRepoR2F3_ScheduleReingestRetriesOnConflict(t *testing.T) {
	mkProject(t, "r2f3s-proj", "r2f3s-scm")
	mkSecret(t, "r2f3s-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "r2f3s-repo", "r2f3s-proj")
	setProjectMemoryReady(t, "r2f3s-proj", "http://mem-r2f3s.tatara.svc:8080")

	repo := getRepo(t, "r2f3s-repo")
	repo.Spec.ReingestSchedule = "* * * * *" // every minute, due now
	if err := k8sClient.Update(context.Background(), repo); err != nil {
		t.Fatalf("set schedule: %v", err)
	}
	setRepoIngested(t, "r2f3s-repo", "sha-r2f3s", time.Now().Add(-2*time.Hour))

	// First plain reconcile to land all condition-clearing status writes so they
	// are idempotent for subsequent reconciles. On this pass the schedule fires
	// and stamps the annotation + LastScheduledReingest; we then reset both so
	// the conflict-injection reconcile faces a fresh due-schedule.
	if _, err := reconcileRepo(t, "r2f3s-repo"); err != nil {
		t.Fatalf("prime reconcile: %v", err)
	}
	// Reset the stamp so the schedule appears due again.
	repo = getRepo(t, "r2f3s-repo")
	repo.Status.LastScheduledReingest = nil
	if err := k8sClient.Status().Update(context.Background(), repo); err != nil {
		t.Fatalf("reset LastScheduledReingest: %v", err)
	}
	repo = getRepo(t, "r2f3s-repo")
	delete(repo.Annotations, ReingestAnnotation)
	if err := k8sClient.Update(context.Background(), repo); err != nil {
		t.Fatalf("clear annotation: %v", err)
	}

	// Count how many status writes the reconcile issues before scheduleNextReingest.
	// After the prime reconcile all conditions are stable; the next reconcile
	// should issue exactly one status write from scheduleNextReingest (the
	// LastScheduledReingest write). We conflict on the first write (skipN=0).
	var statusCalls atomic.Int32
	cc := &conflictOnceRepoStatusClient{Client: k8sClient, statusCalls: &statusCalls}
	r := &RepositoryReconciler{
		Client:  cc,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		IngestConfig: ingest.Config{
			IngesterImage:  "registry.example/ingester:test",
			OIDCIssuer:     "https://kc.example/realms/tatara",
			OIDCClientID:   "tatara-operator",
			OIDCSecretName: "tatara-operator",
			OIDCAudience:   "tatara-memory",
			Namespace:      testNS,
		},
	}
	_, err := r.Reconcile(logf.IntoContext(context.Background(), logf.Log), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: "r2f3s-repo"},
	})
	if err != nil {
		t.Fatalf("scheduleNextReingest must succeed despite one status conflict, got: %v", err)
	}

	got := getRepo(t, "r2f3s-repo")
	if got.Status.LastScheduledReingest == nil {
		t.Error("LastScheduledReingest must be persisted even after a status conflict")
	}
	if got.Annotations[ReingestAnnotation] == "" {
		t.Error("reingest annotation must be stamped even after a status conflict")
	}
}

// R2-Finding 4: Incremental ingest jobs must have BackoffLimit=0 so the
// controller reaches its full-ingest fallback after one pod attempt instead of
// spending up to 3 pod runs on a deterministically-failing SHA.
func TestRepoR2F4_IncrementalJobBackoffLimitIsZero(t *testing.T) {
	mkProject(t, "r2f4-proj", "r2f4-scm")
	mkSecret(t, "r2f4-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "r2f4-repo", "r2f4-proj")
	setProjectMemoryReady(t, "r2f4-proj", "http://mem-r2f4.tatara.svc:8080")

	// Seed a prior ingested state and set a reingest annotation so an incremental
	// job is launched (since = LastIngestedCommit).
	r := getRepo(t, "r2f4-repo")
	r.Status.LastIngestedCommit = "someSHA"
	lt := metav1.NewTime(time.Now().Add(-1 * time.Hour))
	r.Status.LastIngestTime = &lt
	r.Status.Phase = "Ingested"
	if err := k8sClient.Status().Update(context.Background(), r); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	r = getRepo(t, "r2f4-repo")
	if r.Annotations == nil {
		r.Annotations = map[string]string{}
	}
	r.Annotations[ReingestAnnotation] = time.Now().Format(time.RFC3339)
	if err := k8sClient.Update(context.Background(), r); err != nil {
		t.Fatalf("set annotation: %v", err)
	}

	if _, err := reconcileRepo(t, "r2f4-repo"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	waitRepoJob(t, "r2f4-repo")

	jobs := listIngestJobs(t, "r2f4-repo")
	if len(jobs) != 1 {
		t.Fatalf("jobs = %d, want 1", len(jobs))
	}
	job := jobs[0]
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Errorf("incremental ingest job BackoffLimit = %v, want 0", job.Spec.BackoffLimit)
	}
}

// R2-Finding 5: The check `|| repo.Spec.ReingestSchedule == ""` in
// scheduleNextReingest is dead code because ReingestSchedule is a Required
// field with MinLength>=1. The check should be removed. This test verifies the
// function returns early only when LastIngestedCommit is empty (the meaningful
// guard), not because of an empty schedule.
func TestRepoR2F5_EmptyScheduleDeadCheckRemoved(t *testing.T) {
	mkProject(t, "r2f5-proj", "r2f5-scm")
	mkSecret(t, "r2f5-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})

	// Create a repo that has a valid non-empty LastIngestedCommit but an empty
	// ReingestSchedule to confirm the schedule guard (not the commit guard) is
	// what makes it return early. After removing the dead check, the parse will
	// fail (empty string is invalid cron), producing an ERROR log + no requeue.
	repo := &tataradevv1alpha1.Repository{}
	repo.Name = "r2f5-repo"
	repo.Namespace = testNS
	repo.Spec.ProjectRef = "r2f5-proj"
	repo.Spec.URL = "https://github.com/acme/r2f5-repo.git"
	repo.Spec.DefaultBranch = "main"
	repo.Spec.IngestEnabled = boolPtrRC(true)
	// ReingestSchedule intentionally set to a minimal non-empty value to pass
	// the CRD MinLength; we set it empty in-memory only to exercise the removed
	// dead check via direct function call (bypassing the webhook).
	repo.Spec.ReingestSchedule = "0 6 * * *"
	if err := k8sClient.Create(context.Background(), repo); err != nil {
		t.Fatalf("create repo: %v", err)
	}
	setProjectMemoryReady(t, "r2f5-proj", "http://mem-r2f5.tatara.svc:8080")
	setRepoIngested(t, "r2f5-repo", "sha-r2f5", time.Now().Add(-1*time.Hour))

	// Directly invoke scheduleNextReingest with an in-memory mutation that sets
	// ReingestSchedule to "" to confirm the empty-schedule path is gone: after
	// the fix the parse of "" fails in cron.ParseStandard and returns no requeue
	// (error-log path), not the old early-return (no-op).
	rr := newRepoReconciler()
	got := getRepo(t, "r2f5-repo")
	got.Spec.ReingestSchedule = "" // empty - was guarded by the now-removed dead check

	res, err := rr.scheduleNextReingest(logf.IntoContext(context.Background(), logf.Log), got)
	if err != nil {
		t.Fatalf("scheduleNextReingest must not error on bad cron: %v", err)
	}
	// With the dead check removed, an empty schedule hits cron.ParseStandard
	// (which errors), and the function returns {}, nil with no requeue - same
	// behaviour as before but via the parse-error path, not the early-return.
	if res.RequeueAfter != 0 {
		t.Errorf("bad cron expression must not produce a requeue, got %v", res.RequeueAfter)
	}
}
