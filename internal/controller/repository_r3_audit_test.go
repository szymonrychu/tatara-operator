package controller

// Round 3 audit tests for repository_controller.go findings.

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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// conflictOnceMetaClient wraps k8sClient, injecting one conflict on the first
// metadata Update (r.Update, not r.Status().Update) for Repository objects.
type conflictOnceMetaClient struct {
	client.Client
	calls *atomic.Int32
}

func (c *conflictOnceMetaClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if _, ok := obj.(*tataradevv1alpha1.Repository); ok {
		if c.calls.Add(1) == 1 {
			return apierrors.NewConflict(
				schema.GroupResource{Group: "tatara.dev", Resource: "repositories"},
				obj.GetName(), nil,
			)
		}
	}
	return c.Client.Update(ctx, obj, opts...)
}

// R3-Finding 1: ensureResultConfigMap must reset data["sha"] to "" before each
// launch so a stale value from the previous ingest does not slip through when
// the cache delivers a Complete condition before the CM patch propagates.
// Before the fix, an incremental ingest that follows a successful ingest reads
// the PRIOR sha from the CM and records it as LastIngestedCommit, regressing
// the repo's recorded HEAD.
func TestRepoR3F1_EnsureResultConfigMapResetsShaBetweenIngests(t *testing.T) {
	mkProject(t, "r3f1-proj", "r3f1-scm")
	mkSecret(t, "r3f1-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "r3f1-repo", "r3f1-proj")
	setProjectMemoryReady(t, "r3f1-proj", "http://mem-r3f1.tatara.svc:8080")

	// First full ingest: launch -> succeed with sha "sha-first".
	if _, err := reconcileRepo(t, "r3f1-repo"); err != nil {
		t.Fatalf("first launch reconcile: %v", err)
	}
	jobName := waitRepoJob(t, "r3f1-repo")
	setResultSHA(t, "r3f1-repo", "sha-first")
	markJob(t, jobName, batchv1.JobComplete)
	if _, err := reconcileRepo(t, "r3f1-repo"); err != nil {
		t.Fatalf("first finish reconcile: %v", err)
	}
	if getRepo(t, "r3f1-repo").Status.LastIngestedCommit != "sha-first" {
		t.Fatalf("seeding: expected LastIngestedCommit=sha-first")
	}

	// Trigger a second (incremental) ingest. Use a future timestamp so the
	// annotation is guaranteed to be newer than LastIngestTime (which is set to
	// metav1.Now() in handleFinishedJob - same second resolution, so we add 2s).
	r := getRepo(t, "r3f1-repo")
	if r.Annotations == nil {
		r.Annotations = map[string]string{}
	}
	r.Annotations[ReingestAnnotation] = time.Now().Add(2 * time.Second).UTC().Format(time.RFC3339)
	if err := k8sClient.Update(context.Background(), r); err != nil {
		t.Fatalf("set reingest annotation: %v", err)
	}

	// Launch second ingest - this also calls ensureResultConfigMap.
	if _, err := reconcileRepo(t, "r3f1-repo"); err != nil {
		t.Fatalf("second launch reconcile: %v", err)
	}

	// After the second launch, the result CM's sha must be "" (reset), not "sha-first".
	cm := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: "r3f1-repo-ingest-result"}, cm); err != nil {
		t.Fatalf("get result cm: %v", err)
	}
	if cm.Data["sha"] != "" {
		t.Errorf("ensureResultConfigMap must reset sha to \"\" before launch, got %q", cm.Data["sha"])
	}
}

// R3-Finding 1 (regression guard): when the result CM sha is "" on a completed
// job, readResultSHA must return an error so the reconcile retries rather than
// recording an empty LastIngestedCommit.
func TestRepoR3F1_EmptyShaOnSuccessRetriesReconcile(t *testing.T) {
	mkProject(t, "r3f1b-proj", "r3f1b-scm")
	mkSecret(t, "r3f1b-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "r3f1b-repo", "r3f1b-proj")
	setProjectMemoryReady(t, "r3f1b-proj", "http://mem-r3f1b.tatara.svc:8080")

	if _, err := reconcileRepo(t, "r3f1b-repo"); err != nil {
		t.Fatalf("launch reconcile: %v", err)
	}
	jobName := waitRepoJob(t, "r3f1b-repo")
	// Mark complete but do NOT set the result sha (simulating the cache race).
	markJob(t, jobName, batchv1.JobComplete)

	_, err := reconcileRepo(t, "r3f1b-repo")
	if err == nil {
		t.Error("reconcile must return an error when result sha is empty (CM not yet patched by Job)")
	}
	// LastIngestedCommit must remain empty - no regression to a stale value.
	got := getRepo(t, "r3f1b-repo")
	if got.Status.LastIngestedCommit != "" {
		t.Errorf("LastIngestedCommit must not be updated when sha is empty, got %q", got.Status.LastIngestedCommit)
	}
}

// R3-Finding 2: scheduleNextReingest annotation update must be wrapped in
// RetryOnConflict so a transient conflict from an external writer does not
// error the reconcile.
func TestRepoR3F2_ScheduleAnnotationRetriesOnConflict(t *testing.T) {
	mkProject(t, "r3f2-proj", "r3f2-scm")
	mkSecret(t, "r3f2-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "r3f2-repo", "r3f2-proj")
	setProjectMemoryReady(t, "r3f2-proj", "http://mem-r3f2.tatara.svc:8080")

	// Schedule fires every minute and the repo was ingested an hour ago -> due now.
	r := getRepo(t, "r3f2-repo")
	r.Spec.ReingestSchedule = "* * * * *"
	if err := k8sClient.Update(context.Background(), r); err != nil {
		t.Fatalf("set schedule: %v", err)
	}
	setRepoIngested(t, "r3f2-repo", "sha-r3f2", time.Now().Add(-2*time.Hour))

	// Prime reconcile: let all condition-clearing status writes land so only the
	// schedule annotation write is outstanding on the next pass, then reset the
	// stamp so the schedule appears due again.
	if _, err := reconcileRepo(t, "r3f2-repo"); err != nil {
		t.Fatalf("prime reconcile: %v", err)
	}
	r = getRepo(t, "r3f2-repo")
	r.Status.LastScheduledReingest = nil
	if err := k8sClient.Status().Update(context.Background(), r); err != nil {
		t.Fatalf("reset LastScheduledReingest: %v", err)
	}
	r = getRepo(t, "r3f2-repo")
	delete(r.Annotations, ReingestAnnotation)
	if err := k8sClient.Update(context.Background(), r); err != nil {
		t.Fatalf("clear annotation: %v", err)
	}

	// Now reconcile with a conflict-injecting metadata client.
	var calls atomic.Int32
	cc := &conflictOnceMetaClient{Client: k8sClient, calls: &calls}
	rr := &RepositoryReconciler{
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
	_, err := rr.Reconcile(logf.IntoContext(context.Background(), logf.Log), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: "r3f2-repo"},
	})
	if err != nil {
		t.Fatalf("scheduleNextReingest annotation write must succeed despite one conflict, got: %v", err)
	}
	if calls.Load() < 2 {
		t.Errorf("expected at least 2 metadata writes (conflict + retry), got %d", calls.Load())
	}
	got := getRepo(t, "r3f2-repo")
	if got.Annotations[ReingestAnnotation] == "" {
		t.Error("reingest annotation must be stamped even after a metadata conflict")
	}
}

// ingestDurationSumForLabel collects operator_ingest_job_duration_seconds sum
// from the registry (any label set). Returns the sum of all observations.
func ingestDurationSumAll(t *testing.T, reg prometheus.Gatherer) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "operator_ingest_job_duration_seconds" {
			var sum float64
			for _, m := range mf.GetMetric() {
				sum += m.GetHistogram().GetSampleSum()
			}
			return sum
		}
	}
	return 0
}

// R3-Finding 3: for failed Jobs, the duration end time should use the
// LastTransitionTime of the JobFailed condition (set by K8s when it marks the
// job failed) rather than time.Now(). Using time.Now() inflates the duration
// by the reconcile-observation lag.
// This test verifies that when the JobFailed condition carries a
// LastTransitionTime of StartTime+5s, the observed duration is <=10s (i.e.
// it is not inflated to the observation time which would be 30s+ later due to
// markJob setting StartTime to now-30s).
func TestRepoR3F3_FailedJobDurationUsesConditionTime(t *testing.T) {
	mkProject(t, "r3f3-proj", "r3f3-scm")
	mkSecret(t, "r3f3-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "r3f3-repo", "r3f3-proj")
	setProjectMemoryReady(t, "r3f3-proj", "http://mem-r3f3.tatara.svc:8080")

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
			NamespacedName: types.NamespacedName{Namespace: testNS, Name: "r3f3-repo"},
		})
	}

	if _, err := reconcile(); err != nil {
		t.Fatalf("launch reconcile: %v", err)
	}
	jobName := waitRepoJob(t, "r3f3-repo")

	// Set up a job where:
	//   StartTime = now - 30s
	//   JobFailed.LastTransitionTime = now - 25s (i.e. 5s after start)
	// With the fix, observed duration should be ~5s, not ~30s+.
	job := &batchv1.Job{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: jobName}, job); err != nil {
		t.Fatalf("get job: %v", err)
	}
	startTime := metav1.NewTime(time.Now().Add(-30 * time.Second))
	failedAt := metav1.NewTime(time.Now().Add(-25 * time.Second)) // 5s after start
	job.Status.StartTime = &startTime
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, LastTransitionTime: failedAt},
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: failedAt},
	}
	if err := k8sClient.Status().Update(context.Background(), job); err != nil {
		t.Fatalf("update job status: %v", err)
	}

	if _, err := reconcile(); err != nil {
		t.Fatalf("post-failure reconcile: %v", err)
	}

	// Duration sum should be ~5s (condition time) not ~30s+ (time.Now fallback).
	// We assert it is less than 20s to give generous test tolerance.
	durSum := ingestDurationSumAll(t, reg)
	if durSum <= 0 {
		t.Fatal("ingest duration must be observed for failed jobs")
	}
	if durSum >= 20.0 {
		t.Errorf("failed job duration should use condition timestamp (~5s), got %.2fs (>= 20s implies time.Now() fallback)", durSum)
	}
}
