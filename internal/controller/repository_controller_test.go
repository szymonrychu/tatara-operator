package controller

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/ingest"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func boolPtrRC(v bool) *bool { return &v }

func newRepoReconciler() *RepositoryReconciler {
	return &RepositoryReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		IngestConfig: ingest.Config{
			IngesterImage:  "registry.example/ingester:1.2.3",
			OIDCIssuer:     "https://kc.example/realms/tatara",
			OIDCClientID:   "tatara-operator",
			OIDCSecretName: "tatara-operator",
			OIDCAudience:   "tatara-memory",
			Namespace:      testNS,
		},
	}
}

func reconcileRepo(t *testing.T, name string) (ctrl.Result, error) {
	t.Helper()
	r := newRepoReconciler()
	return r.Reconcile(logf.IntoContext(context.Background(), logf.Log), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: name},
	})
}

func mkProject(t *testing.T, name, secretRef string) {
	t.Helper()
	p := &tataradevv1alpha1.Project{}
	p.Name = name
	p.Namespace = testNS
	p.Spec.ScmSecretRef = secretRef
	if err := k8sClient.Create(context.Background(), p); err != nil {
		t.Fatalf("create project %s: %v", name, err)
	}
}

func mkRepo(t *testing.T, name, projectRef string) *tataradevv1alpha1.Repository {
	t.Helper()
	r := &tataradevv1alpha1.Repository{}
	r.Name = name
	r.Namespace = testNS
	r.Spec.ProjectRef = projectRef
	r.Spec.URL = "https://github.com/acme/" + name + ".git"
	r.Spec.DefaultBranch = "main"
	r.Spec.IngestEnabled = boolPtrRC(true)
	r.Spec.ReingestSchedule = "0 6 * * *"
	if err := k8sClient.Create(context.Background(), r); err != nil {
		t.Fatalf("create repo %s: %v", name, err)
	}
	return r
}

func getRepo(t *testing.T, name string) *tataradevv1alpha1.Repository {
	t.Helper()
	r := &tataradevv1alpha1.Repository{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, r); err != nil {
		t.Fatalf("get repo %s: %v", name, err)
	}
	return r
}

func TestReconcileRepo_ComputesPerRepoCounts(t *testing.T) {
	mkProject(t, "p-repo-counts", "p-repo-counts-scm")
	mkRepo(t, "r-repo-counts", "p-repo-counts")
	mkTaskWithKind(t, "t-issue-open-repo", "p-repo-counts", "r-repo-counts", "implement")
	mkTaskWithKind(t, "t-incident-open-repo", "p-repo-counts", "r-repo-counts", "incident")

	if _, err := reconcileRepo(t, "r-repo-counts"); err != nil {
		t.Fatalf("reconcileRepo: %v", err)
	}

	repo := getRepo(t, "r-repo-counts")
	if repo.Status.OpenIssuesCount != 1 || repo.Status.OpenIncidentsCount != 1 {
		t.Errorf("counts = (%d,%d), want (1,1)", repo.Status.OpenIssuesCount, repo.Status.OpenIncidentsCount)
	}
}

// TestReconcileRepo_ComputesPerRepoCounts_IngestDisabled verifies computeRepoCounts
// runs even for a repo with IngestEnabled=false (FIX-2): the printcolumn-backed
// open issue/incident counts must stay live independent of ingest gating, per
// computeRepoCounts's own doc comment ("independent of ingest state/gating").
func TestReconcileRepo_ComputesPerRepoCounts_IngestDisabled(t *testing.T) {
	mkProject(t, "p-repo-counts-off", "p-repo-counts-off-scm")
	repo := mkRepo(t, "r-repo-counts-off", "p-repo-counts-off")
	repo.Spec.IngestEnabled = boolPtrRC(false)
	if err := k8sClient.Update(context.Background(), repo); err != nil {
		t.Fatalf("disable ingest: %v", err)
	}
	mkTaskWithKind(t, "t-issue-open-repo-off", "p-repo-counts-off", "r-repo-counts-off", "implement")
	mkTaskWithKind(t, "t-incident-open-repo-off", "p-repo-counts-off", "r-repo-counts-off", "incident")

	if _, err := reconcileRepo(t, "r-repo-counts-off"); err != nil {
		t.Fatalf("reconcileRepo: %v", err)
	}

	got := getRepo(t, "r-repo-counts-off")
	if got.Status.OpenIssuesCount != 1 || got.Status.OpenIncidentsCount != 1 {
		t.Errorf("counts = (%d,%d), want (1,1) even with ingest disabled", got.Status.OpenIssuesCount, got.Status.OpenIncidentsCount)
	}
}

func listIngestJobs(t *testing.T, repoName string) []batchv1.Job {
	t.Helper()
	var jl batchv1.JobList
	if err := k8sClient.List(context.Background(), &jl,
		client.InNamespace(testNS),
		client.MatchingLabels{"tatara.dev/repository": repoName}); err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	return jl.Items
}

func waitRepoJob(t *testing.T, repoName string) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		r := getRepo(t, repoName)
		if r.Status.JobName != "" {
			return r.Status.JobName
		}
		time.Sleep(interval)
	}
	t.Fatalf("repo %s never set status.jobName", repoName)
	return ""
}

func TestPublishIngestHealth(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := &RepositoryReconciler{Metrics: m}

	mkRepoObj := func(name, phase string, failCount int, enabled bool, last *metav1.Time) *tataradevv1alpha1.Repository {
		rp := &tataradevv1alpha1.Repository{}
		rp.Name = name
		rp.Spec.ProjectRef = "proj"
		rp.Spec.IngestEnabled = boolPtrRC(enabled)
		rp.Status.Phase = phase
		rp.Status.IngestFailureCount = failCount
		rp.Status.LastIngestTime = last
		return rp
	}

	// Failed phase -> failing 1.
	r.publishIngestHealth(mkRepoObj("r-failed", "Failed", 0, true, nil), false)
	if got := testutil.ToFloat64(m.RepositoryIngestFailingGauge("proj", "r-failed")); got != 1 {
		t.Errorf("failed phase: failing = %v, want 1", got)
	}
	// Mid-retry (Ingesting but unresolved consecutive failures) -> failing 1.
	r.publishIngestHealth(mkRepoObj("r-retry", "Ingesting", 2, true, nil), false)
	if got := testutil.ToFloat64(m.RepositoryIngestFailingGauge("proj", "r-retry")); got != 1 {
		t.Errorf("retrying: failing = %v, want 1", got)
	}
	// Healthy (Ingested, no failures) -> failing 0 and timestamp published.
	ts := metav1.Unix(1750000000, 0)
	r.publishIngestHealth(mkRepoObj("r-ok", "Ingested", 0, true, &ts), false)
	if got := testutil.ToFloat64(m.RepositoryIngestFailingGauge("proj", "r-ok")); got != 0 {
		t.Errorf("healthy: failing = %v, want 0", got)
	}
	if got := testutil.ToFloat64(m.RepositoryLastIngestTimestampGauge("proj", "r-ok")); got != 1750000000 {
		t.Errorf("healthy: last_ingest_ts = %v, want 1750000000", got)
	}
	// Recovery on the SAME repo: the gauge must clear, not stay latched - this is
	// the whole point of the current-state signal vs the monotonic counter (#138).
	r.publishIngestHealth(mkRepoObj("r-heal", "Failed", 3, true, nil), false)
	if got := testutil.ToFloat64(m.RepositoryIngestFailingGauge("proj", "r-heal")); got != 1 {
		t.Fatalf("pre-recovery: failing = %v, want 1", got)
	}
	r.publishIngestHealth(mkRepoObj("r-heal", "Ingested", 0, true, &ts), false)
	if got := testutil.ToFloat64(m.RepositoryIngestFailingGauge("proj", "r-heal")); got != 0 {
		t.Errorf("post-recovery: failing = %v, want 0 (must clear)", got)
	}
	// A disabled repo never reports failing even if its status looks failed.
	r.publishIngestHealth(mkRepoObj("r-disabled", "Failed", 5, false, nil), false)
	if got := testutil.ToFloat64(m.RepositoryIngestFailingGauge("proj", "r-disabled")); got != 0 {
		t.Errorf("disabled: failing = %v, want 0", got)
	}
}

// TestPublishIngestHealth_Gated pins the ingest_failing x ingest_gated
// interaction from issue #525: the two gauges are published together and
// "gated" wins. A gated repo launches no ingest Job, and only a successful
// ingest resets Phase/IngestFailureCount, so reporting it as failing pins the
// gauge for as long as the project memory stack is down.
func TestPublishIngestHealth_Gated(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := &RepositoryReconciler{Metrics: m}

	mkRepoObj := func(name string, failCount int, enabled bool) *tataradevv1alpha1.Repository {
		rp := &tataradevv1alpha1.Repository{}
		rp.Name = name
		rp.Spec.ProjectRef = "proj"
		rp.Spec.IngestEnabled = boolPtrRC(enabled)
		rp.Status.Phase = "Failed"
		rp.Status.IngestFailureCount = failCount
		return rp
	}

	cases := []struct {
		name            string
		repo            *tataradevv1alpha1.Repository
		gated           bool
		failing, isGate float64
	}{
		{"failing and gated", mkRepoObj("g-failing", 3, true), true, 0, 1},
		{"failing, not gated", mkRepoObj("g-open", 3, true), false, 1, 0},
		// A disabled repo is not held by the gate, it is switched off.
		{"disabled and gated", mkRepoObj("g-disabled", 3, false), true, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r.publishIngestHealth(tc.repo, tc.gated)
			if got := testutil.ToFloat64(m.RepositoryIngestFailingGauge("proj", tc.repo.Name)); got != tc.failing {
				t.Errorf("ingest_failing = %v, want %v", got, tc.failing)
			}
			if got := testutil.ToFloat64(m.RepositoryIngestGatedGauge("proj", tc.repo.Name)); got != tc.isGate {
				t.Errorf("ingest_gated = %v, want %v", got, tc.isGate)
			}
		})
	}

	// The mask is not latched: the same repo un-masks the moment the gate opens,
	// without any successful ingest in between.
	repo := mkRepoObj("g-cycle", 3, true)
	r.publishIngestHealth(repo, true)
	if got := testutil.ToFloat64(m.RepositoryIngestFailingGauge("proj", "g-cycle")); got != 0 {
		t.Fatalf("gated: ingest_failing = %v, want 0", got)
	}
	r.publishIngestHealth(repo, false)
	if got := testutil.ToFloat64(m.RepositoryIngestFailingGauge("proj", "g-cycle")); got != 1 {
		t.Errorf("gate reopened: ingest_failing = %v, want 1", got)
	}
}

func TestRepoReconcile_FullIngestLaunchesJob(t *testing.T) {
	mkProject(t, "rp-full", "rp-full-scm")
	mkSecret(t, "rp-full-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "full", "rp-full")
	setProjectMemoryReady(t, "rp-full", "http://mem-rp-full.tatara.svc:8080")

	if _, err := reconcileRepo(t, "full"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	jobName := waitRepoJob(t, "full")

	jobs := listIngestJobs(t, "full")
	if len(jobs) != 1 {
		t.Fatalf("jobs = %d, want 1", len(jobs))
	}
	if jobs[0].Name != jobName {
		t.Errorf("status.jobName = %q, job = %q", jobName, jobs[0].Name)
	}
	// full ingest: no --since in the main container script
	script := jobs[0].Spec.Template.Spec.Containers[0].Args[0]
	if contains(script, "--since") {
		t.Errorf("full ingest job must not pass --since: %q", script)
	}
	// result ConfigMap pre-created
	cm := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: "full-ingest-result"}, cm); err != nil {
		t.Fatalf("result configmap not pre-created: %v", err)
	}
	if getRepo(t, "full").Status.Phase != "Ingesting" {
		t.Errorf("phase = %q, want Ingesting", getRepo(t, "full").Status.Phase)
	}
}

func TestRepoReconcile_ClearsMemoryNotReadyWhenReady(t *testing.T) {
	mkProject(t, "rp-clr", "rp-clr-scm")
	mkSecret(t, "rp-clr-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "clr", "rp-clr")
	setProjectMemoryReady(t, "rp-clr", "http://mem-rp-clr.tatara.svc:8080")

	if _, err := reconcileRepo(t, "clr"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	cond := findCond(getRepo(t, "clr").Status.Conditions, "MemoryNotReady")
	if cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("expected MemoryNotReady=False once memory is Ready, got %+v", cond)
	}
}

func TestRepoReconcile_ConcurrencyGuard(t *testing.T) {
	mkProject(t, "rp-guard", "rp-guard-scm")
	mkSecret(t, "rp-guard-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "guard", "rp-guard")
	setProjectMemoryReady(t, "rp-guard", "http://mem-rp-guard.tatara.svc:8080")

	if _, err := reconcileRepo(t, "guard"); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	first := waitRepoJob(t, "guard")

	// second reconcile while the Job is still active must not launch another
	if _, err := reconcileRepo(t, "guard"); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	jobs := listIngestJobs(t, "guard")
	if len(jobs) != 1 {
		t.Fatalf("jobs after second reconcile = %d, want 1 (guard held)", len(jobs))
	}
	if getRepo(t, "guard").Status.JobName != first {
		t.Errorf("jobName changed under guard: %q -> %q", first, getRepo(t, "guard").Status.JobName)
	}
}

func TestRepoReconcile_IncrementalUsesSince(t *testing.T) {
	mkProject(t, "rp-inc", "rp-inc-scm")
	mkSecret(t, "rp-inc-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "inc", "rp-inc")
	setProjectMemoryReady(t, "rp-inc", "http://mem-rp-inc.tatara.svc:8080")

	// simulate a prior successful ingest
	r := getRepo(t, "inc")
	r.Status.LastIngestedCommit = "oldsha99"
	lastTime := metav1.NewTime(time.Now().Add(-1 * time.Hour))
	r.Status.LastIngestTime = &lastTime
	r.Status.Phase = "Ingested"
	if err := k8sClient.Status().Update(context.Background(), r); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	// request a re-ingest via the annotation, newer than lastIngestTime
	r = getRepo(t, "inc")
	if r.Annotations == nil {
		r.Annotations = map[string]string{}
	}
	r.Annotations["tatara.dev/reingest-requested"] = time.Now().Format(time.RFC3339)
	if err := k8sClient.Update(context.Background(), r); err != nil {
		t.Fatalf("set annotation: %v", err)
	}

	if _, err := reconcileRepo(t, "inc"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	waitRepoJob(t, "inc")
	jobs := listIngestJobs(t, "inc")
	if len(jobs) != 1 {
		t.Fatalf("jobs = %d, want 1", len(jobs))
	}
	script := jobs[0].Spec.Template.Spec.Containers[0].Args[0]
	if !contains(script, "--since oldsha99") {
		t.Errorf("incremental job must pass --since oldsha99: %q", script)
	}
}

func TestRepoReconcile_NoReingestWhenAnnotationStale(t *testing.T) {
	mkProject(t, "rp-stale", "rp-stale-scm")
	mkSecret(t, "rp-stale-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "stale", "rp-stale")
	setProjectMemoryReady(t, "rp-stale", "http://mem-rp-stale.tatara.svc:8080")

	r := getRepo(t, "stale")
	r.Status.LastIngestedCommit = "shaA"
	nowTime := metav1.NewTime(time.Now())
	r.Status.LastIngestTime = &nowTime
	r.Status.Phase = "Ingested"
	if err := k8sClient.Status().Update(context.Background(), r); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	r = getRepo(t, "stale")
	if r.Annotations == nil {
		r.Annotations = map[string]string{}
	}
	// annotation OLDER than lastIngestTime -> no new ingest
	r.Annotations["tatara.dev/reingest-requested"] = time.Now().Add(-2 * time.Hour).Format(time.RFC3339)
	if err := k8sClient.Update(context.Background(), r); err != nil {
		t.Fatalf("set annotation: %v", err)
	}

	if _, err := reconcileRepo(t, "stale"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	jobs := listIngestJobs(t, "stale")
	if len(jobs) != 0 {
		t.Fatalf("stale annotation must not launch a job, got %d", len(jobs))
	}
}

func TestRepoReconcile_ClearsMemoryNotReadyWithoutIngest(t *testing.T) {
	mkProject(t, "rp-clr2", "rp-clr2-scm")
	mkSecret(t, "rp-clr2-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "clr2", "rp-clr2")
	setProjectMemoryReady(t, "rp-clr2", "http://mem-rp-clr2.tatara.svc:8080")

	// Already-ingested repo carrying a lingering MemoryNotReady=True condition.
	r := getRepo(t, "clr2")
	r.Status.LastIngestedCommit = "shaX"
	nowTime := metav1.NewTime(time.Now())
	r.Status.LastIngestTime = &nowTime
	r.Status.Phase = "Ingested"
	r.Status.Conditions = append(r.Status.Conditions, metav1.Condition{
		Type: "MemoryNotReady", Status: metav1.ConditionTrue, Reason: "MemoryProvisioning",
		LastTransitionTime: metav1.Now(),
	})
	if err := k8sClient.Status().Update(context.Background(), r); err != nil {
		t.Fatalf("seed status: %v", err)
	}

	if _, err := reconcileRepo(t, "clr2"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if jobs := listIngestJobs(t, "clr2"); len(jobs) != 0 {
		t.Fatalf("no ingest expected for an already-ingested repo, got %d jobs", len(jobs))
	}
	cond := findCond(getRepo(t, "clr2").Status.Conditions, "MemoryNotReady")
	if cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("expected MemoryNotReady cleared to False on a no-ingest reconcile, got %+v", cond)
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func setProjectMemoryReady(t *testing.T, name, endpoint string) {
	t.Helper()
	p := &tataradevv1alpha1.Project{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: name}, p); err != nil {
		t.Fatalf("get project %s: %v", name, err)
	}
	// Set ReadySince well before the stabilization window so that memoryStablyReady
	// returns true for tests that rely on memory being ready immediately.
	readySince := metav1.NewTime(time.Now().Add(-(tataradevv1alpha1.MemoryReadyStabilizationWindow + time.Minute)))
	p.Status.Memory = &tataradevv1alpha1.MemoryStatus{
		Phase:      "Ready",
		Endpoint:   endpoint,
		ReadySince: &readySince,
	}
	if err := k8sClient.Status().Update(context.Background(), p); err != nil {
		t.Fatalf("set project %s memory ready: %v", name, err)
	}
}

// setProjectMemoryNotReady drops a project's memory stack out of Ready, the way
// a crash-looping backend does: the phase moves off Ready and ReadySince is
// cleared, so MemoryStablyReady needs a fresh stabilization window.
func setProjectMemoryNotReady(t *testing.T, name string) {
	t.Helper()
	p := &tataradevv1alpha1.Project{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: name}, p); err != nil {
		t.Fatalf("get project %s: %v", name, err)
	}
	p.Status.Memory = &tataradevv1alpha1.MemoryStatus{Phase: "Degraded"}
	if err := k8sClient.Status().Update(context.Background(), p); err != nil {
		t.Fatalf("set project %s memory not ready: %v", name, err)
	}
}

func TestRepoReconcile_GatesUntilMemoryReady(t *testing.T) {
	mkProject(t, "rp-mem", "rp-mem-scm")
	mkSecret(t, "rp-mem-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "memrepo", "rp-mem")

	// Project memory is not Ready (no status.memory at all).
	res, err := reconcileRepo(t, "memrepo")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("expected requeue while memory not ready")
	}
	if jobs := listIngestJobs(t, "memrepo"); len(jobs) != 0 {
		t.Fatalf("memory not ready must not launch a job, got %d", len(jobs))
	}
	r := getRepo(t, "memrepo")
	cond := findCond(r.Status.Conditions, "MemoryNotReady")
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("expected MemoryNotReady=True condition, got %+v", cond)
	}
}

// TestRepoReconcile_IngestGatedGauge guards issue #434: the memory-readiness
// ingest gate is invisible to alerting. A gated repo is neither Failed nor
// accumulating failures, so operator_repository_ingest_failing reads 0 for it
// forever - a 37h ingest freeze across all 9 repos was caught only by the 24h
// staleness backstop. operator_repository_ingest_gated is 1 while the gate
// holds and clears the moment memory is stably Ready.
func TestRepoReconcile_IngestGatedGauge(t *testing.T) {
	mkProject(t, "rp-gauge", "rp-gauge-scm")
	mkSecret(t, "rp-gauge-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "gaugerepo", "rp-gauge")

	// One reconciler across both passes so the gauge survives between them.
	r := newRepoReconciler()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "gaugerepo"}}
	reconcile := func() {
		t.Helper()
		if _, err := r.Reconcile(logf.IntoContext(context.Background(), logf.Log), req); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
	}

	// Memory Ready but INSIDE the stabilization window: the gate holds.
	p := &tataradevv1alpha1.Project{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: "rp-gauge"}, p); err != nil {
		t.Fatalf("get project: %v", err)
	}
	readySince := metav1.NewTime(time.Now())
	p.Status.Memory = &tataradevv1alpha1.MemoryStatus{
		Phase:      "Ready",
		Endpoint:   "http://mem-rp-gauge.tatara.svc:8080",
		ReadySince: &readySince,
	}
	if err := k8sClient.Status().Update(context.Background(), p); err != nil {
		t.Fatalf("set memory ready-but-unstable: %v", err)
	}

	reconcile()
	if got := testutil.ToFloat64(r.Metrics.RepositoryIngestGatedGauge("rp-gauge", "gaugerepo")); got != 1 {
		t.Fatalf("ingest_gated while inside stabilization window = %v, want 1", got)
	}
	if got := testutil.ToFloat64(r.Metrics.RepositoryIngestFailingGauge("rp-gauge", "gaugerepo")); got != 0 {
		t.Fatalf("ingest_failing while gated = %v, want 0 (this is exactly why the gated gauge exists)", got)
	}

	// Backdate ReadySince past the stabilization window: the gate clears.
	setProjectMemoryReady(t, "rp-gauge", "http://mem-rp-gauge.tatara.svc:8080")
	reconcile()
	if got := testutil.ToFloat64(r.Metrics.RepositoryIngestGatedGauge("rp-gauge", "gaugerepo")); got != 0 {
		t.Fatalf("ingest_gated after memory stably Ready = %v, want 0", got)
	}
}

// TestRepoReconcile_GatedRepoDoesNotReportFailing guards issue #525: a repo that
// was ALREADY failing when the memory-readiness gate closed kept
// operator_repository_ingest_failing pinned at 1 with no way out. Gated means no
// ingest Job is launched, and only a successful ingest resets the failure state,
// so the gauge - and every alert keyed on it - could not clear until the project
// memory stack recovered. mtg-decks held the alert for 18.6h with zero ingest
// attempts in 19.6h. While gated the failing gauge reads 0 and
// operator_repository_ingest_gated carries the truth; the moment the gate opens
// the repo is retryable again and the failing gauge un-masks.
func TestRepoReconcile_GatedRepoDoesNotReportFailing(t *testing.T) {
	mkProject(t, "rp-gatedfail", "rp-gatedfail-scm")
	mkSecret(t, "rp-gatedfail-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	repo := mkRepo(t, "gatedfailrepo", "rp-gatedfail")

	// The repo is already in a failing ingest state when the gate closes. The
	// recent failure timestamp keeps the exponential back-off holding once memory
	// comes back, which is also the case worth pinning: back-off is NOT the gate,
	// so the failing gauge must read 1 there.
	failedAt := metav1.NewTime(time.Now())
	repo.Status.Phase = "Failed"
	repo.Status.IngestFailureCount = 3
	repo.Status.LastIngestedCommit = "deadbee"
	repo.Status.LastIngestFailureTime = &failedAt
	if err := k8sClient.Status().Update(context.Background(), repo); err != nil {
		t.Fatalf("seed failing status: %v", err)
	}

	// One reconciler across both passes so the gauges survive between them.
	r := newRepoReconciler()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "gatedfailrepo"}}
	reconcile := func() {
		t.Helper()
		if _, err := r.Reconcile(logf.IntoContext(context.Background(), logf.Log), req); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
	}

	// Project memory is not Ready at all: the gate holds.
	reconcile()
	if got := testutil.ToFloat64(r.Metrics.RepositoryIngestGatedGauge("rp-gatedfail", "gatedfailrepo")); got != 1 {
		t.Fatalf("ingest_gated while memory not ready = %v, want 1", got)
	}
	if got := testutil.ToFloat64(r.Metrics.RepositoryIngestFailingGauge("rp-gatedfail", "gatedfailrepo")); got != 0 {
		t.Errorf("ingest_failing while gated = %v, want 0: gated means no ingest Job runs, "+
			"so nothing can reset the failure state and the alert would never clear", got)
	}
	if jobs := listIngestJobs(t, "gatedfailrepo"); len(jobs) != 0 {
		t.Fatalf("gated repo must not launch a job, got %d", len(jobs))
	}

	// Gate opens: the repo is retryable again, so its unresolved failures are
	// reportable again. Masking must not become a permanent amnesty.
	setProjectMemoryReady(t, "rp-gatedfail", "http://mem-rp-gatedfail.tatara.svc:8080")
	reconcile()
	if got := testutil.ToFloat64(r.Metrics.RepositoryIngestGatedGauge("rp-gatedfail", "gatedfailrepo")); got != 0 {
		t.Fatalf("ingest_gated after memory stably Ready = %v, want 0", got)
	}
	if got := testutil.ToFloat64(r.Metrics.RepositoryIngestFailingGauge("rp-gatedfail", "gatedfailrepo")); got != 1 {
		t.Errorf("ingest_failing once the gate opens = %v, want 1 (failures are still unresolved)", got)
	}
	if jobs := listIngestJobs(t, "gatedfailrepo"); len(jobs) != 0 {
		t.Fatalf("back-off must still hold the retry, got %d job(s)", len(jobs))
	}
}

// TestRepoReconcile_ActiveJobIsNotReportedGated pins the other half of the #525
// masking rule: the gauge means "the memory-readiness gate is what is holding
// this repo's next ingest", which is false while an ingest Job is in flight. The
// Job concurrency guard returns long before the gate branch, so a memory blip
// mid-ingest must not be reported as gated - and must not mask the failing gauge
// for a repo that is, right now, retrying.
func TestRepoReconcile_ActiveJobIsNotReportedGated(t *testing.T) {
	mkProject(t, "rp-inflight", "rp-inflight-scm")
	mkSecret(t, "rp-inflight-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "inflightrepo", "rp-inflight")
	setProjectMemoryReady(t, "rp-inflight", "http://mem-rp-inflight.tatara.svc:8080")

	r := newRepoReconciler()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "inflightrepo"}}
	reconcile := func() {
		t.Helper()
		if _, err := r.Reconcile(logf.IntoContext(context.Background(), logf.Log), req); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
	}

	reconcile()
	jobName := waitRepoJob(t, "inflightrepo")

	// Earlier attempts left unresolved failures: the in-flight Job IS the retry.
	repo := getRepo(t, "inflightrepo")
	repo.Status.IngestFailureCount = 2
	if err := k8sClient.Status().Update(context.Background(), repo); err != nil {
		t.Fatalf("seed failure count: %v", err)
	}

	// The memory stack drops out while that Job is still running.
	setProjectMemoryNotReady(t, "rp-inflight")
	reconcile()

	if got := getRepo(t, "inflightrepo").Status.JobName; got != jobName {
		t.Fatalf("jobName = %q, want the in-flight %q (guard must still hold)", got, jobName)
	}
	if got := testutil.ToFloat64(r.Metrics.RepositoryIngestGatedGauge("rp-inflight", "inflightrepo")); got != 0 {
		t.Errorf("ingest_gated with an ingest Job in flight = %v, want 0: the gate is not what is holding this repo", got)
	}
	if got := testutil.ToFloat64(r.Metrics.RepositoryIngestFailingGauge("rp-inflight", "inflightrepo")); got != 1 {
		t.Errorf("ingest_failing while actively retrying = %v, want 1: the mask applies to gated repos, not to running ones", got)
	}
}

// TestRepoReconcile_MissingProjectErrors pins the error deferral #525 introduced:
// the owning-Project read moved to the top of Reconcile so the gate state is
// available to the gauges, and its error is now checked 50-odd lines later. Drop
// that check and the reconcile falls through to ingest.BuildJob with a
// zero-value Project.
func TestRepoReconcile_MissingProjectErrors(t *testing.T) {
	mkRepo(t, "orphanrepo", "rp-nonexistent")

	r := newRepoReconciler()
	_, err := r.Reconcile(logf.IntoContext(context.Background(), logf.Log),
		ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "orphanrepo"}})
	if err == nil || !strings.Contains(err.Error(), "get owning project") {
		t.Fatalf("reconcile err = %v, want a 'get owning project' failure", err)
	}
	if got := testutil.ToFloat64(r.Metrics.RepositoryIngestGatedGauge("rp-nonexistent", "orphanrepo")); got != 0 {
		t.Errorf("ingest_gated with an unreadable Project = %v, want 0 (gate state is unknown, not closed)", got)
	}
	if jobs := listIngestJobs(t, "orphanrepo"); len(jobs) != 0 {
		t.Fatalf("unreadable Project must not launch a job, got %d", len(jobs))
	}
}

func TestRepoReconcile_UsesProjectEndpointWhenReady(t *testing.T) {
	mkProject(t, "rp-ep", "rp-ep-scm")
	mkSecret(t, "rp-ep-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "eprepo", "rp-ep")
	setProjectMemoryReady(t, "rp-ep", "http://mem-rp-ep.tatara.svc:8080")

	if _, err := reconcileRepo(t, "eprepo"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	waitRepoJob(t, "eprepo")
	jobs := listIngestJobs(t, "eprepo")
	if len(jobs) != 1 {
		t.Fatalf("jobs = %d, want 1", len(jobs))
	}
	script := jobs[0].Spec.Template.Spec.Containers[0].Args[0]
	if !contains(script, "--base-url http://mem-rp-ep.tatara.svc:8080") {
		t.Errorf("ingest job must use the Project endpoint as base-url: %q", script)
	}
}

func setRepoIngested(t *testing.T, name, sha string, lastIngest time.Time) {
	t.Helper()
	r := getRepo(t, name)
	r.Status.LastIngestedCommit = sha
	lt := metav1.NewTime(lastIngest)
	r.Status.LastIngestTime = &lt
	r.Status.Phase = "Ingested"
	if err := k8sClient.Status().Update(context.Background(), r); err != nil {
		t.Fatalf("seed ingested status for %s: %v", name, err)
	}
}

func TestRepoReconcile_ScheduleStampsAnnotationWhenDue(t *testing.T) {
	mkProject(t, "rp-sch1", "rp-sch1-scm")
	mkSecret(t, "rp-sch1-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "sch1", "rp-sch1")
	setProjectMemoryReady(t, "rp-sch1", "http://mem-rp-sch1.tatara.svc:8080")

	// Schedule fires every minute; last ingest was an hour ago, so it is due now.
	r := getRepo(t, "sch1")
	r.Spec.ReingestSchedule = "* * * * *"
	if err := k8sClient.Update(context.Background(), r); err != nil {
		t.Fatalf("set schedule: %v", err)
	}
	setRepoIngested(t, "sch1", "shaSch1", time.Now().Add(-1*time.Hour))

	if _, err := reconcileRepo(t, "sch1"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getRepo(t, "sch1")
	if got.Annotations[ReingestAnnotation] == "" {
		t.Fatal("due schedule must stamp the reingest-requested annotation")
	}
	if got.Status.LastScheduledReingest == nil {
		t.Fatal("due schedule must set status.lastScheduledReingest")
	}
	// No Job yet: the annotation re-triggers reconcile via the watch; this
	// reconcile pass only stamps.
	if jobs := listIngestJobs(t, "sch1"); len(jobs) != 0 {
		t.Fatalf("schedule stamp pass must not itself launch a job, got %d", len(jobs))
	}
}

// cronAtOffsetHours returns a daily cron whose fire is offsetHours away from
// now. Any test asserting "not due yet" MUST use this instead of hard-coding
// an hour: with a literal "0 6 * * *" the assertion is true for 1439 minutes a
// day and FALSE for the 60 seconds after 06:00 UTC, when the boundary really
// has passed and the operator is right to fire. CI run 30426381616 landed
// exactly there - lastScheduledReingest seeded at 05:59:55Z, reconcile at
// 06:00:55Z - and TestRepoReconcile_ScheduleNoDoubleFireWithinInterval failed
// on correct production behaviour. Because the returned hour is never the
// current or the preceding hour for offsetHours in [2,22], no fire boundary
// can fall inside the seeded [base, now] window whatever the wall clock reads.
func cronAtOffsetHours(offsetHours int) string {
	return fmt.Sprintf("0 %d * * *", (time.Now().UTC().Hour()+offsetHours)%24)
}

func TestRepoReconcile_ScheduleRequeuesWhenNotDue(t *testing.T) {
	mkProject(t, "rp-sch2", "rp-sch2-scm")
	mkSecret(t, "rp-sch2-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "sch2", "rp-sch2")
	setProjectMemoryReady(t, "rp-sch2", "http://mem-rp-sch2.tatara.svc:8080")

	// Far-future daily schedule + a fresh ingest => not due; expect a requeue,
	// clamped to maxScheduleRequeue because the fire is ~12h out.
	r := getRepo(t, "sch2")
	r.Spec.ReingestSchedule = cronAtOffsetHours(12)
	if err := k8sClient.Update(context.Background(), r); err != nil {
		t.Fatalf("set schedule: %v", err)
	}
	setRepoIngested(t, "sch2", "shaSch2", time.Now())

	res, err := reconcileRepo(t, "sch2")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("not-due schedule must set RequeueAfter, got %v", res.RequeueAfter)
	}
	if res.RequeueAfter > 6*time.Hour {
		t.Errorf("RequeueAfter must be clamped to 6h, got %v", res.RequeueAfter)
	}
	if getRepo(t, "sch2").Annotations[ReingestAnnotation] != "" {
		t.Error("not-due schedule must not stamp the annotation")
	}
	if getRepo(t, "sch2").Status.LastScheduledReingest != nil {
		t.Error("not-due schedule must not set lastScheduledReingest")
	}
}

func TestRepoReconcile_ScheduleBadCronSkips(t *testing.T) {
	mkProject(t, "rp-sch3", "rp-sch3-scm")
	mkSecret(t, "rp-sch3-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "sch3", "rp-sch3")
	setProjectMemoryReady(t, "rp-sch3", "http://mem-rp-sch3.tatara.svc:8080")

	// A syntactically-shaped but semantically invalid cron (bad minute field).
	r := getRepo(t, "sch3")
	r.Spec.ReingestSchedule = "99 6 * * *"
	if err := k8sClient.Update(context.Background(), r); err != nil {
		t.Fatalf("set schedule: %v", err)
	}
	setRepoIngested(t, "sch3", "shaSch3", time.Now().Add(-1*time.Hour))

	res, err := reconcileRepo(t, "sch3")
	if err != nil {
		t.Fatalf("bad cron must not error the reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("bad cron must skip scheduling (no requeue), got %v", res.RequeueAfter)
	}
	if getRepo(t, "sch3").Annotations[ReingestAnnotation] != "" {
		t.Error("bad cron must not stamp the annotation")
	}
}

func TestRepoReconcile_ScheduleNoDoubleFireWithinInterval(t *testing.T) {
	mkProject(t, "rp-sch4", "rp-sch4-scm")
	mkSecret(t, "rp-sch4-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "sch4", "rp-sch4")
	setProjectMemoryReady(t, "rp-sch4", "http://mem-rp-sch4.tatara.svc:8080")

	r := getRepo(t, "sch4")
	r.Spec.ReingestSchedule = cronAtOffsetHours(6)
	if err := k8sClient.Update(context.Background(), r); err != nil {
		t.Fatalf("set schedule: %v", err)
	}
	setRepoIngested(t, "sch4", "shaSch4", time.Now().Add(-25*time.Hour))

	// lastScheduledReingest is recent => next fire is in the future => not due,
	// even though lastIngestTime is old. Guards against double-fire.
	r = getRepo(t, "sch4")
	just := metav1.NewTime(time.Now().Add(-1 * time.Minute))
	r.Status.LastScheduledReingest = &just
	if err := k8sClient.Status().Update(context.Background(), r); err != nil {
		t.Fatalf("seed lastScheduledReingest: %v", err)
	}

	res, err := reconcileRepo(t, "sch4")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if getRepo(t, "sch4").Annotations[ReingestAnnotation] != "" {
		t.Error("recent lastScheduledReingest must prevent a second stamp this interval")
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected a requeue to the next fire, got %v", res.RequeueAfter)
	}
}

// seedRepoStatus applies mutate to the repo's status subresource.
func seedRepoStatus(t *testing.T, name string, mutate func(*tataradevv1alpha1.Repository)) {
	t.Helper()
	r := getRepo(t, name)
	mutate(r)
	if err := k8sClient.Status().Update(context.Background(), r); err != nil {
		t.Fatalf("seed status for %s: %v", name, err)
	}
}

// TestRepoReconcile_RepairsStaleFailedPhase guards issue #457. A Repository was
// observed live in (Phase="Failed", IngestFailureCount=0) for ~20.8h with ZERO
// ingest failures in 7 days of logs. Nothing repaired it: Status.Phase is
// written only on job completion, and the IngestIdle branch guarded on the
// failure count alone. operator_repository_ingest_failing derives from
// (Phase=="Failed" || count>0) so the gauge latched at 1 and the alert told a
// 21h false story. Phase=="Failed" must now imply an unresolved failure.
func TestRepoReconcile_RepairsStaleFailedPhase(t *testing.T) {
	mkProject(t, "rp-heal", "rp-heal-scm")
	mkSecret(t, "rp-heal-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "healrepo", "rp-heal")
	setProjectMemoryReady(t, "rp-heal", "http://mem-rp-heal.tatara.svc:8080")

	now := metav1.NewTime(time.Now())
	seedRepoStatus(t, "healrepo", func(r *tataradevv1alpha1.Repository) {
		r.Status.LastIngestedCommit = "shaHeal"
		r.Status.LastIngestTime = &now
		r.Status.Phase = "Failed"
		r.Status.IngestFailureCount = 0
	})

	r := newRepoReconciler()
	if _, err := r.Reconcile(logf.IntoContext(context.Background(), logf.Log), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: "healrepo"},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getRepo(t, "healrepo")
	if got.Status.Phase != "Ingested" {
		t.Errorf("phase = %q, want Ingested (a recorded successful ingest and no unresolved failure)", got.Status.Phase)
	}
	if v := testutil.ToFloat64(r.Metrics.RepositoryIngestFailingGauge("rp-heal", "healrepo")); v != 0 {
		t.Errorf("ingest_failing = %v, want 0: the latched gauge is what pinned the alert for 20.8h", v)
	}
	if jobs := listIngestJobs(t, "healrepo"); len(jobs) != 0 {
		t.Errorf("repairing a stale phase must not launch an ingest, got %d jobs", len(jobs))
	}
}

// TestRepoReconcile_RepairsStaleFailedPhaseNeverIngested covers the other half
// of the repair (issue #457): a repo carrying Phase="Failed" with no recorded
// successful ingest has nothing to be "Ingested" at, so the repair clears the
// phase and the ordinary first-full-ingest path takes over in the same pass.
func TestRepoReconcile_RepairsStaleFailedPhaseNeverIngested(t *testing.T) {
	mkProject(t, "rp-heal2", "rp-heal2-scm")
	mkSecret(t, "rp-heal2-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "healrepo2", "rp-heal2")
	setProjectMemoryReady(t, "rp-heal2", "http://mem-rp-heal2.tatara.svc:8080")

	seedRepoStatus(t, "healrepo2", func(r *tataradevv1alpha1.Repository) {
		r.Status.Phase = "Failed"
		r.Status.IngestFailureCount = 0
	})

	if _, err := reconcileRepo(t, "healrepo2"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	waitRepoJob(t, "healrepo2")
	if jobs := listIngestJobs(t, "healrepo2"); len(jobs) != 1 {
		t.Fatalf("jobs = %d, want 1 (never-ingested repo must get its full ingest)", len(jobs))
	}
	if got := getRepo(t, "healrepo2").Status.Phase; got != "Ingesting" {
		t.Errorf("phase = %q, want Ingesting", got)
	}
}

// TestRepoReconcile_FailedPhaseTriggersReingest guards issue #457's root cause:
// ingestDecision returned want=false once LastIngestedCommit was set and no
// newer re-ingest annotation existed, so Phase=="Failed" was never itself a
// reason to re-ingest and the state was self-perpetuating - the repo's recall
// corpus silently went stale until an unrelated cron tick rescued it. The
// annotation is not a reliable trigger either: the Repository CR is helm-managed
// and an apply strips it.
func TestRepoReconcile_FailedPhaseTriggersReingest(t *testing.T) {
	mkProject(t, "rp-refail", "rp-refail-scm")
	mkSecret(t, "rp-refail-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "refail", "rp-refail")
	setProjectMemoryReady(t, "rp-refail", "http://mem-rp-refail.tatara.svc:8080")

	lastIngest := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	lastFailure := metav1.NewTime(time.Now().Add(-1 * time.Hour))
	seedRepoStatus(t, "refail", func(r *tataradevv1alpha1.Repository) {
		r.Status.LastIngestedCommit = "shaRefail"
		r.Status.LastIngestTime = &lastIngest
		r.Status.Phase = "Failed"
		r.Status.IngestFailureCount = 1
		r.Status.LastIngestFailureTime = &lastFailure
	})

	if _, err := reconcileRepo(t, "refail"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	waitRepoJob(t, "refail")
	jobs := listIngestJobs(t, "refail")
	if len(jobs) != 1 {
		t.Fatalf("jobs = %d, want 1 (a Failed repo with an elapsed backoff must re-ingest)", len(jobs))
	}
	if script := jobs[0].Spec.Template.Spec.Containers[0].Args[0]; !contains(script, "--since shaRefail") {
		t.Errorf("the retry must stay incremental below the full-ingest fallback threshold: %q", script)
	}
}

// TestRepoReconcile_FailedPhaseRespectsBackoff is the other side of making
// Phase=="Failed" a re-ingest trigger (issue #457): a genuinely broken repo must
// still back off instead of hammering a Job per reconcile. The repair guard is
// what makes this safe - Phase=="Failed" now implies IngestFailureCount>0, which
// is exactly the condition the exponential back-off gate keys on.
func TestRepoReconcile_FailedPhaseRespectsBackoff(t *testing.T) {
	mkProject(t, "rp-bo", "rp-bo-scm")
	mkSecret(t, "rp-bo-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "borepo", "rp-bo")
	setProjectMemoryReady(t, "rp-bo", "http://mem-rp-bo.tatara.svc:8080")

	lastIngest := metav1.NewTime(time.Now().Add(-6 * time.Hour))
	justFailed := metav1.NewTime(time.Now())
	seedRepoStatus(t, "borepo", func(r *tataradevv1alpha1.Repository) {
		r.Status.LastIngestedCommit = "shaBo"
		r.Status.LastIngestTime = &lastIngest
		r.Status.Phase = "Failed"
		r.Status.IngestFailureCount = 5
		r.Status.LastIngestFailureTime = &justFailed
	})

	for i := 0; i < 3; i++ {
		res, err := reconcileRepo(t, "borepo")
		if err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
		if res.RequeueAfter <= 0 {
			t.Fatalf("reconcile %d: RequeueAfter = %v, want the remaining back-off", i, res.RequeueAfter)
		}
		if res.RequeueAfter > maxIngestBackoff {
			t.Fatalf("reconcile %d: RequeueAfter = %v, want <= %v", i, res.RequeueAfter, maxIngestBackoff)
		}
	}
	if jobs := listIngestJobs(t, "borepo"); len(jobs) != 0 {
		t.Fatalf("jobs = %d, want 0: a repeatedly failing repo must back off, not hot-loop", len(jobs))
	}
	cond := findCond(getRepo(t, "borepo").Status.Conditions, "IngestBackoff")
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "IngestFailing" {
		t.Fatalf("expected IngestBackoff=True/IngestFailing while held, got %+v", cond)
	}
	if getRepo(t, "borepo").Status.Phase != "Failed" {
		t.Error("a repo with unresolved failures must keep Phase=Failed; only the desynchronised count=0 case is repaired")
	}
}

// TestRepoReconcile_ConcurrentReconcilesLaunchOneJob guards the duplicate-Job
// race in issue #457: two ingest Jobs were created for one Repository 16ms and
// 21ms apart, from the same pod under different reconcileIDs, with the
// "ingest job still active" guard losing the read-then-create race. Only the
// last Job was adopted into status; the orphan ran to completion and its
// outcome was never reconciled. The deterministic Job name makes the second
// Create return AlreadyExists, which is adopted rather than duplicated.
func TestRepoReconcile_ConcurrentReconcilesLaunchOneJob(t *testing.T) {
	mkProject(t, "rp-race", "rp-race-scm")
	mkSecret(t, "rp-race-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "racerepo", "rp-race")
	setProjectMemoryReady(t, "rp-race", "http://mem-rp-race.tatara.svc:8080")

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "racerepo"}}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	start := make(chan struct{})
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := newRepoReconciler()
			<-start
			_, errs[i] = r.Reconcile(logf.IntoContext(context.Background(), logf.Log), req)
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent reconcile %d: %v", i, err)
		}
	}

	jobs := listIngestJobs(t, "racerepo")
	if len(jobs) != 1 {
		names := make([]string, 0, len(jobs))
		for i := range jobs {
			names = append(names, jobs[i].Name)
		}
		t.Fatalf("jobs = %d %v, want exactly 1", len(jobs), names)
	}
	if got := waitRepoJob(t, "racerepo"); got != jobs[0].Name {
		t.Errorf("status.jobName = %q, want the one live job %q (an unadopted Job is never reconciled)", got, jobs[0].Name)
	}
}

// TestRepoReconcile_ForgetsGaugesForDeletedRepo covers the stale-series half of
// the project label (issue #457): the per-repo ingest gauges are now labelled by
// project as well as repo, so a series that outlives its Repository would keep
// naming a repo/project pair that no longer exists.
func TestRepoReconcile_ForgetsGaugesForDeletedRepo(t *testing.T) {
	mkProject(t, "rp-gone", "rp-gone-scm")
	repo := mkRepo(t, "gonerepo", "rp-gone")
	repo.Spec.IngestEnabled = boolPtrRC(false)
	if err := k8sClient.Update(context.Background(), repo); err != nil {
		t.Fatalf("disable ingest: %v", err)
	}

	reg := prometheus.NewRegistry()
	r := newRepoReconciler()
	r.Metrics = obs.NewOperatorMetrics(reg)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "gonerepo"}}
	if _, err := r.Reconcile(logf.IntoContext(context.Background(), logf.Log), req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := repoSeriesCount(t, reg); got != 1 {
		t.Fatalf("ingest_failing series after reconcile = %d, want 1", got)
	}

	if err := k8sClient.Delete(context.Background(), getRepo(t, "gonerepo")); err != nil {
		t.Fatalf("delete repo: %v", err)
	}
	if _, err := r.Reconcile(logf.IntoContext(context.Background(), logf.Log), req); err != nil {
		t.Fatalf("reconcile after delete: %v", err)
	}
	if got := repoSeriesCount(t, reg); got != 0 {
		t.Errorf("ingest_failing series after the Repository was deleted = %d, want 0", got)
	}
}

// repoSeriesCount returns the number of live operator_repository_ingest_failing
// series on reg.
func repoSeriesCount(t *testing.T, reg *prometheus.Registry) int {
	t.Helper()
	n, err := testutil.GatherAndCount(reg, "operator_repository_ingest_failing")
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	return n
}
