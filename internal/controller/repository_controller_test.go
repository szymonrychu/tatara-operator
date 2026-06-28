package controller

import (
	"context"
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
		rp.Spec.IngestEnabled = boolPtrRC(enabled)
		rp.Status.Phase = phase
		rp.Status.IngestFailureCount = failCount
		rp.Status.LastIngestTime = last
		return rp
	}

	// Failed phase -> failing 1.
	r.publishIngestHealth(mkRepoObj("r-failed", "Failed", 0, true, nil))
	if got := testutil.ToFloat64(m.RepositoryIngestFailingGauge("r-failed")); got != 1 {
		t.Errorf("failed phase: failing = %v, want 1", got)
	}
	// Mid-retry (Ingesting but unresolved consecutive failures) -> failing 1.
	r.publishIngestHealth(mkRepoObj("r-retry", "Ingesting", 2, true, nil))
	if got := testutil.ToFloat64(m.RepositoryIngestFailingGauge("r-retry")); got != 1 {
		t.Errorf("retrying: failing = %v, want 1", got)
	}
	// Healthy (Ingested, no failures) -> failing 0 and timestamp published.
	ts := metav1.Unix(1750000000, 0)
	r.publishIngestHealth(mkRepoObj("r-ok", "Ingested", 0, true, &ts))
	if got := testutil.ToFloat64(m.RepositoryIngestFailingGauge("r-ok")); got != 0 {
		t.Errorf("healthy: failing = %v, want 0", got)
	}
	if got := testutil.ToFloat64(m.RepositoryLastIngestTimestampGauge("r-ok")); got != 1750000000 {
		t.Errorf("healthy: last_ingest_ts = %v, want 1750000000", got)
	}
	// Recovery on the SAME repo: the gauge must clear, not stay latched - this is
	// the whole point of the current-state signal vs the monotonic counter (#138).
	r.publishIngestHealth(mkRepoObj("r-heal", "Failed", 3, true, nil))
	if got := testutil.ToFloat64(m.RepositoryIngestFailingGauge("r-heal")); got != 1 {
		t.Fatalf("pre-recovery: failing = %v, want 1", got)
	}
	r.publishIngestHealth(mkRepoObj("r-heal", "Ingested", 0, true, &ts))
	if got := testutil.ToFloat64(m.RepositoryIngestFailingGauge("r-heal")); got != 0 {
		t.Errorf("post-recovery: failing = %v, want 0 (must clear)", got)
	}
	// A disabled repo never reports failing even if its status looks failed.
	r.publishIngestHealth(mkRepoObj("r-disabled", "Failed", 5, false, nil))
	if got := testutil.ToFloat64(m.RepositoryIngestFailingGauge("r-disabled")); got != 0 {
		t.Errorf("disabled: failing = %v, want 0", got)
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
	p.Status.Memory = &tataradevv1alpha1.MemoryStatus{Phase: "Ready", Endpoint: endpoint}
	if err := k8sClient.Status().Update(context.Background(), p); err != nil {
		t.Fatalf("set project %s memory ready: %v", name, err)
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

func TestRepoReconcile_ScheduleRequeuesWhenNotDue(t *testing.T) {
	mkProject(t, "rp-sch2", "rp-sch2-scm")
	mkSecret(t, "rp-sch2-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "sch2", "rp-sch2")
	setProjectMemoryReady(t, "rp-sch2", "http://mem-rp-sch2.tatara.svc:8080")

	// Far-future daily schedule + a fresh ingest => not due; expect a requeue.
	r := getRepo(t, "sch2")
	r.Spec.ReingestSchedule = "0 6 * * *"
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
	r.Spec.ReingestSchedule = "0 6 * * *"
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
