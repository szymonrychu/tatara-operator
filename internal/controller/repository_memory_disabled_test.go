package controller

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// mkProjectMemoryDisabled creates a Project that has never had a memory stack.
func mkProjectMemoryDisabled(t *testing.T, name, secretRef string) {
	t.Helper()
	mkProject(t, name, secretRef)
	setProjectMemoryEnabled(t, name, false)
}

// The ingest gate is the ONE path that blocks forever: it sets MemoryNotReady,
// raises operator_repository_ingest_gated, and requeues at 15s with no terminal
// state. For a project whose memory is DISABLED there is nothing to wait for -
// that loop would spin, and alert, until the end of time (TataraIngestGated
// fires after 1h of it).
func TestRepoReconcile_MemoryDisabledSkipsIngestWithoutRequeue(t *testing.T) {
	mkProjectMemoryDisabled(t, "rp-memoff", "rp-memoff-scm")
	mkSecret(t, "rp-memoff-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "memoff", "rp-memoff")

	r := newRepoReconciler()
	res, err := reconcileRepoWith(t, r, "memoff")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %s, want 0: a disabled memory stack is never going to become ready",
			res.RequeueAfter)
	}
	if len(listIngestJobs(t, "memoff")) != 0 {
		t.Fatal("an ingest job was launched for a project with memory disabled")
	}

	repo := getRepo(t, "memoff")
	cond := findCond(repo.Status.Conditions, "MemoryNotReady")
	if cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("MemoryNotReady = %+v, want False: the repo is not waiting on anything", cond)
	}
	if cond.Reason != repoReasonMemoryDisabled {
		t.Fatalf("MemoryNotReady reason = %q, want %q", cond.Reason, repoReasonMemoryDisabled)
	}
	if repo.Status.Phase != repoPhaseMemoryDisabled {
		t.Fatalf("phase = %q, want %q (ingest not applicable)", repo.Status.Phase, repoPhaseMemoryDisabled)
	}

	// The two alerts that would otherwise fire forever must have nothing to key on.
	if v := testutil.ToFloat64(r.Metrics.RepositoryIngestGatedGauge("rp-memoff", "memoff")); v != 0 {
		t.Errorf("operator_repository_ingest_gated = %v, want 0 (TataraIngestGated would fire at 1h)", v)
	}
	if v := testutil.ToFloat64(r.Metrics.RepositoryLastIngestTimestampGauge("rp-memoff", "memoff")); v != 0 {
		t.Errorf("operator_repository_last_ingest_timestamp_seconds = %v, want the series retired "+
			"(TataraIngestStale reads time() - this gauge)", v)
	}
}

// A repo that HAS been ingested and then has its project's memory disabled must
// have its staleness series retired too - otherwise TataraIngestStale keeps
// ageing a timestamp that will never be refreshed again.
func TestRepoReconcile_MemoryDisabledRetiresStaleIngestSeries(t *testing.T) {
	mkProject(t, "rp-memoff2", "rp-memoff2-scm")
	mkSecret(t, "rp-memoff2-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "memoff2", "rp-memoff2")
	setProjectMemoryReady(t, "rp-memoff2", "http://mem-rp-memoff2.tatara.svc:8080")

	repo := getRepo(t, "memoff2")
	last := metav1.NewTime(time.Now().Add(-72 * time.Hour))
	repo.Status.LastIngestTime = &last
	repo.Status.LastIngestedCommit = "sha0"
	repo.Status.Phase = "Ingested"
	if err := k8sClient.Status().Update(logfIntoTestCtx(), repo); err != nil {
		t.Fatalf("seed status: %v", err)
	}

	r := newRepoReconciler()
	if _, err := reconcileRepoWith(t, r, "memoff2"); err != nil {
		t.Fatalf("reconcile with memory ready: %v", err)
	}
	if v := testutil.ToFloat64(r.Metrics.RepositoryLastIngestTimestampGauge("rp-memoff2", "memoff2")); v == 0 {
		t.Fatal("the stale-ingest series was never published; the test proves nothing")
	}

	setProjectMemoryEnabled(t, "rp-memoff2", false)
	if _, err := reconcileRepoWith(t, r, "memoff2"); err != nil {
		t.Fatalf("reconcile with memory disabled: %v", err)
	}
	if v := testutil.ToFloat64(r.Metrics.RepositoryLastIngestTimestampGauge("rp-memoff2", "memoff2")); v != 0 {
		t.Errorf("operator_repository_last_ingest_timestamp_seconds = %v, want the series retired", v)
	}
}

// The ingestEnabled=false early-return sits before the memory-disabled
// short-circuit, so it must retire the staleness series itself - otherwise a
// memory-disabled project whose repos also have ingest off still walks into
// TataraIngestStale.
func TestRepoReconcile_IngestDisabledRetiresStaleIngestSeries(t *testing.T) {
	mkProject(t, "rp-ingoff", "rp-ingoff-scm")
	mkSecret(t, "rp-ingoff-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "ingoff", "rp-ingoff")
	setProjectMemoryReady(t, "rp-ingoff", "http://mem-rp-ingoff.tatara.svc:8080")

	live := getRepo(t, "ingoff")
	last := metav1.NewTime(time.Now().Add(-72 * time.Hour))
	live.Status.LastIngestTime = &last
	live.Status.LastIngestedCommit = "sha0"
	live.Status.Phase = "Ingested"
	if err := k8sClient.Status().Update(logfIntoTestCtx(), live); err != nil {
		t.Fatalf("seed status: %v", err)
	}

	r := newRepoReconciler()
	if _, err := reconcileRepoWith(t, r, "ingoff"); err != nil {
		t.Fatalf("reconcile with ingest enabled: %v", err)
	}
	if v := testutil.ToFloat64(r.Metrics.RepositoryLastIngestTimestampGauge("rp-ingoff", "ingoff")); v == 0 {
		t.Fatal("the stale-ingest series was never published; the test proves nothing")
	}

	live = getRepo(t, "ingoff")
	live.Spec.IngestEnabled = boolPtrRC(false)
	if err := k8sClient.Update(logfIntoTestCtx(), live); err != nil {
		t.Fatalf("disable ingest: %v", err)
	}
	if _, err := reconcileRepoWith(t, r, "ingoff"); err != nil {
		t.Fatalf("reconcile with ingest disabled: %v", err)
	}
	if v := testutil.ToFloat64(r.Metrics.RepositoryLastIngestTimestampGauge("rp-ingoff", "ingoff")); v != 0 {
		t.Errorf("operator_repository_last_ingest_timestamp_seconds = %v, want the series retired", v)
	}
}

// The pre-existing behaviour must be untouched: memory that is ENABLED but not
// yet stably ready still holds ingest and still polls, because it genuinely is
// going to become ready.
func TestRepoReconcile_MemoryEnabledButNotReadyStillGates(t *testing.T) {
	mkProject(t, "rp-notready", "rp-notready-scm")
	mkSecret(t, "rp-notready-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "notready", "rp-notready")
	setProjectMemory(t, "rp-notready", &tataradevv1alpha1.MemoryStatus{Phase: "Provisioning"})

	r := newRepoReconciler()
	res, err := reconcileRepoWith(t, r, "notready")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("RequeueAfter = 0: an enabled-but-provisioning stack must keep polling the gate")
	}
	cond := findCond(getRepo(t, "notready").Status.Conditions, "MemoryNotReady")
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "MemoryProvisioning" {
		t.Fatalf("MemoryNotReady = %+v, want True/MemoryProvisioning (unchanged behaviour)", cond)
	}
	if v := testutil.ToFloat64(r.Metrics.RepositoryIngestGatedGauge("rp-notready", "notready")); v != 1 {
		t.Errorf("operator_repository_ingest_gated = %v, want 1 for a genuinely gated repo", v)
	}
}
