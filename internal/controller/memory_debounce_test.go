package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// TestReadySince_TransitionEdges verifies that reconcileMemory stamps ReadySince on
// the Provisioning->Ready edge and clears it on the Ready->Provisioning edge.
func TestReadySince_TransitionEdges(t *testing.T) {
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "debounce-edges")

	// First reconcile: stack applied but not yet healthy -> Provisioning, ReadySince nil.
	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	got := getProject(t, p.Name)
	if got.Status.Memory == nil {
		t.Fatalf("status.memory nil after first reconcile")
	}
	if got.Status.Memory.Phase != "Provisioning" {
		t.Fatalf("phase = %q, want Provisioning", got.Status.Memory.Phase)
	}
	if got.Status.Memory.ReadySince != nil {
		t.Fatalf("ReadySince should be nil while Provisioning, got %v", got.Status.Memory.ReadySince)
	}

	// Transition to Ready.
	fakeStackHealthy(t, p.Name)
	beforeReady := time.Now()
	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	afterReady := time.Now()
	got = getProject(t, p.Name)
	if got.Status.Memory.Phase != "Ready" {
		t.Fatalf("phase = %q, want Ready after fakeStackHealthy", got.Status.Memory.Phase)
	}
	if got.Status.Memory.ReadySince == nil {
		t.Fatalf("ReadySince should be set on Provisioning->Ready transition")
	}
	rs := got.Status.Memory.ReadySince.Time
	if rs.Before(beforeReady.Add(-time.Second)) || rs.After(afterReady.Add(time.Second)) {
		t.Fatalf("ReadySince %v not in expected reconcile window [%v, %v]", rs, beforeReady, afterReady)
	}

	// Steady-state Ready reconcile must NOT reset ReadySince.
	firstReadySince := got.Status.Memory.ReadySince.DeepCopy()
	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("third reconcile (steady-state Ready): %v", err)
	}
	got = getProject(t, p.Name)
	if got.Status.Memory.ReadySince == nil {
		t.Fatalf("ReadySince must be preserved on steady-state Ready reconcile")
	}
	if !got.Status.Memory.ReadySince.Equal(firstReadySince) {
		t.Fatalf("ReadySince changed on steady-state Ready reconcile: was %v, now %v",
			firstReadySince, got.Status.Memory.ReadySince)
	}

	// Transition back to Provisioning (clear fake health) -> ReadySince must be nil.
	fakeStackUnhealthy(t, p.Name)
	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("fourth reconcile (back to Provisioning): %v", err)
	}
	got = getProject(t, p.Name)
	if got.Status.Memory.Phase != "Provisioning" {
		t.Fatalf("phase = %q, want Provisioning after fakeStackUnhealthy", got.Status.Memory.Phase)
	}
	if got.Status.Memory.ReadySince != nil {
		t.Fatalf("ReadySince should be nil after Ready->Provisioning, got %v", got.Status.Memory.ReadySince)
	}
}

// ---------------------------------------------------------------------------
// MEMORY IS NOT A SPAWN GATE. The admission gate (task_controller.go) and the
// pre-SubmitTurn gate (task_stage.go, issue #355) are GONE: they held every
// Task indefinitely at a 15s poll with no timeout and no escape hatch, and the
// documented outage that motivated them ran 7h+. An agent now SPAWNS with
// memory down, is told so via TATARA_MEMORY_DEGRADED and the degraded prompt
// appendix, and completes its assignment with reduced recall. The stably-ready
// predicate survives - it gates INGEST (repository_controller.go), where a
// partial corpus would actually be written - and it drives the degraded flag.
// ---------------------------------------------------------------------------

func setProjectMemory(t *testing.T, name string, mem *tatarav1alpha1.MemoryStatus) {
	t.Helper()
	p := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: name}, p); err != nil {
		t.Fatalf("get project: %v", err)
	}
	p.Status.Memory = mem
	if err := k8sClient.Status().Update(context.Background(), p); err != nil {
		t.Fatalf("set memory: %v", err)
	}
}

// TestTaskSpawn_ProceedsWhenMemoryNotStablyReady is the reversal of issue #355's
// gate: a Task with no pod yet must reach the pod-spawn path with memory NOT
// stably ready, and must be MARKED degraded rather than held.
func TestTaskSpawn_ProceedsWhenMemoryNotStablyReady(t *testing.T) {
	mkTaskProject(t, "p-degraded", 3)
	mkTaskRepository(t, "r-degraded", "p-degraded")
	mkTaskWithKind(t, "t-degraded", "p-degraded", "r-degraded", "clarify")

	// Ready, but ReadySince is NOW: inside the stabilization window, so the
	// project is not stably ready. This is exactly what used to hold the spawn.
	now := metav1.Now()
	setProjectMemory(t, "p-degraded", &tatarav1alpha1.MemoryStatus{
		Phase:      "Ready",
		Endpoint:   "http://mem-p-degraded.tatara.svc:8080",
		ReadySince: &now,
	})
	setTaskStage(t, "t-degraded", tatarav1alpha1.StageImplementing)

	r := newTaskReconciler(newFakeSession())
	res, err := reconcileTask(t, r, "t-degraded")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != agentBootRequeue {
		t.Fatalf("RequeueAfter = %v, want agentBootRequeue %v (the spawn must proceed, not poll a gate)",
			res.RequeueAfter, agentBootRequeue)
	}
	if !podExists(t, agent.PodName(getTask(t, "t-degraded"))) {
		t.Fatalf("no wrapper pod created: the memory gate is still holding the spawn")
	}
	if v := testutil.ToFloat64(r.Metrics.AgentPodDegradedCounter("p-degraded", "clarify", "memory")); v != 1 {
		t.Fatalf("operator_agent_pod_degraded_total{project=p-degraded,kind=clarify,subsystem=memory} = %v, want 1", v)
	}
	cond := findCond(getTask(t, "t-degraded").Status.Conditions, tatarav1alpha1.ConditionMemoryDegraded)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("MemoryDegraded condition = %+v, want True", cond)
	}
	if cond.Reason != tatarav1alpha1.ReasonSpawnedWithoutRecall {
		t.Fatalf("MemoryDegraded reason = %q, want %q", cond.Reason, tatarav1alpha1.ReasonSpawnedWithoutRecall)
	}
}

// A stably-ready project spawns exactly the same pod and must NOT be flagged.
func TestTaskSpawn_NotDegradedWhenMemoryStablyReady(t *testing.T) {
	mkTaskProject(t, "p-healthy", 3)
	mkTaskRepository(t, "r-healthy", "p-healthy")
	mkTaskWithKind(t, "t-healthy", "p-healthy", "r-healthy", "clarify")

	setProjectMemory(t, "p-healthy", stableMemStatus("http://mem-p-healthy.tatara.svc:8080"))
	setTaskStage(t, "t-healthy", tatarav1alpha1.StageImplementing)

	r := newTaskReconciler(newFakeSession())
	if _, err := reconcileTask(t, r, "t-healthy"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !podExists(t, agent.PodName(getTask(t, "t-healthy"))) {
		t.Fatalf("no wrapper pod created for a healthy project")
	}
	if v := testutil.ToFloat64(r.Metrics.AgentPodDegradedCounter("p-healthy", "clarify", "memory")); v != 0 {
		t.Fatalf("operator_agent_pod_degraded_total = %v, want 0 for a stably-ready project", v)
	}
	if c := findCond(getTask(t, "t-healthy").Status.Conditions, tatarav1alpha1.ConditionMemoryDegraded); c != nil {
		t.Fatalf("MemoryDegraded condition set on a healthy spawn: %+v", c)
	}
}
