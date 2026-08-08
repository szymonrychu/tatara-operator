package controller

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// A Task spawned under a memory-DISABLED project must not be recorded as a
// degradation: no operator_agent_pod_degraded_total increment, and a
// False/MemoryDisabled condition instead of True/SpawnedWithoutRecall. Otherwise
// every single turn of a memory-free project reads as an incident forever.
func TestTaskSpawn_MemoryDisabledIsNotADegradation(t *testing.T) {
	mkTaskProject(t, "p-memoff-task", 3)
	mkTaskRepository(t, "r-memoff-task", "p-memoff-task")
	mkTaskWithKind(t, "t-memoff-task", "p-memoff-task", "r-memoff-task", "implement")
	setProjectMemoryEnabled(t, "p-memoff-task", false)
	setProjectMemory(t, "p-memoff-task", &tatarav1alpha1.MemoryStatus{
		Phase: tatarav1alpha1.MemoryPhaseDisabled,
	})
	setTaskStage(t, "t-memoff-task", tatarav1alpha1.StateUnderImplementation)

	r := newTaskReconciler(newFakeSession())
	if _, err := reconcileTask(t, r, "t-memoff-task"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !podExists(t, agent.PodName(getTask(t, "t-memoff-task"))) {
		t.Fatal("no wrapper pod created: memory must never be a spawn gate, disabled or not")
	}
	if v := testutil.ToFloat64(
		r.Metrics.AgentPodDegradedCounter("p-memoff-task", "implement", "memory")); v != 0 {
		t.Errorf("operator_agent_pod_degraded_total = %v, want 0: memory is off by configuration", v)
	}
	cond := findCond(getTask(t, "t-memoff-task").Status.Conditions, tatarav1alpha1.ConditionMemoryDegraded)
	if cond == nil {
		t.Fatal("no MemoryDegraded condition: a human must still see the agent had no recall")
	}
	if cond.Status != metav1.ConditionFalse || cond.Reason != tatarav1alpha1.ReasonMemoryDisabled {
		t.Fatalf("MemoryDegraded = %s/%s, want False/%s",
			cond.Status, cond.Reason, tatarav1alpha1.ReasonMemoryDisabled)
	}
}
