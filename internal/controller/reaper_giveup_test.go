package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// mkParkedLifecycleTask creates a terminal Parked issueLifecycle task
// for reaper GC tests with given giveUps and parkReason.
func mkParkedLifecycleTask(t *testing.T, name, project, repoRef string, giveUps int, parkReason string) {
	t.Helper()
	tk := &tatarav1alpha1.Task{}
	tk.Name = name
	tk.Namespace = testNS
	tk.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef:    project,
		RepositoryRef: repoRef,
		Goal:          "implement something",
		Kind:          "issueLifecycle",
	}
	if err := k8sClient.Create(context.Background(), tk); err != nil {
		t.Fatalf("mkParkedLifecycleTask create %s: %v", name, err)
	}
	tk.Status.LifecycleState = "Parked"
	tk.Status.ParkReason = parkReason
	tk.Status.ImplementGiveUps = giveUps
	if err := k8sClient.Status().Update(context.Background(), tk); err != nil {
		t.Fatalf("mkParkedLifecycleTask status %s: %v", name, err)
	}
}

// TestReapGC_GiveUp_UnderCap_Spared verifies that a Parked issueLifecycle task
// with a recoverable ParkReason and 0 < ImplementGiveUps < maxImplGiveUps is
// spared from GC so recoverOrphans can reroll it.
func TestReapGC_GiveUp_UnderCap_Spared(t *testing.T) {
	mkTaskProject(t, "p-gc-gu-spare", 3)
	mkTaskRepository(t, "r-gc-gu-spare", "p-gc-gu-spare")
	mkParkedLifecycleTask(t, "t-gc-gu-spare", "p-gc-gu-spare", "r-gc-gu-spare", 1, "implement-failed")

	// Retention of 1ns: the task would normally be GC-ed.
	gcServer(prometheus.NewRegistry(), time.Nanosecond).ReapOrphans(context.Background())

	if !taskExists(t, "t-gc-gu-spare") {
		t.Error("expected under-cap give-up task to be spared from GC")
	}
}

// TestReapGC_GiveUp_AtCap_Spared verifies that a Parked task AT the give-up cap
// is still spared from time-based GC: while its issue is open the counter must
// persist so recoverOrphans keeps it blocked (not restart from zero). The
// closed-issue sweep transitions it to Done once the issue closes, after which
// it GCs normally.
func TestReapGC_GiveUp_AtCap_Spared(t *testing.T) {
	mkTaskProject(t, "p-gc-gu-cap", 3)
	mkTaskRepository(t, "r-gc-gu-cap", "p-gc-gu-cap")
	mkParkedLifecycleTask(t, "t-gc-gu-cap", "p-gc-gu-cap", "r-gc-gu-cap", maxImplGiveUps, "maxIterations")

	gcServer(prometheus.NewRegistry(), time.Nanosecond).ReapOrphans(context.Background())

	if !taskExists(t, "t-gc-gu-cap") {
		t.Error("expected at-cap give-up task to be spared while its issue is open")
	}
}

// TestReapGC_GiveUp_NonRecoverable_GCed verifies that a Parked task with a
// non-recoverable reason is garbage-collected even if ImplementGiveUps > 0.
func TestReapGC_GiveUp_NonRecoverable_GCed(t *testing.T) {
	mkTaskProject(t, "p-gc-gu-nonrec", 3)
	mkTaskRepository(t, "r-gc-gu-nonrec", "p-gc-gu-nonrec")
	mkParkedLifecycleTask(t, "t-gc-gu-nonrec", "p-gc-gu-nonrec", "r-gc-gu-nonrec", 1, "refused-declined")

	gcServer(prometheus.NewRegistry(), time.Nanosecond).ReapOrphans(context.Background())

	if taskExists(t, "t-gc-gu-nonrec") {
		t.Error("expected non-recoverable parked task to be GC-ed normally")
	}
}

// TestReapGC_GiveUp_ZeroGiveUps_GCed verifies that a Parked task with recoverable
// reason but ImplementGiveUps=0 is GC-ed normally (counter is not yet set,
// meaning the task predates the give-up tracking; no reroll outstanding).
func TestReapGC_GiveUp_ZeroGiveUps_GCed(t *testing.T) {
	mkTaskProject(t, "p-gc-gu-zero", 3)
	mkTaskRepository(t, "r-gc-gu-zero", "p-gc-gu-zero")
	mkParkedLifecycleTask(t, "t-gc-gu-zero", "p-gc-gu-zero", "r-gc-gu-zero", 0, "implement-failed")

	gcServer(prometheus.NewRegistry(), time.Nanosecond).ReapOrphans(context.Background())

	if taskExists(t, "t-gc-gu-zero") {
		t.Error("expected zero-giveUps parked task to be GC-ed (no reroll outstanding)")
	}
}

// TestReapGC_GiveUp_MaxMinusOne_Spared verifies the boundary: giveUps == maxImplGiveUps-1
// is still spared (strictly under cap).
func TestReapGC_GiveUp_MaxMinusOne_Spared(t *testing.T) {
	mkTaskProject(t, "p-gc-gu-maxm1", 3)
	mkTaskRepository(t, "r-gc-gu-maxm1", "p-gc-gu-maxm1")
	mkParkedLifecycleTask(t, "t-gc-gu-maxm1", "p-gc-gu-maxm1", "r-gc-gu-maxm1", maxImplGiveUps-1, "deadline")

	gcServer(prometheus.NewRegistry(), time.Nanosecond).ReapOrphans(context.Background())

	if !taskExists(t, "t-gc-gu-maxm1") {
		t.Error("expected maxImplGiveUps-1 give-up task to be spared (still under cap)")
	}
}
