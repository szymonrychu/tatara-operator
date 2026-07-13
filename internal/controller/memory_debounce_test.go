package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
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

// TestMemoryStablyReady covers the stabilization helper.
func TestMemoryStablyReady(t *testing.T) {
	now := time.Now()
	pastWindow := now.Add(-(memoryReadyStabilizationWindow + time.Minute))
	withinWindow := now.Add(-(memoryReadyStabilizationWindow / 2))
	rs := func(t time.Time) *metav1.Time { mt := metav1.NewTime(t); return &mt }

	cases := []struct {
		name      string
		memory    *tatarav1alpha1.MemoryStatus
		wantReady bool
	}{
		{
			name:      "nil memory status",
			memory:    nil,
			wantReady: false,
		},
		{
			name:      "provisioning phase",
			memory:    &tatarav1alpha1.MemoryStatus{Phase: "Provisioning"},
			wantReady: false,
		},
		{
			name:      "ready but no ReadySince",
			memory:    &tatarav1alpha1.MemoryStatus{Phase: "Ready"},
			wantReady: false,
		},
		{
			name:      "ready but ReadySince within stabilization window",
			memory:    &tatarav1alpha1.MemoryStatus{Phase: "Ready", ReadySince: rs(withinWindow)},
			wantReady: false,
		},
		{
			name:      "ready and ReadySince past stabilization window",
			memory:    &tatarav1alpha1.MemoryStatus{Phase: "Ready", ReadySince: rs(pastWindow)},
			wantReady: true,
		},
		{
			name:      "ready and ReadySince exactly at window boundary",
			memory:    &tatarav1alpha1.MemoryStatus{Phase: "Ready", ReadySince: rs(now.Add(-memoryReadyStabilizationWindow))},
			wantReady: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &tatarav1alpha1.Project{}
			p.Status.Memory = tc.memory
			got := memoryStablyReady(p, now)
			if got != tc.wantReady {
				t.Fatalf("memoryStablyReady = %v, want %v", got, tc.wantReady)
			}
		})
	}
}

// TestTaskGate_SpawnOnly_InflightTurnNotGated verifies that a task with an
// in-flight turn is not blocked by the memory gate even when memory is not yet
// stably ready (ReadySince within window). The gate must be spawn-only.
func TestTaskGate_SpawnOnly_InflightTurnNotGated(t *testing.T) {
	mkTaskProject(t, "p-spawnonly", 3)
	mkTaskRepository(t, "r-spawnonly", "p-spawnonly")
	mkTask(t, "t-spawnonly", "p-spawnonly", "r-spawnonly")

	// Set memory Ready but with ReadySince just now (within the stabilization window).
	now := metav1.Now()
	p := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: "p-spawnonly"}, p); err != nil {
		t.Fatalf("get project: %v", err)
	}
	p.Status.Memory = &tatarav1alpha1.MemoryStatus{
		Phase:      "Ready",
		Endpoint:   "http://mem-p-spawnonly.tatara.svc:8080",
		ReadySince: &now, // within window -> not stably ready
	}
	if err := k8sClient.Status().Update(context.Background(), p); err != nil {
		t.Fatalf("set memory: %v", err)
	}

	// Give the task a live pod stage with a RUNNING pod (podStartedAt set) and an
	// in-flight turn: an already-running agent surviving a memory blip. The gate
	// is SPAWN-ONLY, keyed on podStartedAt == nil.
	tk := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: "t-spawnonly"}, tk); err != nil {
		t.Fatalf("get task: %v", err)
	}
	started := metav1.Now()
	tk.Status.Stage = tatarav1alpha1.StageImplementing
	tk.Status.PodStartedAt = &started
	if err := k8sClient.Status().Update(context.Background(), tk); err != nil {
		t.Fatalf("set task stage: %v", err)
	}
	annotate(t, "t-spawnonly", map[string]string{
		annCurrentTurn: "turn-abc-123",
	})

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	res, err := reconcileTask(t, r, "t-spawnonly")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter == memGateRequeue {
		t.Fatalf("in-flight turn must bypass memory gate (spawn-only); got memGateRequeue=%v", memGateRequeue)
	}
}
