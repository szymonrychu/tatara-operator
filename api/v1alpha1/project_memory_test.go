package v1alpha1_test

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// TestMemoryStablyReady covers the stabilization predicate. It moved here from
// internal/controller (memory_debounce_test.go) when the degraded-not-blocking
// change made internal/agent a caller: the agent package cannot import
// internal/controller, and the predicate now feeds the ingest gate, the
// TATARA_MEMORY_DEGRADED pod env and the degraded prompt appendix rather than
// blocking a spawn.
func TestMemoryStablyReady(t *testing.T) {
	now := time.Now()
	pastWindow := now.Add(-(v1alpha1.MemoryReadyStabilizationWindow + time.Minute))
	withinWindow := now.Add(-(v1alpha1.MemoryReadyStabilizationWindow / 2))
	rs := func(t time.Time) *metav1.Time { mt := metav1.NewTime(t); return &mt }

	cases := []struct {
		name      string
		memory    *v1alpha1.MemoryStatus
		wantReady bool
	}{
		{
			name:      "nil memory status",
			memory:    nil,
			wantReady: false,
		},
		{
			name:      "provisioning phase",
			memory:    &v1alpha1.MemoryStatus{Phase: "Provisioning"},
			wantReady: false,
		},
		{
			name:      "ready but no ReadySince",
			memory:    &v1alpha1.MemoryStatus{Phase: "Ready"},
			wantReady: false,
		},
		{
			name:      "ready but ReadySince within stabilization window",
			memory:    &v1alpha1.MemoryStatus{Phase: "Ready", ReadySince: rs(withinWindow)},
			wantReady: false,
		},
		{
			name:      "ready and ReadySince past stabilization window",
			memory:    &v1alpha1.MemoryStatus{Phase: "Ready", ReadySince: rs(pastWindow)},
			wantReady: true,
		},
		{
			name:      "ready and ReadySince exactly at window boundary",
			memory:    &v1alpha1.MemoryStatus{Phase: "Ready", ReadySince: rs(now.Add(-v1alpha1.MemoryReadyStabilizationWindow))},
			wantReady: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &v1alpha1.Project{}
			p.Status.Memory = tc.memory
			if got := v1alpha1.MemoryStablyReady(p, now); got != tc.wantReady {
				t.Fatalf("MemoryStablyReady = %v, want %v", got, tc.wantReady)
			}
		})
	}
}

// TestMemoryStablyReady_NilProject guards the pod-build call site, which runs
// on whatever Project the reconciler holds.
func TestMemoryStablyReady_NilProject(t *testing.T) {
	if v1alpha1.MemoryStablyReady(nil, time.Now()) {
		t.Fatalf("MemoryStablyReady(nil) = true, want false")
	}
}
