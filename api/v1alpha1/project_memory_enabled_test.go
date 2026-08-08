package v1alpha1

import (
	"testing"
	"time"
)

// Memory is OPTIONAL but ON BY DEFAULT. spec.memory.enabled is a *bool with no
// kubebuilder default precisely so nil is distinguishable from an explicit
// false: every Project that predates the field - and every Project that never
// mentions spec.memory at all - must keep its memory stack untouched.
func TestMemoryEnabled_DefaultsOnWhenUnset(t *testing.T) {
	tr, fa := true, false
	cases := []struct {
		name string
		p    *Project
		want bool
	}{
		{"nil project", nil, true},
		{"no spec.memory at all", &Project{}, true},
		{"spec.memory present, enabled unset", &Project{Spec: ProjectSpec{Memory: &MemorySpec{PgStorage: "20Gi"}}}, true},
		{"enabled explicitly true", &Project{Spec: ProjectSpec{Memory: &MemorySpec{Enabled: &tr}}}, true},
		{"enabled explicitly false", &Project{Spec: ProjectSpec{Memory: &MemorySpec{Enabled: &fa}}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.p.MemoryEnabled(); got != tc.want {
				t.Fatalf("MemoryEnabled() = %v, want %v", got, tc.want)
			}
			if got := MemoryDisabled(tc.p); got != !tc.want {
				t.Fatalf("MemoryDisabled() = %v, want %v", got, !tc.want)
			}
		})
	}
}

// The Disabled phase is terminal and must be distinct from every phase that
// means "something is wrong" - the memory alert set keys on Failed/Degraded.
func TestMemoryPhaseDisabled_IsDistinct(t *testing.T) {
	for _, other := range []string{"", "Provisioning", "Ready", "Degraded", "Failed"} {
		if MemoryPhaseDisabled == other {
			t.Fatalf("MemoryPhaseDisabled collides with %q", other)
		}
	}
}

// A disabled project is never "stably ready": the ingest gate, the pod env and
// the turn-0 appendix all read this predicate and must not be told recall works.
func TestMemoryStablyReady_FalseWhenDisabled(t *testing.T) {
	fa := false
	p := &Project{Spec: ProjectSpec{Memory: &MemorySpec{Enabled: &fa}}}
	p.Status.Memory = &MemoryStatus{Phase: MemoryPhaseDisabled}
	if MemoryStablyReady(p, time.Now()) {
		t.Fatal("a memory-disabled project must not read stably ready")
	}
}
