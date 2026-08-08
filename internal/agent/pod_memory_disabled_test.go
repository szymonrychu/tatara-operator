package agent_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// TATARA_MEMORY_DEGRADED answers "will the recall tools fail this turn?" and
// stays true for a disabled project - they will. TATARA_MEMORY_DISABLED answers
// the different question "is that a fault?", so the agent side can tell an
// outage apart from a project that simply does not use memory and must not
// raise an incident about it.
func TestBuildPod_MemoryDisabledEnv(t *testing.T) {
	fa, tr := false, true
	cases := []struct {
		name         string
		spec         *tatarav1alpha1.MemorySpec
		wantDisabled string
		wantDegraded string
	}{
		{"no spec.memory at all", nil, "false", "true"},
		{"spec.memory with enabled unset", &tatarav1alpha1.MemorySpec{}, "false", "true"},
		{"explicitly enabled", &tatarav1alpha1.MemorySpec{Enabled: &tr}, "false", "true"},
		{"explicitly disabled", &tatarav1alpha1.MemorySpec{Enabled: &fa}, "true", "true"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proj, repo, task, cfg := sampleInputs()
			proj.Spec.Memory = tc.spec
			c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]

			got, ok := envValue(c, "TATARA_MEMORY_DISABLED")
			require.True(t, ok, "TATARA_MEMORY_DISABLED missing")
			require.Equal(t, tc.wantDisabled, got)

			deg, ok := envValue(c, "TATARA_MEMORY_DEGRADED")
			require.True(t, ok, "TATARA_MEMORY_DEGRADED missing")
			require.Equal(t, tc.wantDegraded, deg)
		})
	}
}
