package agent_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// The memory stack is never a spawn gate any more: an agent pod is built and
// run whatever the stack's phase, and TATARA_MEMORY_DEGRADED is how the pod
// LEARNS that recall will fail this turn. The wrapper hands os.Environ() to the
// agent process wholesale, so no wrapper change carries it through.
func TestBuildPod_MemoryDegradedEnv(t *testing.T) {
	cases := []struct {
		name   string
		memory *tatarav1alpha1.MemoryStatus
		want   string
	}{
		{name: "no memory status yet", memory: nil, want: "true"},
		{name: "provisioning", memory: &tatarav1alpha1.MemoryStatus{Phase: "Provisioning"}, want: "true"},
		{name: "degraded", memory: &tatarav1alpha1.MemoryStatus{Phase: "Degraded"}, want: "true"},
		{
			name:   "ready but inside the stabilization window",
			memory: readyMemory(time.Now()),
			want:   "true",
		},
		{
			name:   "stably ready",
			memory: readyMemory(time.Now().Add(-(tatarav1alpha1.MemoryReadyStabilizationWindow + time.Minute))),
			want:   "false",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proj, repo, task, cfg := sampleInputs()
			proj.Status.Memory = tc.memory
			c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]

			got, ok := envValue(c, "TATARA_MEMORY_DEGRADED")
			require.True(t, ok, "TATARA_MEMORY_DEGRADED missing")
			require.Equal(t, tc.want, got)

			// The endpoint is still handed over even when degraded: the tools must
			// FAIL against the real backend, not be silently unconfigured.
			url, ok := envValue(c, "TATARA_MEMORY_URL")
			require.True(t, ok, "TATARA_MEMORY_URL missing")
			require.Equal(t, testMemoryEndpoint, url)
		})
	}
}

func readyMemory(since time.Time) *tatarav1alpha1.MemoryStatus {
	rs := metav1.NewTime(since)
	return &tatarav1alpha1.MemoryStatus{Phase: "Ready", ReadySince: &rs}
}
