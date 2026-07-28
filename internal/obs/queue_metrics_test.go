package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestAdmissionWakeCountsPerClassAndAgentKind pins the admission-wake counter's
// name and its two-label shape. The wake is the whole point of the QueuedEvent
// watch: without a counter, "the 5-minute stall is gone" is only provable in
// envtest, never in production.
func TestAdmissionWakeCountsPerClassAndAgentKind(t *testing.T) {
	m := NewOperatorMetrics(prometheus.NewRegistry())

	m.AdmissionWake("normal", "implement")
	m.AdmissionWake("normal", "implement")
	m.AdmissionWake("alert", "incident")

	if got := testutil.ToFloat64(m.AdmissionWakeCounter("normal", "implement")); got != 2 {
		t.Fatalf("operator_admission_wake_total{class=normal,agent_kind=implement} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.AdmissionWakeCounter("alert", "incident")); got != 1 {
		t.Fatalf("operator_admission_wake_total{class=alert,agent_kind=incident} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.AdmissionWakeCounter("normal", "review")); got != 0 {
		t.Fatalf("an untouched label pair must be 0, got %v", got)
	}
}
