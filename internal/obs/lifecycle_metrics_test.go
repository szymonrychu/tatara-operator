package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestImplementEmptyRetry(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewLifecycleMetrics(reg)

	// Before any call, counter should be 0.
	if got := testutil.ToFloat64(m.implementEmptyRetry); got != 0 {
		t.Fatalf("implement_empty_retry_total before Inc = %v, want 0", got)
	}

	m.ImplementEmptyRetry()
	m.ImplementEmptyRetry()

	if got := testutil.ToFloat64(m.implementEmptyRetry); got != 2 {
		t.Fatalf("implement_empty_retry_total = %v, want 2", got)
	}
}

func TestLifecycleMetricsNamesStable(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewLifecycleMetrics(reg)
	// Touch each metric so it emits at least one series in Gather.
	m.RecordTransition("Triage", "Implement")
	m.RecordGiveup("triage-failed")
	m.SetDeployState("Triage", 0)
	m.RecordHandover()
	m.RecordIdleStop()
	m.ObserveMRCIWait(1)
	m.ObserveLifecycle(1)
	m.ImplementEmptyRetry()

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	want := map[string]bool{
		"tatara_lifecycle_state":                       false,
		"tatara_lifecycle_transition_total":            false,
		"tatara_lifecycle_handover_total":              false,
		"tatara_lifecycle_giveup_total":                false,
		"tatara_lifecycle_idle_stop_total":             false,
		"tatara_mrci_wait_seconds":                     false,
		"tatara_lifecycle_seconds":                     false,
		"tatara_lifecycle_implement_empty_retry_total": false,
	}
	for _, mf := range mfs {
		if _, ok := want[mf.GetName()]; ok {
			want[mf.GetName()] = true
		}
	}
	for name, seen := range want {
		if !seen {
			t.Errorf("metric %q not registered", name)
		}
	}
}
