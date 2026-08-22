package obs

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestSetAccountUsageSnapshotAge locks the exposition of the snapshot-age gauge,
// including that the feed name is carried as a "source" label. The label lives
// on THIS gauge and deliberately not on tatara_account_usage_utilization or
// _resets_at_seconds, whose existing series must not be relabelled.
func TestSetAccountUsageSnapshotAge(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.SetAccountUsageSnapshotAge("wrapper", 42)
	m.SetAccountUsageSnapshotAge("poller", 7)

	want := `
# HELP tatara_account_usage_snapshot_age_seconds Age in seconds of the account usage snapshot currently governing admission, by feed source.
# TYPE tatara_account_usage_snapshot_age_seconds gauge
tatara_account_usage_snapshot_age_seconds{source="poller"} 7
tatara_account_usage_snapshot_age_seconds{source="wrapper"} 42
`
	if err := testutil.CollectAndCompare(reg, strings.NewReader(want), "tatara_account_usage_snapshot_age_seconds"); err != nil {
		t.Fatal(err)
	}
}

// TestSetAccountUsageGateReady locks BOTH halves of the gate-ready contract: the
// series must be ABSENT until the gate reports, so an operator with the budget
// gate disabled never renders a 0 that TataraAccountUsageFeedDead would fire on;
// and once reported it must be 0/1.
func TestSetAccountUsageGateReady(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	if n := testutil.CollectAndCount(reg, "tatara_account_usage_gate_ready"); n != 0 {
		t.Fatalf("gate_ready rendered %d series before any report, want 0: a disabled gate must create no series at all", n)
	}

	m.SetAccountUsageGateReady(false)
	if got := testutil.ToFloat64(m.accountUsageGateReady); got != 0 {
		t.Fatalf("gate_ready = %v after SetAccountUsageGateReady(false), want 0", got)
	}
	m.SetAccountUsageGateReady(true)
	if got := testutil.ToFloat64(m.accountUsageGateReady); got != 1 {
		t.Fatalf("gate_ready = %v after SetAccountUsageGateReady(true), want 1", got)
	}
}
