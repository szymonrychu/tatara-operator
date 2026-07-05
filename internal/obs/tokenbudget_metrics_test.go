package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestSetTokenBudgetUsedRatio(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.SetTokenBudgetUsedRatio("proj-a", "used", 0.62)
	m.SetTokenBudgetUsedRatio("proj-a", "proactive", 0.5)
	m.SetTokenBudgetUsedRatio("proj-a", "emergency", 0.8)
	// A later set overwrites (gauge semantics).
	m.SetTokenBudgetUsedRatio("proj-a", "used", 0.7)

	if got := testutil.ToFloat64(m.tokenBudgetUsedRatio.WithLabelValues("proj-a", "used")); got != 0.7 {
		t.Fatalf("used = %v, want 0.7", got)
	}
	if got := testutil.ToFloat64(m.tokenBudgetUsedRatio.WithLabelValues("proj-a", "proactive")); got != 0.5 {
		t.Fatalf("proactive = %v, want 0.5", got)
	}
	if got := testutil.ToFloat64(m.tokenBudgetUsedRatio.WithLabelValues("proj-a", "emergency")); got != 0.8 {
		t.Fatalf("emergency = %v, want 0.8", got)
	}
}

func TestAdmissionBlocked(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.AdmissionBlocked("proj-a", "normal", "", "token_budget")
	m.AdmissionBlocked("proj-a", "normal", "", "token_budget")
	m.AdmissionBlocked("proj-a", "alert", "", "token_budget")

	if got := testutil.ToFloat64(m.AdmissionBlockedCounter("proj-a", "normal", "", "token_budget")); got != 2 {
		t.Fatalf("normal/token_budget = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.AdmissionBlockedCounter("proj-a", "alert", "", "token_budget")); got != 1 {
		t.Fatalf("alert/token_budget = %v, want 1", got)
	}
}

// TestTokenBudgetMetricsGathered confirms the two collectors are registered and
// appear in Gather once set live (they are intentionally not pre-seeded because
// the project label is unbounded, mirroring queue_depth).
func TestTokenBudgetMetricsGathered(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.SetTokenBudgetUsedRatio("proj-a", "used", 0.1)
	m.AdmissionBlocked("proj-a", "normal", "", "token_budget")

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	seen := map[string]bool{}
	for _, mf := range mfs {
		seen[mf.GetName()] = true
	}
	for _, name := range []string{"operator_token_budget_used_ratio", "operator_admission_blocked_total"} {
		if !seen[name] {
			t.Errorf("%s not gathered after a live set", name)
		}
	}
}
