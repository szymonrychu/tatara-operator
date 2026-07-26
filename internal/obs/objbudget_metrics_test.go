package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/szymonrychu/tatara-operator/internal/objbudget"
)

func TestObjBudgetMetricsRecord(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewObjBudgetMetrics(reg)

	m.ObserveObjectSize("Issue", 12345)
	m.IncObjectTooLarge("Task", "proj-implement-2026-07-12-abcde")
	m.IncCommentSpill("Issue")
	m.IncCommentSpill("Issue")
	m.IncSpillBlocked("Task", objbudget.SpillBlockedReasonError)
	m.IncSpillBlocked("Task", objbudget.SpillBlockedReasonUnconfigured)
	m.IncSpillBlocked("Task", objbudget.SpillBlockedReasonUnconfigured)

	if got := testutil.ToFloat64(m.commentSpillTotal.WithLabelValues("Issue")); got != 2 {
		t.Fatalf("commentSpillTotal[Issue] = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.spillBlockedTotal.WithLabelValues("Task", objbudget.SpillBlockedReasonError)); got != 1 {
		t.Fatalf("spillBlockedTotal[Task,spill_error] = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.spillBlockedTotal.WithLabelValues("Task", objbudget.SpillBlockedReasonUnconfigured)); got != 2 {
		t.Fatalf("spillBlockedTotal[Task,unconfigured] = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.objectTooLargeTotal.WithLabelValues("Task", "proj-implement-2026-07-12-abcde")); got != 1 {
		t.Fatalf("objectTooLargeTotal = %v, want 1", got)
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	seen := map[string]bool{}
	for _, mf := range mfs {
		seen[mf.GetName()] = true
	}
	for _, name := range []string{
		"operator_object_size_bytes", "operator_object_too_large_total",
		"operator_comment_spill_total", "operator_objbudget_spill_blocked_total",
	} {
		if !seen[name] {
			t.Errorf("%s not gathered after a live record", name)
		}
	}
}

// TestObjBudgetSpillBlocked_PreseededZeroBaseline pins the startup baseline:
// three guarded kinds times two reasons. CollectAndCount does not lazily
// create series (unlike WithLabelValues), so this genuinely fails if the
// pre-seed loop is removed and a rate alert would have no series to evaluate
// on the first real memory outage.
func TestObjBudgetSpillBlocked_PreseededZeroBaseline(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewObjBudgetMetrics(reg)

	if got := testutil.CollectAndCount(m.spillBlockedTotal); got != 6 {
		t.Fatalf("operator_objbudget_spill_blocked_total has %d series, want 6 (3 kinds x 2 reasons, pre-seeded)", got)
	}
}
