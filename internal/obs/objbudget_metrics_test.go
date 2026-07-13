package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestObjBudgetMetricsRecord(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewObjBudgetMetrics(reg)

	m.ObserveObjectSize("Issue", 12345)
	m.IncObjectTooLarge("Task", "proj-implement-2026-07-12-abcde")
	m.IncCommentSpill("Issue")
	m.IncCommentSpill("Issue")

	if got := testutil.ToFloat64(m.commentSpillTotal.WithLabelValues("Issue")); got != 2 {
		t.Fatalf("commentSpillTotal[Issue] = %v, want 2", got)
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
	for _, name := range []string{"operator_object_size_bytes", "operator_object_too_large_total", "operator_comment_spill_total"} {
		if !seen[name] {
			t.Errorf("%s not gathered after a live record", name)
		}
	}
}
