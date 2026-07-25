package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// gatheredLabelNames returns the label names of the first gathered sample of
// `name` from a throwaway registry. A collector may be registered in more than
// one registry, so this reads the REAL collector without disturbing
// ctrlmetrics.Registry.
func gatheredLabelNames(t *testing.T, c prometheus.Collector, name string) []string {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather %s: %v", name, err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name || len(mf.GetMetric()) == 0 {
			continue
		}
		var out []string
		for _, lp := range mf.GetMetric()[0].GetLabel() {
			out = append(out, lp.GetName())
		}
		return out
	}
	t.Fatalf("metric %s produced no samples", name)
	return nil
}

// The agent-internal-issue alert moves off Loki onto this counter (issue #63):
// the operator decides the condition in its own code, so hard rule 13 requires a
// Prometheus counter. category/severity are the wrapper's already-clamped enum
// values; description is free text and is deliberately NOT a label.
func TestAgentInternalIssueTotalLabels(t *testing.T) {
	AgentInternalIssueTotal.WithLabelValues("label-test-category", "warn").Inc()
	got := gatheredLabelNames(t, AgentInternalIssueTotal, "agent_internal_issue_total")
	want := []string{"category", "severity"}
	if len(got) != len(want) {
		t.Fatalf("label names = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("label names = %v, want %v", got, want)
		}
	}
}
