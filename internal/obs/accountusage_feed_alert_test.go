package obs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// trafficGuardGauges are the gauges TataraAccountUsageFeedDead must be gated on.
//
// They are the OPERATOR'S OWN queue gauges (queue_metrics.go), deliberately not
// kube-state-metrics or any other exporter: they are published by the same
// leader process that publishes tatara_account_usage_gate_ready, so the guard
// shares fate with the metric it guards. A guard sourced from a different
// exporter can go absent on its own, and an absent guard silently disables the
// compensating control for a fail-open gate - which is the exact class of
// invisible failure this alert exists to end.
var trafficGuardGauges = []string{"operator_queue_inflight", "operator_queue_depth"}

// accountUsageFeedDeadExpr reads the one alert this file is about.
func accountUsageFeedDeadExpr(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "charts", "tatara-operator", "templates", "prometheusrule.yaml")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return alertExpr(t, string(src), "TataraAccountUsageFeedDead")
}

// AN IDLE FLEET IS NOT A DEAD FEED.
//
// The fleet snapshot only leaves an agent pod attached to the turn-complete
// callback, so store freshness is bounded by fleet turn-COMPLETION cadence, not
// by the statusline's own cadence. A fleet with an empty queue therefore ages
// its snapshot past MaxSnapshotAge and drops gate_ready to 0 BY DESIGN - and an
// idle fleet burning nothing against an ungoverned gate costs nothing. Ungated,
// this rule pages every night on designed behaviour, and an alert that pages on
// designed behaviour gets its `for:` raised until it is decoration.
//
// The guard is the chart's own idiom: TataraTurnSubmitLatencyHigh is already
// gated on sum(rate(..._count[15m])) > 0 "so idle windows stay NaN-safe".
func TestAccountUsageFeedDeadIsTrafficGated(t *testing.T) {
	expr := accountUsageFeedDeadExpr(t)
	if !strings.Contains(expr, "tatara_account_usage_gate_ready") {
		t.Fatalf("TataraAccountUsageFeedDead no longer reads tatara_account_usage_gate_ready:\n  %s", expr)
	}
	for _, g := range trafficGuardGauges {
		if !strings.Contains(expr, g) {
			t.Fatalf("TataraAccountUsageFeedDead expr is not traffic-gated on %s:\n  %s\n"+
				"gate_ready == 0 on an idle fleet is designed behaviour, not a fault; ungated "+
				"this rule pages nightly and the reflex is to weaken it until it is decoration.", g, expr)
		}
	}
}

// A GUARD COMPOSED WITH ARITHMETIC IS NOT A GUARD.
//
// The obvious spelling of "work in flight AND the gate is inert" is a product
// of the guard with a `== bool 0`:
//
//	(sum(operator_queue_inflight) + sum(operator_queue_depth) > 0) * (min(tatara_account_usage_gate_ready) == bool 0)
//
// It does not work. `== bool 0` yields 0 rather than dropping the sample, so a
// BUSY FLEET WITH A PERFECTLY HEALTHY GATE still produces one element, valued
// 0 - and a Prometheus alerting rule fires on any element the expression
// returns, whatever its value. Measured against this cluster over 14h at 15m
// resolution: 25 samples, 11 of them the 0-valued healthy case, with runs of
// them up to 1.5h - well past `for: 30m`.
//
// `and` is a set operation: it drops the sample instead of valuing it 0, so the
// rule returns an element only in the true condition. Same 14h window, `and`
// returns exactly the 14 true-condition samples.
func TestAccountUsageFeedDeadGuardDropsSamplesRatherThanValuingThemZero(t *testing.T) {
	expr := accountUsageFeedDeadExpr(t)
	if !strings.Contains(expr, " and ") {
		t.Fatalf("TataraAccountUsageFeedDead does not compose its traffic guard with `and`:\n  %s\n"+
			"a set operation is required; arithmetic keeps the healthy sample at value 0 and "+
			"an alerting rule fires on any returned element regardless of value.", expr)
	}
	if strings.Contains(expr, "bool") {
		t.Fatalf("TataraAccountUsageFeedDead uses a `bool` modifier:\n  %s\n"+
			"`== bool 0` converts the healthy case into a 0-valued sample instead of dropping "+
			"it, so the rule fires on a busy fleet whose gate is fine.", expr)
	}
}

// NO SERIES MUST STAY NO SERIES.
//
// tatara_account_usage_gate_ready is a label-less GaugeVec whose child is
// created on first Set, and Set runs only under Enabled && claudeSubscription
// on the leader. So a disabled gate, a customWindow gate and every non-leader
// replica produce NO SERIES, and `== 0` matches nothing. `or vector(0)`
// manufactures the series the shape exists to withhold and makes the rule fire
// on every operator that never turned the gate on - the same defect as
// "simplifying" the GaugeVec to a Gauge, which reads 0 from process start.
// accountusage_metrics.go and accountusage_gauges_test.go pin the producer side;
// this pins the consumer.
func TestAccountUsageFeedDeadNeverSynthesisesAMissingSeries(t *testing.T) {
	expr := accountUsageFeedDeadExpr(t)
	if strings.Contains(strings.ReplaceAll(expr, " ", ""), "orvector(") {
		t.Fatalf("TataraAccountUsageFeedDead synthesises a series with `or vector(...)`:\n  %s\n"+
			"a gate that was never enabled emits no series on purpose; vector() makes this "+
			"fire on every such operator.", expr)
	}
}

// The guard must not be aggregated in a direction that hides a stale child.
//
// min() over the gate gauge is identity today (one leader, one series), but if
// a demoted leader's child ever lingers at 0 alongside a healthy leader at 1,
// min() fails TOWARD alerting and max() fails toward silence. For a rule whose
// whole job is to witness a fail-open, a false page is the cheap direction.
func TestAccountUsageFeedDeadAggregatesTowardAlerting(t *testing.T) {
	expr := accountUsageFeedDeadExpr(t)
	if !strings.Contains(expr, "min(tatara_account_usage_gate_ready)") {
		t.Fatalf("TataraAccountUsageFeedDead does not aggregate gate_ready with min():\n  %s\n"+
			"max() fails toward silence if a demoted leader's child lingers; this rule must "+
			"fail toward alerting.", expr)
	}
}
