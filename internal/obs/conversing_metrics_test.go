package obs_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/szymonrychu/tatara-operator/internal/obs"
)

func TestConversingMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := obs.NewOperatorMetrics(reg)

	m.SetLivePods("infrastructure", 3)
	m.LiveEntryDeclined("infrastructure", "over-ceiling")
	m.LiveEntryDeclined("infrastructure", "over-ceiling")
	m.LiveClosed("infrastructure", "idle")
	m.LiveClosed("infrastructure", "evicted")

	if got := testutil.ToFloat64(m.LivePodsGauge("infrastructure")); got != 3 {
		t.Errorf("operator_conversing_pods{infrastructure} = %v, want 3", got)
	}
	if got := testutil.ToFloat64(m.LiveEntryDeclinedCounter("infrastructure", "over-ceiling")); got != 2 {
		t.Errorf("operator_conversing_entry_declined_total{infrastructure,over-ceiling} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.LiveClosedCounter("infrastructure", "evicted")); got != 1 {
		t.Errorf("operator_conversing_closed_total{infrastructure,evicted} = %v, want 1", got)
	}
}

func TestConversingMetrics_NilSafe(t *testing.T) {
	var m *obs.OperatorMetrics
	m.SetLivePods("p", 1)
	m.LiveEntryDeclined("p", "over-ceiling")
	m.LiveClosed("p", "idle")
}

func TestBotRoundsMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := obs.NewOperatorMetrics(reg)

	m.SetBotRounds("infrastructure", 3)

	if got := testutil.ToFloat64(m.BotRoundsGauge("infrastructure")); got != 3 {
		t.Errorf("operator_bot_rounds{infrastructure} = %v, want 3", got)
	}
}

func TestBotRoundsMetric_NilSafe(t *testing.T) {
	var m *obs.OperatorMetrics
	m.SetBotRounds("p", 1)
}
