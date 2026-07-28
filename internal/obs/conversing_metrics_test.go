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

	m.SetConversingPods("infrastructure", 3)
	m.ConversingEntryDeclined("infrastructure", "over-ceiling")
	m.ConversingEntryDeclined("infrastructure", "over-ceiling")
	m.ConversingClosed("infrastructure", "idle")
	m.ConversingClosed("infrastructure", "evicted")

	if got := testutil.ToFloat64(m.ConversingPodsGauge("infrastructure")); got != 3 {
		t.Errorf("operator_conversing_pods{infrastructure} = %v, want 3", got)
	}
	if got := testutil.ToFloat64(m.ConversingEntryDeclinedCounter("infrastructure", "over-ceiling")); got != 2 {
		t.Errorf("operator_conversing_entry_declined_total{infrastructure,over-ceiling} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.ConversingClosedCounter("infrastructure", "evicted")); got != 1 {
		t.Errorf("operator_conversing_closed_total{infrastructure,evicted} = %v, want 1", got)
	}
}

func TestConversingMetrics_NilSafe(t *testing.T) {
	var m *obs.OperatorMetrics
	m.SetConversingPods("p", 1)
	m.ConversingEntryDeclined("p", "over-ceiling")
	m.ConversingClosed("p", "idle")
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
