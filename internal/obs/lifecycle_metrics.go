// Copyright 2026 tatara authors.

package obs

import (
	"github.com/prometheus/client_golang/prometheus"
)

// LifecycleMetrics holds the lifecycle-specific Prometheus collectors for the
// issueLifecycle state machine. Separate from OperatorMetrics to keep the
// lifecycle concerns isolated and testable with a fresh registry.
type LifecycleMetrics struct {
	lifecycleState   *prometheus.GaugeVec
	transitionTotal  *prometheus.CounterVec
	handoverTotal    prometheus.Counter
	giveupTotal      *prometheus.CounterVec
	idleStopTotal    prometheus.Counter
	mrciWaitSeconds  prometheus.Histogram
	lifecycleSeconds prometheus.Histogram
}

// NewLifecycleMetrics registers the lifecycle collectors on reg and returns the
// bundle. Using a fresh prometheus.NewRegistry() in tests avoids global state.
func NewLifecycleMetrics(reg prometheus.Registerer) *LifecycleMetrics {
	m := &LifecycleMetrics{
		lifecycleState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tatara_lifecycle_state",
			Help: "Number of issueLifecycle Tasks per lifecycle state.",
		}, []string{"state"}),
		transitionTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tatara_lifecycle_transition_total",
			Help: "Total lifecycle state transitions.",
		}, []string{"from", "to"}),
		handoverTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tatara_lifecycle_handover_total",
			Help: "Total agent context-handover events.",
		}),
		giveupTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tatara_lifecycle_giveup_total",
			Help: "Total lifecycle give-up events by reason.",
		}, []string{"reason"}),
		idleStopTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tatara_lifecycle_idle_stop_total",
			Help: "Total idle-stop transitions (Conversation -> Stopped).",
		}),
		mrciWaitSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "tatara_mrci_wait_seconds",
			Help:    "Wall-clock seconds a Task spent in the MRCI poll state.",
			Buckets: prometheus.ExponentialBuckets(10, 2, 10),
		}),
		lifecycleSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "tatara_lifecycle_seconds",
			Help:    "Wall-clock seconds from issue open to Done (full lifecycle).",
			Buckets: prometheus.ExponentialBuckets(60, 2, 12),
		}),
	}
	reg.MustRegister(
		m.lifecycleState,
		m.transitionTotal,
		m.handoverTotal,
		m.giveupTotal,
		m.idleStopTotal,
		m.mrciWaitSeconds,
		m.lifecycleSeconds,
	)
	return m
}

// RecordTransition increments tatara_lifecycle_transition_total{from,to}.
func (m *LifecycleMetrics) RecordTransition(from, to string) {
	m.transitionTotal.WithLabelValues(from, to).Inc()
}

// TransitionTotal returns the counter for use in test assertions.
func (m *LifecycleMetrics) TransitionTotal(from, to string) prometheus.Counter {
	return m.transitionTotal.WithLabelValues(from, to)
}

// SetLifecycleState sets tatara_lifecycle_state{state} to n. This is the only
// writer of the gauge: ProjectReconciler.updateLifecycleStateCounts recomputes
// every state from authoritative cluster state on each Project reconcile.
func (m *LifecycleMetrics) SetLifecycleState(state string, n float64) {
	m.lifecycleState.WithLabelValues(state).Set(n)
}

// RecordHandover increments tatara_lifecycle_handover_total.
func (m *LifecycleMetrics) RecordHandover() {
	m.handoverTotal.Inc()
}

// HandoverTotal returns the handover counter for use in test assertions.
func (m *LifecycleMetrics) HandoverTotal() prometheus.Counter {
	return m.handoverTotal
}

// RecordGiveup increments tatara_lifecycle_giveup_total{reason}.
func (m *LifecycleMetrics) RecordGiveup(reason string) {
	m.giveupTotal.WithLabelValues(reason).Inc()
}

// RecordIdleStop increments tatara_lifecycle_idle_stop_total.
func (m *LifecycleMetrics) RecordIdleStop() {
	m.idleStopTotal.Inc()
}

// ObserveMRCIWait records the seconds spent in MRCI.
func (m *LifecycleMetrics) ObserveMRCIWait(seconds float64) {
	m.mrciWaitSeconds.Observe(seconds)
}

// ObserveLifecycle records the full issue-open-to-Done duration.
func (m *LifecycleMetrics) ObserveLifecycle(seconds float64) {
	m.lifecycleSeconds.Observe(seconds)
}
