package obs

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/szymonrychu/tatara-operator/internal/objbudget"
)

// ObjBudgetMetrics holds the Prometheus collectors for the A.7 etcd
// byte-budget guard (internal/objbudget). It is a standalone collector
// bundle, not embedded into OperatorMetrics here - Task 19 wires an
// instance into objbudget.SetMetrics as part of the rest of the metric
// surface. Kept in its own file so parallel obs work never touches it.
type ObjBudgetMetrics struct {
	objectSizeBytes     *prometheus.HistogramVec
	objectTooLargeTotal *prometheus.CounterVec
	commentSpillTotal   *prometheus.CounterVec
	evictedDroppedTotal *prometheus.CounterVec
	spillBlockedTotal   *prometheus.CounterVec
}

// NewObjBudgetMetrics registers and returns the A.7 byte-budget collectors
// on reg. It satisfies objbudget.Metrics.
func NewObjBudgetMetrics(reg prometheus.Registerer) *ObjBudgetMetrics {
	m := &ObjBudgetMetrics{
		objectSizeBytes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "operator_object_size_bytes",
			Help:    "Marshalled size in bytes of a CR guarded by the A.7 byte-budget check, by kind.",
			Buckets: prometheus.ExponentialBuckets(1024, 2, 12),
		}, []string{"kind"}),
		objectTooLargeTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_object_too_large_total",
			Help: "CRs that exceeded the A.7 byte budget with nothing left to evict, by kind and name.",
		}, []string{"kind", "name"}),
		commentSpillTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_comment_spill_total",
			Help: "Eviction batches spilled to tatara-memory by the A.7 byte-budget guard, by kind.",
		}, []string{"kind"}),
		// The A.7 inversion (objbudget.Discarding): on a Project with
		// spec.memory.enabled=false there is no spill target and never will be,
		// so evicted items are DROPPED rather than the write being refused.
		// Separate from commentSpillTotal because these items are gone, not
		// stored, and separate from spillBlockedTotal because nothing failed -
		// an alert on either that swallowed this would be wrong in both
		// directions.
		evictedDroppedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_objbudget_evicted_dropped_total",
			Help: "Eviction batches DISCARDED rather than spilled, because the owning project has no memory stack by configuration, by kind.",
		}, []string{"kind"}),
		// The A.7 guard REFUSES a write it cannot spill (SPILL FIRST, DROP ONLY
		// ON SPILL SUCCESS). That policy is deliberate - a memory outage is a
		// fault that gets repaired, and no comment or note should be lost
		// meanwhile - but it used to be entirely silent, so an object wedged by
		// the outage looked exactly like a healthy one. reason separates a
		// spill_error (tatara-memory down; self-heals) from unconfigured (a
		// wiring bug; needs a deploy).
		spillBlockedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_objbudget_spill_blocked_total",
			Help: "Guarded writes refused because their eviction batch could not be spilled to tatara-memory, by kind and reason.",
		}, []string{"kind", "reason"}),
	}
	reg.MustRegister(m.objectSizeBytes, m.objectTooLargeTotal, m.commentSpillTotal,
		m.evictedDroppedTotal, m.spillBlockedTotal)
	// Pre-seed the two real reasons per guarded kind so a healthy operator
	// exposes a zero baseline from startup (metric-wiring audit convention,
	// issue #370) instead of a rate alert with no series to evaluate on the
	// first real outage.
	for _, kind := range []string{"Issue", "MergeRequest", "Task"} {
		m.spillBlockedTotal.WithLabelValues(kind, objbudget.SpillBlockedReasonError)
		m.spillBlockedTotal.WithLabelValues(kind, objbudget.SpillBlockedReasonUnconfigured)
		m.evictedDroppedTotal.WithLabelValues(kind)
	}
	return m
}

// ObserveObjectSize implements objbudget.Metrics.
func (m *ObjBudgetMetrics) ObserveObjectSize(kind string, bytes int) {
	m.objectSizeBytes.WithLabelValues(kind).Observe(float64(bytes))
}

// IncObjectTooLarge implements objbudget.Metrics.
func (m *ObjBudgetMetrics) IncObjectTooLarge(kind, name string) {
	m.objectTooLargeTotal.WithLabelValues(kind, name).Inc()
}

// IncCommentSpill implements objbudget.Metrics.
func (m *ObjBudgetMetrics) IncCommentSpill(kind string) {
	m.commentSpillTotal.WithLabelValues(kind).Inc()
}

// IncEvictedDropped implements objbudget.Metrics.
func (m *ObjBudgetMetrics) IncEvictedDropped(kind string) {
	m.evictedDroppedTotal.WithLabelValues(kind).Inc()
}

// IncSpillBlocked implements objbudget.Metrics.
func (m *ObjBudgetMetrics) IncSpillBlocked(kind, reason string) {
	m.spillBlockedTotal.WithLabelValues(kind, reason).Inc()
}
