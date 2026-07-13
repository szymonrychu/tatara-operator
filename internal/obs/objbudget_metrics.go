package obs

import "github.com/prometheus/client_golang/prometheus"

// ObjBudgetMetrics holds the Prometheus collectors for the A.7 etcd
// byte-budget guard (internal/objbudget). It is a standalone collector
// bundle, not embedded into OperatorMetrics here - Task 19 wires an
// instance into objbudget.SetMetrics as part of the rest of the metric
// surface. Kept in its own file so parallel obs work never touches it.
type ObjBudgetMetrics struct {
	objectSizeBytes     *prometheus.HistogramVec
	objectTooLargeTotal *prometheus.CounterVec
	commentSpillTotal   *prometheus.CounterVec
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
	}
	reg.MustRegister(m.objectSizeBytes, m.objectTooLargeTotal, m.commentSpillTotal)
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
