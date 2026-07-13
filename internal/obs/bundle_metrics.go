package obs

import "github.com/prometheus/client_golang/prometheus"

// BundleMetrics holds the context-bundle collectors (contract K.1 / E.5):
// how big a rendered bundle was, and how much of it the byte budget had to
// elide. Constructed standalone (NewBundleMetrics) rather than embedded into
// OperatorMetrics, so the render path can be metered from the REST context
// handler and the pod prompt builder alike.
//
// It satisfies prompt.Metrics.
type BundleMetrics struct {
	bundleBytes       *prometheus.HistogramVec
	bundleElidedTotal *prometheus.CounterVec
}

// NewBundleMetrics registers the bundle collectors on reg and returns them.
func NewBundleMetrics(reg prometheus.Registerer) *BundleMetrics {
	m := &BundleMetrics{
		bundleBytes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "operator_bundle_bytes",
			Help: "Rendered context-bundle size in bytes, by agent kind. The hard budget is Project.spec.maxBundleBytes (default 400000).",
			// 4 KB to ~1 MB: the interesting range is "comfortably under" vs
			// "pressed against the 400 KB budget".
			Buckets: []float64{4_000, 16_000, 64_000, 128_000, 200_000, 300_000, 400_000, 800_000},
		}, []string{"agent_kind"}),
		bundleElidedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_bundle_elided_total",
			Help: "Comments and notes elided from rendered context bundles by the byte budget, by agent kind. A rising rate means agents are losing history behind the fetch marker.",
		}, []string{"agent_kind"}),
	}
	reg.MustRegister(m.bundleBytes, m.bundleElidedTotal)
	return m
}

// ObserveBundleBytes records the size of one rendered bundle.
func (m *BundleMetrics) ObserveBundleBytes(agentKind string, n int) {
	m.bundleBytes.WithLabelValues(agentKind).Observe(float64(n))
}

// AddBundleElided records how many comments + notes one render left out.
func (m *BundleMetrics) AddBundleElided(agentKind string, n int) {
	if n > 0 {
		m.bundleElidedTotal.WithLabelValues(agentKind).Add(float64(n))
	}
}
