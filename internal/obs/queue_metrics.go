package obs

import "github.com/prometheus/client_golang/prometheus"

// queueMetrics holds the QueuedEvent-admission Prometheus collectors,
// embedded into OperatorMetrics.
type queueMetrics struct {
	queueAdmittedTotal *prometheus.CounterVec
	queueDepth         *prometheus.GaugeVec
	queueInflight      *prometheus.GaugeVec
}

// newQueueMetrics registers the queue collectors on reg and returns the bundle.
func newQueueMetrics(reg prometheus.Registerer) *queueMetrics {
	m := &queueMetrics{
		queueAdmittedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_queue_admitted_total",
			Help: "Total QueuedEvents admitted to a Task, by pool class and event kind.",
		}, []string{"class", "kind"}),
		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_queue_depth",
			Help: "Number of Queued (not yet admitted) QueuedEvents per project and pool class.",
		}, []string{"project", "class"}),
		queueInflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_queue_inflight",
			Help: "Number of admitted in-flight QueuedEvents per project and pool class.",
		}, []string{"project", "class"}),
	}
	reg.MustRegister(
		m.queueAdmittedTotal,
		m.queueDepth,
		m.queueInflight,
	)
	return m
}

// QueueAdmitted increments operator_queue_admitted_total for the pool class and event kind.
func (m *queueMetrics) QueueAdmitted(class, kind string) {
	m.queueAdmittedTotal.WithLabelValues(class, kind).Inc()
}

// SetQueueDepth sets operator_queue_depth for a project and pool class to n (Queued-state count).
func (m *queueMetrics) SetQueueDepth(project, class string, n int) {
	m.queueDepth.WithLabelValues(project, class).Set(float64(n))
}

// SetQueueInflight sets operator_queue_inflight for a project and pool class to n (in-flight admitted count).
func (m *queueMetrics) SetQueueInflight(project, class string, n int) {
	m.queueInflight.WithLabelValues(project, class).Set(float64(n))
}
