package obs

import "github.com/prometheus/client_golang/prometheus"

// queueMetrics holds the QueuedEvent-admission Prometheus collectors,
// embedded into OperatorMetrics.
type queueMetrics struct {
	queueAdmittedTotal *prometheus.CounterVec
	queueDepth         *prometheus.GaugeVec
	queueInflight      *prometheus.GaugeVec
	queueAge           *prometheus.GaugeVec
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
		queueAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_queue_age_seconds",
			Help: "Age of the OLDEST QueuedEvent per (class,priority,state) bucket (contract K.1).",
		}, []string{"class", "priority", "state"}),
	}
	reg.MustRegister(
		m.queueAdmittedTotal,
		m.queueDepth,
		m.queueInflight,
		m.queueAge,
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

// ResetQueueAge clears operator_queue_age_seconds so a recompute pass leaves
// no stale bucket for a class/priority/state combination with no QueuedEvents
// left (contract M22). Nil-safe.
func (m *OperatorMetrics) ResetQueueAge() {
	if m == nil || m.queueAge == nil {
		return
	}
	m.queueAge.Reset()
}

// SetQueueAge sets operator_queue_age_seconds{class,priority,state} to
// ageSeconds, the age of the OLDEST QueuedEvent in that bucket. Nil-safe.
func (m *OperatorMetrics) SetQueueAge(class, priority, state string, ageSeconds float64) {
	if m == nil || m.queueAge == nil {
		return
	}
	m.queueAge.WithLabelValues(class, priority, state).Set(ageSeconds)
}

// QueueAgeGauge returns the operator_queue_age_seconds gauge for
// (class,priority,state) for test assertions.
func (m *OperatorMetrics) QueueAgeGauge(class, priority, state string) prometheus.Gauge {
	return m.queueAge.WithLabelValues(class, priority, state)
}
