package obs

import "github.com/prometheus/client_golang/prometheus"

// OperatorMetrics holds the reconciler-facing Prometheus collectors for the
// tatara-operator. Construct one with NewOperatorMetrics and pass it to the
// reconcilers.
type OperatorMetrics struct {
	reconcileTotal          *prometheus.CounterVec
	ingestJobDuration       prometheus.Histogram
	turnDuration            prometheus.Histogram
	webhookEvents           *prometheus.CounterVec
	tasksInflight           prometheus.Gauge
	memoryProvisionDuration prometheus.Histogram
	memoryStacks            *prometheus.GaugeVec
}

// NewOperatorMetrics registers the operator collectors on reg and returns the
// bundle. Names and labels are pinned by the shared-contracts pin set.
func NewOperatorMetrics(reg prometheus.Registerer) *OperatorMetrics {
	m := &OperatorMetrics{
		reconcileTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_reconcile_total",
			Help: "Total reconcile outcomes by kind and result.",
		}, []string{"kind", "result"}),
		ingestJobDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "operator_ingest_job_duration_seconds",
			Help:    "Wall-clock duration of completed ingest Jobs.",
			Buckets: prometheus.ExponentialBuckets(5, 2, 8),
		}),
		turnDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "operator_turn_duration_seconds",
			Help:    "Wall-clock duration of agent turns.",
			Buckets: prometheus.ExponentialBuckets(5, 2, 8),
		}),
		webhookEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_webhook_events_total",
			Help: "Total webhook events by provider, kind and result.",
		}, []string{"provider", "kind", "result"}),
		tasksInflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "operator_tasks_inflight",
			Help: "Number of Tasks currently running.",
		}),
		memoryProvisionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "operator_memory_provision_duration_seconds",
			Help:    "Wall-clock duration of a per-project memory stack reaching Ready.",
			Buckets: prometheus.ExponentialBuckets(5, 2, 8),
		}),
		memoryStacks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_memory_stacks",
			Help: "Number of per-project memory stacks by phase.",
		}, []string{"phase"}),
	}
	reg.MustRegister(
		m.reconcileTotal,
		m.ingestJobDuration,
		m.turnDuration,
		m.webhookEvents,
		m.tasksInflight,
		m.memoryProvisionDuration,
		m.memoryStacks,
	)
	// Pre-initialise label combinations so the counter vecs appear in Gather
	// even before any reconcile or webhook event completes.
	for _, kind := range []string{"Project", "Repository"} {
		for _, result := range []string{"success", "error"} {
			m.reconcileTotal.WithLabelValues(kind, result)
		}
	}
	for _, provider := range []string{"github", "gitlab"} {
		for _, kind := range []string{"push"} {
			for _, result := range []string{"accepted", "rejected"} {
				m.webhookEvents.WithLabelValues(provider, kind, result)
			}
		}
	}
	for _, phase := range []string{"Provisioning", "Ready", "Failed"} {
		m.memoryStacks.WithLabelValues(phase)
	}
	return m
}

// ObserveMemoryProvisionDuration records the wall-clock seconds a per-project
// memory stack took to reach Ready.
func (m *OperatorMetrics) ObserveMemoryProvisionDuration(seconds float64) {
	m.memoryProvisionDuration.Observe(seconds)
}

// SetMemoryStackCounts sets the operator_memory_stacks gauge for all three
// phases atomically to the given cluster-wide counts. Pass 0 for any phase
// that has no stacks so stale values are cleared.
func (m *OperatorMetrics) SetMemoryStackCounts(provisioning, ready, failed int) {
	m.memoryStacks.WithLabelValues("Provisioning").Set(float64(provisioning))
	m.memoryStacks.WithLabelValues("Ready").Set(float64(ready))
	m.memoryStacks.WithLabelValues("Failed").Set(float64(failed))
}

// ReconcileResult increments operator_reconcile_total for the given kind and
// result ("success" or "error").
func (m *OperatorMetrics) ReconcileResult(kind, result string) {
	m.reconcileTotal.WithLabelValues(kind, result).Inc()
}

// ObserveIngestJobDuration records the wall-clock seconds a completed ingest
// Job took.
func (m *OperatorMetrics) ObserveIngestJobDuration(seconds float64) {
	m.ingestJobDuration.Observe(seconds)
}

// ObserveTurnDuration records the wall-clock seconds an agent turn took.
func (m *OperatorMetrics) ObserveTurnDuration(seconds float64) {
	m.turnDuration.Observe(seconds)
}

// WebhookEvent increments operator_webhook_events_total for the given
// provider, kind and result.
func (m *OperatorMetrics) WebhookEvent(provider, kind, result string) {
	m.webhookEvents.WithLabelValues(provider, kind, result).Inc()
}

// SetTasksInflight sets the operator_tasks_inflight gauge to n.
func (m *OperatorMetrics) SetTasksInflight(n float64) {
	m.tasksInflight.Set(n)
}
