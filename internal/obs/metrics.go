package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics holds the operator's Prometheus collectors and the registry they
// are registered against. One Metrics per process.
type Metrics struct {
	Registry          *prometheus.Registry
	ReconcileTotal    *prometheus.CounterVec
	IngestJobDuration prometheus.Histogram
	TurnDuration      prometheus.Histogram
	WebhookEvents     *prometheus.CounterVec
	TasksInflight     prometheus.Gauge
}

// NewMetrics constructs and registers all operator metrics on a fresh registry
// pre-populated with the Go and process collectors.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{
		Registry: reg,
		ReconcileTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_reconcile_total",
			Help: "Total reconcile invocations by kind and result.",
		}, []string{"kind", "result"}),
		IngestJobDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "operator_ingest_job_duration_seconds",
			Help:    "Duration of repository ingest Jobs in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
		TurnDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "operator_turn_duration_seconds",
			Help:    "Duration of agent turns in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
		WebhookEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_webhook_events_total",
			Help: "Total webhook events by provider, kind and result.",
		}, []string{"provider", "kind", "result"}),
		TasksInflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "operator_tasks_inflight",
			Help: "Number of Tasks currently running.",
		}),
	}
	reg.MustRegister(m.ReconcileTotal, m.IngestJobDuration, m.TurnDuration, m.WebhookEvents, m.TasksInflight)
	return m
}
