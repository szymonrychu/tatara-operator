package obs_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/szymonrychu/tatara-operator/internal/obs"
)

func TestNewMetrics_RegistersAll(t *testing.T) {
	m := obs.NewMetrics()
	if m.Registry == nil {
		t.Fatal("Registry is nil")
	}

	m.ReconcileTotal.WithLabelValues("Project", "success").Inc()
	m.IngestJobDuration.Observe(1.5)
	m.TurnDuration.Observe(2.5)
	m.WebhookEvents.WithLabelValues("github", "push", "accepted").Inc()
	m.TasksInflight.Set(3)

	want := []string{
		"operator_reconcile_total",
		"operator_ingest_job_duration_seconds",
		"operator_turn_duration_seconds",
		"operator_webhook_events_total",
		"operator_tasks_inflight",
	}
	for _, name := range want {
		t.Run(name, func(t *testing.T) {
			if testutil.CollectAndCount(m.Registry, name) == 0 {
				t.Fatalf("metric %q not registered/collected", name)
			}
		})
	}
}
