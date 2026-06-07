package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestReconcileTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.ReconcileResult("Project", "success")
	m.ReconcileResult("Project", "success")
	m.ReconcileResult("Repository", "error")

	got := testutil.ToFloat64(m.reconcileTotal.WithLabelValues("Project", "success"))
	if got != 2 {
		t.Fatalf("Project/success = %v, want 2", got)
	}
	got = testutil.ToFloat64(m.reconcileTotal.WithLabelValues("Repository", "error"))
	if got != 1 {
		t.Fatalf("Repository/error = %v, want 1", got)
	}
}

func TestIngestJobDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.ObserveIngestJobDuration(12.5)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var found bool
	for _, mf := range mfs {
		if mf.GetName() == "operator_ingest_job_duration_seconds" {
			found = true
			if got := mf.GetMetric()[0].GetHistogram().GetSampleCount(); got != 1 {
				t.Fatalf("sample count = %d, want 1", got)
			}
		}
	}
	if !found {
		t.Fatal("operator_ingest_job_duration_seconds not registered")
	}
}

func TestTurnDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.ObserveTurnDuration(30.0)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var found bool
	for _, mf := range mfs {
		if mf.GetName() == "operator_turn_duration_seconds" {
			found = true
			if got := mf.GetMetric()[0].GetHistogram().GetSampleCount(); got != 1 {
				t.Fatalf("sample count = %d, want 1", got)
			}
		}
	}
	if !found {
		t.Fatal("operator_turn_duration_seconds not registered")
	}
}

func TestWebhookEvent(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.WebhookEvent("github", "push", "accepted")
	m.WebhookEvent("github", "push", "accepted")
	m.WebhookEvent("gitlab", "push", "rejected")

	got := testutil.ToFloat64(m.webhookEvents.WithLabelValues("github", "push", "accepted"))
	if got != 2 {
		t.Fatalf("github/push/accepted = %v, want 2", got)
	}
	got = testutil.ToFloat64(m.webhookEvents.WithLabelValues("gitlab", "push", "rejected"))
	if got != 1 {
		t.Fatalf("gitlab/push/rejected = %v, want 1", got)
	}
}

func TestTasksInflight(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.SetTasksInflight(5)

	got := testutil.ToFloat64(m.tasksInflight)
	if got != 5 {
		t.Fatalf("tasks_inflight = %v, want 5", got)
	}
}

func TestMemoryProvisionDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.ObserveMemoryProvisionDuration(7.5)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var found bool
	for _, mf := range mfs {
		if mf.GetName() == "operator_memory_provision_duration_seconds" {
			found = true
			if got := mf.GetMetric()[0].GetHistogram().GetSampleCount(); got != 1 {
				t.Fatalf("sample count = %d, want 1", got)
			}
		}
	}
	if !found {
		t.Fatal("operator_memory_provision_duration_seconds not registered")
	}
}

func TestMemoryStacksGauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.SetMemoryStacks("Ready", 3)
	m.SetMemoryStacks("Provisioning", 1)

	if got := testutil.ToFloat64(m.memoryStacks.WithLabelValues("Ready")); got != 3 {
		t.Fatalf("Ready stacks = %v, want 3", got)
	}
	if got := testutil.ToFloat64(m.memoryStacks.WithLabelValues("Provisioning")); got != 1 {
		t.Fatalf("Provisioning stacks = %v, want 1", got)
	}
}

func TestOperatorMetricsNamesStable(t *testing.T) {
	reg := prometheus.NewRegistry()
	_ = NewOperatorMetrics(reg)
	mfs, _ := reg.Gather()
	want := map[string]bool{
		"operator_reconcile_total":                   false,
		"operator_ingest_job_duration_seconds":       false,
		"operator_turn_duration_seconds":             false,
		"operator_webhook_events_total":              false,
		"operator_tasks_inflight":                    false,
		"operator_memory_provision_duration_seconds": false,
		"operator_memory_stacks":                     false,
	}
	for _, mf := range mfs {
		if _, ok := want[mf.GetName()]; ok {
			want[mf.GetName()] = true
		}
	}
	for name, seen := range want {
		if !seen {
			t.Errorf("metric %q not registered", name)
		}
	}
}
