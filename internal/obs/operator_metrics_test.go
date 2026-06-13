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

func TestAgentBootRaceRequeue(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.AgentBootRaceRequeue()
	m.AgentBootRaceRequeue()

	if got := testutil.ToFloat64(m.agentBootRaceRequeue); got != 2 {
		t.Fatalf("operator_agent_boot_race_requeue_total = %v, want 2", got)
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

	m.WebhookEvent("github", "push", "other", "accepted")
	m.WebhookEvent("github", "push", "other", "accepted")
	m.WebhookEvent("gitlab", "push", "other", "rejected")

	got := testutil.ToFloat64(m.webhookEvents.WithLabelValues("github", "push", "other", "accepted"))
	if got != 2 {
		t.Fatalf("github/push/other/accepted = %v, want 2", got)
	}
	got = testutil.ToFloat64(m.webhookEvents.WithLabelValues("gitlab", "push", "other", "rejected"))
	if got != 1 {
		t.Fatalf("gitlab/push/other/rejected = %v, want 1", got)
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

	m.SetMemoryStackCounts(1, 3, 0)

	if got := testutil.ToFloat64(m.memoryStacks.WithLabelValues("Provisioning")); got != 1 {
		t.Fatalf("Provisioning stacks = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.memoryStacks.WithLabelValues("Ready")); got != 3 {
		t.Fatalf("Ready stacks = %v, want 3", got)
	}
	if got := testutil.ToFloat64(m.memoryStacks.WithLabelValues("Failed")); got != 0 {
		t.Fatalf("Failed stacks = %v, want 0", got)
	}
}

func TestMemoryStacksGauge_ZeroesStalePhase(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	// Set Ready=2, then transition: Ready=0, Provisioning=1.
	m.SetMemoryStackCounts(0, 2, 0)
	m.SetMemoryStackCounts(1, 0, 0)

	if got := testutil.ToFloat64(m.memoryStacks.WithLabelValues("Ready")); got != 0 {
		t.Fatalf("Ready stacks after transition = %v, want 0", got)
	}
	if got := testutil.ToFloat64(m.memoryStacks.WithLabelValues("Provisioning")); got != 1 {
		t.Fatalf("Provisioning stacks = %v, want 1", got)
	}
}

func TestWebhookEventActionLabel(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.WebhookEvent("github", "issue", "labeled", "ignored")
	got := testutil.ToFloat64(m.webhookEvents.WithLabelValues("github", "issue", "labeled", "ignored"))
	if got != 1 {
		t.Fatalf("github/issue/labeled/ignored = %v, want 1", got)
	}
}

func TestSCMWritesTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.SCMWrite("github", "merge", "ok")
	m.SCMWrite("github", "merge", "ok")
	got := testutil.ToFloat64(m.scmWritesTotal.WithLabelValues("github", "merge", "ok"))
	if got != 2 {
		t.Fatalf("github/merge/ok = %v, want 2", got)
	}
}

func TestScanMetricsRegistered(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.ScanItem("mrScan", "picked")
	m.ScanTaskCreated("mrScan", "review")
	m.ObserveScanDuration("mrScan", 0.5)
	m.IssueOutcome("close")
	m.SetTasksInflightKind("triageIssue", 2)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	want := map[string]bool{
		"tatara_scan_items_total":         false,
		"tatara_scan_tasks_created_total": false,
		"tatara_scan_duration_seconds":    false,
		"tatara_issue_outcome_total":      false,
		"tatara_tasks_inflight":           false,
	}
	for _, mf := range mfs {
		if _, ok := want[mf.GetName()]; ok {
			want[mf.GetName()] = true
		}
	}
	for name, seen := range want {
		if !seen {
			t.Fatalf("metric %q not registered/gathered", name)
		}
	}
}

func TestOpenProposalsGauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.SetOpenProposals("o/r", 2)
	if got := testutil.ToFloat64(m.openProposals.WithLabelValues("o/r")); got != 2 {
		t.Fatalf("openProposals{o/r} = %v, want 2", got)
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
