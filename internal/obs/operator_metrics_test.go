package obs

import (
	"testing"

	dto "github.com/prometheus/client_model/go"

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

func TestTurnTimeoutTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.TurnTimeout("reconcile")
	m.TurnTimeout("poll_backstop")
	m.TurnTimeout("poll_backstop")
	if got := testutil.ToFloat64(m.turnTimeoutTotal.WithLabelValues("reconcile")); got != 1 {
		t.Fatalf("turn_timeout reconcile = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.turnTimeoutTotal.WithLabelValues("poll_backstop")); got != 2 {
		t.Fatalf("turn_timeout poll_backstop = %v, want 2", got)
	}
}

func TestIngestJobResultTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.IngestJobResult("success")
	m.IngestJobResult("failure")
	m.IngestJobResult("failure")
	if got := testutil.ToFloat64(m.ingestJobTotal.WithLabelValues("success")); got != 1 {
		t.Fatalf("ingest_job success = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.ingestJobTotal.WithLabelValues("failure")); got != 2 {
		t.Fatalf("ingest_job failure = %v, want 2", got)
	}
}

func TestAgentUnreachableTermination(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.AgentUnreachableTermination()
	if got := testutil.ToFloat64(m.agentUnreachableTermTotal); got != 1 {
		t.Fatalf("agent_unreachable_termination_total = %v, want 1", got)
	}
}

func TestOperatorMetricsNamesStable(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	// Touch counters/vecs that require a label-value observation to appear.
	m.OrphanReaped("test")
	m.ReapDeleteError("pod")
	mfs, _ := reg.Gather()
	want := map[string]bool{
		"operator_reconcile_total":                     false,
		"operator_ingest_job_duration_seconds":         false,
		"operator_turn_duration_seconds":               false,
		"operator_webhook_events_total":                false,
		"operator_tasks_inflight":                      false,
		"operator_memory_provision_duration_seconds":   false,
		"operator_memory_stacks":                       false,
		"operator_turn_timeout_total":                  false,
		"operator_ingest_job_total":                    false,
		"operator_agent_unreachable_termination_total": false,
		"operator_orphan_reaped_total":                 false,
		"operator_reap_delete_error_total":             false,
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

func TestOrphanReaped(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.OrphanReaped("task absent")
	m.OrphanReaped("task absent")
	m.OrphanReaped("stale task incarnation")
	if got := testutil.ToFloat64(m.orphanReapedTotal.WithLabelValues("task absent")); got != 2 {
		t.Fatalf("orphan_reaped{task absent} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.orphanReapedTotal.WithLabelValues("stale task incarnation")); got != 1 {
		t.Fatalf("orphan_reaped{stale task incarnation} = %v, want 1", got)
	}
}

func TestReapDeleteError(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.ReapDeleteError("pod")
	m.ReapDeleteError("service")
	m.ReapDeleteError("pod")
	if got := testutil.ToFloat64(m.reapDeleteErrorTotal.WithLabelValues("pod")); got != 2 {
		t.Fatalf("reap_delete_error{pod} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.reapDeleteErrorTotal.WithLabelValues("service")); got != 1 {
		t.Fatalf("reap_delete_error{service} = %v, want 1", got)
	}
}

// Finding 25: planning_watchdog must be pre-seeded so the series exists from
// startup without waiting for the first stall to fire.
func TestTurnTimeoutTotal_PlanningWatchdogPreSeeded(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var found bool
	for _, mf := range mfs {
		if mf.GetName() != "operator_turn_timeout_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "source" && lp.GetValue() == "planning_watchdog" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("operator_turn_timeout_total{source=planning_watchdog} not pre-seeded")
	}
	// Calling TurnTimeout must increment it.
	m.TurnTimeout("planning_watchdog")
	if got := testutil.ToFloat64(m.turnTimeoutTotal.WithLabelValues("planning_watchdog")); got != 1 {
		t.Fatalf("turn_timeout planning_watchdog = %v, want 1", got)
	}
}

// Finding 8: auth_total counter must exist and RecordAuth must increment it.
func TestRecordAuth(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.RecordAuth("accepted")
	m.RecordAuth("accepted")
	m.RecordAuth("invalid_token")
	m.RecordAuth("missing_token")
	m.RecordAuth("invalid_scheme")

	if got := testutil.ToFloat64(m.AuthCounter("accepted")); got != 2 {
		t.Fatalf("auth accepted = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.AuthCounter("invalid_token")); got != 1 {
		t.Fatalf("auth invalid_token = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.AuthCounter("missing_token")); got != 1 {
		t.Fatalf("auth missing_token = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.AuthCounter("invalid_scheme")); got != 1 {
		t.Fatalf("auth invalid_scheme = %v, want 1", got)
	}
}

// Finding 8: all auth result labels must be pre-seeded so the series exist
// from startup (zero-baseline for alerts on auth rejection spikes).
func TestAuthTotal_PreSeeded(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewOperatorMetrics(reg)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	want := map[string]bool{
		"accepted":       false,
		"missing_token":  false,
		"invalid_scheme": false,
		"invalid_token":  false,
	}
	for _, mf := range mfs {
		if mf.GetName() != "operator_auth_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "result" {
					want[lp.GetValue()] = true
				}
			}
		}
	}
	for label, seen := range want {
		if !seen {
			t.Errorf("operator_auth_total{result=%q} not pre-seeded", label)
		}
	}
}

// Finding 15: WritebackOutcome counter must exist and be incrementable.
func TestWritebackOutcome(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.WritebackOutcome("no_change")
	m.WritebackOutcome("skip_4xx")
	m.WritebackOutcome("no_pr")
	m.WritebackOutcome("opened")
	m.WritebackOutcome("opened")

	if got := testutil.ToFloat64(m.WritebackOutcomeCounter("no_change")); got != 1 {
		t.Fatalf("writeback no_change = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.WritebackOutcomeCounter("skip_4xx")); got != 1 {
		t.Fatalf("writeback skip_4xx = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.WritebackOutcomeCounter("no_pr")); got != 1 {
		t.Fatalf("writeback no_pr = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.WritebackOutcomeCounter("opened")); got != 2 {
		t.Fatalf("writeback opened = %v, want 2", got)
	}
}

// Finding 15: all writeback outcome labels must be pre-seeded.
func TestWritebackOutcome_PreSeeded(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewOperatorMetrics(reg)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	want := map[string]bool{
		"no_change": false,
		"skip_4xx":  false,
		"no_pr":     false,
		"opened":    false,
	}
	for _, mf := range mfs {
		if mf.GetName() != "operator_writeback_outcome_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "result" {
					want[lp.GetValue()] = true
				}
			}
		}
	}
	for label, seen := range want {
		if !seen {
			t.Errorf("operator_writeback_outcome_total{result=%q} not pre-seeded", label)
		}
	}
}

// All brainstorm-outcome labels must be pre-seeded so both series exist at zero
// before any brainstorm run completes (the yield rate is graphable from startup).
func TestBrainstormOutcome_PreSeeded(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewOperatorMetrics(reg)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	want := map[string]bool{
		"proposed": false,
		"no_yield": false,
	}
	for _, mf := range mfs {
		if mf.GetName() != "operator_brainstorm_outcome_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "result" {
					want[lp.GetValue()] = true
				}
			}
		}
	}
	for label, seen := range want {
		if !seen {
			t.Errorf("operator_brainstorm_outcome_total{result=%q} not pre-seeded", label)
		}
	}
}

// BrainstormOutcome increments the right series per result.
func TestBrainstormOutcome(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.BrainstormOutcome("proposed")
	m.BrainstormOutcome("no_yield")
	m.BrainstormOutcome("no_yield")

	if got := testutil.ToFloat64(m.BrainstormOutcomeCounter("proposed")); got != 1 {
		t.Fatalf("brainstorm proposed = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.BrainstormOutcomeCounter("no_yield")); got != 2 {
		t.Fatalf("brainstorm no_yield = %v, want 2", got)
	}
}

// Finding 2: REST API metrics - counter and histogram must exist and be recordable.
func TestRecordRESTRequest(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.RecordRESTRequest("patch_task", "ok", 0.123)
	m.RecordRESTRequest("patch_task", "ok", 0.456)
	m.RecordRESTRequest("patch_task", "error", 0.001)

	if got := testutil.ToFloat64(m.RESTRequestsCounter("patch_task", "ok")); got != 2 {
		t.Fatalf("restapi{patch_task,ok} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.RESTRequestsCounter("patch_task", "error")); got != 1 {
		t.Fatalf("restapi{patch_task,error} = %v, want 1", got)
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var foundDuration bool
	for _, mf := range mfs {
		if mf.GetName() != "operator_restapi_request_duration_seconds" {
			continue
		}
		foundDuration = true
		for _, metric := range mf.GetMetric() {
			// Find the patch_task series (the one we observed into).
			var ep string
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "endpoint" {
					ep = lp.GetValue()
				}
			}
			if ep == "patch_task" {
				if got := metric.GetHistogram().GetSampleCount(); got != 3 {
					t.Fatalf("restapi duration sample count for patch_task = %d, want 3", got)
				}
			}
		}
	}
	if !foundDuration {
		t.Fatal("operator_restapi_request_duration_seconds not registered")
	}
}

// Finding 2: REST API endpoints must be pre-seeded so series appear from startup.
func TestRESTAPIMetrics_PreSeeded(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewOperatorMetrics(reg)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	wantEndpoints := []string{"patch_task", "propose_issue", "review_verdict", "pr_outcome", "issue_outcome"}
	found := map[string]bool{}
	for _, mf := range mfs {
		if mf.GetName() != "operator_restapi_requests_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "endpoint" {
					found[lp.GetValue()] = true
				}
			}
		}
	}
	for _, ep := range wantEndpoints {
		if !found[ep] {
			t.Errorf("operator_restapi_requests_total{endpoint=%q} not pre-seeded", ep)
		}
	}
}

// Finding 13: MemoryHealthReadError must increment the counter.
func TestMemoryHealthReadError(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.MemoryHealthReadError()
	m.MemoryHealthReadError()
	m.MemoryHealthReadError()

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var got float64
	for _, mf := range mfs {
		if mf.GetName() == "operator_memory_health_read_errors_total" {
			got = mf.GetMetric()[0].GetCounter().GetValue()
		}
	}
	if got != 3 {
		t.Fatalf("memory_health_read_errors_total = %v, want 3", got)
	}
}

// Finding 14: webhook duration histogram must exist and ObserveWebhookDuration must record.
func TestObserveWebhookDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.ObserveWebhookDuration("github", "ok", 0.05)
	m.ObserveWebhookDuration("github", "ok", 0.1)
	m.ObserveWebhookDuration("gitlab", "error", 0.2)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var found bool
	for _, mf := range mfs {
		if mf.GetName() != "operator_webhook_duration_seconds" {
			continue
		}
		found = true
		for _, metric := range mf.GetMetric() {
			h := metric.GetHistogram()
			if h.GetSampleCount() > 0 {
				break
			}
		}
	}
	if !found {
		t.Fatal("operator_webhook_duration_seconds not registered")
	}
}

// Finding 14: webhook duration must be pre-seeded for github/gitlab x ok/error.
func TestWebhookDuration_PreSeeded(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewOperatorMetrics(reg)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	type key struct{ provider, result string }
	want := map[key]bool{
		{"github", "ok"}:    false,
		{"github", "error"}: false,
		{"gitlab", "ok"}:    false,
		{"gitlab", "error"}: false,
	}
	for _, mf := range mfs {
		if mf.GetName() != "operator_webhook_duration_seconds" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			var provider, result string
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "provider" {
					provider = lp.GetValue()
				}
				if lp.GetName() == "result" {
					result = lp.GetValue()
				}
			}
			want[key{provider, result}] = true
		}
	}
	for k, seen := range want {
		if !seen {
			t.Errorf("operator_webhook_duration_seconds{provider=%q,result=%q} not pre-seeded", k.provider, k.result)
		}
	}
}

func hasMetric(mfs []*dto.MetricFamily, name string) bool {
	for _, mf := range mfs {
		if mf.GetName() == name {
			return true
		}
	}
	return false
}

func TestQueueMetrics_RegisterAndObserve(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.QueueAdmitted("alert", "incident")
	m.SetQueueDepth("myproject", "normal", 3)
	m.SetQueueInflight("myproject", "alert", 1)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if !hasMetric(mfs, "operator_queue_admitted_total") ||
		!hasMetric(mfs, "operator_queue_depth") ||
		!hasMetric(mfs, "operator_queue_inflight") {
		t.Fatal("queue metrics not registered")
	}
	// Verify project label is present in the depth gauge.
	found := false
	for _, mf := range mfs {
		if mf.GetName() != "operator_queue_depth" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "project" && lp.GetValue() == "myproject" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("operator_queue_depth missing project label")
	}
}

// Finding 28: webhookEvents must be pre-seeded for all relevant (kind, action) pairs,
// not just push/other. Verify a non-trivial combination exists at startup.
func TestWebhookEvents_FullPreSeed(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewOperatorMetrics(reg)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	type combo struct{ kind, action string }
	want := map[combo]bool{
		{"issue", "labeled"}:  false,
		{"mr", "opened"}:      false,
		{"mr", "closed"}:      false,
		{"mr", "synchronize"}: false,
		{"other", "other"}:    false,
		{"push", "create"}:    false,
	}
	for _, mf := range mfs {
		if mf.GetName() != "operator_webhook_events_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			var kind, action string
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "kind" {
					kind = lp.GetValue()
				}
				if lp.GetName() == "action" {
					action = lp.GetValue()
				}
			}
			want[combo{kind, action}] = true
		}
	}
	for k, seen := range want {
		if !seen {
			t.Errorf("operator_webhook_events_total{kind=%q,action=%q} not pre-seeded", k.kind, k.action)
		}
	}
}

func TestAddTaskTokens(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.AddTaskTokens("tatara", "tatara-operator", "issueLifecycle", "szymonrychu/tatara-operator#68", 1200, 300)
	m.AddTaskTokens("tatara", "tatara-operator", "issueLifecycle", "szymonrychu/tatara-operator#68", 800, 100)
	// Project-scoped task: empty repo and issue labels, and a zero output delta is skipped.
	m.AddTaskTokens("tatara", "", "brainstorm", "", 500, 0)

	in := testutil.ToFloat64(m.taskTokensTotal.WithLabelValues("tatara", "tatara-operator", "issueLifecycle", "szymonrychu/tatara-operator#68", "input"))
	if in != 2000 {
		t.Fatalf("issue input tokens = %v, want 2000", in)
	}
	out := testutil.ToFloat64(m.taskTokensTotal.WithLabelValues("tatara", "tatara-operator", "issueLifecycle", "szymonrychu/tatara-operator#68", "output"))
	if out != 400 {
		t.Fatalf("issue output tokens = %v, want 400", out)
	}
	brainstormIn := testutil.ToFloat64(m.taskTokensTotal.WithLabelValues("tatara", "", "brainstorm", "", "input"))
	if brainstormIn != 500 {
		t.Fatalf("brainstorm input tokens = %v, want 500", brainstormIn)
	}
	// Zero output delta must not create an output series.
	if got := testutil.CollectAndCount(m.taskTokensTotal); got != 3 {
		t.Fatalf("token series count = %d, want 3 (no zero-output series)", got)
	}
}

func TestTaskTerminal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.TaskTerminal("issueLifecycle", "Succeeded", "NoPendingSubtasks")
	m.TaskTerminal("issueLifecycle", "Failed", "PodLost")
	m.TaskTerminal("issueLifecycle", "Failed", "PodLost")

	if got := testutil.ToFloat64(m.taskTerminalTotal.WithLabelValues("issueLifecycle", "Succeeded", "NoPendingSubtasks")); got != 1 {
		t.Fatalf("Succeeded/NoPendingSubtasks = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.taskTerminalTotal.WithLabelValues("issueLifecycle", "Failed", "PodLost")); got != 2 {
		t.Fatalf("Failed/PodLost = %v, want 2", got)
	}
}

func TestSetLightragDocuments(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.SetLightragDocuments("tatara", "PROCESSED", 130)
	m.SetLightragDocuments("tatara", "PENDING", 10)
	// A re-set replaces (gauge), it does not accumulate.
	m.SetLightragDocuments("tatara", "PROCESSED", 131)

	if got := testutil.ToFloat64(m.lightragDocuments.WithLabelValues("tatara", "PROCESSED")); got != 131 {
		t.Fatalf("PROCESSED = %v, want 131", got)
	}
	if got := testutil.ToFloat64(m.lightragDocuments.WithLabelValues("tatara", "PENDING")); got != 10 {
		t.Fatalf("PENDING = %v, want 10", got)
	}

	m.LightragQueryError()
	m.LightragQueryError()
	if got := testutil.ToFloat64(m.lightragQueryErrors); got != 2 {
		t.Fatalf("operator_lightrag_query_errors_total = %v, want 2", got)
	}
}

func TestMemoryRetrievalProbe(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.MemoryRetrievalProbe("/queries", "present")
	m.MemoryRetrievalProbe("/queries", "absent")
	m.MemoryRetrievalProbe("/queries", "absent")
	m.MemoryRetrievalProbe("/code-graph/stats", "error")

	if got := testutil.ToFloat64(m.memoryRetrievalProbe.WithLabelValues("/queries", "present")); got != 1 {
		t.Fatalf("probe{/queries,present} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.memoryRetrievalProbe.WithLabelValues("/queries", "absent")); got != 2 {
		t.Fatalf("probe{/queries,absent} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.memoryRetrievalProbe.WithLabelValues("/code-graph/stats", "error")); got != 1 {
		t.Fatalf("probe{/code-graph/stats,error} = %v, want 1", got)
	}
}

// The probe matrix (route x result) must be pre-seeded so every series exists at
// a zero baseline before the first probe, for alerts on a sudden absent/error spike.
func TestMemoryRetrievalProbe_PreSeeded(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewOperatorMetrics(reg)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	type key struct{ route, result string }
	want := map[key]bool{}
	for _, route := range []string{"/queries", "/code-graph/stats"} {
		for _, result := range []string{"present", "absent", "error"} {
			want[key{route, result}] = false
		}
	}
	for _, mf := range mfs {
		if mf.GetName() != "operator_memory_retrieval_probe_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			var route, result string
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "route" {
					route = lp.GetValue()
				}
				if lp.GetName() == "result" {
					result = lp.GetValue()
				}
			}
			want[key{route, result}] = true
		}
	}
	for k, seen := range want {
		if !seen {
			t.Errorf("operator_memory_retrieval_probe_total{route=%q,result=%q} not pre-seeded", k.route, k.result)
		}
	}
}
