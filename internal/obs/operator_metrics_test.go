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

	m.SetMemoryStackCounts(1, 3, 0, 2)

	if got := testutil.ToFloat64(m.memoryStacks.WithLabelValues("Provisioning")); got != 1 {
		t.Fatalf("Provisioning stacks = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.memoryStacks.WithLabelValues("Ready")); got != 3 {
		t.Fatalf("Ready stacks = %v, want 3", got)
	}
	if got := testutil.ToFloat64(m.memoryStacks.WithLabelValues("Failed")); got != 0 {
		t.Fatalf("Failed stacks = %v, want 0", got)
	}
	if got := testutil.ToFloat64(m.memoryStacks.WithLabelValues("Degraded")); got != 2 {
		t.Fatalf("Degraded stacks = %v, want 2", got)
	}
}

func TestMemoryStacksGauge_ZeroesStalePhase(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	// Set Ready=2, then transition: Ready=0, Provisioning=1.
	m.SetMemoryStackCounts(0, 2, 0, 0)
	m.SetMemoryStackCounts(1, 0, 0, 0)

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
	got := testutil.ToFloat64(m.scmWritesTotal.WithLabelValues("github", "merge", "write", "ok"))
	if got != 2 {
		t.Fatalf("github/merge/write/ok = %v, want 2", got)
	}
}

func TestSCMVerbKind(t *testing.T) {
	cases := map[string]string{
		"merge":            "write",
		"comment":          "write",
		"create_issue":     "write",
		"list_open_issues": "read",
		"list_open_prs":    "read",
		"get_pr_state":     "read",
	}
	for verb, want := range cases {
		if got := SCMVerbKind(verb); got != want {
			t.Errorf("SCMVerbKind(%q) = %q, want %q", verb, got, want)
		}
	}
}

func TestSCMWriteKindLabel(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.SCMWrite("github", "list_open_prs", "ok")
	if got := testutil.ToFloat64(m.scmWritesTotal.WithLabelValues("github", "list_open_prs", "read", "ok")); got != 1 {
		t.Fatalf("read verb labelled kind=read = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.SCMWriteCounter("github", "list_open_prs", "ok")); got != 1 {
		t.Fatalf("SCMWriteCounter helper = %v, want 1", got)
	}
}

func TestSCMRequestErrorByStatus(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.SCMRequestErrorByStatus("github", "comment", "401")
	if got := testutil.ToFloat64(m.scmReqErrorsByStatus.WithLabelValues("github", "comment", "401")); got != 1 {
		t.Fatalf("github/comment/401 = %v, want 1", got)
	}
}

func TestTasksInflightKindRegistered(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.SetTasksInflightKind("clarify", 2)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	found := false
	for _, mf := range mfs {
		if mf.GetName() == "tatara_tasks_inflight" {
			found = true
		}
	}
	if !found {
		t.Fatalf("metric %q not registered/gathered", "tatara_tasks_inflight")
	}
}

// tatara_scan_items_total, tatara_scan_tasks_created_total, and
// tatara_scan_duration_seconds died with the B.4 sweep (task-centric
// redesign): tatara-observability's own dashboards document them as
// superseded by operator_sweep_last_success_timestamp_seconds and
// operator_tasks_minted_per_sweep, and scripts/metrics_allowlist.txt
// already excludes all three. This is a regression guard against
// reintroducing the dead family (metric-wiring audit, issue #370).
func TestScanMetricsPruned(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewOperatorMetrics(reg)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	dead := map[string]bool{
		"tatara_scan_items_total":         true,
		"tatara_scan_tasks_created_total": true,
		"tatara_scan_duration_seconds":    true,
	}
	for _, mf := range mfs {
		if dead[mf.GetName()] {
			t.Fatalf("metric %q must be pruned, still registered", mf.GetName())
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
	m.IngestJobResult("success", "full")
	m.IngestJobResult("failure", "full")
	m.IngestJobResult("failure", "incremental")
	m.IngestJobResult("failure", "incremental")
	if got := testutil.ToFloat64(m.ingestJobTotal.WithLabelValues("success", "full")); got != 1 {
		t.Fatalf("ingest_job success/full = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.ingestJobTotal.WithLabelValues("failure", "full")); got != 1 {
		t.Fatalf("ingest_job failure/full = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.ingestJobTotal.WithLabelValues("failure", "incremental")); got != 2 {
		t.Fatalf("ingest_job failure/incremental = %v, want 2", got)
	}
}

func TestSetRepositoryIngestFailing(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.SetRepositoryIngestFailing("repo-a", true)
	m.SetRepositoryIngestFailing("repo-b", false)
	if got := testutil.ToFloat64(m.RepositoryIngestFailingGauge("repo-a")); got != 1 {
		t.Fatalf("ingest_failing{repo-a} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.RepositoryIngestFailingGauge("repo-b")); got != 0 {
		t.Fatalf("ingest_failing{repo-b} = %v, want 0", got)
	}
	// Recovery: setting back to false must clear the gauge (no monotonicity).
	m.SetRepositoryIngestFailing("repo-a", false)
	if got := testutil.ToFloat64(m.RepositoryIngestFailingGauge("repo-a")); got != 0 {
		t.Fatalf("ingest_failing{repo-a} after recovery = %v, want 0", got)
	}
}

func TestSetRepositoryLastIngestTimestamp(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.SetRepositoryLastIngestTimestamp("repo-a", 1750000000)
	if got := testutil.ToFloat64(m.RepositoryLastIngestTimestampGauge("repo-a")); got != 1750000000 {
		t.Fatalf("last_ingest_timestamp{repo-a} = %v, want 1750000000", got)
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
	m.SetRepositoryIngestFailing("touch", false)
	m.SetRepositoryLastIngestTimestamp("touch", 1)
	mfs, _ := reg.Gather()
	want := map[string]bool{
		"operator_reconcile_total":                          false,
		"operator_ingest_job_duration_seconds":              false,
		"operator_turn_duration_seconds":                    false,
		"operator_webhook_events_total":                     false,
		"operator_tasks_inflight":                           false,
		"operator_memory_provision_duration_seconds":        false,
		"operator_memory_stacks":                            false,
		"operator_turn_timeout_total":                       false,
		"operator_ingest_job_total":                         false,
		"operator_repository_ingest_failing":                false,
		"operator_repository_last_ingest_timestamp_seconds": false,
		"operator_agent_unreachable_termination_total":      false,
		"operator_orphan_reaped_total":                      false,
		"operator_reap_delete_error_total":                  false,
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
// Finding 15: all writeback outcome labels must be pre-seeded.
// All brainstorm-outcome labels must be pre-seeded so both series exist at zero
// before any brainstorm run completes (the yield rate is graphable from startup).
// BrainstormOutcome increments the right series per result.
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
	wantEndpoints := []string{"task_context", "task_note", "submit_outcome", "scm_read", "issue_write", "mr_write"}
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

func TestSetAccountUsageGauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.SetAccountUsage("five_hour", 42.5)
	m.SetAccountUsagePollHealth(true)
	if v := testutil.ToFloat64(m.accountUsageUtil.WithLabelValues("five_hour")); v != 42.5 {
		t.Fatalf("gauge=%v", v)
	}
	if v := testutil.ToFloat64(m.accountUsagePollHealth); v != 1 {
		t.Fatalf("poll_health=%v, want 1", v)
	}
}

// Issue #339: tatara_account_usage_poller_enabled must reflect USAGE_ENABLED
// independently of poll_health, so the "disabled" and "unhealthy" states are
// distinguishable at the metric level (the alert guards on this gauge).
func TestSetAccountUsagePollerEnabled(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.SetAccountUsagePollerEnabled(false)
	if v := testutil.ToFloat64(m.accountUsagePollerEnabled); v != 0 {
		t.Fatalf("poller_enabled=%v, want 0", v)
	}
	// poll_health defaults to 0 (never polled) while disabled - the two gauges
	// must stay independent so a disabled poller doesn't read as "healthy".
	if v := testutil.ToFloat64(m.accountUsagePollHealth); v != 0 {
		t.Fatalf("poll_health=%v, want 0 (unset)", v)
	}

	m.SetAccountUsagePollerEnabled(true)
	if v := testutil.ToFloat64(m.accountUsagePollerEnabled); v != 1 {
		t.Fatalf("poller_enabled=%v, want 1", v)
	}
}

func TestSetAccountUsageResetAndOverage(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.SetAccountUsageReset("weekly", 1750000000)
	if v := testutil.ToFloat64(m.accountUsageReset.WithLabelValues("weekly")); v != 1750000000 {
		t.Fatalf("reset=%v", v)
	}
	m.SetAccountOverage(12.5, 125, 1000)
	if v := testutil.ToFloat64(m.accountOveragePercent); v != 12.5 {
		t.Fatalf("overage percent=%v", v)
	}
	if v := testutil.ToFloat64(m.accountOverageUsed); v != 125 {
		t.Fatalf("overage used=%v", v)
	}
	if v := testutil.ToFloat64(m.accountOverageLimit); v != 1000 {
		t.Fatalf("overage limit=%v", v)
	}
}

func TestAdmissionBlocked_KindLabel(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.AdmissionBlocked("tatara", "normal", "review", "kind_ceiling")
	m.AdmissionBlocked("tatara", "normal", "", "project_paused")
	if got := testutil.ToFloat64(m.AdmissionBlockedCounter("tatara", "normal", "review", "kind_ceiling")); got != 1 {
		t.Fatalf("admission_blocked{kind=review} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.AdmissionBlockedCounter("tatara", "normal", "", "project_paused")); got != 1 {
		t.Fatalf("admission_blocked{kind=} = %v, want 1", got)
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

	m.AddTaskTokens("tatara", "tatara-operator", "issueLifecycle", "szymonrychu/tatara-operator#68", "claude-opus-4-8", 1200, 300, 400, 50)
	m.AddTaskTokens("tatara", "tatara-operator", "issueLifecycle", "szymonrychu/tatara-operator#68", "claude-opus-4-8", 800, 100, 0, 0)
	// Project-scoped task: empty repo and issue labels, and zero cache/output deltas are skipped.
	m.AddTaskTokens("tatara", "", "brainstorm", "", "claude-sonnet-5", 500, 0, 0, 0)

	in := testutil.ToFloat64(m.taskTokensTotal.WithLabelValues("tatara", "tatara-operator", "issueLifecycle", "szymonrychu/tatara-operator#68", "claude-opus-4-8", "input"))
	if in != 2000 {
		t.Fatalf("issue input tokens = %v, want 2000", in)
	}
	out := testutil.ToFloat64(m.taskTokensTotal.WithLabelValues("tatara", "tatara-operator", "issueLifecycle", "szymonrychu/tatara-operator#68", "claude-opus-4-8", "output"))
	if out != 400 {
		t.Fatalf("issue output tokens = %v, want 400", out)
	}
	cacheRead := testutil.ToFloat64(m.taskTokensTotal.WithLabelValues("tatara", "tatara-operator", "issueLifecycle", "szymonrychu/tatara-operator#68", "claude-opus-4-8", "cache_read"))
	if cacheRead != 400 {
		t.Fatalf("issue cache_read tokens = %v, want 400", cacheRead)
	}
	cacheCreation := testutil.ToFloat64(m.taskTokensTotal.WithLabelValues("tatara", "tatara-operator", "issueLifecycle", "szymonrychu/tatara-operator#68", "claude-opus-4-8", "cache_creation"))
	if cacheCreation != 50 {
		t.Fatalf("issue cache_creation tokens = %v, want 50", cacheCreation)
	}
	brainstormIn := testutil.ToFloat64(m.taskTokensTotal.WithLabelValues("tatara", "", "brainstorm", "", "claude-sonnet-5", "input"))
	if brainstormIn != 500 {
		t.Fatalf("brainstorm input tokens = %v, want 500", brainstormIn)
	}
	// Zero classes must not create a series: issue tuple has input+output+cache_read+cache_creation (4),
	// brainstorm tuple has input only (1) = 5 total.
	if got := testutil.CollectAndCount(m.taskTokensTotal); got != 5 {
		t.Fatalf("token series count = %d, want 5 (no zero-class series)", got)
	}
}

func TestTaskTerminal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.TaskTerminal("implement", "delivered", "NoPendingSubtasks")
	m.TaskTerminal("implement", "failed", "PodLost")
	m.TaskTerminal("implement", "failed", "PodLost")

	if got := testutil.ToFloat64(m.taskTerminalTotal.WithLabelValues("implement", "delivered", "NoPendingSubtasks")); got != 1 {
		t.Fatalf("delivered/NoPendingSubtasks = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.taskTerminalTotal.WithLabelValues("implement", "failed", "PodLost")); got != 2 {
		t.Fatalf("failed/PodLost = %v, want 2", got)
	}
}

// TestTaskTerminalLabels asserts operator_task_terminal_total is registered
// with exactly the label set {kind,stage,stageReason} (contract K.1 / D1):
// the tatara-observability alert rules select on stage/stageReason by name,
// so a label rename here would silently break them without a build error.
func TestTaskTerminalLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.TaskTerminal("implement", "failed", "TurnTimeout")

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var found *dto.MetricFamily
	for _, mf := range mfs {
		if mf.GetName() == "operator_task_terminal_total" {
			found = mf
			break
		}
	}
	if found == nil {
		t.Fatal("operator_task_terminal_total not gathered after a live record")
	}
	gotLabels := map[string]bool{}
	for _, l := range found.GetMetric()[0].GetLabel() {
		gotLabels[l.GetName()] = true
	}
	for _, want := range []string{"kind", "stage", "stageReason"} {
		if !gotLabels[want] {
			t.Errorf("operator_task_terminal_total missing label %q, got %v", want, gotLabels)
		}
	}
	if len(gotLabels) != 3 {
		t.Errorf("operator_task_terminal_total has %d labels, want exactly 3: %v", len(gotLabels), gotLabels)
	}
}

func TestAddTerminalTokens(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.AddTerminalTokens("tatara", "tatara-operator", "churned", "claude-opus-4-8", 2000, 500, 800, 100)

	if got := testutil.ToFloat64(m.taskTerminalTokensTotal.WithLabelValues("tatara", "tatara-operator", "churned", "claude-opus-4-8", "input")); got != 2000 {
		t.Fatalf("input = %v, want 2000", got)
	}
	if got := testutil.ToFloat64(m.taskTerminalTokensTotal.WithLabelValues("tatara", "tatara-operator", "churned", "claude-opus-4-8", "output")); got != 500 {
		t.Fatalf("output = %v, want 500", got)
	}
	if got := testutil.ToFloat64(m.taskTerminalTokensTotal.WithLabelValues("tatara", "tatara-operator", "churned", "claude-opus-4-8", "cache_read")); got != 800 {
		t.Fatalf("cache_read = %v, want 800", got)
	}
	if got := testutil.ToFloat64(m.taskTerminalTokensTotal.WithLabelValues("tatara", "tatara-operator", "churned", "claude-opus-4-8", "cache_creation")); got != 100 {
		t.Fatalf("cache_creation = %v, want 100", got)
	}

	// Zero classes must not create a series.
	m.AddTerminalTokens("tatara", "tatara-operator", "delivered", "claude-sonnet-5", 0, 0, 0, 0)
	if got := testutil.CollectAndCount(m.taskTerminalTokensTotal); got != 4 {
		t.Fatalf("terminal token series count = %d, want 4 (no zero-class series)", got)
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
func TestAddTaskTurn_Increments(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.AddTaskTurn("tatara", "tatara-operator", "issueLifecycle", "szymonrychu/tatara-operator#42")
	m.AddTaskTurn("tatara", "tatara-operator", "issueLifecycle", "szymonrychu/tatara-operator#42")

	got := testutil.ToFloat64(m.taskTurnsTotal.WithLabelValues("tatara", "tatara-operator", "issueLifecycle", "szymonrychu/tatara-operator#42"))
	if got != 2 {
		t.Fatalf("taskTurnsTotal = %v, want 2", got)
	}
}

func TestAddTaskTurn_IsolatesIssueLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.AddTaskTurn("tatara", "tatara-operator", "issueLifecycle", "szymonrychu/tatara-operator#1")
	m.AddTaskTurn("tatara", "tatara-operator", "issueLifecycle", "szymonrychu/tatara-operator#2")

	got1 := testutil.ToFloat64(m.taskTurnsTotal.WithLabelValues("tatara", "tatara-operator", "issueLifecycle", "szymonrychu/tatara-operator#1"))
	got2 := testutil.ToFloat64(m.taskTurnsTotal.WithLabelValues("tatara", "tatara-operator", "issueLifecycle", "szymonrychu/tatara-operator#2"))
	if got1 != 1 || got2 != 1 {
		t.Fatalf("issue#1=%v issue#2=%v, want both 1", got1, got2)
	}
}

func TestSetIssueState_SetsOne(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.SetIssueState("tatara", "tatara-operator", "szymonrychu/tatara-operator#5", "issueLifecycle", "implementing", "false")

	got := testutil.ToFloat64(m.taskIssueState.WithLabelValues("tatara", "tatara-operator", "szymonrychu/tatara-operator#5", "issueLifecycle", "implementing", "false"))
	if got != 1 {
		t.Fatalf("tatara_issue_state = %v, want 1", got)
	}
}

func TestResetIssueState_ClearsStale(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.SetIssueState("tatara", "repo", "repo#10", "issueLifecycle", "implementing", "false")
	m.SetIssueState("tatara", "repo", "repo#20", "issueLifecycle", "triage", "false")

	m.ResetIssueState()
	m.SetIssueState("tatara", "repo", "repo#10", "issueLifecycle", "implementing", "false")

	// repo#10 is still set; repo#20 must be gone after Reset.
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	issue20Found := false
	for _, mf := range mfs {
		if mf.GetName() != "tatara_issue_state" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "issue" && lp.GetValue() == "repo#20" {
					issue20Found = true
				}
			}
		}
	}
	if issue20Found {
		t.Fatal("repo#20 series must be absent after Reset+Set-only-10, but was still present")
	}
}

func TestDeleteTaskSeries_RemovesTokenAndTurn(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	// Add tokens and a turn for an issue-scoped task.
	m.AddTaskTokens("tatara", "op", "issueLifecycle", "op#7", "claude-opus-4-8", 100, 50, 30, 10)
	m.AddTaskTurn("tatara", "op", "issueLifecycle", "op#7")

	// Also add a project-scoped (empty issue) series that must NOT be deleted.
	m.AddTaskTokens("tatara", "", "brainstorm", "", "claude-sonnet-5", 200, 0, 0, 0)

	m.DeleteTaskSeries("tatara", "op", "issueLifecycle", "op#7")

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	issue7TokenFound := false
	issue7TurnFound := false
	brainstormFound := false
	for _, mf := range mfs {
		switch mf.GetName() {
		case "operator_task_tokens_total":
			for _, metric := range mf.GetMetric() {
				var iss string
				for _, lp := range metric.GetLabel() {
					if lp.GetName() == "issue" {
						iss = lp.GetValue()
					}
				}
				if iss == "op#7" {
					issue7TokenFound = true
				}
				if iss == "" {
					brainstormFound = true
				}
			}
		case "operator_task_turns_total":
			for _, metric := range mf.GetMetric() {
				for _, lp := range metric.GetLabel() {
					if lp.GetName() == "issue" && lp.GetValue() == "op#7" {
						issue7TurnFound = true
					}
				}
			}
		}
	}
	if issue7TokenFound {
		t.Error("operator_task_tokens_total{issue=op#7} must be absent after DeleteTaskSeries")
	}
	if issue7TurnFound {
		t.Error("operator_task_turns_total{issue=op#7} must be absent after DeleteTaskSeries")
	}
	if !brainstormFound {
		t.Error("operator_task_tokens_total{issue=} (brainstorm) must not be deleted")
	}
}

// A Task's Status.ResolvedModel can change across its life (a respawn or a
// stage change may re-resolve a different model), so per-turn token/turn
// series for the SAME (project,repo,kind,issue) can be split across several
// model label values. DeleteTaskSeries must clear all of them on GC, not just
// the Task's final model - otherwise a reassigned-model task leaks its
// earlier model's series forever (metric-wiring audit, issue #370).
// A project-scoped Task (e.g. brainstorm) shares issue=="" with every other
// project-scoped Task of the same (project,repo,kind); GC'ing one must not
// wipe the others' still-live series.
func TestDeleteTaskSeries_EmptyIssueIsANoOp(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.AddTaskTokens("tatara", "", "brainstorm", "", "claude-sonnet-5", 200, 0, 0, 0)
	m.AddTaskTurn("tatara", "", "brainstorm", "")

	m.DeleteTaskSeries("tatara", "", "brainstorm", "")

	if got := testutil.ToFloat64(m.taskTurnsTotal.WithLabelValues("tatara", "", "brainstorm", "")); got != 1 {
		t.Errorf("operator_task_turns_total{issue=} must survive a project-scoped DeleteTaskSeries call, got %v", got)
	}
}

func TestDeleteTaskSeries_RemovesAcrossModels(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.AddTaskTokens("tatara", "op", "implement", "op#9", "claude-opus-4-8", 100, 50, 30, 10)
	m.AddTaskTurn("tatara", "op", "implement", "op#9")
	m.AddTaskTokens("tatara", "op", "implement", "op#9", "claude-sonnet-5", 40, 20, 5, 1)

	m.DeleteTaskSeries("tatara", "op", "implement", "op#9")

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "operator_task_tokens_total" && mf.GetName() != "operator_task_turns_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "issue" && lp.GetValue() == "op#9" {
					t.Errorf("%s{issue=op#9} must be absent after DeleteTaskSeries regardless of model", mf.GetName())
				}
			}
		}
	}
}

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
		for _, result := range []string{"present", "absent", "error", "unauthorized", "degraded"} {
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

// RecordReviewOutcome is nil-safe: a reconciler wired without metrics is a
// test, not an outage, matching TaskTerminalEntry's convention.
func TestRecordReviewOutcome_NilSafe(t *testing.T) {
	var m *OperatorMetrics
	m.RecordReviewOutcome("tatara", "op", "claude-sonnet-5", "approved")
}

func TestQualityMetrics_Emit(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.RecordReviewOutcome("tatara", "op", "claude-sonnet-5", "approved")
	m.RecordReviewOutcome("tatara", "op", "claude-sonnet-5", "changes_requested")
	m.AddReviewFindings("tatara", "op", "claude-sonnet-5", 3)
	m.RecordImplementCI("tatara", "op", "issueLifecycle", "claude-opus-4-8", "fail")

	if got := testutil.ToFloat64(m.reviewOutcomeTotal.WithLabelValues("tatara", "op", "claude-sonnet-5", "approved")); got != 1 {
		t.Fatalf("reviewOutcomeTotal approved = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.reviewOutcomeTotal.WithLabelValues("tatara", "op", "claude-sonnet-5", "changes_requested")); got != 1 {
		t.Fatalf("reviewOutcomeTotal changes_requested = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.reviewFindingsTotal.WithLabelValues("tatara", "op", "claude-sonnet-5")); got != 3 {
		t.Fatalf("reviewFindingsTotal = %v, want 3", got)
	}
	if got := testutil.ToFloat64(m.implementCITotal.WithLabelValues("tatara", "op", "issueLifecycle", "claude-opus-4-8", "fail")); got != 1 {
		t.Fatalf("implementCITotal fail = %v, want 1", got)
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	wantNames := map[string]bool{
		"operator_review_outcome_total":  false,
		"operator_review_findings_total": false,
		"operator_implement_ci_total":    false,
	}
	for _, mf := range mfs {
		if _, ok := wantNames[mf.GetName()]; ok {
			wantNames[mf.GetName()] = true
		}
	}
	for name, seen := range wantNames {
		if !seen {
			t.Errorf("%s not registered/gathered", name)
		}
	}
}

// ---------------------------------------------------------------------------
// Contract K.1: five metrics that were named but never emitted.
// ---------------------------------------------------------------------------

// operator_task_stage is a low-cardinality COUNT per (stage,kind) bucket, not
// per-task. ResetTaskStageGauges clears both it and the per-task age gauge so a
// Task that left its bucket does not linger (contract M22).
func TestTaskStageGauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.SetTaskStage("implementing", "implement", 3)
	if got := testutil.ToFloat64(m.TaskStageGauge("implementing", "implement")); got != 3 {
		t.Fatalf("operator_task_stage{implementing,implement} = %v, want 3", got)
	}

	m.ResetTaskStageGauges()
	if got := testutil.ToFloat64(m.TaskStageGauge("implementing", "implement")); got != 0 {
		t.Fatalf("operator_task_stage after Reset = %v, want 0 (series gone)", got)
	}
}

func TestTaskStageGauge_NilSafe(t *testing.T) {
	var m *OperatorMetrics
	m.SetTaskStage("implementing", "implement", 3)
	m.ResetTaskStageGauges()
}

// operator_task_stage_age_seconds is per-task.
func TestTaskStageAgeGauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.SetTaskStageAge("task-1", "implementing", "implement", 120)
	if got := testutil.ToFloat64(m.TaskStageAgeGauge("task-1", "implementing", "implement")); got != 120 {
		t.Fatalf("operator_task_stage_age_seconds = %v, want 120", got)
	}

	m.ResetTaskStageGauges()
	if got := testutil.ToFloat64(m.TaskStageAgeGauge("task-1", "implementing", "implement")); got != 0 {
		t.Fatalf("operator_task_stage_age_seconds after Reset = %v, want 0 (series gone)", got)
	}
}

func TestTaskStageAgeGauge_NilSafe(t *testing.T) {
	var m *OperatorMetrics
	m.SetTaskStageAge("task-1", "implementing", "implement", 120)
}

// operator_task_parked_total increments once per park TRANSITION, labelled by
// the stage the Task parked FROM and the park reason.
func TestTaskParked(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.TaskParked("implementing", "implement-declined")
	m.TaskParked("implementing", "implement-declined")
	m.TaskParked("triaging", "triage-stalled")

	if got := testutil.ToFloat64(m.TaskParkedCounter("implementing", "implement-declined")); got != 2 {
		t.Fatalf("operator_task_parked_total{implementing,implement-declined} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.TaskParkedCounter("triaging", "triage-stalled")); got != 1 {
		t.Fatalf("operator_task_parked_total{triaging,triage-stalled} = %v, want 1", got)
	}
}

func TestTaskParked_NilSafe(t *testing.T) {
	var m *OperatorMetrics
	m.TaskParked("implementing", "implement-declined")
}

func TestUnparkDeclined(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.UnparkDeclined("awaiting-human", "guard")
	m.UnparkDeclined("awaiting-human", "rule")
	m.UnparkDeclined("awaiting-human", "rule")

	if got := testutil.ToFloat64(m.UnparkDeclinedCounter("awaiting-human", "guard")); got != 1 {
		t.Fatalf("operator_unpark_declined_total{awaiting-human,guard} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.UnparkDeclinedCounter("awaiting-human", "rule")); got != 2 {
		t.Fatalf("operator_unpark_declined_total{awaiting-human,rule} = %v, want 2", got)
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	found := false
	for _, mf := range mfs {
		if mf.GetName() == "operator_unpark_declined_total" {
			found = true
		}
	}
	if !found {
		t.Fatal("operator_unpark_declined_total not registered")
	}
}

func TestUnparkDeclined_NilSafe(t *testing.T) {
	var m *OperatorMetrics
	m.UnparkDeclined("awaiting-human", "guard")
}

// operator_orphan_adopted_total increments once per orphan work item the sweep
// mints a Task for.
func TestOrphanAdopted(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.OrphanAdopted("clarify")
	m.OrphanAdopted("clarify")
	m.OrphanAdopted("review")

	if got := testutil.ToFloat64(m.OrphanAdoptedCounter("clarify")); got != 2 {
		t.Fatalf("operator_orphan_adopted_total{clarify} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.OrphanAdoptedCounter("review")); got != 1 {
		t.Fatalf("operator_orphan_adopted_total{review} = %v, want 1", got)
	}
}

func TestOrphanAdopted_NilSafe(t *testing.T) {
	var m *OperatorMetrics
	m.OrphanAdopted("clarify")
}

// operator_queue_age_seconds is the age of the OLDEST QueuedEvent per
// (class,priority,state) bucket. ResetQueueAge clears stale buckets each pass.
func TestQueueAgeGauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)

	m.SetQueueAge("normal", "2", "Queued", 45)
	if got := testutil.ToFloat64(m.QueueAgeGauge("normal", "2", "Queued")); got != 45 {
		t.Fatalf("operator_queue_age_seconds = %v, want 45", got)
	}

	m.ResetQueueAge()
	if got := testutil.ToFloat64(m.QueueAgeGauge("normal", "2", "Queued")); got != 0 {
		t.Fatalf("operator_queue_age_seconds after Reset = %v, want 0 (series gone)", got)
	}
}

func TestQueueAgeGauge_NilSafe(t *testing.T) {
	var m *OperatorMetrics
	m.SetQueueAge("normal", "2", "Queued", 45)
	m.ResetQueueAge()
}

// TestK1MetricsNamesRegistered pins the five contract K.1 metric names onto the
// registry, mirroring TestOperatorMetricsNamesStable.
func TestK1MetricsNamesRegistered(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperatorMetrics(reg)
	m.SetTaskStage("implementing", "implement", 1)
	m.SetTaskStageAge("task-1", "implementing", "implement", 1)
	m.TaskParked("implementing", "implement-declined")
	m.OrphanAdopted("clarify")
	m.SetQueueAge("normal", "2", "Queued", 1)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	want := map[string]bool{
		"operator_task_stage":             false,
		"operator_task_stage_age_seconds": false,
		"operator_task_parked_total":      false,
		"operator_orphan_adopted_total":   false,
		"operator_queue_age_seconds":      false,
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
