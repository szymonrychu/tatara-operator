package obs

import "github.com/prometheus/client_golang/prometheus"

// OperatorMetrics holds the reconciler-facing Prometheus collectors for the
// tatara-operator. Construct one with NewOperatorMetrics and pass it to the
// reconcilers.
type OperatorMetrics struct {
	reconcileTotal            *prometheus.CounterVec
	ingestJobDuration         prometheus.Histogram
	turnDuration              prometheus.Histogram
	webhookEvents             *prometheus.CounterVec
	tasksInflight             prometheus.Gauge
	memoryProvisionDuration   prometheus.Histogram
	memoryStacks              *prometheus.GaugeVec
	scmWritesTotal            *prometheus.CounterVec
	scmReqErrorsByStatus      *prometheus.CounterVec
	scanItemsTotal            *prometheus.CounterVec
	scanTasksCreatedTotal     *prometheus.CounterVec
	scanDurationSeconds       *prometheus.HistogramVec
	issueOutcomeTotal         *prometheus.CounterVec
	tasksInflightKind         *prometheus.GaugeVec
	agentBootRaceRequeue      prometheus.Counter
	openProposals             *prometheus.GaugeVec
	turnTimeoutTotal          *prometheus.CounterVec
	ingestJobTotal            *prometheus.CounterVec
	agentUnreachableTermTotal prometheus.Counter
	agentBootCrashTotal       *prometheus.CounterVec
	orphanReapedTotal         *prometheus.CounterVec
	reapDeleteErrorTotal      *prometheus.CounterVec
	tasksGCTotal              *prometheus.CounterVec
	conversationGCTotal       *prometheus.CounterVec
	turnSubmitTotal           *prometheus.CounterVec
	turnSubmitDuration        *prometheus.HistogramVec
	agentHTTPTotal            *prometheus.CounterVec
	agentHTTPDuration         *prometheus.HistogramVec
	authTotal                 *prometheus.CounterVec
	writebackOutcomeTotal     *prometheus.CounterVec
	brainstormOutcomeTotal    *prometheus.CounterVec
	webhookDuration           *prometheus.HistogramVec
	restapiRequestsTotal      *prometheus.CounterVec
	restapiRequestDuration    *prometheus.HistogramVec
	memoryHealthReadErrors    prometheus.Counter
	queueAdmittedTotal        *prometheus.CounterVec
	queueDepth                *prometheus.GaugeVec
	queueInflight             *prometheus.GaugeVec
	taskTokensTotal           *prometheus.CounterVec
	taskTerminalTotal         *prometheus.CounterVec
	lightragDocuments         *prometheus.GaugeVec
	lightragQueryErrors       prometheus.Counter
	memoryRetrievalProbe      *prometheus.CounterVec
	systemicSiblingsCollapsed *prometheus.CounterVec
	systemicGroupsLed         *prometheus.CounterVec
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
		}, []string{"provider", "kind", "action", "result"}),
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
		scmWritesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_scm_writes_total",
			Help: "Total SCM operations by provider, verb, kind (read|write), and result (ok|error|blocked).",
		}, []string{"provider", "verb", "kind", "result"}),
		scmReqErrorsByStatus: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_scm_request_errors_by_status_total",
			Help: "SCM operations that errored, by provider, verb, and classified status (HTTP code, or \"network\" for connect/timeout failures).",
		}, []string{"provider", "verb", "status"}),
		scanItemsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tatara_scan_items_total",
			Help: "Total scan candidates by activity and outcome.",
		}, []string{"activity", "outcome"}),
		scanTasksCreatedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tatara_scan_tasks_created_total",
			Help: "Tasks created by scan activity and Task kind.",
		}, []string{"activity", "kind"}),
		scanDurationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "tatara_scan_duration_seconds",
			Help:    "Wall-clock duration of one scan activity.",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 10),
		}, []string{"activity"}),
		issueOutcomeTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tatara_issue_outcome_total",
			Help: "Issue-triage outcomes by action.",
		}, []string{"action"}),
		tasksInflightKind: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tatara_tasks_inflight",
			Help: "In-flight Tasks by kind.",
		}, []string{"kind"}),
		agentBootRaceRequeue: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "operator_agent_boot_race_requeue_total",
			Help: "Times a turn submit hit a still-booting wrapper and was requeued instead of erroring.",
		}),
		openProposals: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_open_proposals",
			Help: "Open, unapproved agent-proposed issues per repo.",
		}, []string{"repo"}),
		turnTimeoutTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_turn_timeout_total",
			Help: "Agent turns terminated for stalling (no activity past the inactivity deadline), by detection source.",
		}, []string{"source"}),
		ingestJobTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_ingest_job_total",
			Help: "Finished ingest Jobs by terminal result and ingest mode (incremental|full).",
		}, []string{"result", "mode"}),
		agentUnreachableTermTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "operator_agent_unreachable_termination_total",
			Help: "Tasks terminated because the wrapper agent stayed unreachable past the boot deadline.",
		}),
		agentBootCrashTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_agent_boot_crash_total",
			Help: "Wrapper Pods that failed to boot before /readyz came up, by reason and outcome.",
		}, []string{"reason", "outcome"}),
		orphanReapedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_orphan_reaped_total",
			Help: "Orphan wrapper pods reaped by the backstop reaper, by reason.",
		}, []string{"reason"}),
		reapDeleteErrorTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_reap_delete_error_total",
			Help: "Errors deleting orphan wrappers by resource kind.",
		}, []string{"kind"}),
		tasksGCTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_tasks_gc_total",
			Help: "Terminal Tasks garbage-collected by the reaper past the retention window, by Task kind.",
		}, []string{"kind"}),
		conversationGCTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_conversation_gc_total",
			Help: "S3 conversation objects garbage-collected by the reaper when a batch fully closed, by result.",
		}, []string{"result"}),
		turnSubmitTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_turn_submit_total",
			Help: "Total turn submissions to agent wrappers by kind and result.",
		}, []string{"kind", "result"}),
		turnSubmitDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "operator_turn_submit_duration_seconds",
			Help:    "Wall-clock duration of SubmitTurn calls to agent wrappers.",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 10),
		}, []string{"kind"}),
		agentHTTPTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_agent_http_total",
			Help: "Total agent wrapper HTTP calls by method and outcome.",
		}, []string{"method", "outcome"}),
		agentHTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "operator_agent_http_duration_seconds",
			Help:    "Wall-clock duration of agent wrapper HTTP calls by method.",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 10),
		}, []string{"method"}),
		authTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_auth_total",
			Help: "Total REST API auth attempts by result.",
		}, []string{"result"}),
		writebackOutcomeTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_writeback_outcome_total",
			Help: "Writeback terminal outcomes by result.",
		}, []string{"result"}),
		// Per-run brainstorm yield. The brainstorm Task itself never opens a PR, so
		// a dedicated counter (rather than overloading writeback_outcome) keeps the
		// "a PR/MR write was attempted" semantics of writeback_outcome clean.
		brainstormOutcomeTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_brainstorm_outcome_total",
			Help: "Brainstorm run yield outcomes by result.",
		}, []string{"result"}),
		// Finding 14: webhook duration histogram so slow apiserver/secret lookups
		// during webhook handling surface before GitHub's 10s delivery timeout.
		webhookDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "operator_webhook_duration_seconds",
			Help:    "Wall-clock duration of webhook request handling, by provider and result.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 10),
		}, []string{"provider", "result"}),
		// Finding 2: REST API request counter and latency histogram. Endpoint label
		// is the handler name (e.g. "patch_task", "propose_issue").
		restapiRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_restapi_requests_total",
			Help: "Total REST API requests by endpoint and result.",
		}, []string{"endpoint", "result"}),
		restapiRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "operator_restapi_request_duration_seconds",
			Help:    "Wall-clock duration of REST API handler execution by endpoint.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
		}, []string{"endpoint"}),
		// Finding 13: counter for transient memory-health read errors so repeated
		// blips surfacing as healthy reconciles are visible in Prometheus.
		memoryHealthReadErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "operator_memory_health_read_errors_total",
			Help: "Total transient errors reading memory-stack health (not real stack failures).",
		}),
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
		taskTokensTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_tokens_total",
			Help: "Agent token usage by project, repo, Task kind, issue, and type (input|output).",
		}, []string{"project", "repo", "kind", "issue", "type"}),
		taskTerminalTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_terminal_total",
			Help: "Tasks reaching a terminal phase by kind, phase (Succeeded|Failed), and condition reason.",
		}, []string{"kind", "phase", "reason"}),
		lightragDocuments: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_lightrag_documents",
			Help: "Documents in each per-project lightrag memory corpus by ingestion status.",
		}, []string{"project", "status"}),
		lightragQueryErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "operator_lightrag_query_errors_total",
			Help: "Failed attempts to read document counts from a project's lightrag.",
		}),
		// Unauthenticated route-presence probe of each project's tatara-memory
		// retrieval surface. result is "present" (route served any non-404 status,
		// e.g. a 401 auth rejection that still proves the route exists), "absent"
		// (404 -> drifted/stale binary), or "error" (transport failure -> down).
		memoryRetrievalProbe: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_memory_retrieval_probe_total",
			Help: "Unauthenticated route-presence probes of a project's tatara-memory retrieval surface by route and result.",
		}, []string{"route", "result"}),
		systemicSiblingsCollapsed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tatara_systemic_siblings_collapsed_total",
			Help: "Systemic-group sibling issues collapsed (no separate agent spawned), by project.",
		}, []string{"project"}),
		systemicGroupsLed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tatara_systemic_groups_led_total",
			Help: "Systemic-group leads elected (lead issue gets a combined-PR agent), by project.",
		}, []string{"project"}),
	}
	reg.MustRegister(
		m.reconcileTotal,
		m.ingestJobDuration,
		m.turnDuration,
		m.webhookEvents,
		m.tasksInflight,
		m.memoryProvisionDuration,
		m.memoryStacks,
		m.scmWritesTotal,
		m.scmReqErrorsByStatus,
		m.scanItemsTotal,
		m.scanTasksCreatedTotal,
		m.scanDurationSeconds,
		m.issueOutcomeTotal,
		m.tasksInflightKind,
		m.agentBootRaceRequeue,
		m.openProposals,
		m.turnTimeoutTotal,
		m.ingestJobTotal,
		m.agentUnreachableTermTotal,
		m.agentBootCrashTotal,
		m.orphanReapedTotal,
		m.reapDeleteErrorTotal,
		m.tasksGCTotal,
		m.conversationGCTotal,
		m.turnSubmitTotal,
		m.turnSubmitDuration,
		m.agentHTTPTotal,
		m.agentHTTPDuration,
		m.authTotal,
		m.writebackOutcomeTotal,
		m.brainstormOutcomeTotal,
		m.webhookDuration,
		m.restapiRequestsTotal,
		m.restapiRequestDuration,
		m.memoryHealthReadErrors,
		m.queueAdmittedTotal,
		m.queueDepth,
		m.queueInflight,
		m.taskTokensTotal,
		m.taskTerminalTotal,
		m.lightragDocuments,
		m.lightragQueryErrors,
		m.memoryRetrievalProbe,
		m.systemicSiblingsCollapsed,
		m.systemicGroupsLed,
	)
	// Pre-initialise label combinations so the counter vecs appear in Gather
	// even before any reconcile or webhook event completes.
	for _, kind := range []string{"Project", "Repository"} {
		for _, result := range []string{"success", "error"} {
			m.reconcileTotal.WithLabelValues(kind, result)
		}
	}
	// Finding 28: pre-seed all (kind,action) pairs the webhook handler emits so
	// dashboards/alerts see a zero baseline rather than gaps before the first event.
	for _, provider := range []string{"github", "gitlab"} {
		for _, kind := range []string{"push", "issue", "mr", "other"} {
			for _, action := range []string{"other", "labeled", "opened", "closed", "synchronize", "create"} {
				for _, result := range []string{"accepted", "rejected", "ignored", "error", "task_created", "duplicate", "unknown_project", "bad_signature", "provider_mismatch", "too_large", "bad_request", "reactivated"} {
					m.webhookEvents.WithLabelValues(provider, kind, action, result)
				}
			}
		}
	}
	for _, phase := range []string{"Provisioning", "Ready", "Failed"} {
		m.memoryStacks.WithLabelValues(phase)
	}
	for _, activity := range []string{"mrScan", "issueScan", "brainstorm"} {
		for _, outcome := range []string{"scanned", "picked", "skipped_dedup", "skipped_cap"} {
			m.scanItemsTotal.WithLabelValues(activity, outcome)
		}
	}
	for _, action := range []string{"implement", "close", "discuss", "close-withheld"} {
		m.issueOutcomeTotal.WithLabelValues(action)
	}
	// Pre-seed terminal-Task GC by kind so the series exist before the first sweep.
	for _, kind := range []string{
		"implement", "review", "selfImprove", "triageIssue",
		"brainstorm", "issueLifecycle", "incident",
	} {
		m.tasksGCTotal.WithLabelValues(kind)
	}
	for _, source := range []string{"reconcile", "poll_backstop", "planning_watchdog"} {
		m.turnTimeoutTotal.WithLabelValues(source)
	}
	for _, result := range []string{"success", "failure"} {
		for _, mode := range []string{"incremental", "full"} {
			m.ingestJobTotal.WithLabelValues(result, mode)
		}
	}
	for _, result := range []string{"accepted", "missing_token", "invalid_scheme", "invalid_token", "rejected"} {
		m.authTotal.WithLabelValues(result)
	}
	for _, result := range []string{"no_change", "skip_4xx", "no_pr", "opened"} {
		m.writebackOutcomeTotal.WithLabelValues(result)
	}
	for _, result := range []string{"proposed", "no_yield"} {
		m.brainstormOutcomeTotal.WithLabelValues(result)
	}
	// Pre-seed webhook duration by provider/result so the series exist from startup.
	for _, provider := range []string{"github", "gitlab"} {
		for _, result := range []string{"ok", "error"} {
			m.webhookDuration.WithLabelValues(provider, result)
		}
	}
	// Pre-seed REST API metrics for common endpoints.
	for _, endpoint := range []string{
		"patch_task", "create_subtask", "patch_subtask", "propose_issue",
		"review_verdict", "pr_outcome", "issue_outcome", "implement_outcome",
		"post_comment", "change_summary", "handover",
	} {
		for _, result := range []string{"ok", "error"} {
			m.restapiRequestsTotal.WithLabelValues(endpoint, result)
		}
		m.restapiRequestDuration.WithLabelValues(endpoint)
	}
	// Pre-seed the memory retrieval-probe series so the route x result matrix
	// exists at a zero baseline before the first probe (alertable from startup).
	// The route labels must mirror memoryProbeRoutes in the controller package.
	for _, route := range []string{"/queries", "/code-graph/stats"} {
		for _, result := range []string{"present", "absent", "error"} {
			m.memoryRetrievalProbe.WithLabelValues(route, result)
		}
	}
	return m
}

// TurnTimeout increments operator_turn_timeout_total for the detection source
// ("reconcile" or "poll_backstop").
func (m *OperatorMetrics) TurnTimeout(source string) {
	m.turnTimeoutTotal.WithLabelValues(source).Inc()
}

// IngestJobResult increments operator_ingest_job_total for a finished Job's
// terminal result ("success" or "failure") and ingest mode ("incremental" or
// "full"). The mode lets alerting page only on terminal full-ingest failures: a
// failed incremental ingest self-heals by falling back to a full ingest, so it
// is benign on its own, whereas a failed full ingest means the corpus is
// genuinely going stale.
func (m *OperatorMetrics) IngestJobResult(result, mode string) {
	m.ingestJobTotal.WithLabelValues(result, mode).Inc()
}

// AgentUnreachableTermination increments
// operator_agent_unreachable_termination_total: a Task was terminated because
// its wrapper agent stayed unreachable past the boot deadline.
func (m *OperatorMetrics) AgentUnreachableTermination() {
	m.agentUnreachableTermTotal.Inc()
}

// ScanItem increments tatara_scan_items_total for an activity + outcome.
func (m *OperatorMetrics) ScanItem(activity, outcome string) {
	m.scanItemsTotal.WithLabelValues(activity, outcome).Inc()
}

// ScanTaskCreated increments tatara_scan_tasks_created_total for an activity + kind.
func (m *OperatorMetrics) ScanTaskCreated(activity, kind string) {
	m.scanTasksCreatedTotal.WithLabelValues(activity, kind).Inc()
}

// ObserveScanDuration records the seconds one scan activity took.
func (m *OperatorMetrics) ObserveScanDuration(activity string, seconds float64) {
	m.scanDurationSeconds.WithLabelValues(activity).Observe(seconds)
}

// IssueOutcome increments tatara_issue_outcome_total for an action.
func (m *OperatorMetrics) IssueOutcome(action string) {
	m.issueOutcomeTotal.WithLabelValues(action).Inc()
}

// IssueOutcomeTotal returns the counter for a specific action, for test assertions.
func (m *OperatorMetrics) IssueOutcomeTotal(action string) prometheus.Counter {
	return m.issueOutcomeTotal.WithLabelValues(action)
}

// AgentBootRaceRequeue increments operator_agent_boot_race_requeue_total: a
// turn submit reached a still-booting wrapper and was requeued (not errored).
func (m *OperatorMetrics) AgentBootRaceRequeue() {
	m.agentBootRaceRequeue.Inc()
}

// AgentBootCrash increments operator_agent_boot_crash_total for a wrapper Pod
// that failed to boot before /readyz came up. reason is the detection signal
// ("PodFailed", "CrashLoopBackOff", "ContainerExited", "BootTimeout"); outcome
// is "respawn" (recreation budget remained) or "failed" (budget exhausted).
func (m *OperatorMetrics) AgentBootCrash(reason, outcome string) {
	m.agentBootCrashTotal.WithLabelValues(reason, outcome).Inc()
}

// SetTasksInflightKind sets tatara_tasks_inflight for one Task kind.
func (m *OperatorMetrics) SetTasksInflightKind(kind string, n float64) {
	m.tasksInflightKind.WithLabelValues(kind).Set(n)
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
// provider, kind, action and result.
func (m *OperatorMetrics) WebhookEvent(provider, kind, action, result string) {
	m.webhookEvents.WithLabelValues(provider, kind, action, result).Inc()
}

// SetTasksInflight sets the operator_tasks_inflight gauge to n.
func (m *OperatorMetrics) SetTasksInflight(n float64) {
	m.tasksInflight.Set(n)
}

// scmReadVerbs are SCM verbs that only read; every other verb is a write. Used
// to label operator_scm_writes_total with kind so the write-failure ratio alert
// is not diluted by read traffic that shares the same credentials.
var scmReadVerbs = map[string]bool{
	"list_open_issues":     true,
	"list_open_prs":        true,
	"get_pr_state":         true,
	"get_commit_ci_status": true,
}

// SCMVerbKind returns "read" or "write" for an SCM verb.
func SCMVerbKind(verb string) string {
	if scmReadVerbs[verb] {
		return "read"
	}
	return "write"
}

// SCMWrite increments operator_scm_writes_total for the given provider, verb,
// and result ("ok", "error", or "blocked"). The kind label (read|write) is
// derived from the verb.
func (m *OperatorMetrics) SCMWrite(provider, verb, result string) {
	m.scmWritesTotal.WithLabelValues(provider, verb, SCMVerbKind(verb), result).Inc()
}

// SCMRequestErrorByStatus increments operator_scm_request_errors_by_status_total,
// recording the classified status (HTTP code or "network") behind an SCM error.
func (m *OperatorMetrics) SCMRequestErrorByStatus(provider, verb, status string) {
	m.scmReqErrorsByStatus.WithLabelValues(provider, verb, status).Inc()
}

// SCMWriteCounter returns the counter for (provider, verb, result) for test assertions.
func (m *OperatorMetrics) SCMWriteCounter(provider, verb, result string) prometheus.Counter {
	return m.scmWritesTotal.WithLabelValues(provider, verb, SCMVerbKind(verb), result)
}

// SetOpenProposals sets operator_open_proposals for a repo slug.
func (m *OperatorMetrics) SetOpenProposals(repo string, n float64) {
	m.openProposals.WithLabelValues(repo).Set(n)
}

// OrphanReaped increments operator_orphan_reaped_total for the given reason
// (e.g. "task absent", "stale task incarnation", "task phase Failed").
func (m *OperatorMetrics) OrphanReaped(reason string) {
	m.orphanReapedTotal.WithLabelValues(reason).Inc()
}

// ReapDeleteError increments operator_reap_delete_error_total for the resource
// kind that failed to delete ("pod", "service", or "task").
func (m *OperatorMetrics) ReapDeleteError(kind string) {
	m.reapDeleteErrorTotal.WithLabelValues(kind).Inc()
}

// TasksGC increments operator_tasks_gc_total for a terminal Task of the given
// kind garbage-collected past the retention window.
func (m *OperatorMetrics) TasksGC(kind string) {
	m.tasksGCTotal.WithLabelValues(kind).Inc()
}

// ConversationGC increments operator_conversation_gc_total for an S3 conversation
// object the reaper deleted (result "deleted"), failed to delete/probe for a
// genuine per-object reason ("error"), or skipped because the object store was
// store-wide unreachable ("unavailable", recorded once per skipped pass; issue
// #149) - the last is intentionally separate from "error" so a quieter,
// dedicated alert can key off it without the per-object errors.
func (m *OperatorMetrics) ConversationGC(result string) {
	m.conversationGCTotal.WithLabelValues(result).Inc()
}

// TurnSubmit increments operator_turn_submit_total for the task kind and result,
// and records the SubmitTurn call latency in seconds. result is one of:
//   - "ok": the turn dispatched.
//   - "unreachable": wrapper boot-race (pod Ready but turn server still cold);
//     self-heals on a later reconcile.
//   - "busy": wrapper returned 409 session-busy back-pressure; retried on a later
//     reconcile.
//   - "error": a genuine dispatch failure (5xx, timeout, decode) that backs off.
//
// Only "error" is a real loss; the turn-submit-failure-ratio alert keys on it so
// the self-healing transients ("unreachable", "busy") stay observable without
// tripping it.
func (m *OperatorMetrics) TurnSubmit(kind, result string, seconds float64) {
	m.turnSubmitTotal.WithLabelValues(kind, result).Inc()
	m.turnSubmitDuration.WithLabelValues(kind).Observe(seconds)
}

// AgentHTTP increments operator_agent_http_total for the HTTP method and outcome
// ("ok", "http_error", "unreachable", "timeout"), and records the call latency.
// method is the logical operation name (e.g. "submit_turn", "get_turn",
// "delete_session", "interject").
func (m *OperatorMetrics) AgentHTTP(method, outcome string, seconds float64) {
	m.agentHTTPTotal.WithLabelValues(method, outcome).Inc()
	m.agentHTTPDuration.WithLabelValues(method).Observe(seconds)
}

// RecordAuth increments operator_auth_total for the given result.
// Valid results: "accepted", "missing_token", "invalid_scheme", "invalid_token".
func (m *OperatorMetrics) RecordAuth(result string) {
	m.authTotal.WithLabelValues(result).Inc()
}

// AuthCounter returns the counter for (result) for test assertions.
func (m *OperatorMetrics) AuthCounter(result string) prometheus.Counter {
	return m.authTotal.WithLabelValues(result)
}

// WritebackOutcome increments operator_writeback_outcome_total for the given
// terminal result ("no_change", "skip_4xx", "no_pr", "opened").
func (m *OperatorMetrics) WritebackOutcome(result string) {
	m.writebackOutcomeTotal.WithLabelValues(result).Inc()
}

// WritebackOutcomeCounter returns the counter for (result) for test assertions.
func (m *OperatorMetrics) WritebackOutcomeCounter(result string) prometheus.Counter {
	return m.writebackOutcomeTotal.WithLabelValues(result)
}

// BrainstormOutcome increments operator_brainstorm_outcome_total for the given
// per-run yield result ("proposed", "no_yield").
func (m *OperatorMetrics) BrainstormOutcome(result string) {
	m.brainstormOutcomeTotal.WithLabelValues(result).Inc()
}

// BrainstormOutcomeCounter returns the counter for (result) for test assertions.
func (m *OperatorMetrics) BrainstormOutcomeCounter(result string) prometheus.Counter {
	return m.brainstormOutcomeTotal.WithLabelValues(result)
}

// ObserveWebhookDuration records the wall-clock seconds a webhook request took,
// labeled by provider and result ("ok" or "error"). Finding 14.
func (m *OperatorMetrics) ObserveWebhookDuration(provider, result string, seconds float64) {
	m.webhookDuration.WithLabelValues(provider, result).Observe(seconds)
}

// RecordRESTRequest increments operator_restapi_requests_total and records the
// handler duration in operator_restapi_request_duration_seconds. Finding 2.
// endpoint is the handler name (e.g. "patch_task"); result is "ok" or "error".
func (m *OperatorMetrics) RecordRESTRequest(endpoint, result string, seconds float64) {
	m.restapiRequestsTotal.WithLabelValues(endpoint, result).Inc()
	m.restapiRequestDuration.WithLabelValues(endpoint).Observe(seconds)
}

// RESTRequestsCounter returns the counter for (endpoint, result) for test assertions.
func (m *OperatorMetrics) RESTRequestsCounter(endpoint, result string) prometheus.Counter {
	return m.restapiRequestsTotal.WithLabelValues(endpoint, result)
}

// MemoryHealthReadError increments operator_memory_health_read_errors_total.
// Called when memoryStackHealth returns a transient non-NotFound error. Finding 13.
func (m *OperatorMetrics) MemoryHealthReadError() {
	m.memoryHealthReadErrors.Inc()
}

// QueueAdmitted increments operator_queue_admitted_total for the pool class and event kind.
func (m *OperatorMetrics) QueueAdmitted(class, kind string) {
	m.queueAdmittedTotal.WithLabelValues(class, kind).Inc()
}

// SetQueueDepth sets operator_queue_depth for a project and pool class to n (Queued-state count).
func (m *OperatorMetrics) SetQueueDepth(project, class string, n int) {
	m.queueDepth.WithLabelValues(project, class).Set(float64(n))
}

// SetQueueInflight sets operator_queue_inflight for a project and pool class to n (in-flight admitted count).
func (m *OperatorMetrics) SetQueueInflight(project, class string, n int) {
	m.queueInflight.WithLabelValues(project, class).Set(float64(n))
}

// AddTaskTokens increments operator_task_tokens_total by the input/output token
// deltas a single agent turn consumed, labelled by the Task's project, repo,
// kind, and issue. issue is "" for non-issue-scoped tasks to bound cardinality.
// Zero or negative deltas are skipped so the series only ever moves forward.
func (m *OperatorMetrics) AddTaskTokens(project, repo, kind, issue string, input, output int64) {
	if input > 0 {
		m.taskTokensTotal.WithLabelValues(project, repo, kind, issue, "input").Add(float64(input))
	}
	if output > 0 {
		m.taskTokensTotal.WithLabelValues(project, repo, kind, issue, "output").Add(float64(output))
	}
}

// SetLightragDocuments sets operator_lightrag_documents for a project and
// ingestion status (e.g. PROCESSED, PENDING, PROCESSING, FAILED) to n.
func (m *OperatorMetrics) SetLightragDocuments(project, status string, n int) {
	m.lightragDocuments.WithLabelValues(project, status).Set(float64(n))
}

// LightragQueryError increments operator_lightrag_query_errors_total: a
// best-effort read of a project's lightrag document counts failed.
func (m *OperatorMetrics) LightragQueryError() {
	m.lightragQueryErrors.Inc()
}

// MemoryRetrievalProbe increments operator_memory_retrieval_probe_total for a
// probed route and result ("present", "absent", or "error").
func (m *OperatorMetrics) MemoryRetrievalProbe(route, result string) {
	m.memoryRetrievalProbe.WithLabelValues(route, result).Inc()
}

// TaskTerminal increments operator_task_terminal_total for a Task reaching a
// terminal phase ("Succeeded" or "Failed") with the given kind and the condition
// reason recorded on the terminal transition. This is the uniform loop
// success/failure denominator: every terminal transition is metered here exactly
// once, including failure paths (PodLost, TurnTimeout, PlanningStalled, ...) that
// the per-reason fault counters do not all cover.
func (m *OperatorMetrics) TaskTerminal(kind, phase, reason string) {
	m.taskTerminalTotal.WithLabelValues(kind, phase, reason).Inc()
}

// SystemicSiblingCollapsed increments tatara_systemic_siblings_collapsed_total:
// a sibling issue in a systemic group was collapsed (no separate agent spawned).
func (m *OperatorMetrics) SystemicSiblingCollapsed(project string) {
	m.systemicSiblingsCollapsed.WithLabelValues(project).Inc()
}

// SystemicGroupLed increments tatara_systemic_groups_led_total: a lead issue in a
// systemic group was elected and its combined-PR agent was enqueued.
func (m *OperatorMetrics) SystemicGroupLed(project string) {
	m.systemicGroupsLed.WithLabelValues(project).Inc()
}
