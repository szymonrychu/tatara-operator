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
	turnSubmitTotal           *prometheus.CounterVec
	turnSubmitDuration        *prometheus.HistogramVec
	agentHTTPTotal            *prometheus.CounterVec
	agentHTTPDuration         *prometheus.HistogramVec
	authTotal                 *prometheus.CounterVec
	writebackOutcomeTotal     *prometheus.CounterVec
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
			Help: "Total SCM write operations by provider, verb, and result.",
		}, []string{"provider", "verb", "result"}),
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
			Help: "Agent turns that exceeded their deadline and were terminated, by detection source.",
		}, []string{"source"}),
		ingestJobTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_ingest_job_total",
			Help: "Finished ingest Jobs by terminal result.",
		}, []string{"result"}),
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
		m.turnSubmitTotal,
		m.turnSubmitDuration,
		m.agentHTTPTotal,
		m.agentHTTPDuration,
		m.authTotal,
		m.writebackOutcomeTotal,
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
			for _, action := range []string{"other"} {
				for _, result := range []string{"accepted", "rejected"} {
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
	for _, action := range []string{"implement", "close"} {
		m.issueOutcomeTotal.WithLabelValues(action)
	}
	for _, source := range []string{"reconcile", "poll_backstop", "planning_watchdog"} {
		m.turnTimeoutTotal.WithLabelValues(source)
	}
	for _, result := range []string{"success", "failure"} {
		m.ingestJobTotal.WithLabelValues(result)
	}
	for _, result := range []string{"accepted", "missing_token", "invalid_scheme", "invalid_token"} {
		m.authTotal.WithLabelValues(result)
	}
	for _, result := range []string{"no_change", "skip_4xx", "no_pr", "opened"} {
		m.writebackOutcomeTotal.WithLabelValues(result)
	}
	return m
}

// TurnTimeout increments operator_turn_timeout_total for the detection source
// ("reconcile" or "poll_backstop").
func (m *OperatorMetrics) TurnTimeout(source string) {
	m.turnTimeoutTotal.WithLabelValues(source).Inc()
}

// IngestJobResult increments operator_ingest_job_total for a finished Job's
// terminal result ("success" or "failure").
func (m *OperatorMetrics) IngestJobResult(result string) {
	m.ingestJobTotal.WithLabelValues(result).Inc()
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

// SCMWrite increments operator_scm_writes_total for the given provider, verb,
// and result ("ok" or "error").
func (m *OperatorMetrics) SCMWrite(provider, verb, result string) {
	m.scmWritesTotal.WithLabelValues(provider, verb, result).Inc()
}

// SCMWriteCounter returns the counter for (provider, verb, result) for test assertions.
func (m *OperatorMetrics) SCMWriteCounter(provider, verb, result string) prometheus.Counter {
	return m.scmWritesTotal.WithLabelValues(provider, verb, result)
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
// kind that failed to delete ("pod" or "service").
func (m *OperatorMetrics) ReapDeleteError(kind string) {
	m.reapDeleteErrorTotal.WithLabelValues(kind).Inc()
}

// TurnSubmit increments operator_turn_submit_total for the task kind and result
// ("ok" or "error"), and records the SubmitTurn call latency in seconds.
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
