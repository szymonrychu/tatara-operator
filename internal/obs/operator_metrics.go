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
	scmWritesTotal          *prometheus.CounterVec
	approvalGateSeconds     prometheus.Histogram
	scanItemsTotal          *prometheus.CounterVec
	scanTasksCreatedTotal   *prometheus.CounterVec
	scanDurationSeconds     *prometheus.HistogramVec
	issueOutcomeTotal       *prometheus.CounterVec
	tasksInflightKind       *prometheus.GaugeVec
	agentBootRaceRequeue    prometheus.Counter
	openProposals           *prometheus.GaugeVec
	approvalBackstopFlips   prometheus.Counter
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
		approvalGateSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "operator_approval_gate_seconds",
			Help:    "Wall-clock seconds a Task spent in AwaitingApproval.",
			Buckets: prometheus.ExponentialBuckets(60, 2, 10),
		}),
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
		approvalBackstopFlips: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "operator_approval_backstop_flips_total",
			Help: "Approvals recovered by the backstop after a missed webhook.",
		}),
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
		m.approvalGateSeconds,
		m.scanItemsTotal,
		m.scanTasksCreatedTotal,
		m.scanDurationSeconds,
		m.issueOutcomeTotal,
		m.tasksInflightKind,
		m.agentBootRaceRequeue,
		m.openProposals,
		m.approvalBackstopFlips,
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
	return m
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

// AgentBootRaceRequeue increments operator_agent_boot_race_requeue_total: a
// turn submit reached a still-booting wrapper and was requeued (not errored).
func (m *OperatorMetrics) AgentBootRaceRequeue() {
	m.agentBootRaceRequeue.Inc()
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

// ObserveApprovalGate records the seconds a Task spent in AwaitingApproval.
func (m *OperatorMetrics) ObserveApprovalGate(seconds float64) {
	m.approvalGateSeconds.Observe(seconds)
}

// SetOpenProposals sets operator_open_proposals for a repo slug.
func (m *OperatorMetrics) SetOpenProposals(repo string, n float64) {
	m.openProposals.WithLabelValues(repo).Set(n)
}

// ApprovalBackstopFlip increments operator_approval_backstop_flips_total.
func (m *OperatorMetrics) ApprovalBackstopFlip() { m.approvalBackstopFlips.Inc() }
