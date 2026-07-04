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
	agentSessionBusyRequeue   prometheus.Counter
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
	writebackSkip4xxTotal     *prometheus.CounterVec
	brainstormOutcomeTotal    *prometheus.CounterVec
	webhookDuration           *prometheus.HistogramVec
	restapiRequestsTotal      *prometheus.CounterVec
	restapiRequestDuration    *prometheus.HistogramVec
	memoryHealthReadErrors    prometheus.Counter
	queueAdmittedTotal        *prometheus.CounterVec
	queueDepth                *prometheus.GaugeVec
	queueInflight             *prometheus.GaugeVec
	taskTokensTotal           *prometheus.CounterVec
	taskTurnsTotal            *prometheus.CounterVec
	taskIssueState            *prometheus.GaugeVec
	taskTerminalTotal         *prometheus.CounterVec
	taskTerminalTokensTotal   *prometheus.CounterVec
	lightragDocuments         *prometheus.GaugeVec
	lightragQueryErrors       prometheus.Counter
	memoryRetrievalProbe      *prometheus.CounterVec
	toolSurfaceProbe          *prometheus.CounterVec
	toolSurfaceProbeDuration  *prometheus.HistogramVec
	systemicSiblingsCollapsed *prometheus.CounterVec
	systemicGroupsLed         *prometheus.CounterVec
	tokenBudgetUsedRatio      *prometheus.GaugeVec
	admissionBlockedTotal     *prometheus.CounterVec
	repositoryIngestFailing   *prometheus.GaugeVec
	repositoryLastIngestTime  *prometheus.GaugeVec
	reviewOutcomeTotal        *prometheus.CounterVec
	reviewFindingsTotal       *prometheus.CounterVec
	implementCITotal          *prometheus.CounterVec
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
		agentSessionBusyRequeue: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "operator_agent_session_busy_requeue_total",
			Help: "Times a turn submit hit a wrapper 409 \"session busy\" and was requeued as transient backpressure instead of erroring (issue #168).",
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
		repositoryIngestFailing: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_repository_ingest_failing",
			Help: "1 when a Repository is currently in a failing ingest state (status Phase=Failed or IngestFailureCount>0), else 0. Current-state, recovery-aware signal that clears the moment a re-ingest succeeds, unlike the monotonic operator_ingest_job_total counter (issue #138).",
		}, []string{"repo"}),
		repositoryLastIngestTime: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_repository_last_ingest_timestamp_seconds",
			Help: "Unix timestamp (seconds) of a Repository's last successful ingest (status LastIngestTime). Compute staleness in PromQL as time() - this (issue #138).",
		}, []string{"repo"}),
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
			Help: "Total turn submissions to agent wrappers by kind, result and outcome. result is ok, error (hard failure -> reconcile backoff) or transient (a handled requeue, NOT a failure: a wrapper-not-ready boot-race or HTTP 503/425, or a 409 \"session busy\" backpressure requeue). outcome is the specific cause (ok/unreachable/http_503/http_409/http_425/http_error/timeout/error); fine-grained transport reasons live in operator_agent_http_total. Alert on result=error to exclude benign readiness races and session-busy backpressure (issues #164, #168).",
		}, []string{"kind", "result", "outcome"}),
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
			Help: "Writeback terminal outcomes by result (no_change, in_scope_no_branch, skip_4xx, no_pr, skip_4xx_capped, opened).",
		}, []string{"result"}),
		// Self-diagnosing companion to writeback_outcome_total{result="skip_4xx"}
		// (issue #166): the un-triageable 4xx-skip loop emitted a metric that could
		// not name the failure. status is the 4xx HTTP code, reason the skip
		// classification. Bounded cardinality (a handful of 4xx codes x few reasons);
		// deliberately no repo label - the repo slug lives in the writeback_skip_4xx
		// log line, and a per-repo series would be unbounded across all projects.
		writebackSkip4xxTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_writeback_skip_4xx_total",
			Help: "Writeback repo-skips on a permanent 4xx from OpenChange, by HTTP status and reason (already_exists|other). Lets the skip-4xx alert annotation name the failure mode without needing the leader pod's logs (issue #166).",
		}, []string{"status", "reason"}),
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
			Help: "Agent token usage by project, repo, Task kind, issue, model, and type (input|output|cache_read|cache_creation).",
		}, []string{"project", "repo", "kind", "issue", "model", "type"}),
		taskTurnsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_turns_total",
			Help: "Agent turns completed by project, repo, Task kind, and issue.",
		}, []string{"project", "repo", "kind", "issue"}),
		taskIssueState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tatara_issue_state",
			Help: "Current state of open issues tracked by an agent Task, by project, repo, issue, kind, state, and incident flag. Value is always 1; stale series are removed on each recompute.",
		}, []string{"project", "repo", "issue", "kind", "state", "incident"}),
		taskTerminalTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_terminal_total",
			Help: "Tasks reaching a terminal phase by kind, phase (Succeeded|Failed), and condition reason.",
		}, []string{"kind", "phase", "reason"}),
		taskTerminalTokensTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_terminal_tokens_total",
			Help: "Cumulative agent token usage of terminated Tasks by project, repo, terminal outcome (delivered|churned|abandoned), model, and type (input|output|cache_read|cache_creation). No issue label - churn is outcome-keyed, not issue-keyed.",
		}, []string{"project", "repo", "outcome", "model", "type"}),
		lightragDocuments: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_lightrag_documents",
			Help: "Documents in each per-project lightrag memory corpus by ingestion status.",
		}, []string{"project", "status"}),
		lightragQueryErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "operator_lightrag_query_errors_total",
			Help: "Failed attempts to read document counts from a project's lightrag.",
		}),
		// Authenticated functional probe of each project's tatara-memory retrieval
		// surface (the contract agents consume). result is "present" (HTTP 2xx +
		// well-formed JSON body -> healthy), "absent" (404 -> drifted/stale binary),
		// "unauthorized" (401/403 -> a valid memory-audience token was rejected:
		// auth/contract drift), "degraded" (auth ok but 5xx or a malformed/empty
		// body -> broken handler/backend), or "error" (transport failure or token
		// mint failure -> probe could not complete). All but "present" count
		// unhealthy for the cycle.
		memoryRetrievalProbe: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_memory_retrieval_probe_total",
			Help: "Authenticated functional probes of a project's tatara-memory retrieval surface by route and result.",
		}, []string{"route", "result"}),
		// Synthetic probe of the operator-write and chat tool-backend surfaces from
		// the in-cluster agent vantage (the sibling of memoryRetrievalProbe, which
		// covers tatara-memory). result is "ok" (2xx), "present" (401/403 or other
		// 4xx: route + auth gate served, no token to assert handler health),
		// "absent" (404 -> route drift / stale binary), "error" (5xx -> handler
		// broken), or "unreachable" (transport failure -> process down). vantage is
		// "in-cluster" today; the label is reserved so an external-ingress vantage
		// can be added later without a series reset.
		toolSurfaceProbe: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_tool_surface_probe_total",
			Help: "Synthetic probes of the operator-write and chat tool backends by backend, vantage, and result.",
		}, []string{"backend", "vantage", "result"}),
		toolSurfaceProbeDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "operator_tool_surface_probe_duration_seconds",
			Help: "Wall-clock duration of a single tool-surface probe by backend and vantage.",
			// Network call bounded by toolSurfaceProbeTimeout (3s); the 0.05->25.6s
			// range (matching the agent/turn HTTP histograms) captures a timeout in
			// a real bucket rather than +Inf.
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 10),
		}, []string{"backend", "vantage"}),
		systemicSiblingsCollapsed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tatara_systemic_siblings_collapsed_total",
			Help: "Systemic-group sibling issues collapsed (no separate agent spawned), by project.",
		}, []string{"project"}),
		systemicGroupsLed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tatara_systemic_groups_led_total",
			Help: "Systemic-group leads elected (lead issue gets a combined-PR agent), by project.",
		}, []string{"project"}),
		// Token-budget admission gate (issue #189). scope is "used" (the project's
		// current per-window usage ratio 0..1), "proactive" (the proactive-pause
		// threshold ratio), or "emergency" (the incident-pause threshold ratio), so
		// a dashboard can plot used against both thresholds per project. Cardinality
		// is bounded by live projects x 3 scopes; set live (not pre-seeded) like the
		// other per-project series (queue_depth).
		tokenBudgetUsedRatio: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_token_budget_used_ratio",
			Help: "Token-budget usage as a fraction of the window limit, by project and scope (used|proactive|emergency); used is current usage, proactive/emergency are the active pause thresholds.",
		}, []string{"project", "scope"}),
		// Work the admission gate held back rather than admitting. reason is
		// "token_budget" today; class is the pool (normal|alert). Bounded by live
		// projects x classes x reasons; set live (not pre-seeded).
		admissionBlockedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_admission_blocked_total",
			Help: "QueuedEvents the dispatcher declined to admit for a pool, by project, pool class (normal|alert), and reason (token_budget).",
		}, []string{"project", "class", "reason"}),
		reviewOutcomeTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_review_outcome_total",
			Help: "Review tasks by verdict (approved|changes_requested), keyed by the model that ran the review.",
		}, []string{"project", "repo", "model", "verdict"}),
		reviewFindingsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_review_findings_total",
			Help: "Sum of review findings (suggestions/comments) per review, by model.",
		}, []string{"project", "repo", "model"}),
		implementCITotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_implement_ci_total",
			Help: "Implement-task PR CI conclusions (pass|fail), by model.",
		}, []string{"project", "repo", "model", "result"}),
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
		m.agentSessionBusyRequeue,
		m.openProposals,
		m.turnTimeoutTotal,
		m.ingestJobTotal,
		m.repositoryIngestFailing,
		m.repositoryLastIngestTime,
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
		m.writebackSkip4xxTotal,
		m.brainstormOutcomeTotal,
		m.webhookDuration,
		m.restapiRequestsTotal,
		m.restapiRequestDuration,
		m.memoryHealthReadErrors,
		m.queueAdmittedTotal,
		m.queueDepth,
		m.queueInflight,
		m.taskTokensTotal,
		m.taskTurnsTotal,
		m.taskIssueState,
		m.taskTerminalTotal,
		m.taskTerminalTokensTotal,
		m.lightragDocuments,
		m.lightragQueryErrors,
		m.memoryRetrievalProbe,
		m.toolSurfaceProbe,
		m.toolSurfaceProbeDuration,
		m.systemicSiblingsCollapsed,
		m.systemicGroupsLed,
		m.tokenBudgetUsedRatio,
		m.admissionBlockedTotal,
		m.reviewOutcomeTotal,
		m.reviewFindingsTotal,
		m.implementCITotal,
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
	for _, result := range []string{"no_change", "in_scope_no_branch", "skip_4xx", "no_pr", "skip_4xx_capped", "opened"} {
		m.writebackOutcomeTotal.WithLabelValues(result)
	}
	// Pre-seed the realistic skip-4xx (status, reason) combos so the diagnosing
	// series exist at a zero baseline from startup (issue #166).
	for _, sr := range [][2]string{{"403", "other"}, {"404", "other"}, {"422", "already_exists"}, {"422", "other"}} {
		m.writebackSkip4xxTotal.WithLabelValues(sr[0], sr[1])
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
	// The route labels must mirror memoryProbeRoutes and the result labels the
	// classifier in probeMemoryRoute, both in the controller package.
	for _, route := range []string{"/queries", "/code-graph/stats"} {
		for _, result := range []string{"present", "absent", "error", "unauthorized", "degraded"} {
			m.memoryRetrievalProbe.WithLabelValues(route, result)
		}
	}
	// Pre-seed the tool-surface probe series so the backend x result matrix
	// exists at a zero baseline before the first probe (alertable from startup).
	// The backend labels must mirror the probes in updateToolSurfaceProbe and the
	// result labels the classifier in probeToolSurfaceRoute.
	for _, backend := range []string{"operator", "chat"} {
		for _, result := range []string{"ok", "present", "absent", "error", "unreachable"} {
			m.toolSurfaceProbe.WithLabelValues(backend, "in-cluster", result)
		}
		m.toolSurfaceProbeDuration.WithLabelValues(backend, "in-cluster")
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

// SetRepositoryIngestFailing sets operator_repository_ingest_failing for a repo
// to 1 when its ingest is currently failing, else 0. Unlike the monotonic
// operator_ingest_job_total counter, this reflects the CURRENT ingest health and
// clears as soon as a re-ingest succeeds, so alerting on it does not keep firing
// for an hour after a self-healed transient burst (issue #138).
func (m *OperatorMetrics) SetRepositoryIngestFailing(repo string, failing bool) {
	v := 0.0
	if failing {
		v = 1.0
	}
	m.repositoryIngestFailing.WithLabelValues(repo).Set(v)
}

// SetRepositoryLastIngestTimestamp sets operator_repository_last_ingest_timestamp_seconds
// for a repo to the Unix seconds of its last successful ingest.
func (m *OperatorMetrics) SetRepositoryLastIngestTimestamp(repo string, unixSeconds float64) {
	m.repositoryLastIngestTime.WithLabelValues(repo).Set(unixSeconds)
}

// RepositoryIngestFailingGauge returns the gauge for a repo, for test assertions.
func (m *OperatorMetrics) RepositoryIngestFailingGauge(repo string) prometheus.Gauge {
	return m.repositoryIngestFailing.WithLabelValues(repo)
}

// RepositoryLastIngestTimestampGauge returns the gauge for a repo, for test assertions.
func (m *OperatorMetrics) RepositoryLastIngestTimestampGauge(repo string) prometheus.Gauge {
	return m.repositoryLastIngestTime.WithLabelValues(repo)
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

// AgentSessionBusyRequeue increments operator_agent_session_busy_requeue_total: a
// turn submit hit a wrapper 409 "session busy" and was requeued as transient
// backpressure instead of erroring (issue #168).
func (m *OperatorMetrics) AgentSessionBusyRequeue() {
	m.agentSessionBusyRequeue.Inc()
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

// TurnSubmit increments operator_turn_submit_total for the task kind, result
// ("ok", "error", or "transient" for a handled wrapper-not-ready requeue) and
// outcome (the specific cause, e.g. ok/unreachable/http_503/http_error), and
// records the SubmitTurn call latency in seconds.
func (m *OperatorMetrics) TurnSubmit(kind, result, outcome string, seconds float64) {
	m.turnSubmitTotal.WithLabelValues(kind, result, outcome).Inc()
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
// terminal result ("no_change", "no_branch", "in_scope_no_branch", "skip_4xx",
// "no_pr", "skip_4xx_capped", "opened").
func (m *OperatorMetrics) WritebackOutcome(result string) {
	m.writebackOutcomeTotal.WithLabelValues(result).Inc()
}

// WritebackOutcomeCounter returns the counter for (result) for test assertions.
func (m *OperatorMetrics) WritebackOutcomeCounter(result string) prometheus.Counter {
	return m.writebackOutcomeTotal.WithLabelValues(result)
}

// WritebackSkip4xx increments operator_writeback_skip_4xx_total for a repo-skip
// on a permanent 4xx. status is the HTTP status code (e.g. "404"); reason is the
// skip classification ("already_exists" or "other"). Issue #166.
func (m *OperatorMetrics) WritebackSkip4xx(status, reason string) {
	m.writebackSkip4xxTotal.WithLabelValues(status, reason).Inc()
}

// WritebackSkip4xxCounter returns the counter for (status, reason) for test assertions.
func (m *OperatorMetrics) WritebackSkip4xxCounter(status, reason string) prometheus.Counter {
	return m.writebackSkip4xxTotal.WithLabelValues(status, reason)
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

// AddTaskTokens increments operator_task_tokens_total by the per-class token
// deltas a single agent turn consumed, labelled by the Task's project, repo,
// kind, issue, and the model that ran. issue is "" for non-issue-scoped tasks
// to bound cardinality; model is "" when unstamped (fail-open). Zero or
// negative deltas are skipped so each series only ever moves forward.
func (m *OperatorMetrics) AddTaskTokens(project, repo, kind, issue, model string, input, output, cacheRead, cacheCreation int64) {
	if input > 0 {
		m.taskTokensTotal.WithLabelValues(project, repo, kind, issue, model, "input").Add(float64(input))
	}
	if output > 0 {
		m.taskTokensTotal.WithLabelValues(project, repo, kind, issue, model, "output").Add(float64(output))
	}
	if cacheRead > 0 {
		m.taskTokensTotal.WithLabelValues(project, repo, kind, issue, model, "cache_read").Add(float64(cacheRead))
	}
	if cacheCreation > 0 {
		m.taskTokensTotal.WithLabelValues(project, repo, kind, issue, model, "cache_creation").Add(float64(cacheCreation))
	}
}

// AddTerminalTokens increments operator_task_terminal_tokens_total by a
// terminated Task's cumulative per-class token totals, labelled by project,
// repo, the task's classified terminal outcome (delivered|churned|abandoned),
// and the model that ran. No issue label: churn is outcome-keyed, not
// issue-keyed, to bound cardinality. Zero or negative deltas are skipped.
func (m *OperatorMetrics) AddTerminalTokens(project, repo, outcome, model string, input, output, cacheRead, cacheCreation int64) {
	if input > 0 {
		m.taskTerminalTokensTotal.WithLabelValues(project, repo, outcome, model, "input").Add(float64(input))
	}
	if output > 0 {
		m.taskTerminalTokensTotal.WithLabelValues(project, repo, outcome, model, "output").Add(float64(output))
	}
	if cacheRead > 0 {
		m.taskTerminalTokensTotal.WithLabelValues(project, repo, outcome, model, "cache_read").Add(float64(cacheRead))
	}
	if cacheCreation > 0 {
		m.taskTerminalTokensTotal.WithLabelValues(project, repo, outcome, model, "cache_creation").Add(float64(cacheCreation))
	}
}

// TaskTerminalTokensCounter returns the counter for (project,repo,outcome,model,type) for test assertions.
func (m *OperatorMetrics) TaskTerminalTokensCounter(project, repo, outcome, model, typ string) prometheus.Counter {
	return m.taskTerminalTokensTotal.WithLabelValues(project, repo, outcome, model, typ)
}

// TaskTokensCounter returns the counter for (project,repo,kind,issue,model,type) for test assertions.
func (m *OperatorMetrics) TaskTokensCounter(project, repo, kind, issue, model, typ string) prometheus.Counter {
	return m.taskTokensTotal.WithLabelValues(project, repo, kind, issue, model, typ)
}

// TaskTurnsCounter returns the counter for (project,repo,kind,issue) for test assertions.
func (m *OperatorMetrics) TaskTurnsCounter(project, repo, kind, issue string) prometheus.Counter {
	return m.taskTurnsTotal.WithLabelValues(project, repo, kind, issue)
}

// AddTaskTurn increments operator_task_turns_total by 1 for a completed agent
// turn. Called at the same site as AddTaskTokens (once per turn-complete
// callback), guarded by the same stale/duplicate-callback recorded flag.
func (m *OperatorMetrics) AddTaskTurn(project, repo, kind, issue string) {
	m.taskTurnsTotal.WithLabelValues(project, repo, kind, issue).Inc()
}

// SetIssueState sets tatara_issue_state{...}=1 for a live issue. Labels:
// project, repo, issue, kind (joins token/turn counters), state, incident.
func (m *OperatorMetrics) SetIssueState(project, repo, issue, kind, state, incident string) {
	m.taskIssueState.WithLabelValues(project, repo, issue, kind, state, incident).Set(1)
}

// ResetIssueState clears all tatara_issue_state series. Called at the start of
// each updateIssueStateCounts pass so stale (closed/terminal) issues vanish.
func (m *OperatorMetrics) ResetIssueState() {
	m.taskIssueState.Reset()
}

// DeleteTaskSeries removes the operator_task_tokens_total and
// operator_task_turns_total series for a specific issue-scoped Task when it is
// garbage-collected. Bounds counter cardinality to live + recently-live issues.
// Skip when issue=="" (project-scoped tasks share that label value and must not
// be cleared on any individual task's GC).
func (m *OperatorMetrics) DeleteTaskSeries(project, repo, kind, issue, model string) {
	m.taskTokensTotal.DeleteLabelValues(project, repo, kind, issue, model, "input")
	m.taskTokensTotal.DeleteLabelValues(project, repo, kind, issue, model, "output")
	m.taskTokensTotal.DeleteLabelValues(project, repo, kind, issue, model, "cache_read")
	m.taskTokensTotal.DeleteLabelValues(project, repo, kind, issue, model, "cache_creation")
	m.taskTurnsTotal.DeleteLabelValues(project, repo, kind, issue)
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
// probed route and result ("present", "absent", "error", "unauthorized", or
// "degraded").
func (m *OperatorMetrics) MemoryRetrievalProbe(route, result string) {
	m.memoryRetrievalProbe.WithLabelValues(route, result).Inc()
}

// ToolSurfaceProbe increments operator_tool_surface_probe_total for one probe of
// (backend, vantage) with the classified result, and records the probe latency
// in operator_tool_surface_probe_duration_seconds. result is one of "ok",
// "present", "absent", "error", "unreachable".
func (m *OperatorMetrics) ToolSurfaceProbe(backend, vantage, result string, seconds float64) {
	m.toolSurfaceProbe.WithLabelValues(backend, vantage, result).Inc()
	m.toolSurfaceProbeDuration.WithLabelValues(backend, vantage).Observe(seconds)
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

// SetTokenBudgetUsedRatio sets operator_token_budget_used_ratio for a project and
// scope ("used", "proactive", or "emergency") to ratio (a fraction of the window
// limit, 0..1+).
func (m *OperatorMetrics) SetTokenBudgetUsedRatio(project, scope string, ratio float64) {
	m.tokenBudgetUsedRatio.WithLabelValues(project, scope).Set(ratio)
}

// AdmissionBlocked increments operator_admission_blocked_total: the dispatcher
// declined to admit a pool's work for the given project, pool class
// ("normal"|"alert"), and reason ("token_budget").
func (m *OperatorMetrics) AdmissionBlocked(project, class, reason string) {
	m.admissionBlockedTotal.WithLabelValues(project, class, reason).Inc()
}

// AdmissionBlockedCounter returns the counter for (project, class, reason) for
// test assertions.
func (m *OperatorMetrics) AdmissionBlockedCounter(project, class, reason string) prometheus.Counter {
	return m.admissionBlockedTotal.WithLabelValues(project, class, reason)
}

// RecordReviewOutcome increments operator_review_outcome_total for a review
// Task's verdict ("approved" or "changes_requested"), keyed by the model that
// ran the review (G4 quality-proxy signal).
func (m *OperatorMetrics) RecordReviewOutcome(project, repo, model, verdict string) {
	m.reviewOutcomeTotal.WithLabelValues(project, repo, model, verdict).Inc()
}

// AddReviewFindings adds n to operator_review_findings_total for the model
// that ran a review. Skipped when n <= 0 so the series only ever moves forward.
func (m *OperatorMetrics) AddReviewFindings(project, repo, model string, n int) {
	if n > 0 {
		m.reviewFindingsTotal.WithLabelValues(project, repo, model).Add(float64(n))
	}
}

// ReviewOutcomeCounter returns the counter for (project, repo, model, verdict)
// for test assertions.
func (m *OperatorMetrics) ReviewOutcomeCounter(project, repo, model, verdict string) prometheus.Counter {
	return m.reviewOutcomeTotal.WithLabelValues(project, repo, model, verdict)
}

// ReviewFindingsCounter returns the counter for (project, repo, model) for
// test assertions.
func (m *OperatorMetrics) ReviewFindingsCounter(project, repo, model string) prometheus.Counter {
	return m.reviewFindingsTotal.WithLabelValues(project, repo, model)
}

// RecordImplementCI increments operator_implement_ci_total for an
// implement-task PR CI conclusion ("pass" or "fail"), keyed by the model that
// ran the implement Task (G4 quality-proxy signal).
func (m *OperatorMetrics) RecordImplementCI(project, repo, model, result string) {
	m.implementCITotal.WithLabelValues(project, repo, model, result).Inc()
}
