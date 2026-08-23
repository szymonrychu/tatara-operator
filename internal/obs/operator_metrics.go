package obs

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// seedLabels walks the cartesian product of dims and calls set with each
// combination, in the same nested-loop order the pre-seed blocks used before
// this helper existed (leftmost dim outermost).
func seedLabels(set func(labels ...string), dims ...[]string) {
	seedLabelsRec(set, make([]string, 0, len(dims)), dims)
}

func seedLabelsRec(set func(labels ...string), prefix []string, dims [][]string) {
	if len(dims) == 0 {
		set(prefix...)
		return
	}
	for _, v := range dims[0] {
		next := make([]string, len(prefix), len(prefix)+1)
		copy(next, prefix)
		seedLabelsRec(set, append(next, v), dims[1:])
	}
}

// OperatorMetrics holds the reconciler-facing Prometheus collectors for the
// tatara-operator. Construct one with NewOperatorMetrics and pass it to the
// reconcilers. SCM, queue, Task-lifecycle, and account-usage collectors live
// in their own domain structs (scm_metrics.go, queue_metrics.go,
// task_metrics.go, accountusage_metrics.go), embedded here so their fields
// and methods are still reached as m.field / m.Method.
type OperatorMetrics struct {
	*scmMetrics
	*queueMetrics
	*taskMetrics
	*accountUsageMetrics
	reconcileTotal                *prometheus.CounterVec
	ingestJobDuration             prometheus.Histogram
	turnDuration                  prometheus.Histogram
	webhookEvents                 *prometheus.CounterVec
	tasksInflight                 prometheus.Gauge
	memoryProvisionDuration       prometheus.Histogram
	memoryStacks                  *prometheus.GaugeVec
	autoApproveTotal              *prometheus.CounterVec
	autoApproveRefusedTotal       *prometheus.CounterVec
	approvalRefusedTotal          *prometheus.CounterVec
	tasksInflightKind             *prometheus.GaugeVec
	agentBootRaceRequeue          prometheus.Counter
	agentSessionBusyRequeue       prometheus.Counter
	openProposals                 *prometheus.GaugeVec
	brainstormTarget              *prometheus.GaugeVec
	brainstormPending             *prometheus.GaugeVec
	brainstormPaused              *prometheus.GaugeVec
	brainstormRefillTotal         *prometheus.CounterVec
	brainstormQuotaTruncatedTotal *prometheus.CounterVec
	turnTimeoutTotal              *prometheus.CounterVec
	ingestJobTotal                *prometheus.CounterVec
	agentUnreachableTermTotal     prometheus.Counter
	agentBootCrashTotal           *prometheus.CounterVec
	orphanReapedTotal             *prometheus.CounterVec
	reapDeleteErrorTotal          *prometheus.CounterVec
	conversationGCTotal           *prometheus.CounterVec
	turnSubmitTotal               *prometheus.CounterVec
	turnSubmitDuration            *prometheus.HistogramVec
	agentHTTPTotal                *prometheus.CounterVec
	agentHTTPDuration             *prometheus.HistogramVec
	authTotal                     *prometheus.CounterVec
	webhookDuration               *prometheus.HistogramVec
	restapiRequestsTotal          *prometheus.CounterVec
	restapiRequestDuration        *prometheus.HistogramVec
	restapiResponseBytes          *prometheus.HistogramVec
	memoryHealthReadErrors        prometheus.Counter
	memoryApplyTransientErrors    *prometheus.CounterVec
	memoryStorageShrinkGuard      *prometheus.CounterVec
	lightragDocuments             *prometheus.GaugeVec
	lightragQueryErrors           prometheus.Counter
	memoryRetrievalProbe          *prometheus.CounterVec
	toolSurfaceProbe              *prometheus.CounterVec
	toolSurfaceProbeDuration      *prometheus.HistogramVec
	tokenBudgetUsedRatio          *prometheus.GaugeVec
	admissionBlockedTotal         *prometheus.CounterVec
	agentPodDegradedTotal         *prometheus.CounterVec
	mrBindingBackstopTotal        *prometheus.CounterVec
	repositoryIngestFailing       *prometheus.GaugeVec
	repositoryIngestGated         *prometheus.GaugeVec
	repositoryLastIngestTime      *prometheus.GaugeVec
	repositoryPhaseRepairedTotal  *prometheus.CounterVec
	ingestJobDedupTotal           *prometheus.CounterVec
	// repoProjectMu guards repoProject, the last project label published for
	// each repo. A Repository's projectRef is mutable, so without it a repo
	// moved between projects would keep reporting under both.
	repoProjectMu                sync.Mutex
	repoProject                  map[string]string
	reviewOutcomeTotal           *prometheus.CounterVec
	reviewHeadMovedTotal         *prometheus.CounterVec
	reviewFindingsTotal          *prometheus.CounterVec
	implementCITotal             *prometheus.CounterVec
	cdCascadeFailed              *prometheus.GaugeVec
	cdCascadeStalled             *prometheus.GaugeVec
	cdResolvedTotal              prometheus.Counter
	incidentDedupSuppressedTotal *prometheus.CounterVec
	incidentRefireCommentTotal   *prometheus.CounterVec
	incidentSublinkTotal         *prometheus.CounterVec
	incidentGroupLinkedTotal     *prometheus.CounterVec
	incidentEscalatedTotal       *prometheus.CounterVec
	incidentTrackerCommentTotal  *prometheus.CounterVec
}

// NewOperatorMetrics registers the operator collectors on reg and returns the
// bundle. Names and labels are pinned by the shared-contracts pin set.
func NewOperatorMetrics(reg prometheus.Registerer) *OperatorMetrics {
	m := &OperatorMetrics{
		repoProject:         map[string]string{},
		scmMetrics:          newSCMMetrics(reg),
		queueMetrics:        newQueueMetrics(reg),
		taskMetrics:         newTaskMetrics(reg),
		accountUsageMetrics: newAccountUsageMetrics(reg),
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
		// Per (project,phase): 1 for the project's current phase, 0 for the other
		// three. The project label (issue #425) is what makes a Provisioning or
		// Failed stack name the project it belongs to; the old cluster-wide count
		// left tatara, infrastructure and mtg indistinguishable. Summing over
		// project recovers the old count, so `max(operator_memory_stacks{...})`
		// and `sum by (phase) (...)` both keep working.
		memoryStacks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_memory_stacks",
			Help: "Per-project memory stack phase: 1 for the project's current phase, 0 for the others.",
		}, []string{"project", "phase"}),
		autoApproveTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_auto_approve_total",
			Help: "Auto-approve releases (item 4a) by proposal kind (brainstorm|incident|refine).",
		}, []string{"kind"}),
		// The CAUSE behind operator_approval_refused_total{reason="no-maintainer-comment"},
		// which is the terminal symptom of five different fail-closed gates and
		// names none of them. Without this series a live refusal cannot be
		// attributed at all: the Issue CR the verdict was about is routinely
		// reaped before anyone looks, and the five causes (flag off, a human's
		// close, a botLogin problem, a filer that declared no proposalKind, an
		// Issue CR minted by the mirror instead of by the proposal filer so it
		// carries no integrity anchor) have five different remedies. It fires
		// ONLY on that one reason, so it is strictly a decomposition of it and
		// never exceeds it.
		autoApproveRefusedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_auto_approve_refused_total",
			Help: "Auto-approve carve-out refusals by the AXIS that fail-closed (flag-off|issue-not-in-scope|not-bot-authored|no-proposal-marker|anchor-mismatch). Decomposes operator_approval_refused_total{reason=\"no-maintainer-comment\"}, which names the symptom and not the cause.",
		}, []string{"axis"}),
		approvalRefusedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_approval_refused_total",
			Help: "Approval verifications the operator refused, by structural reason.",
		}, []string{"reason"}),
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
		// Set inside brainstorm() (projectscan.go) from
		// pendingProposalCountByRepo, the UNCOLLAPSED per-repo pending count
		// over Issue CRs: the project-wide count collapses systemic groups,
		// which span repos, so splitting THAT per repo is undefined. That map
		// is keyed by Repository CR name and re-keyed to the owner/name slug at
		// the call site, because the label has always been the slug. Every
		// enrolled repo is written every pass, zeros included, so a repo whose
		// proposals were all approved falls to 0 instead of latching. It is
		// written on every brainstorm refill decision (cron tick or Issue
		// event), not on the 60s maybeRecomputeGauges pass most other
		// per-project gauges use. Confirmed correctly wired (real nonzero
		// backlog counts per repo) across prior pod generations via 7-day
		// Prometheus history during the metric-wiring audit (issue #370);
		// a flat absence right after a pod restart just means no project has
		// had a brainstorm decision yet, not a bug.
		openProposals: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_open_proposals",
			Help: "Open, unapproved agent-proposed issues per repo, counted from Issue CRs. Uncollapsed: a systemic group spans repos and counts once per repo here, once in total in operator_brainstorm_pending_proposals.",
		}, []string{"repo"}),
		brainstormTarget: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_brainstorm_target_proposals",
			Help: "Configured brainstorm backlog TARGET per project (spec.scm.cron.brainstorm.targetOpenProposals, resolved). Pair with operator_brainstorm_pending_proposals: a backlog that never reaches the target is the signal that refill is wedged.",
		}, []string{"project"}),
		brainstormPending: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_brainstorm_pending_proposals",
			Help: "Brainstorm proposals open and awaiting a maintainer decision per project, counted from Issue CRs with systemic groups collapsed to one slot. NOT the same as operator_open_proposals{repo}, which is the uncollapsed per-repo split.",
		}, []string{"project"}),
		brainstormPaused: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_brainstorm_paused",
			Help: "1 while a project's brainstorm is paused on an action=exhausted verdict (Status.BrainstormPausedAt set), 0 otherwise. The only metric-level signal for a project paused and never resuming - pair with operator_brainstorm_pending_proposals staying flat.",
		}, []string{"project"}),
		brainstormRefillTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_brainstorm_refill_total",
			Help: "Brainstorm refills dispatched per project. The trigger label is gone: the cron tick and the event wake now run the same level-triggered decision.",
		}, []string{"project"}),
		brainstormQuotaTruncatedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_brainstorm_quota_truncated_total",
			Help: "Proposals DROPPED by operator-side quota truncation. Nonzero means agents are overshooting the stated quota; operator-side truncation is the authority, so the target still holds.",
		}, []string{"project"}),
		turnTimeoutTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_turn_timeout_total",
			Help: "Agent turns terminated for stalling (no activity past the inactivity deadline), by detection source.",
		}, []string{"source"}),
		ingestJobTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_ingest_job_total",
			Help: "Finished ingest Jobs by terminal result and ingest mode (incremental|full).",
		}, []string{"result", "mode"}),
		// The project label (issue #457) is what lets an alert page the project
		// that owns the repo. The rule selected the operator's NAMESPACE, so a
		// firing mtg-decks (projectRef mtg) was routed to the tatara project's
		// incident agent, whose repo list does not contain it. It is a pure label
		// addition, so `max by (repo) (...)` and `sum(...)` expressions keep
		// working unchanged.
		repositoryIngestFailing: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_repository_ingest_failing",
			Help: "1 when a Repository is currently in a failing ingest state (status Phase=Failed or IngestFailureCount>0) AND is not held by the project memory-readiness gate, else 0. Current-state, recovery-aware signal that clears the moment a re-ingest succeeds, unlike the monotonic operator_ingest_job_total counter (issue #138). Reads 0 while the repo is gated AND its project memory stack has been continuously non-Ready for 18m (ingestGateMaskDelay): once qualified, a gated repo launches no new ingest Job, so nothing could reset its failure state and this gauge would pin forever (issue #525). A SHORT gate does NOT mask it - a chattering memory stack would otherwise punch holes in this gauge and stop the 1h alert on it from ever firing. Exponential back-off between retries does NOT mask it - only the qualified gate does. Labelled by owning project (issue #457).",
		}, []string{"project", "repo"}),
		// Issue #434: a Repository whose re-ingest is held by the project
		// memory-readiness gate is neither Failed nor accumulating failures, so
		// repositoryIngestFailing reads 0 for it indefinitely. Without this gauge
		// a fleet-wide ingest freeze is invisible until the 24h staleness backstop
		// fires.
		//
		// Issue #525 corrected the "reads 0 for it indefinitely" premise: that
		// held only for a repo that was HEALTHY when the gate closed. One that
		// was already failing kept repositoryIngestFailing at 1 with no way out,
		// because clearing it needs a successful ingest and the gate forbids one.
		// The publisher now masks it after a qualification delay (18m of
		// continuous non-Ready project memory stack), so the two are mutually
		// exclusive for a SUSTAINED gate, but deliberately NOT for a chattering
		// one.
		repositoryIngestGated: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_repository_ingest_gated",
			Help: "1 while a Repository's NEXT re-ingest is held by the project memory-readiness gate, else 0; a repo with an ingest Job already in flight reads 0, because the gate is not what is holding it. Mutually exclusive with operator_repository_ingest_failing only once this has been 1 for a sustained outage (18m of a non-Ready project memory stack, ingestGateMaskDelay) - during a shorter gate both can read 1. A gated repo runs no new ingest Job, so once the gate is sustained it is waiting, not stuck failing (issues #434, #525). Labelled by owning project (issue #457).",
		}, []string{"project", "repo"}),
		repositoryLastIngestTime: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_repository_last_ingest_timestamp_seconds",
			Help: "Unix timestamp (seconds) of a Repository's last successful ingest (status LastIngestTime). Compute staleness in PromQL as time() - this (issue #138). Labelled by owning project (issue #457).",
		}, []string{"project", "repo"}),
		// Issue #457: (Phase="Failed", IngestFailureCount=0) is a state nothing
		// repaired and nothing re-ingested - one repo sat in it for 20.8h with zero
		// actual ingest failures. The reconciler now repairs it on sight; the
		// origin of the desync could not be proven from logs, so this counter is
		// how we learn whether anything is still producing it.
		repositoryPhaseRepairedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_repository_phase_repaired_total",
			Help: "Repository status repairs of the desynchronised (Phase=Failed, IngestFailureCount=0) state, by project and repo (issue #457).",
		}, []string{"project", "repo"}),
		// Issue #457: two ingest Jobs were created for one Repository 16ms and
		// 21ms apart from the same pod. Non-zero here means the read-then-create
		// race is still being run into and the deterministic Job name is what is
		// holding it.
		ingestJobDedupTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_ingest_job_deduplicated_total",
			Help: "Ingest Job creations collapsed into adopting an existing Job of the same deterministic name, by project and repo (issue #457).",
		}, []string{"project", "repo"}),
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
		// Self-diagnosing companion to writeback_outcome_total{result="skip_4xx"}
		// (issue #166): the un-triageable 4xx-skip loop emitted a metric that could
		// not name the failure. status is the 4xx HTTP code, reason the skip
		// classification. Bounded cardinality (a handful of 4xx codes x few reasons);
		// deliberately no repo label - the repo slug lives in the writeback_skip_4xx
		// log line, and a per-repo series would be unbounded across all projects.
		// Per-run brainstorm yield. The brainstorm Task itself never opens a PR, so
		// a dedicated counter (rather than overloading writeback_outcome) keeps the
		// "a PR/MR write was attempted" semantics of writeback_outcome clean.
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
		// #641: task_list served 725 KB for 57 Tasks, the MCP client discarded
		// it, and no series anywhere recorded that it happened. The bundle path
		// has had operator_bundle_bytes since it got its budget; the JSON path
		// delivering the same Task free text to the same agents had nothing.
		// Bucketed like operator_bundle_bytes so the two channels are readable
		// against the same 400000 line. route is chi's route TEMPLATE, so the
		// cardinality is the 16 entries in restapi routes().
		restapiResponseBytes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "operator_restapi_response_bytes",
			Help:    "REST API response size in bytes, by route template. The platform's budget for the same Task free text on the bundle channel is Project.spec.maxBundleBytes (default 400000).",
			Buckets: []float64{1_000, 4_000, 16_000, 64_000, 128_000, 200_000, 400_000, 800_000},
		}, []string{"route"}),
		// Finding 13: counter for transient memory-health read errors so repeated
		// blips surfacing as healthy reconciles are visible in Prometheus.
		memoryHealthReadErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "operator_memory_health_read_errors_total",
			Help: "Total transient errors reading memory-stack health (not real stack failures).",
		}),
		// Issue #439: counts memory-stack applies that failed on a RETRYABLE
		// dependency (an admission webhook that could not be reached, an
		// apiserver 429/503/timeout) and were therefore soft-requeued instead of
		// flipping the stack to Failed. This is the only live signal for that
		// window, because the soft path deliberately logs at INFO and returns no
		// reconcile error - a rate that keeps climbing without an accompanying
		// ApplyError is a dependency that never came back.
		memoryApplyTransientErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_memory_apply_transient_errors_total",
			Help: "Total memory-stack applies soft-requeued on a retryable dependency failure (unreachable admission webhook, apiserver 429/503/timeout), by project.",
		}, []string{"project"}),
		// Counts every reconcile where the rendered cnpg storage was clamped up to
		// the provisioned volume to avoid a cnpg shrink rejection (issue #248). A
		// nonzero value means a Project spec is asking for less storage than is live.
		memoryStorageShrinkGuard: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_memory_storage_shrink_guarded_total",
			Help: "Total reconciles where rendered cnpg storage was raised to the provisioned size to avoid a shrink rejection, by project.",
		}, []string{"project"}),
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
		// "token_budget"|"project_paused"|"kind_ceiling"|"pool_full"|"live_ceiling";
		// class is the pool (normal|alert); kind is the Task kind for a per-kind
		// account-usage hold and for live_ceiling, "" for pool-class blocks
		// (project_paused, token_budget, pool_full). Bounded by live projects x
		// classes x kinds x reasons; set live (not pre-seeded). pool_full (issue
		// #440) is the ordinary concurrency cap: it is emitted at most once per
		// pool per pass, and only when work was actually left Queued behind it.
		// live_ceiling (#570) is the per-project LIVE-POD ceiling applied to the
		// MINT path: it counts fresh Tasks the dispatcher refused to create
		// because the project is at its live ceiling or because an owed
		// resumption - a parked conversation holding a human's queued reply - has
		// reserved the slot. A rate here with no matching un-park progress is the
		// signal that the ceiling is genuinely too small for the project.
		admissionBlockedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_admission_blocked_total",
			Help: "QueuedEvents the dispatcher declined to admit for a pool, by project, pool class (normal|alert), Task kind (empty for pool-class blocks), and reason (token_budget|project_paused|kind_ceiling|pool_full|live_ceiling).",
		}, []string{"project", "class", "kind", "reason"}),
		// An agent pod was spawned while one of its supporting subsystems was
		// unavailable, so the agent runs DEGRADED rather than not at all. This is
		// distinct from "the subsystem is down" (operator_memory_stacks) and from
		// "ingest is gated" (operator_repository_ingest_gated): it is the count of
		// real work that went out with reduced capability, and it does not depend
		// on the agent noticing or self-reporting. subsystem is "memory" today.
		agentPodDegradedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_agent_pod_degraded_total",
			Help: "Agent pods spawned with a supporting subsystem unavailable, by project, Task kind and subsystem (memory).",
		}, []string{"project", "kind", "subsystem"}),
		// Issue #381 bug B part 2: a Source-bearing Task that still owns ZERO
		// MergeRequest and ZERO Issue CRs past a grace window - the residue of
		// an interrupted two-phase mint write (Task created, the bind that
		// would have given it an owned MR/Issue never landed). Distinguishes
		// this specific backstop firing from every other route into
		// parked(awaiting-human), which the shared TaskParked{reason} counter
		// (EnterStage) does not.
		mrBindingBackstopTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_mr_binding_backstop_total",
			Help: "Tasks parked by the MR/Issue-binding backstop: a Source-bearing Task that still owns zero MergeRequest and zero Issue CRs past the grace window, by project and Task kind.",
		}, []string{"project", "kind"}),
		// Event-driven on a review Task's /outcome submission - correctly
		// wired (RecordReviewOutcome call site is unconditional in the
		// open-MRs loop) and confirmed firing across several prior pod
		// generations via 7-day Prometheus history during the
		// metric-wiring audit (issue #370). A flat 0 window simply means
		// no review verdict has been submitted since the current pod
		// became leader, not a wiring gap.
		reviewOutcomeTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_review_outcome_total",
			Help: "Review tasks by verdict (approved|changes_requested), keyed by the model that ran the review.",
		}, []string{"project", "repo", "model", "verdict"}),
		reviewHeadMovedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_review_head_moved_total",
			Help: "Review submits refused because the PR/MR head moved since the agent checked out. A spike is a review stuck on a fast-moving MR - the mirror is refreshed to the live head on demand and the agent is told to re-review it.",
		}, []string{"repo"}),
		reviewFindingsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_review_findings_total",
			Help: "Sum of review findings (suggestions/comments) per review, by model.",
		}, []string{"project", "repo", "model"}),
		implementCITotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_implement_ci_total",
			Help: "PR CI conclusions (pass|fail) for any PR-producing Task kind reaching handleMRCI, by kind and model.",
		}, []string{"project", "repo", "kind", "model", "result"}),
		// push-CD deploy-supervision CD-health gauges (G5). These reflect the CURRENT
		// number of cascades in a failed / stalled state, derived from authoritative
		// Task state each cdScan (not per-event counters), so max()>0 means "a cascade
		// is broken right now" and the value self-clears once bounded recovery (reroll)
		// or a human resolves it. A monotonic counter could never decrement, so the
		// tatara-observability critical page would never clear after a reroll recovered.
		cdCascadeFailed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tatara_cd_cascade_failed",
			Help: "push-CD deploy cascades currently in a FAILED state (parked recoverable after the bounded auto-reroll budget was spent), by project. >0 means merged code is not reaching the cluster and a human is needed.",
		}, []string{"project"}),
		cdCascadeStalled: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tatara_cd_cascade_stalled",
			Help: "push-CD deploy cascades currently STALLED past 1.5x the Deploying deadline with the auto-reroll budget spent (cdScan backstop, no live driver), by project.",
		}, []string{"project"}),
		cdResolvedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tatara_cd_resolved_total",
			Help: "push-CD Deploying Tasks resolved Done after a confirmed tatara-helmfile apply.",
		}),
		incidentDedupSuppressedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_incident_dedup_suppressed_total",
			Help: "Incident refires suppressed at admission, by reason (live_task|open_issue).",
		}, []string{"reason"}),
		incidentRefireCommentTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_incident_refire_comment_total",
			Help: "Coalesced incident refire comments, by result (posted|coalesced).",
		}, []string{"result"}),
		incidentSublinkTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_incident_sublink_total",
			Help: "Incident sub-issue link attempts, by result (linked|fallback_comment|failed).",
		}, []string{"result"}),
		incidentGroupLinkedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_incident_group_linked_total",
			Help: "Incident file_issue outcomes auto-linked under an open group sibling tracker (correlation), by result (linked|no_sibling).",
		}, []string{"result"}),
		incidentEscalatedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_incident_escalated_total",
			Help: "Persistent/stale incident trackers re-admitted as a fresh investigation, by result (minted|in_flight|error).",
		}, []string{"result"}),
		incidentTrackerCommentTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_incident_tracker_comment_total",
			Help: "Incident agent comment_issue outcomes appended to an existing tracker, by result (posted|failed).",
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
		m.autoApproveTotal,
		m.autoApproveRefusedTotal,
		m.approvalRefusedTotal,
		m.tasksInflightKind,
		m.agentBootRaceRequeue,
		m.agentSessionBusyRequeue,
		m.openProposals,
		m.brainstormTarget,
		m.brainstormPending,
		m.brainstormPaused,
		m.brainstormRefillTotal,
		m.brainstormQuotaTruncatedTotal,
		m.turnTimeoutTotal,
		m.ingestJobTotal,
		m.repositoryIngestFailing,
		m.repositoryIngestGated,
		m.repositoryLastIngestTime,
		m.repositoryPhaseRepairedTotal,
		m.ingestJobDedupTotal,
		m.agentUnreachableTermTotal,
		m.agentBootCrashTotal,
		m.orphanReapedTotal,
		m.reapDeleteErrorTotal,
		m.conversationGCTotal,
		m.turnSubmitTotal,
		m.turnSubmitDuration,
		m.agentHTTPTotal,
		m.agentHTTPDuration,
		m.authTotal,
		m.webhookDuration,
		m.restapiRequestsTotal,
		m.restapiRequestDuration,
		m.restapiResponseBytes,
		m.memoryHealthReadErrors,
		m.memoryApplyTransientErrors,
		m.memoryStorageShrinkGuard,
		m.lightragDocuments,
		m.lightragQueryErrors,
		m.memoryRetrievalProbe,
		m.toolSurfaceProbe,
		m.toolSurfaceProbeDuration,
		m.tokenBudgetUsedRatio,
		m.admissionBlockedTotal,
		m.agentPodDegradedTotal,
		m.mrBindingBackstopTotal,
		m.reviewOutcomeTotal,
		m.reviewHeadMovedTotal,
		m.reviewFindingsTotal,
		m.implementCITotal,
		m.cdCascadeFailed,
		m.cdCascadeStalled,
		m.cdResolvedTotal,
		m.incidentDedupSuppressedTotal,
		m.incidentRefireCommentTotal,
		m.incidentSublinkTotal,
		m.incidentGroupLinkedTotal,
		m.incidentEscalatedTotal,
		m.incidentTrackerCommentTotal,
	)
	// Pre-initialise label combinations so the counter vecs appear in Gather
	// even before any reconcile or webhook event completes.
	seedLabels(func(l ...string) { m.reconcileTotal.WithLabelValues(l...) },
		[]string{"Project", "Repository"},
		[]string{"success", "error"},
	)
	// Finding 28: pre-seed all (kind,action) pairs the webhook handler emits so
	// dashboards/alerts see a zero baseline rather than gaps before the first event.
	seedLabels(func(l ...string) { m.webhookEvents.WithLabelValues(l...) },
		[]string{"github", "gitlab"},
		[]string{"push", "issue", "mr", "other"},
		[]string{"other", "labeled", "opened", "closed", "synchronize", "create"},
		[]string{"accepted", "rejected", "ignored", "error", "task_created", "duplicate", "unknown_project", "bad_signature", "provider_mismatch", "too_large", "bad_request", "reactivated"},
	)
	// operator_memory_stacks is NOT pre-seeded: its project label is only known
	// at recompute time, and updateMemoryStackCounts Resets the vec each pass
	// anyway (that Reset is what drops a deleted project's series), so any seed
	// would be wiped within one recompute interval.
	for _, source := range []string{"reconcile", "poll_backstop"} {
		m.turnTimeoutTotal.WithLabelValues(source)
	}
	seedLabels(func(l ...string) { m.ingestJobTotal.WithLabelValues(l...) },
		[]string{"success", "failure"},
		[]string{"incremental", "full"},
	)
	for _, result := range []string{"accepted", "missing_token", "invalid_scheme", "invalid_token", "rejected"} {
		m.authTotal.WithLabelValues(result)
	}
	// Pre-seed webhook duration by provider/result so the series exist from startup.
	seedLabels(func(l ...string) { m.webhookDuration.WithLabelValues(l...) },
		[]string{"github", "gitlab"},
		[]string{"ok", "error"},
	)
	// Pre-seed REST API metrics for the contract C.1 endpoints.
	for _, endpoint := range []string{
		"task_context", "task_note", "submit_outcome",
		"scm_read", "issue_write", "mr_write",
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
	seedLabels(func(l ...string) { m.memoryRetrievalProbe.WithLabelValues(l...) },
		[]string{"/queries", "/code-graph/stats"},
		[]string{"present", "absent", "error", "unauthorized", "degraded"},
	)
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

// TurnTimeoutCounter returns the operator_turn_timeout_total counter for a
// detection source, so a test can assert the ABSENCE of detections as well as
// their presence.
func (m *OperatorMetrics) TurnTimeoutCounter(source string) prometheus.Counter {
	return m.turnTimeoutTotal.WithLabelValues(source)
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

// trackRepoProject records the project a repo's series are published under and
// retires the series it carried under a previous project. projectRef is a
// mutable Repository field, so without this a repo moved between projects would
// report under both forever - the per-repo analogue of the per-pass Reset the
// per-project memory-stack gauge uses (issue #425). ForgetRepository covers the
// other stale case, a Repository that is deleted outright.
func (m *OperatorMetrics) trackRepoProject(project, repo string) {
	m.repoProjectMu.Lock()
	defer m.repoProjectMu.Unlock()
	prev, ok := m.repoProject[repo]
	if ok && prev == project {
		return
	}
	if ok {
		m.deleteRepoSeries(prev, repo)
	}
	m.repoProject[repo] = project
}

// deleteRepoSeries drops all three per-repo ingest gauges for one project/repo pair.
func (m *OperatorMetrics) deleteRepoSeries(project, repo string) {
	m.repositoryIngestFailing.DeleteLabelValues(project, repo)
	m.repositoryIngestGated.DeleteLabelValues(project, repo)
	m.repositoryLastIngestTime.DeleteLabelValues(project, repo)
}

// ForgetRepository drops every per-repo ingest gauge series for repo, whatever
// project it was published under. Called when the Repository is gone so a
// deleted repo does not leave a series pinning an alert on a repo/project pair
// that no longer exists (issue #457).
func (m *OperatorMetrics) ForgetRepository(repo string) {
	m.repoProjectMu.Lock()
	defer m.repoProjectMu.Unlock()
	if prev, ok := m.repoProject[repo]; ok {
		m.deleteRepoSeries(prev, repo)
		delete(m.repoProject, repo)
	}
	labels := prometheus.Labels{"repo": repo}
	m.repositoryIngestFailing.DeletePartialMatch(labels)
	m.repositoryIngestGated.DeletePartialMatch(labels)
	m.repositoryLastIngestTime.DeletePartialMatch(labels)
}

// SetRepositoryIngestFailing sets operator_repository_ingest_failing for a repo
// to 1 when its ingest is currently failing, else 0. Unlike the monotonic
// operator_ingest_job_total counter, this reflects the CURRENT ingest health and
// clears as soon as a re-ingest succeeds, so alerting on it does not keep firing
// for an hour after a self-healed transient burst (issue #138).
//
// Callers must pass failing=false while the repo is gated AND that gate has
// held past ingestGateMaskDelay (18m; issue #525); see
// (*RepositoryReconciler).publishIngestHealth, the single writer of both gauges.
func (m *OperatorMetrics) SetRepositoryIngestFailing(project, repo string, failing bool) {
	m.trackRepoProject(project, repo)
	v := 0.0
	if failing {
		v = 1.0
	}
	m.repositoryIngestFailing.WithLabelValues(project, repo).Set(v)
}

// SetRepositoryIngestGated sets operator_repository_ingest_gated for a repo to
// 1 while its re-ingest is held by the project memory-readiness gate, else 0.
// The gate is a silent hold - nothing fails, nothing retries - so
// operator_repository_ingest_failing cannot see it (issue #434), and a repo that
// was already failing when the gate closed has that gauge masked only after the
// qualification delay (ingestGateMaskDelay, 18m), because no new ingest Job can
// run to clear it once qualified (issue #525).
//
// Only the repo's NEXT ingest is gated: a repo whose Job is already in flight
// is not held by the gate and must be reported gated=false, or its failing
// gauge would be masked while it is genuinely retrying.
func (m *OperatorMetrics) SetRepositoryIngestGated(project, repo string, gated bool) {
	m.trackRepoProject(project, repo)
	v := 0.0
	if gated {
		v = 1.0
	}
	m.repositoryIngestGated.WithLabelValues(project, repo).Set(v)
}

// SetRepositoryLastIngestTimestamp sets operator_repository_last_ingest_timestamp_seconds
// for a repo to the Unix seconds of its last successful ingest.
func (m *OperatorMetrics) SetRepositoryLastIngestTimestamp(project, repo string, unixSeconds float64) {
	m.trackRepoProject(project, repo)
	m.repositoryLastIngestTime.WithLabelValues(project, repo).Set(unixSeconds)
}

// ClearRepositoryLastIngestTimestamp retires
// operator_repository_last_ingest_timestamp_seconds for a repo.
//
// TataraIngestStale alerts on time() - this gauge, so a series that will never
// be refreshed again is not "stale data", it is a permanent false alarm. That
// is exactly a repo whose project has memory disabled: ingest is not applicable
// and no future ingest will ever stamp it. Deleting the series (rather than
// zeroing it - 0 would read as 1970 and fire immediately) removes it from the
// alert's input entirely. publishIngestHealth re-publishes it on the next pass
// if the repo starts ingesting again.
func (m *OperatorMetrics) ClearRepositoryLastIngestTimestamp(project, repo string) {
	m.repositoryLastIngestTime.DeleteLabelValues(project, repo)
}

// RepositoryPhaseRepaired increments operator_repository_phase_repaired_total:
// the reconciler found a Repository in the desynchronised (Phase=Failed,
// IngestFailureCount=0) state and repaired it (issue #457).
func (m *OperatorMetrics) RepositoryPhaseRepaired(project, repo string) {
	m.repositoryPhaseRepairedTotal.WithLabelValues(project, repo).Inc()
}

// IngestJobDeduplicated increments operator_ingest_job_deduplicated_total: an
// ingest Job creation hit AlreadyExists on its deterministic name and adopted
// the existing Job instead of launching a duplicate (issue #457).
func (m *OperatorMetrics) IngestJobDeduplicated(project, repo string) {
	m.ingestJobDedupTotal.WithLabelValues(project, repo).Inc()
}

// RepositoryIngestFailingGauge returns the gauge for a repo, for test assertions.
func (m *OperatorMetrics) RepositoryIngestFailingGauge(project, repo string) prometheus.Gauge {
	return m.repositoryIngestFailing.WithLabelValues(project, repo)
}

// RepositoryIngestGatedGauge returns the gauge for a repo, for test assertions.
func (m *OperatorMetrics) RepositoryIngestGatedGauge(project, repo string) prometheus.Gauge {
	return m.repositoryIngestGated.WithLabelValues(project, repo)
}

// RepositoryLastIngestTimestampGauge returns the gauge for a repo, for test assertions.
func (m *OperatorMetrics) RepositoryLastIngestTimestampGauge(project, repo string) prometheus.Gauge {
	return m.repositoryLastIngestTime.WithLabelValues(project, repo)
}

// AgentUnreachableTermination increments
// operator_agent_unreachable_termination_total: a Task was terminated because
// its wrapper agent stayed unreachable past the boot deadline.
func (m *OperatorMetrics) AgentUnreachableTermination() {
	m.agentUnreachableTermTotal.Inc()
}

// SetCDCascadeFailed sets tatara_cd_cascade_failed{project} to the current count
// of cascades in a durable failed state. Called once per cdScan with the count
// derived from authoritative Task state, so the gauge self-clears on recovery.
func (m *OperatorMetrics) SetCDCascadeFailed(project string, n float64) {
	if m == nil {
		return
	}
	m.cdCascadeFailed.WithLabelValues(project).Set(n)
}

// SetCDCascadeStalled sets tatara_cd_cascade_stalled{project} to the current count
// of cascades the cdScan backstop found stalled with the auto-reroll budget spent.
func (m *OperatorMetrics) SetCDCascadeStalled(project string, n float64) {
	if m == nil {
		return
	}
	m.cdCascadeStalled.WithLabelValues(project).Set(n)
}

// CDCascadeFailedGauge / CDCascadeStalledGauge return the per-project gauges for
// test assertions.
func (m *OperatorMetrics) CDCascadeFailedGauge(project string) prometheus.Gauge {
	return m.cdCascadeFailed.WithLabelValues(project)
}

func (m *OperatorMetrics) CDCascadeStalledGauge(project string) prometheus.Gauge {
	return m.cdCascadeStalled.WithLabelValues(project)
}

// CDResolved increments tatara_cd_resolved_total (Deploying Task reached applied).
func (m *OperatorMetrics) CDResolved() {
	if m == nil {
		return
	}
	m.cdResolvedTotal.Inc()
}

// IncidentDedupSuppressed records one admission-time incident suppression.
// reason is "live_task" (a live QueuedEvent or non-terminal Task matched) or
// "open_issue" (only an open tracker Issue matched).
func (m *OperatorMetrics) IncidentDedupSuppressed(reason string) {
	m.incidentDedupSuppressedTotal.WithLabelValues(reason).Inc()
}

// IncidentSublink records one sub-issue link attempt result.
func (m *OperatorMetrics) IncidentSublink(result string) {
	m.incidentSublinkTotal.WithLabelValues(result).Inc()
}

// IncidentSublinkCounter returns the counter for result for test assertions.
func (m *OperatorMetrics) IncidentSublinkCounter(result string) prometheus.Counter {
	return m.incidentSublinkTotal.WithLabelValues(result)
}

// IncidentRefireComment records a refire-comment decision: "posted" (enqueued a
// comment) or "coalesced" (within cooldown, counter bumped only).
func (m *OperatorMetrics) IncidentRefireComment(result string) {
	m.incidentRefireCommentTotal.WithLabelValues(result).Inc()
}

// IncidentGroupLinked records a file_issue correlation outcome: "linked" (the
// new tracker was auto-parented under an open group sibling) or "no_sibling".
func (m *OperatorMetrics) IncidentGroupLinked(result string) {
	if m == nil {
		return
	}
	m.incidentGroupLinkedTotal.WithLabelValues(result).Inc()
}

// IncidentEscalated records a liveness-escalation result: "minted", "in_flight"
// (a prior escalation Task is still live) or "error".
func (m *OperatorMetrics) IncidentEscalated(result string) {
	if m == nil {
		return
	}
	m.incidentEscalatedTotal.WithLabelValues(result).Inc()
}

// IncidentTrackerComment records a comment_issue outcome: "posted" or "failed".
func (m *OperatorMetrics) IncidentTrackerComment(result string) {
	if m == nil {
		return
	}
	m.incidentTrackerCommentTotal.WithLabelValues(result).Inc()
}

// IncidentGroupLinkedCounter returns the counter for result for test assertions.
func (m *OperatorMetrics) IncidentGroupLinkedCounter(result string) prometheus.Counter {
	return m.incidentGroupLinkedTotal.WithLabelValues(result)
}

// IncidentTrackerCommentCounter returns the counter for result for test assertions.
func (m *OperatorMetrics) IncidentTrackerCommentCounter(result string) prometheus.Counter {
	return m.incidentTrackerCommentTotal.WithLabelValues(result)
}

// AutoApproveTotal increments operator_auto_approve_total for a proposal kind
// (item 4a auto-approve release path).
func (m *OperatorMetrics) AutoApproveTotal(kind string) {
	if m == nil {
		return
	}
	m.autoApproveTotal.WithLabelValues(kind).Inc()
}

// ApprovalRefused increments operator_approval_refused_total for one structural
// refusal reason. Hard rule 13: the approval gate must be queryable without
// log-scraping, and this is the counterpart of operator_auto_approve_total on
// the refusal side.
func (m *OperatorMetrics) ApprovalRefused(reason string) {
	if m == nil {
		return
	}
	m.approvalRefusedTotal.WithLabelValues(reason).Inc()
}

// AutoApproveRefused increments operator_auto_approve_refused_total for the axis
// the carve-out fail-closed on. axis is never empty at the call site: the empty
// string is the GRANT, and a grant reaches no refusal path.
func (m *OperatorMetrics) AutoApproveRefused(axis string) {
	if m == nil {
		return
	}
	m.autoApproveRefusedTotal.WithLabelValues(axis).Inc()
}

// AutoApproveRefusedCounter returns the counter for axis for test assertions.
func (m *OperatorMetrics) AutoApproveRefusedCounter(axis string) prometheus.Counter {
	return m.autoApproveRefusedTotal.WithLabelValues(axis)
}

// ApprovalRefusedCounter returns the counter for reason for test assertions.
func (m *OperatorMetrics) ApprovalRefusedCounter(reason string) prometheus.Counter {
	return m.approvalRefusedTotal.WithLabelValues(reason)
}

// AutoApproveCounter returns the counter for kind for test assertions.
func (m *OperatorMetrics) AutoApproveCounter(kind string) prometheus.Counter {
	return m.autoApproveTotal.WithLabelValues(kind)
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

// ResetMemoryStacks clears operator_memory_stacks so a recompute pass leaves no
// stale series behind for a project that was deleted since the last pass.
func (m *OperatorMetrics) ResetMemoryStacks() {
	m.memoryStacks.Reset()
}

// SetMemoryStackPhase sets operator_memory_stacks{project,phase} to n. Callers
// set every known phase per project (1 for the current one, 0 for the rest) so
// a phase a project has left reads 0 rather than retaining its last value.
// Degraded (issue #355) is a stack that has been Provisioning past
// MemoryConfig.ProvisioningTimeout.
func (m *OperatorMetrics) SetMemoryStackPhase(project, phase string, n float64) {
	m.memoryStacks.WithLabelValues(project, phase).Set(n)
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

// SetOpenProposals sets operator_open_proposals for a repo slug.
func (m *OperatorMetrics) SetOpenProposals(repo string, n float64) {
	m.openProposals.WithLabelValues(repo).Set(n)
}

// SetBrainstormTarget sets operator_brainstorm_target_proposals for a project.
func (m *OperatorMetrics) SetBrainstormTarget(project string, n float64) {
	if m == nil {
		return
	}
	m.brainstormTarget.WithLabelValues(project).Set(n)
}

// SetBrainstormPending sets operator_brainstorm_pending_proposals for a project.
func (m *OperatorMetrics) SetBrainstormPending(project string, n float64) {
	if m == nil {
		return
	}
	m.brainstormPending.WithLabelValues(project).Set(n)
}

// SetBrainstormPaused sets operator_brainstorm_paused for a project: 1 while
// paused on an action=exhausted verdict, 0 otherwise.
func (m *OperatorMetrics) SetBrainstormPaused(project string, paused bool) {
	if m == nil {
		return
	}
	v := 0.0
	if paused {
		v = 1.0
	}
	m.brainstormPaused.WithLabelValues(project).Set(v)
}

// BrainstormRefill counts one dispatched refill.
func (m *OperatorMetrics) BrainstormRefill(project string) {
	if m == nil {
		return
	}
	m.brainstormRefillTotal.WithLabelValues(project).Inc()
}

// BrainstormQuotaTruncated counts proposals dropped by quota truncation.
func (m *OperatorMetrics) BrainstormQuotaTruncated(project string, dropped int) {
	if m == nil || dropped <= 0 {
		return
	}
	m.brainstormQuotaTruncatedTotal.WithLabelValues(project).Add(float64(dropped))
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
// "delete_session").
func (m *OperatorMetrics) AgentHTTP(method, outcome string, seconds float64) {
	m.agentHTTPTotal.WithLabelValues(method, outcome).Inc()
	m.agentHTTPDuration.WithLabelValues(method).Observe(seconds)
}

// RecordAuth increments operator_auth_total for the given result.
// Valid results: "accepted", "missing_token", "invalid_scheme", "invalid_token",
// "discovery_unavailable" (the OIDC issuer could not be reached - the request
// got a 503, not a 401; also emitted once at startup when the discovery
// warm-up fails, so a permanently bad issuer is visible with zero traffic),
// "rejected" (HMAC callback signature check).
func (m *OperatorMetrics) RecordAuth(result string) {
	m.authTotal.WithLabelValues(result).Inc()
}

// AuthCounter returns the counter for (result) for test assertions.
func (m *OperatorMetrics) AuthCounter(result string) prometheus.Counter {
	return m.authTotal.WithLabelValues(result)
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

// ObserveRESTResponseBytes records the size of one REST response into
// operator_restapi_response_bytes, labelled by chi route template (#641).
//
// A nil receiver is a no-op: the REST middleware runs on every request and test
// harnesses build a Server with no Metrics, so the guard belongs here rather
// than at the one call site.
func (m *OperatorMetrics) ObserveRESTResponseBytes(route string, n int) {
	if m == nil {
		return
	}
	m.restapiResponseBytes.WithLabelValues(route).Observe(float64(n))
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

// MemoryApplyTransientError increments operator_memory_apply_transient_errors_total
// for a project. Called when applyMemoryStack failed on a retryable dependency
// (unreachable admission webhook, apiserver 429/503/timeout) and the reconcile
// soft-requeued instead of recording phase=Failed (issue #439).
func (m *OperatorMetrics) MemoryApplyTransientError(project string) {
	m.memoryApplyTransientErrors.WithLabelValues(project).Inc()
}

// MemoryApplyTransientErrorCounter returns the counter for a project for test
// assertions.
func (m *OperatorMetrics) MemoryApplyTransientErrorCounter(project string) prometheus.Counter {
	return m.memoryApplyTransientErrors.WithLabelValues(project)
}

// MemoryStorageShrinkGuarded increments operator_memory_storage_shrink_guarded_total
// for a project. Called when the rendered cnpg storage was clamped up to the
// provisioned volume to avoid a cnpg shrink rejection (issue #248).
func (m *OperatorMetrics) MemoryStorageShrinkGuarded(project string) {
	m.memoryStorageShrinkGuard.WithLabelValues(project).Inc()
}

// MemoryStorageShrinkGuardedCounter returns the counter for (project) for test assertions.
func (m *OperatorMetrics) MemoryStorageShrinkGuardedCounter(project string) prometheus.Counter {
	return m.memoryStorageShrinkGuard.WithLabelValues(project)
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

// SetTokenBudgetUsedRatio sets operator_token_budget_used_ratio for a project and
// scope ("used", "proactive", or "emergency") to ratio (a fraction of the window
// limit, 0..1+).
func (m *OperatorMetrics) SetTokenBudgetUsedRatio(project, scope string, ratio float64) {
	m.tokenBudgetUsedRatio.WithLabelValues(project, scope).Set(ratio)
}

// AdmissionBlocked increments operator_admission_blocked_total: the dispatcher
// declined to admit a pool's work for the given project, pool class
// ("normal"|"alert"), Task kind (empty for pool-class blocks that are not tied
// to one kind), and reason
// ("token_budget"|"project_paused"|"kind_ceiling"|"pool_full"|"live_ceiling").
func (m *OperatorMetrics) AdmissionBlocked(project, class, kind, reason string) {
	m.admissionBlockedTotal.WithLabelValues(project, class, kind, reason).Inc()
}

// AdmissionBlockedCounter returns the counter for (project, class, kind,
// reason) for test assertions.
func (m *OperatorMetrics) AdmissionBlockedCounter(project, class, kind, reason string) prometheus.Counter {
	return m.admissionBlockedTotal.WithLabelValues(project, class, kind, reason)
}

// AgentPodDegraded increments operator_agent_pod_degraded_total: an agent pod
// was spawned with a supporting subsystem unavailable (subsystem "memory": the
// project memory stack was not stably ready, so recall fails for the whole
// turn). Nil-safe, matching TaskTerminalEntry's convention.
func (m *OperatorMetrics) AgentPodDegraded(project, kind, subsystem string) {
	if m == nil {
		return
	}
	m.agentPodDegradedTotal.WithLabelValues(project, kind, subsystem).Inc()
}

// AgentPodDegradedCounter returns the counter for (project, kind, subsystem)
// for test assertions.
func (m *OperatorMetrics) AgentPodDegradedCounter(project, kind, subsystem string) prometheus.Counter {
	return m.agentPodDegradedTotal.WithLabelValues(project, kind, subsystem)
}

// MRBindingBackstopParked increments operator_mr_binding_backstop_total:
// issue #381's park+alert backstop caught a Source-bearing Task that still
// owns zero MergeRequest and zero Issue CRs past the grace window - an
// interrupted two-phase mint with nothing left to repair it automatically.
func (m *OperatorMetrics) MRBindingBackstopParked(project, kind string) {
	m.mrBindingBackstopTotal.WithLabelValues(project, kind).Inc()
}

// MRBindingBackstopParkedCounter returns the counter for (project, kind) for
// test assertions.
func (m *OperatorMetrics) MRBindingBackstopParkedCounter(project, kind string) prometheus.Counter {
	return m.mrBindingBackstopTotal.WithLabelValues(project, kind)
}

// RecordReviewOutcome increments operator_review_outcome_total for a review
// Task's verdict ("approved" or "changes_requested"), keyed by the model that
// ran the review (G4 quality-proxy signal). It is nil-safe (a reconciler
// wired without metrics is a test, not an outage), matching
// TaskTerminalEntry's convention.
func (m *OperatorMetrics) RecordReviewOutcome(project, repo, model, verdict string) {
	if m == nil {
		return
	}
	m.reviewOutcomeTotal.WithLabelValues(project, repo, model, verdict).Inc()
}

// RecordReviewHeadMoved increments operator_review_head_moved_total for the repo
// whose review submit was refused because its head moved since checkout. A nil
// receiver is a no-op, matching RecordReviewOutcome's convention.
func (m *OperatorMetrics) RecordReviewHeadMoved(repo string) {
	if m == nil {
		return
	}
	m.reviewHeadMovedTotal.WithLabelValues(repo).Inc()
}

// ReviewHeadMovedCounter returns the counter for repo for test assertions.
func (m *OperatorMetrics) ReviewHeadMovedCounter(repo string) prometheus.Counter {
	return m.reviewHeadMovedTotal.WithLabelValues(repo)
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

// RecordImplementCI increments operator_implement_ci_total for a PR CI
// conclusion ("pass" or "fail") on a Task of the given kind, keyed by the
// model that ran the Task (G4 quality-proxy signal).
func (m *OperatorMetrics) RecordImplementCI(project, repo, kind, model, result string) {
	m.implementCITotal.WithLabelValues(project, repo, kind, model, result).Inc()
}

// ImplementCICounter returns the counter for (project, repo, kind, model,
// result) for test assertions.
func (m *OperatorMetrics) ImplementCICounter(project, repo, kind, model, result string) prometheus.Counter {
	return m.implementCITotal.WithLabelValues(project, repo, kind, model, result)
}
