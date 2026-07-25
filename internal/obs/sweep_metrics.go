package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Sweep cap labels (contract K.1): the two creation budgets that BOTH bind on
// every sweep pass.
const (
	SweepCapMaxNewTasksPerSweep = "maxNewTasksPerSweep"
	SweepCapMaxOpenTasks        = "maxOpenTasks"
)

// TasksMintedPerSweep is observed on EVERY sweep pass, per mint stage, including
// the zero (contract B.4, fix B1/B2). It is what makes the accepted risk of the
// tatara-parked label read a MONITORED one: a project whose parked mints suddenly
// become triaging mints is a project whose park markers stopped landing.
var TasksMintedPerSweep = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "operator_tasks_minted_per_sweep",
	Help:    "Tasks minted by one sweep pass, by project and mint stage (contract B.4).",
	Buckets: []float64{0, 1, 2, 3, 5, 8, 13, 21},
}, []string{"project", "stage"})

// SweepMintCapHitTotal counts sweep passes in which a creation budget BOUND, so
// orphans were left for the next pass. cap is maxNewTasksPerSweep or
// maxOpenTasks. A sustained rate means the backlog is growing faster than the
// platform mints, which is a capacity signal, not an error.
var SweepMintCapHitTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_sweep_mint_cap_hit_total",
	Help: "Sweep passes in which a Task creation budget bound, by project and cap (contract B.4).",
}, []string{"project", "cap"})

// SweepLastSuccessTimestamp is a HEARTBEAT covering two different activity
// families with two different stamp semantics, both intentional for their own
// activity's shape. For "sweep"/"nightlySweep" (sweep.go's B.4 pass) it is
// LIVENESS, not zero-error health: stamped whenever the repos loop RUNS TO
// COMPLETION, even with per-item errors (those are metered separately via
// SweepErrorsTotal) - one stale CR or transient forge error must never
// silence the heartbeat for the whole pass. A sweep that cannot even begin
// (activeTaskCount fails) returns before stamping, so it stays unset. For
// "brainstorm"/"documentation"/"issueScan" (projectscan.go's stampScan) it IS
// zero-error: each is a single Status().Update, not a multi-item loop, so
// success-only stamping is the correct (and simplest) signal there; this is
// also the successor for tatara_scan_items_total, pruned as dead-per-redesign
// (metric-wiring audit, issue #370). Its alert sets noDataState: Alerting,
// because for a heartbeat NoData IS the failure - the gauge is process-local
// and resets on restart, so it is also rehydrated from the persisted
// Status.LastIssueScan/LastBrainstorm/LastDocumentation stamps at the top of
// every runScans reconcile (fix #386), not only stamped on a freshly-run
// pass; an absent series still means that activity has never completed at
// all, never scanned or rehydrated.
//
// `project` is positionally FIRST and is always proj.Name. Without it (issue
// #441) all three Projects wrote one series: the rehydration below runs on every
// Project's reconcile, so the series moved backward and forward - one activity
// cycled through three values within 8 seconds and then held for four hours -
// and a Project whose cron was genuinely dead was masked by a healthy sibling's
// write. The fix is the label set, not backward-movement detection; that is the
// confirmed practice (temporalio/temporal#9600) and no source supports alerting
// on non-monotonicity.
var SweepLastSuccessTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "operator_sweep_last_success_timestamp_seconds",
	Help: "Unix timestamp of the last completed pass, by project and activity (contract K.1): liveness (per-item-error-tolerant) for sweep/nightlySweep, zero-error for brainstorm/documentation/issueScan.",
}, []string{"project", "activity"})

// SweepNextExpectedTimestamp is the CADENCE half of the heartbeat: the absolute
// unix timestamp of the next expected run for one project's activity, computed
// by the operator from that activity's own cron in the Project CR. It exists so
// the alert rule carries a single grace period instead of a per-activity
// threshold table - one flat 6h threshold breached for ~18h of every 24 on the
// 0 3 * * * / 0 6 * * * nightly crons (tatara-observability#65). This is
// kube-state-metrics' kube_cronjob_next_schedule_time pattern, the only prior art
// that survives adding an activity or a Project without editing the alert rule.
//
// A series exists ONLY for an activity that is actually enabled: publishing one
// for a configured-but-off activity would page for a run that is never going to
// happen. It is refreshed on every Project reconcile from the same persisted
// Status.Last* stamps the heartbeat rehydrates from, so it survives a leader
// change and a pod restart.
var SweepNextExpectedTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "operator_sweep_next_expected_timestamp_seconds",
	Help: "Unix timestamp of the next expected run for one project's activity, computed from its cron (contract K.1).",
}, []string{"project", "activity"})

// SweepErrorsTotal counts sweep failures by project, activity and reason. Every
// reason is a closed-set string, so the label never takes a forge error message.
var SweepErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_sweep_errors_total",
	Help: "Sweep errors, by project, activity and reason (contract K.1).",
}, []string{"project", "activity", "reason"})

// sweepSeedReasons is the closed fail(reason, ...) set for sweep.go's B.4 pass
// (SweepActivity/SweepNightlyActivity), plus list_tasks. Literal here (not
// imported from internal/controller) to avoid a reverse import; keep in sync
// with sweep.go's constants and its fail(reason, ...) call sites.
var sweepSeedReasons = []string{
	"list_tasks", "owner_repo", "list_issues", "list_prs", "get_issue_cr",
	"list_comments", "get_issue", "mint_issue_task", "clear_webhook_marker",
	"get_owning_task", "get_mr_cr", "adopt_pr", "mint_review_task",
}

// scanSeedReasons is the second closed reason set, for projectscan.go's cron
// activities (issue #401's refine-barrier stall fix). Cross-seeded for all three
// activities even though refine_barrier_* / refine_*_check_failed only ever fire
// for brainstorm (the refine pre-scan barrier is brainstorm-only) - a
// permanently-zero series for documentation/issueScan on those reasons is a
// harmless baseline, and one seedLabels call is simpler than splitting the cross
// product per-activity.
var scanSeedReasons = []string{
	"refine_barrier_held", "refine_check_failed", "refine_inflight_check_failed",
	"invalid_cron", "stamp_failed", "refine_barrier_timeout",
}

// SeedSweepErrorsForProject pre-seeds the closed (activity x reason) label set of
// SweepErrorsTotal for ONE project, so a healthy sweep with zero errors still
// exposes a zero baseline and increase(operator_sweep_errors_total[1h]) is
// well-defined from that project's first reconcile rather than from its first
// error (a CounterVec with no WithLabelValues call has NO series at all;
// metric-wiring audit, issue #370).
//
// This used to run in init(). It cannot any more: `project` joined the label set
// in issue #441 and project names are not known at process start. The Project
// reconciler calls this on every pass; WithLabelValues returns the existing child
// for an already-seeded combination, so it is idempotent and cheap (44 map
// lookups per Project per reconcile).
func SeedSweepErrorsForProject(project string) {
	seed := func(l ...string) { SweepErrorsTotal.WithLabelValues(l...) }
	seedLabels(seed, []string{project}, []string{"sweep", "nightlySweep"}, sweepSeedReasons)
	seedLabels(seed, []string{project}, []string{"brainstorm", "documentation", "issueScan"}, scanSeedReasons)
}

func init() {
	ctrlmetrics.Registry.MustRegister(
		TasksMintedPerSweep,
		SweepMintCapHitTotal,
		SweepLastSuccessTimestamp,
		SweepNextExpectedTimestamp,
		SweepErrorsTotal,
	)
}
