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
var SweepLastSuccessTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "operator_sweep_last_success_timestamp_seconds",
	Help: "Unix timestamp of the last completed pass, by activity (contract K.1): liveness (per-item-error-tolerant) for sweep/nightlySweep, zero-error for brainstorm/documentation/issueScan.",
}, []string{"activity"})

// SweepErrorsTotal counts sweep failures by activity and reason. Every reason is
// a closed-set string, so the label never takes a forge error message.
var SweepErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_sweep_errors_total",
	Help: "Sweep errors, by activity and reason (contract K.1).",
}, []string{"activity", "reason"})

func init() {
	ctrlmetrics.Registry.MustRegister(
		TasksMintedPerSweep,
		SweepMintCapHitTotal,
		SweepLastSuccessTimestamp,
		SweepErrorsTotal,
	)
	// Pre-seed the closed (activity x reason) label set so a healthy sweep
	// with zero errors still exposes a zero baseline - a CounterVec with no
	// WithLabelValues call has NO series at all, and the TataraSweepErrors
	// alert would undercount the first evaluation of a real error storm
	// (metric-wiring audit, issue #370). activity/reason are literal here
	// (not imported from internal/controller) to avoid a reverse import;
	// keep in sync with sweep.go's SweepActivity/SweepNightlyActivity
	// constants and its fail(reason, ...) call sites.
	seedLabels(func(l ...string) { SweepErrorsTotal.WithLabelValues(l...) },
		[]string{"sweep", "nightlySweep"},
		[]string{
			"list_tasks", "owner_repo", "list_issues", "list_prs", "get_issue_cr",
			"list_comments", "get_issue", "mint_issue_task", "clear_webhook_marker",
			"get_owning_task", "get_mr_cr", "adopt_pr", "mint_review_task",
		},
	)
}
