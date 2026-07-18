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

// SweepLastSuccessTimestamp is a HEARTBEAT: the unix time of the last sweep pass
// that RAN TO COMPLETION (the repos loop finished). It is LIVENESS, not
// zero-error health - a pass with per-item errors still stamps it, because those
// errors are metered separately (SweepErrorsTotal) and one stale CR or transient
// forge error must never silence the heartbeat for the whole pass. A sweep that
// cannot even begin (activeTaskCount fails) returns before stamping, so it stays
// unset. Its alert sets noDataState: Alerting, because for a heartbeat NoData IS
// the failure - the gauge resets on restart and an absent series means the
// operator is not sweeping at all.
var SweepLastSuccessTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "operator_sweep_last_success_timestamp_seconds",
	Help: "Unix timestamp of the last completed sweep pass (liveness, not zero-error), by activity (contract K.1).",
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
}
