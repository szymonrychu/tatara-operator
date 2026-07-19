package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// IllegalStageTransitionTotal counts F.3 edges the stage machine REFUSED
// (contract K.1). It was NAMED in v2 and never emitted, which is the exact
// "named and never emitted" defect K.1 calls out twice: a non-zero value is a
// CODE BUG, and a metric that can never be non-zero cannot report one.
//
// It is emitted from the ONE transition choke point
// (internal/controller.EnterStage), never at a call site.
var IllegalStageTransitionTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_illegal_stage_transition_total",
	Help: "Stage transitions refused because the from->to edge is not in the F.3 table (contract F.3/K.1). A non-zero value is a code bug.",
}, []string{"from", "to"})

func init() {
	ctrlmetrics.Registry.MustRegister(IllegalStageTransitionTotal, StageDriftTotal)
	// Pre-seed every stage so a healthy operator (0 drift, the expected
	// steady state) exposes a zero baseline from startup instead of no
	// series at all - a sustained-rate alert added later has something to
	// evaluate against on the very first real drift (metric-wiring audit,
	// issue #370). IllegalStageTransitionTotal is NOT pre-seeded: its
	// {from,to} cardinality (15x15) is unbounded-by-comparison for a metric
	// with no consuming alert yet, and K.1 cardinality discipline argues
	// against seeding a matrix nobody reads.
	for _, stg := range []string{
		tatarav1alpha1.StageTriaging, tatarav1alpha1.StageBrainstorming, tatarav1alpha1.StageClarifying,
		tatarav1alpha1.StageInvestigating, tatarav1alpha1.StageRefining, tatarav1alpha1.StageApproved,
		tatarav1alpha1.StageImplementing, tatarav1alpha1.StageReviewing, tatarav1alpha1.StageMerging,
		tatarav1alpha1.StageDeploying, tatarav1alpha1.StageDelivered, tatarav1alpha1.StageDocumenting,
		tatarav1alpha1.StageRejected, tatarav1alpha1.StageFailed, tatarav1alpha1.StageParked,
	} {
		StageDriftTotal.WithLabelValues(stg)
	}
}

// IllegalStageTransition increments operator_illegal_stage_transition_total.
func IllegalStageTransition(from, to string) {
	IllegalStageTransitionTotal.WithLabelValues(from, to).Inc()
}

// IllegalStageTransitionCounter returns the counter for test assertions.
func IllegalStageTransitionCounter(from, to string) prometheus.Counter {
	return IllegalStageTransitionTotal.WithLabelValues(from, to)
}

// StageDriftTotal counts reconciles that started from an informer cache BEHIND
// the API server: the live status.stage no longer matched the cached one the
// reconcile was about to derive its next edge from. It is
// IllegalStageTransitionTotal's early-warning sibling - drift DETECTED, before
// TaskReconciler adopts the live object and carries on, versus drift
// MATERIALIZED as a refused X -> X edge - and it lives here for that reason.
//
// A trickle is normal (an informer is eventually consistent, and every write
// this operator makes races its own next reconcile). A SUSTAINED rate on one
// stage is not: it means a wedged watch or a dropped event, which the adoption
// path papers over silently and which the default 10h SyncPeriod will not
// rescue for hours.
//
// The label is the CACHED stage - the stale one an edge would have been
// re-derived from, which is what names the failure - not the live one.
var StageDriftTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_stage_drift_total",
	Help: "Reconciles whose CACHED task status.stage was behind the API server's live one, by the cached (stale) stage. A trickle is normal informer lag; a sustained rate on one stage means a wedged watch.",
}, []string{"stage"})

// StageDrift increments operator_stage_drift_total for one detected drift.
func StageDrift(cachedStage string) {
	StageDriftTotal.WithLabelValues(cachedStage).Inc()
}

// StageDriftCounter returns the counter for the cached stage for test assertions.
func StageDriftCounter(cachedStage string) prometheus.Counter {
	return StageDriftTotal.WithLabelValues(cachedStage)
}

// TaskTerminalEntry is the D1 emit: operator_task_terminal_total
// {kind, stage, stageReason} for a Task that has just ENTERED a terminal stage.
// 29 tatara-observability rules ride on this counter and it is the ONLY counter
// of terminal outcomes the platform has, so it must fire on EVERY terminal
// entry, from every writer - not just the ones the old Phase machine knew about.
//
// It is nil-safe (a reconciler wired without metrics is a test, not an outage)
// and it never double-counts:
//
//   - from == "" is a MINT, not an outcome. The sweep mints a Task straight into
//     parked(backlog-sweep) (B.4): that Task never ran and never failed, it is
//     the durable owner of an Issue CR at zero agent cost. Counting it as a park
//     would drown the park-rate alerts in Tasks that never did anything.
//   - a `to` outside the terminal set is not an outcome at all.
func (m *OperatorMetrics) TaskTerminalEntry(kind, from, to, reason string) {
	if m == nil || m.taskMetrics == nil {
		return
	}
	if from == "" || !tatarav1alpha1.StageIsTerminalOutcome(to) {
		return
	}
	m.TaskTerminal(kind, to, reason)
}
