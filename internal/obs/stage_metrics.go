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
	ctrlmetrics.Registry.MustRegister(IllegalStageTransitionTotal, StageDriftTotal, StageRaceLostTotal)
	// Pre-seed every stage so a healthy operator (0 drift, the expected
	// steady state) exposes a zero baseline from startup instead of no
	// series at all - a sustained-rate alert added later has something to
	// evaluate against on the very first real drift (metric-wiring audit,
	// issue #370). IllegalStageTransitionTotal and StageRaceLostTotal are NOT
	// pre-seeded: their {from,to} cardinality (8x8) makes a full matrix
	// 64 series apiece against K.1 cardinality discipline, and no rule reads
	// a pair that has never fired. The cost is real and known: an alert using
	// increase() sees a series BORN mid-window and has no zero to extrapolate
	// from, so it under-reports the true count (issue #478 - the counter had
	// reached 2 while the 15m increase() reported 1). Under-reporting a
	// must-be-zero counter is acceptable; 256 idle series are not.
	for _, st := range []string{
		tatarav1alpha1.StateNew, tatarav1alpha1.StateRefined, tatarav1alpha1.StateUnderImplementation,
		tatarav1alpha1.StateAwaitingReview, tatarav1alpha1.StateMerged, tatarav1alpha1.StateDeployed,
		tatarav1alpha1.StateDone, tatarav1alpha1.StateRejected,
	} {
		StageDriftTotal.WithLabelValues(st)
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

// StageRaceLostTotal counts transitions DROPPED because another writer moved
// status.stage between the caller's pre-check and the write, leaving the
// caller's (legal, correctly computed) edge illegal from the stage the Task
// ACTUALLY has by the time the write lands (issue #478).
//
// It is the third member of the family, and the distinction is the whole point:
//
//   - StageDriftTotal      - drift DETECTED before the edge is computed, then
//     adopted. Nothing was lost.
//   - StageRaceLostTotal   - drift materialised INSIDE the write. The loser's
//     edge is dropped. NOT a code bug: two controllers legitimately raced for
//     the same exit and one of them wrote first. A trickle is the expected
//     cost of concurrency; a sustained rate on one {from,to} pair means two
//     writers are fighting over an edge that needs an owner.
//   - IllegalStageTransitionTotal - the caller computed an edge that is not in
//     the F.3 table AT ALL, from the stage the Task really had. THAT is the
//     code bug, and charging lost races to it (which is what shipped before
//     #478) made the alert's "any non-zero value is a code bug" annotation a
//     lie the first time two controllers raced.
//
// The labels are the LIVE from (the winner's stage) and the loser's intended
// to, i.e. the edge that was refused - not the stale from the loser started
// out believing.
var StageRaceLostTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_stage_race_lost_total",
	Help: "Stage transitions dropped because another writer moved status.stage between the caller's pre-check and the write. Labelled by the live (winner's) from and the dropped to. A trickle is normal concurrency, not a code bug.",
}, []string{"from", "to"})

// StageRaceLost increments operator_stage_race_lost_total for one lost race.
func StageRaceLost(liveFrom, droppedTo string) {
	StageRaceLostTotal.WithLabelValues(liveFrom, droppedTo).Inc()
}

// StageRaceLostCounter returns the counter for test assertions.
func StageRaceLostCounter(liveFrom, droppedTo string) prometheus.Counter {
	return StageRaceLostTotal.WithLabelValues(liveFrom, droppedTo)
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
	if from == "" || !tatarav1alpha1.TaskIsTerminalOutcome(to) {
		return
	}
	m.TaskTerminal(kind, to, reason)
}

// DeployPinFanoutStalledTotal counts merged changes whose component tag never
// reached the tatara-helmfile main pin inside the fan-out window (#512).
//
// The deploying stage is pod-less and poll-driven, and until this existed a dead
// fan-out was indistinguishable from a slow one: task
// mt-c-tatara-operator-506 polled for 2h06m59s emitting ZERO log lines against a
// pin nothing was going to write, then discarded merged PR #509 at
// deploy-blocked. ANY non-zero rate here means the tag -> bump PR cascade is not
// running and NOTHING can deploy, not just this Task.
var DeployPinFanoutStalledTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_deploy_pin_fanout_stalled_total",
	Help: "Merged changes whose component tag never reached the tatara-helmfile main pin within the CD fan-out window. The whole delivery cascade is down, not one task.",
}, []string{"repo"})

func init() {
	ctrlmetrics.Registry.MustRegister(DeployPinFanoutStalledTotal)
}
