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
	ctrlmetrics.Registry.MustRegister(IllegalStageTransitionTotal)
}

// IllegalStageTransition increments operator_illegal_stage_transition_total.
func IllegalStageTransition(from, to string) {
	IllegalStageTransitionTotal.WithLabelValues(from, to).Inc()
}

// IllegalStageTransitionCounter returns the counter for test assertions.
func IllegalStageTransitionCounter(from, to string) prometheus.Counter {
	return IllegalStageTransitionTotal.WithLabelValues(from, to)
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
