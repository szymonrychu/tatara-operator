package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Stall-probe outcomes (closed set). The probe asks a mid-turn agent whether it
// is alive; the outcome says what came back, and the three failure values are
// NOT interchangeable - they name three different things being wrong.
const (
	// StallProbeSent: a probe was written into the agent's PTY. It is the
	// DENOMINATOR - every other value here is an outcome of a probe that was sent,
	// so without it "nobody ever answers" and "we never asked" are the same
	// series shape. Emitted once per attempt, at the attempt.
	StallProbeSent = "sent"
	// StallProbeAnswered: the agent replied with the TATARA-ALIVE marker. The turn
	// was never stalled; it was working, or waiting on its own subagent.
	StallProbeAnswered = "answered"
	// StallProbeUnanswered: the probe was DELIVERED (the agent consumed it) and no
	// answer followed within the grace window. The agent has the message and is
	// not responding to it.
	StallProbeUnanswered = "unanswered"
	// StallProbeNeverDelivered: the probe was written and never consumed - the
	// `enqueue` line with no matching `remove`. The agent is blocked INSIDE a tool
	// call, which is a different failure from ignoring a delivered message and has
	// a different fix (interrupt, not wait).
	StallProbeNeverDelivered = "never_delivered"
	// StallProbeUnsupported: the wrapper serves no probe endpoint
	// (agent.ErrProbeUnsupported). Expected during a release train and on any
	// not-yet-rolled pod; the caller falls back to the pre-probe behaviour. It is
	// counted so "the probe is not firing" can be told apart from "the probe is
	// firing and nobody answers", which look identical on every other series.
	StallProbeUnsupported = "unsupported"
	// StallProbeError: the probe call itself failed - transport, 5xx, timeout. An
	// operator-side or network fault, not a verdict about the agent.
	StallProbeError = "error"
)

// stallProbeTotal counts stall probes by outcome.
//
// The outcome label carries the whole diagnosis, so it is the one dimension:
// project/task would multiply the series count by the fleet size to answer a
// question ("is the probe mechanism working") that is fleet-wide by nature. The
// per-Task detail lives in the structured log line, which is not cardinality-
// bound.
var stallProbeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_stall_probe_total",
	Help: "Mid-turn stall probes sent to agent wrappers, by outcome (sent, answered, unanswered, never_delivered, unsupported, error).",
}, []string{"outcome"})

// podRecreationsTotal counts every agent-pod recreation the operator drives, on
// all three paths that spend the recreation budget: a pod that ran and vanished,
// a pod that never became ready, and a live pod that ended with no agent handoff.
//
// IT IS THE INPUT TO THE ALERT THAT REPLACES THE CAP. maxPodRecreations bounds a
// crash loop today by PARKING the Task, which converts a boot-crash into a
// terminal state a human has to dig out - and gives no signal at all until the
// budget is spent. The later phase deletes that enforcement and pages on
// `increase(operator_pod_recreations_total[1h]) > 6` instead, so this counter has
// to exist and be trustworthy BEFORE the cap comes out, not with it.
//
// project and kind, never task: the alert asks "is a project churning pods",
// which is a per-project rate, and a task label would put one series per Task in
// the fleet behind a counter that only ever moves during a fault.
//
// Counted at the ATTEMPT, not at success, and counted even on the attempt that
// exhausts the budget: the runaway shape this pages on is N attempts per hour,
// and a loop that terminates in a park is exactly the shape whose last attempt
// must not go missing.
var podRecreationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_pod_recreations_total",
	Help: "Agent pod recreations driven by the operator (pod lost, pod never ready, or a live pod that ended with no agent handoff), by project and Task kind.",
}, []string{"project", "kind"})

func init() {
	ctrlmetrics.Registry.MustRegister(stallProbeTotal, podRecreationsTotal)
}

// StallProbe increments operator_stall_probe_total for one probe outcome.
func StallProbe(outcome string) {
	stallProbeTotal.WithLabelValues(outcome).Inc()
}

// StallProbeCounter returns the counter for test assertions.
func StallProbeCounter(outcome string) prometheus.Counter {
	return stallProbeTotal.WithLabelValues(outcome)
}

// PodRecreation increments operator_pod_recreations_total for one recreation.
func PodRecreation(project, kind string) {
	podRecreationsTotal.WithLabelValues(project, kind).Inc()
}

// PodRecreationCounter returns the counter for test assertions.
func PodRecreationCounter(project, kind string) prometheus.Counter {
	return podRecreationsTotal.WithLabelValues(project, kind)
}
