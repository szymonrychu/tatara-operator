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
// reason is the THIRD dimension, and it is what makes the alert actionable. The
// runbook for "Operator agent pod recreation loop" instructs the operator to
// split by recreation path - an OOMKill loop wants a memory limit raised, a boot
// timeout wants an image or a probe looked at, and a handoff-less live pod wants
// neither - and until this label existed that split was simply not in the data.
// Its cardinality is bounded by the RecreationReason* set below.
//
// Counted at the ATTEMPT, not at success, and counted even on the attempt that
// exhausts the budget: the runaway shape this pages on is N attempts per hour,
// and a loop that terminates in a park is exactly the shape whose last attempt
// must not go missing.
var podRecreationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_pod_recreations_total",
	Help: "Agent pod recreations driven by the operator (pod lost, pod never ready, or a live pod that ended with no agent handoff), by project, Task kind and termination reason.",
}, []string{"project", "kind", "reason"})

// RecreationReason* is the closed set of values on the `reason` label of
// operator_pod_recreations_total: WHY the operator had to recreate the pod.
//
// CrashLoopBackOff is deliberately absent. Agent pods run RestartPolicy: Never
// (internal/agent/pod.go), so the kubelet never restarts a wrapper and the state
// is unreachable; emitting a value nothing can ever produce would put a
// permanently-zero series behind every project.
const (
	// RecreationReasonPodGone: the Pod object was NotFound. Evicted, reaped,
	// deleted by hand, or its node went away.
	RecreationReasonPodGone = "PodGone"
	// RecreationReasonOOMKilled: the kernel OOM killer took the container out for
	// exceeding its memory limit. The workspace died with it.
	RecreationReasonOOMKilled = "OOMKilled"
	// RecreationReasonContainerExited: a container terminated with a non-zero exit
	// code for some reason other than an OOM kill.
	RecreationReasonContainerExited = "ContainerExited"
	// RecreationReasonPodFailed: the Pod reached phase Failed with no terminated
	// container status to attribute it to (e.g. a node-pressure eviction the
	// kubelet recorded on the Pod alone).
	RecreationReasonPodFailed = "PodFailed"
	// RecreationReasonBootTimeout: the pod existed but never became Ready within
	// PodReadyTimeout. CLOCK 2.
	RecreationReasonBootTimeout = "BootTimeout"
	// RecreationReasonNoHandoff: a LIVE conversation's pod ended without the agent
	// writing a handoff note, so the Task was re-armed with a replacement pod
	// rather than parked.
	RecreationReasonNoHandoff = "NoHandoff"
	// RecreationReasonAgentStop: the agent ASKED to be stopped - it wrote a
	// kind=handoff note with no turn in flight - so the operator took the pod
	// down and re-armed the Task with a replacement.
	//
	// IT IS THE VALUE THIS COUNTER WAS MISSING, and its absence is half of the
	// upgrade-qe-e4016501fd9107d9 incident. api/v1alpha1/constants.go's
	// PodReadyTimeout comment asserts that a pod loop is "made visible by the
	// operator_pod_recreations_total{reason=BootTimeout} churn alert"; the
	// agent-requested stop re-arms through NORMAL admission rather than through
	// any of the three paths that counted a recreation, so the metric had ZERO
	// series while one Task minted 127 pods in under four hours and produced 99%
	// of the platform's agent turns. The pod was recreated by the operator on
	// every one of those laps - which is precisely what this counter's Help says
	// it measures - so the fix is a value, not a new metric.
	RecreationReasonAgentStop = "AgentStop"
)

// AgentStop* is the closed set of values on the `disposition` label of
// operator_agent_requested_stop_total: what the operator DID about an agent
// asking to be stopped.
const (
	// AgentStopReArmed: a replacement pod was minted. Healthy in isolation - it
	// is the ordinary continuation handoff - and a runaway RATE of it is the
	// respawn loop.
	AgentStopReArmed = "rearmed"
	// AgentStopCapped: the re-arm budget for this state occupancy was spent, so
	// the Task was parked no-outcome and NO pod was minted. Any non-zero rate
	// means a Task asked to stop, was asked again, and had nothing new to say.
	AgentStopCapped = "capped"
)

// agentRequestedStopTotal counts agent-requested stops by what the operator did
// about them.
//
// IT IS THE SERIES THE BOUND IS ALERTABLE ON, and it is deliberately NOT folded
// into operator_pod_recreations_total. That counter answers "is a project
// churning pods" and cannot express the `capped` disposition at all - a capped
// stop mints no pod, so it is not a recreation and must not be counted as one -
// yet `capped` is the one value that proves the bound fired rather than the loop
// simply having stopped on its own.
//
// project and kind, never task: the same rule podRecreationsTotal states. A task
// label would put one series per Task in the fleet behind a counter that only
// moves during a fault, and the per-Task quantity is already durable and
// inspectable as Task.status.stats.agentStops - which is what BOUNDS the loop,
// where the metric only watches it. The task name is on the structured log line
// (action=agent_requested_stop / agent_stop_rearm_capped), which is not
// cardinality-bound.
var agentRequestedStopTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_agent_requested_stop_total",
	Help: "Agent-requested pod stops (a kind=handoff note with no turn in flight), by project, Task kind and what the operator did (rearmed, capped).",
}, []string{"project", "kind", "disposition"})

func init() {
	ctrlmetrics.Registry.MustRegister(stallProbeTotal, podRecreationsTotal, agentRequestedStopTotal)
}

// AgentRequestedStop increments operator_agent_requested_stop_total for one
// agent-requested stop. disposition is one of the AgentStop* values.
func AgentRequestedStop(project, kind, disposition string) {
	agentRequestedStopTotal.WithLabelValues(project, kind, disposition).Inc()
}

// AgentRequestedStopCounter returns the counter for test assertions.
func AgentRequestedStopCounter(project, kind, disposition string) prometheus.Counter {
	return agentRequestedStopTotal.WithLabelValues(project, kind, disposition)
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
// reason is one of the RecreationReason* values.
func PodRecreation(project, kind, reason string) {
	podRecreationsTotal.WithLabelValues(project, kind, reason).Inc()
}

// PodRecreationCounter returns the counter for test assertions.
func PodRecreationCounter(project, kind, reason string) prometheus.Counter {
	return podRecreationsTotal.WithLabelValues(project, kind, reason)
}
