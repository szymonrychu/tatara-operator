package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// agentContractMismatchTotal counts agent pods whose wrapper reported a
// contractVersion the operator does not speak (contract G.10). ANY non-zero
// rate is CRITICAL: the operator and the wrapper image are pinned in DIFFERENT
// helm releases and helmfile applies releases concurrently, so a new-operator +
// old-agent skew is reachable. Without the handshake such a pod burns its whole
// turn budget producing nothing, silently, because a tool 404 is just a tool
// error the model tries to work around.
//
// expected is the operator's compiled contract version, got is the wrapper's
// (0 when the wrapper reported no contractVersion field at all - an old
// wrapper), image is the agent image the Project pinned.
var agentContractMismatchTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_agent_contract_mismatch_total",
	Help: "Agent pods whose wrapper reported an unsupported contractVersion at pod-ready (contract G.10). The Task fails instantly, before turn-0.",
}, []string{"expected", "got", "image"})

// agentPodTTLExpiredTotal counts agent pods stopped by the G.7 TTL stop
// sequence, on TWO INDEPENDENT dimensions.
//
// outcome - how the POD was stopped:
//
//	graceful      - the wrapper session closed and the pod came down
//	force_deleted - the graceful stop failed against a live pod, so it was
//	                deleted with a zero grace period
//
// handoff - how the CONTINUATION STATE was captured, which is the dimension that
// decides whether work was lost:
//
//	agent     - the agent answered the handoff turn and wrote its own note
//	synthetic - the operator wrote the note from the last turn's
//	            finalText/pushedRepos
//	none      - the operator held NOTHING to write; the note that landed is a
//	            placeholder. THIS is the silent-work-loss bucket to alert on.
//
// They were ONE label until #527, and every consumer read it wrong as a result:
// finish() overwrote the capture dimension with the stop dimension on any
// teardown error, so synthetic_handoff was structurally unreachable and never
// once recorded over 30 days, while tatara-observability's rule fired on
// force_deleted claiming "neither an agent handoff nor a synthetic one" - a
// state the code could not produce.
//
// Task.status.notes is NEVER empty after a TTL stop, on any pairing. handoff=none
// is precisely the case where non-empty is not the same as useful.
var agentPodTTLExpiredTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_agent_pod_ttl_expired_total",
	Help: "Agent pods stopped by the pod TTL stop sequence (contract G.7), by agent kind, how the pod was stopped (outcome), and how continuation state was captured (handoff).",
}, []string{"agent_kind", "outcome", "handoff"})

// agentContractSkew is the STATE the mismatch counter could never express.
//
// #544: skew is a state, and it was monitored with a counter of events. Three
// proven consequences, all of them from one 71-minute outage: a single isolated
// mismatch never alerts at all (the series is BORN at 1, so increase(...[5m]) is
// 0 until a SECOND event lands - 8 minutes green with a Task already destroyed);
// the counter goes flat while the skew is still fully present, so the rule
// self-clears and was green for ~86% of the outage; and any operator restart
// wipes the series entirely, which is exactly what a roll-forward does.
//
// This gauge is 1 for as long as a live agent pod is observed on a version the
// operator does not speak, and is retracted when that image passes the
// handshake. An alert built on `operator_agent_contract_skew > 0` covers the
// whole window instead of two 5-minute slivers of it.
var agentContractSkew = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "operator_agent_contract_skew",
	Help: "1 while an agent pod is observed reporting a contractVersion the operator does not speak (contract G.10). Unlike the mismatch counter this is a STATE and stays 1 for the whole skew window.",
}, []string{"expected", "got", "image"})

// agentContractExpected exports the operator's OWN compiled contract version.
// #544 found no gauge of it anywhere: ContractVersion is a compile-time const
// that was never exported, so "which version does the operator speak" was only
// ever knowable from a mismatch event - i.e. only once damage had been done.
var agentContractExpected = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "operator_agent_contract_expected",
	Help: "The agent wire-contract version this operator process speaks (contract G.10).",
})

// syntheticHandoffEmptyTotal counts G.7 synthetic handoff notes written with NO
// continuation state to carry - the note renders "(none)"/"none" and the next
// pod resumes from nothing.
//
// #527: for ~19 days this was the ONLY synthetic note the operator could write,
// because ttlStop never populated LastFinalText/PushedRepos, and nothing counted
// it. The non-empty-notes invariant was satisfied vacuously and the Task looked
// healthy while a turn's work was discarded. A non-zero rate here now means the
// last-turn state is not reaching the stop - not that a Task legitimately had no
// completed turn to report.
var syntheticHandoffEmptyTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_agent_synthetic_handoff_empty_total",
	Help: "G.7 synthetic handoff notes written with no last-turn continuation state to carry (contract G.7). The next pod resumes from nothing.",
}, []string{"agent_kind"})

func init() {
	ctrlmetrics.Registry.MustRegister(agentContractMismatchTotal, agentPodTTLExpiredTotal,
		agentContractSkew, agentContractExpected, syntheticHandoffEmptyTotal)
}

// AgentSyntheticHandoffEmpty increments
// operator_agent_synthetic_handoff_empty_total for one content-free synthetic
// handoff note.
func AgentSyntheticHandoffEmpty(agentKind string) {
	syntheticHandoffEmptyTotal.WithLabelValues(agentKind).Inc()
}

// AgentSyntheticHandoffEmptyCounter returns the counter for test assertions.
func AgentSyntheticHandoffEmptyCounter(agentKind string) prometheus.Counter {
	return syntheticHandoffEmptyTotal.WithLabelValues(agentKind)
}

// SetAgentContractExpected publishes the operator's compiled contract version.
// Called once at manager start; the internal/obs package cannot read the
// constant itself without importing internal/agent.
func SetAgentContractExpected(version int) {
	agentContractExpected.Set(float64(version))
}

// AgentContractSkewObserved marks image as skewed: the operator speaks expected,
// the wrapper reported got.
func AgentContractSkewObserved(expected, got, image string) {
	agentContractSkew.WithLabelValues(expected, got, image).Set(1)
}

// AgentContractSkewResolved retracts every skew series for image. It runs on a
// SUCCESSFUL handshake, so the gauge falls the moment the train finishes -
// including the case where it finished by rolling the OPERATOR forward, which is
// what actually resolved #544 and which the counter-based rule could not see.
func AgentContractSkewResolved(image string) {
	agentContractSkew.DeletePartialMatch(prometheus.Labels{"image": image})
}

// AgentContractSkewGauge returns the gauge for test assertions.
func AgentContractSkewGauge(expected, got, image string) prometheus.Gauge {
	return agentContractSkew.WithLabelValues(expected, got, image)
}

// AgentContractMismatch increments operator_agent_contract_mismatch_total for
// one wrapper that failed the G.10 handshake.
func AgentContractMismatch(expected, got, image string) {
	agentContractMismatchTotal.WithLabelValues(expected, got, image).Inc()
}

// AgentContractMismatchCounter returns the counter for test assertions.
func AgentContractMismatchCounter(expected, got, image string) prometheus.Counter {
	return agentContractMismatchTotal.WithLabelValues(expected, got, image)
}

// AgentPodTTLExpired increments operator_agent_pod_ttl_expired_total for one
// TTL-stopped pod. outcome is graceful|force_deleted; handoff is
// agent|synthetic|none.
func AgentPodTTLExpired(agentKind, outcome, handoff string) {
	agentPodTTLExpiredTotal.WithLabelValues(agentKind, outcome, handoff).Inc()
}

// AgentPodTTLExpiredCounter returns the counter for test assertions.
func AgentPodTTLExpiredCounter(agentKind, outcome, handoff string) prometheus.Counter {
	return agentPodTTLExpiredTotal.WithLabelValues(agentKind, outcome, handoff)
}
