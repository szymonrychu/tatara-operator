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
// sequence, by how the handoff was captured:
//
//	agent_handoff     - the agent answered the handoff turn and wrote its note
//	synthetic_handoff - the handoff turn was refused/failed; the operator wrote
//	                    a synthetic note from finalText + pushedRepos
//	force_deleted     - as synthetic_handoff, and the pod had to be force-deleted
//
// Task.status.notes is NEVER empty after a TTL stop, in any of the three.
var agentPodTTLExpiredTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_agent_pod_ttl_expired_total",
	Help: "Agent pods stopped by the pod TTL stop sequence (contract G.7), by agent kind and handoff outcome.",
}, []string{"agent_kind", "outcome"})

func init() {
	ctrlmetrics.Registry.MustRegister(agentContractMismatchTotal, agentPodTTLExpiredTotal)
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
// TTL-stopped pod. outcome is agent_handoff|synthetic_handoff|force_deleted.
func AgentPodTTLExpired(agentKind, outcome string) {
	agentPodTTLExpiredTotal.WithLabelValues(agentKind, outcome).Inc()
}

// AgentPodTTLExpiredCounter returns the counter for test assertions.
func AgentPodTTLExpiredCounter(agentKind, outcome string) prometheus.Counter {
	return agentPodTTLExpiredTotal.WithLabelValues(agentKind, outcome)
}
