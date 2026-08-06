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
//	synthetic - the operator wrote the note from lastTurn's finalText/pushedRepos
//	none      - the operator held NOTHING to write; the note that landed is a
//	            placeholder. THIS is the silent-work-loss bucket to alert on.
//
// They were ONE label until issue #527, and every consumer read it wrong as a
// result: the stop dimension overwrote the capture dimension on any teardown
// error, so synthetic_handoff was unreachable and never once recorded, while
// alerts/tatara-operator.yaml fired on force_deleted claiming "neither an agent
// handoff nor a synthetic one" - a state the code could not produce.
//
// Task.status.notes is NEVER empty after a TTL stop, on any pairing. handoff=none
// is precisely the case where non-empty is not the same as useful.
var agentPodTTLExpiredTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_agent_pod_ttl_expired_total",
	Help: "Agent pods stopped by the pod TTL stop sequence (contract G.7), by agent kind, how the pod was stopped (outcome), and how continuation state was captured (handoff).",
}, []string{"agent_kind", "outcome", "handoff"})

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
// TTL-stopped pod. outcome is graceful|force_deleted; handoff is
// agent|synthetic|none.
func AgentPodTTLExpired(agentKind, outcome, handoff string) {
	agentPodTTLExpiredTotal.WithLabelValues(agentKind, outcome, handoff).Inc()
}

// AgentPodTTLExpiredCounter returns the counter for test assertions.
func AgentPodTTLExpiredCounter(agentKind, outcome, handoff string) prometheus.Counter {
	return agentPodTTLExpiredTotal.WithLabelValues(agentKind, outcome, handoff)
}
