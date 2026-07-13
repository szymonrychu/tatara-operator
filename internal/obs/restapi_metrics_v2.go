package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Metrics for the contract-C REST surface. Every new failure mode on the agent
// hot path gets one: an agent that cannot terminate its Task, or that is
// silently refused a write, is invisible without them.
var (
	// RestOutcomeAcceptedTotal counts ACCEPTED submit_outcome calls.
	// outcome is the payload's action/verdict/decision.
	RestOutcomeAcceptedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "operator_rest_outcome_accepted_total",
		Help: "Accepted submit_outcome calls, by agent kind and outcome.",
	}, []string{"kind", "outcome"})

	// RestOutcomeRejectedTotal counts REFUSED submit_outcome calls. A non-zero
	// head-moved or review-coverage rate is a real signal, not noise: it means
	// agents are reviewing stale heads or under-reporting their coverage.
	RestOutcomeRejectedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "operator_rest_outcome_rejected_total",
		Help: "Refused submit_outcome calls, by agent kind and refusal reason.",
	}, []string{"kind", "reason"})

	// RestOwnershipRefusedTotal counts controller-ownership refusals on
	// issue_write / mr_write. ANY value means an agent tried to write to an
	// artifact another Task owns (contract fix 7).
	RestOwnershipRefusedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "operator_rest_ownership_refused_total",
		Help: "Forge writes refused because the calling Task does not control the artifact.",
	}, []string{"target"})

	// RestCIReadTotal counts scm_read(kind=ci) calls by whether they left the
	// cluster. It is how the C.2.10 pacer is observed: a live/cached ratio that
	// climbs means agents are polling faster than the 20s floor.
	RestCIReadTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "operator_rest_ci_read_total",
		Help: "scm_read(kind=ci) calls, by whether the result came from the forge or the pacer.",
	}, []string{"result"})
)

func init() {
	metrics.Registry.MustRegister(
		RestOutcomeAcceptedTotal,
		RestOutcomeRejectedTotal,
		RestOwnershipRefusedTotal,
		RestCIReadTotal,
	)
}
