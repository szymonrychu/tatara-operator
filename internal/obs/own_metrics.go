package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// OrphanNoControllerTotal counts firings of contract B.2 rule 5's repair
// guard: an Issue/MergeRequest found with owner refs but NO controller owner.
// Such an artifact is worked by nobody and re-minted by nobody (the sweep's
// orphan predicate sees an OWNED Issue), so every firing is a bug in a fold
// (B.3) or a reap (B.5) and the alert on it is a correctness alarm, not a
// capacity one.
//
// It is a package-level collector registered on the controller-runtime
// registry, not a field on OperatorMetrics, because its only caller -
// own.RepairZeroController - is a free function that mutates an object and
// issues one Update; threading an *OperatorMetrics through it would put a
// metrics handle in the signature of every ownership helper for one counter.
var OrphanNoControllerTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "operator_orphan_no_controller_total",
	Help: "Times the zero-controller-owner repair guard fired on an Issue or MergeRequest (contract B.2 rule 5). Every increment is a fold or reap bug.",
})

func init() {
	ctrlmetrics.Registry.MustRegister(OrphanNoControllerTotal)
}
