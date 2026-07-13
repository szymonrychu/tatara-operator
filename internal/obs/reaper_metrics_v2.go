package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// GC-blocked reasons (contract K.1, closed set). A reap that DID NOT happen is
// invisible without this counter: the artifact just sits there, worked by
// nobody, and the only symptom is a Task count that never comes down.
const (
	// GCBlockedNoControllerOwner: the B.5 handover could not be completed, so
	// the reap was abandoned rather than leave an artifact with zero controller
	// owners (worked by nobody, re-minted by nobody: the orphan predicate sees
	// an OWNED Issue).
	GCBlockedNoControllerOwner = "no_controller_owner"
	// GCBlockedFoldInFlight: the Task is named in a LIVE Task's
	// status.foldInFlight. Reaping a fold member mid-adoption destroys the
	// artifacts the umbrella is halfway through adopting (B.3).
	GCBlockedFoldInFlight = "fold_in_flight"
	// GCBlockedDocReference: a delivered Task whose work is not documented yet
	// (documentedBy == "" with >= 1 merged MR). It is held until the nightly
	// batch covers it.
	GCBlockedDocReference = "doc_reference"
)

// Doc-batch abandonment reasons (contract K.1, fixes L29 and M21).
const (
	// DocAbandonedNeverRan: the batch reached its terminal with stats.podRuns
	// == 0. It STARVED - it never got an agent slot. Its members are stamped
	// with NOTHING and are picked up by the NEXT night's batch. The
	// "docs never written" alert keys on exactly this.
	DocAbandonedNeverRan = "never_ran"
	// DocAbandonedTimeout: the batch RAN and timed out. Its members are stamped
	// anyway: the work was attempted and we do not retry it forever.
	DocAbandonedTimeout = "timeout"
)

// GCBlockedTotal counts reaps the reaper REFUSED, by reason. It is the
// observability half of the B.6 SKIP list: without it a Task that is blocked
// forever looks identical to a Task that is simply young.
var GCBlockedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_gc_blocked_total",
	Help: "Terminal-stage reaps refused, by reason (contract B.6/K.1).",
}, []string{"reason"})

// DocTaskAbandonedTotal counts nightly documentation batches that reached their
// terminal without delivering docs.
//
// reason=never_ran is a CAPACITY alert, not an error: a priority-2 doc batch on
// a busy project starves, times out at docStageBudget having run zero pods, and
// its members go back in the queue. A sustained rate means the docs are never
// being written at all.
var DocTaskAbandonedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_doc_task_abandoned_total",
	Help: "Nightly documentation batches abandoned, by reason (contract B.6/K.1, fixes L29/M21).",
}, []string{"reason"})

func init() {
	ctrlmetrics.Registry.MustRegister(GCBlockedTotal, DocTaskAbandonedTotal)
}
