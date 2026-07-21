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

// Doc-batch mint outcomes (issue #423). A stalled nightly mint used to be
// invisible until the DOWNSTREAM operator_gc_blocked_total{doc_reference} counter
// tripped an alert. These make the mint itself observable: a firing cron that
// merely has nothing to do (result=empty) is indistinguishable from a cron that
// is not firing at all unless the attempt is counted.
const (
	// DocMintMinted: a documentation batch Task was created this tick.
	DocMintMinted = "minted"
	// DocMintEmpty: the tick fired but nothing delivered needed documenting.
	DocMintEmpty = "empty"
	// DocMintDeferred: a batch was already in flight, so this tick's mint was
	// deferred to avoid racing two docs PRs over the same parents.
	DocMintDeferred = "deferred"
	// DocMintNoDocsRepo: documentation is disabled or the docs repo is not
	// enrolled as a Repository CR, so there is nowhere to write.
	DocMintNoDocsRepo = "no_docs_repo"
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

// DocBatchMintTotal counts nightly documentation-batch mint attempts by outcome
// (issue #423). It is the observability half of MintDocBatch: rate == 0 across
// ALL results means the cron is not firing at all; a steady result=empty is a
// healthy quiet night; a sustained result=deferred means a batch is wedged
// in flight. Any of these is a stalled mint that this counter surfaces BEFORE
// the downstream operator_gc_blocked_total{doc_reference} counter trips.
var DocBatchMintTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_doc_batch_mint_total",
	Help: "Nightly documentation-batch mint attempts, by outcome (issue #423).",
}, []string{"result"})

// DocReferenceBlockedTasks is the number of delivered+merged Tasks currently held
// PAST their legitimate documentation-hold window by the doc_reference GC gate,
// per project - the true DISTINCT stuck-object count. operator_gc_blocked_total
// counts one EVENT per reconcile pass per held Task, so it cannot answer "how
// many objects are actually stuck" (the alert's "35 object(s)" was 35 re-scan
// events of 2 objects). A routine daily hold - a Task simply waiting for
// tonight's batch, or one a live batch is carrying through review/merge - is NOT
// counted here; only a genuinely stalled mint is (issue #423).
var DocReferenceBlockedTasks = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "operator_doc_reference_blocked_tasks",
	Help: "Delivered Tasks stuck past their documentation-hold window, by project (issue #423).",
}, []string{"project"})

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
	ctrlmetrics.Registry.MustRegister(GCBlockedTotal, DocTaskAbandonedTotal,
		DocBatchMintTotal, DocReferenceBlockedTasks)
}
