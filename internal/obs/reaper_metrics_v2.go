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
	//
	// A CONFLICT IS NOT A BLOCK (issue #530). releaseOwnership writes owner refs
	// under a fresh-Get + RetryOnConflict loop, and a 409 that outlives even that
	// is still resolved by the reconcile's requeue - measured at 0.83 s and
	// 0.91 s on the two traced production cases, both of which ended in a
	// completed reap. Counting them made one sub-second blip hold `Operator GC
	// blocked` firing for ~1 h behind the annotation "Tasks and their
	// Issue/MergeRequest CRs are accumulating in etcd", which was false: nothing
	// accumulated, and operator_orphan_no_controller_total stayed at 0 across the
	// whole window. Only a failure the requeue will NOT resolve - a refused
	// handover, a 403, an admission rejection - is counted under this reason.
	GCBlockedNoControllerOwner = "no_controller_owner"
	// GCBlockedFoldInFlight: the Task is named in the status.foldInFlight of an
	// umbrella whose adoption CAN STILL COMPLETE (v1alpha1.FoldInFlightActive).
	// Reaping a fold member mid-adoption destroys the artifacts the umbrella is
	// halfway through adopting (B.3). Counted only once the hold has outlived
	// FoldInFlightGrace: the adoption is one request, so a hold inside that
	// window is healthy and counting it alerted on every ordinary fold.
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
//
// REFUSED, NOT RETRIED (issue #530). An increment must mean the reap could not
// be completed, never "one write failed once and the requeue fixed it" - the
// alert on this counter reads it as durable state ("could not garbage-collect N
// object(s)"), and it has no decrement to walk that back with. Every increment
// site owes that distinction; see GCBlockedNoControllerOwner.
var GCBlockedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_gc_blocked_total",
	Help: "Terminal-stage reaps refused for a reason a requeue does not resolve, by reason (contract B.6/K.1).",
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

// FoldInFlightBlockedTasks is the number of Tasks currently held off the reaper
// by a fold adoption that has outlived FoldInFlightGrace, per project - the
// DISTINCT stuck-object count, the same thing DocReferenceBlockedTasks is for
// doc_reference. Issue #467's alert read "334 object(s)" against 26 real ones,
// because operator_gc_blocked_total counts one EVENT per reconcile pass per held
// Task and the rule summed that across pod replicas. Alert on this instead.
var FoldInFlightBlockedTasks = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "operator_fold_in_flight_blocked_tasks",
	Help: "Tasks held off the reaper past their fold-adoption window, by project (issue #467).",
}, []string{"project"})

// FoldStrandedReleasedTotal counts fold markers the reaper REFUSED to honour
// because the adoption can never complete, by why. It is the observability half
// of FoldInFlightActive: a release is always an anomaly upstream (an umbrella
// that died mid-adoption, or a request that never reached step 5), and without
// this counter the fix is silent - the members simply start getting collected
// and nobody learns that an adoption was lost.
var FoldStrandedReleasedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_fold_stranded_released_total",
	Help: "Stranded fold markers released by the reaper, by reason (issue #467).",
}, []string{"reason"})

// Stranded-fold release reasons (closed set).
const (
	// FoldStrandedUmbrellaDone: the umbrella is delivered/rejected/failed/parked.
	// It runs no agent pod and will never submit another outcome, so nothing can
	// finish the adoption. This is issue #467's shape exactly.
	FoldStrandedUmbrellaDone = "umbrella_done"
	// FoldStrandedTTLExpired: the umbrella is still live but its adoption started
	// more than FoldInFlightTTL ago. A fold is one request; an hour means the
	// request died without reaching step 5.
	FoldStrandedTTLExpired = "ttl_expired"
)

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
		DocBatchMintTotal, DocReferenceBlockedTasks,
		FoldInFlightBlockedTasks, FoldStrandedReleasedTotal)
}
