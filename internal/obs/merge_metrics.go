package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// UnexpectedMergeTotal is the C.9 accepted-risk DETECTOR (contract K.1):
// an owned MergeRequest found MERGED on the forge while the Task's mergeCursor
// never advanced past its repo. The operator is the SOLE merge caller, so a
// merge it did not initiate can only be a human, or a native auto-merge armed
// before the cutover. Any non-zero value is CRITICAL: the sequential mergeOrder
// - the thing that stops tatara-cli shipping before tatara-operator - was
// bypassed.
var UnexpectedMergeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_unexpected_merge_total",
	Help: "MergeRequests found merged with no mergeCursor advance, by repo (contract C.9/K.1).",
}, []string{"repo"})

// MergeCursorStalledSeconds is how long the sequential merge (contract C.5.2)
// has been sitting on one repo without advancing status.mergeCursor. It carries
// a per-task label, so the series MUST be deleted from the registry when the
// Task leaves merging (ClearMergeCursorStalled) - a gauge that is never deleted
// is scraped forever and /metrics grows without bound (K.1 CARDINALITY).
var MergeCursorStalledSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "operator_merge_cursor_stalled_seconds",
	Help: "Seconds the sequential merge has been stalled on one repo, by task and repo (contract K.1).",
}, []string{"task", "repo"})

// ReviewPostTotal counts C.5.3 phase-2 review posts by result:
//
//	posted  - the forge-side dedup check found nothing and the review was posted
//	skipped - the round marker was already on the forge; only the mirror was reconciled
//	refused - a structural 4xx (scm.ErrReviewRefused) -> parked(review-post-refused)
//	error   - a retryable failure; the reconciler re-runs
//
// Correctly wired (DrainPendingReview is called on every MergeRequest
// reconcile, gated on the same PendingReview outcome.go sets on every
// review submission as operator_review_outcome_total) and confirmed firing
// across several prior pod generations via 7-day Prometheus history during
// the metric-wiring audit (issue #370). Not on the tatara-observability
// allowlist yet - see the companion observability PR. A flat 0 window means
// no review has drained since the current pod became leader, not a bug.
var ReviewPostTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_review_post_total",
	Help: "Review posts driven by the MergeRequest reconciler, by result (contract C.5.3).",
}, []string{"result"})

// ClearMergeCursorStalled deletes every MergeCursorStalledSeconds series for a
// Task. Called when the Task leaves merging, for any reason.
func ClearMergeCursorStalled(task string) {
	MergeCursorStalledSeconds.DeletePartialMatch(prometheus.Labels{"task": task})
}

func init() {
	ctrlmetrics.Registry.MustRegister(UnexpectedMergeTotal, MergeCursorStalledSeconds, ReviewPostTotal)
}
