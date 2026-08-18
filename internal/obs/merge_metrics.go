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

// reviewFindingDegradedTotal counts inline review findings that could NOT be
// anchored to a diff line and were downgraded to a plain (non-inline) note
// instead of an anchored discussion (#394). reason distinguishes:
//
//	unanchorable  - the finding's (path, line) fell outside every new-side diff
//	                hunk, or carried no line at all (a file-level finding, #398)
//	post-refused  - the anchored POST itself was deterministically refused (a 4xx
//	                classified terminal), so the finding fell back to a note rather
//	                than aborting the whole round
//
// It is emitted from internal/scm (GitLab.PostReview), the same way SCMRateLimited
// is, and lives here beside ReviewPostTotal because it is a review-post metric.
var reviewFindingDegradedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_review_finding_degraded_total",
	Help: "Inline review findings downgraded to a non-inline note, by provider and reason (contract C.5.3, #394).",
}, []string{"provider", "reason"})

// ReviewFindingDegraded increments operator_review_finding_degraded_total for one
// finding that could not be posted as an anchored inline discussion.
func ReviewFindingDegraded(provider, reason string) {
	reviewFindingDegradedTotal.WithLabelValues(provider, reason).Inc()
}

// ReviewFindingDegradedCounter returns the counter for (provider, reason) for
// test assertions.
func ReviewFindingDegradedCounter(provider, reason string) prometheus.Counter {
	return reviewFindingDegradedTotal.WithLabelValues(provider, reason)
}

// CIRedExitTotal counts red-CI exits (issue #476), by the repo whose required
// check failed and the stage the Task left for. `from` separates the two guards
// that share the edge: `reviewing` is the promotion that never happened,
// `merging` is the 4h poll that was cut short. A flat 0 on `merging` with a
// non-zero `reviewing` is the intended steady state - the reviewing guard is
// supposed to make the merging one unreachable.
var CIRedExitTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_ci_red_exit_total",
	Help: "Tasks routed off a red required check at the reviewed head, by repo, source stage and target stage (issue #476).",
}, []string{"repo", "from", "to"})

// MergeConflictExitTotal counts conflict self-heal exits from the merge
// corridor, by the repo whose merge request went DIRTY and the stage the Task
// left for. There is no `from` label because there is only one site: the merge
// gate, at merged.
//
// `to` separates the outcomes that matter operationally:
// under-implementation is an agent being put on the branch, and (park) is the
// bound being spent - a sustained non-zero rate on the latter means main is
// moving faster than the agent can reconcile, which is a human's problem.
var MergeConflictExitTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_merge_conflict_exit_total",
	Help: "Tasks routed off a DIRTY tatara-owned merge request at the merge gate, by repo and target stage.",
}, []string{"repo", "to"})

// ConflictSweepTotal counts what the G2 conflict sweep DID to each owned merge
// request it confirmed against the forge, one increment per confirmed
// candidate. It exists because GetMergeState had exactly two call sites before
// the sweep - the merge corridor, reachable only from `merged`, and the submit
// gate - so a merge request that went DIRTY anywhere else was invisible for as
// long as the Task stayed there (tatara-operator#625: CONFLICTING for days
// behind a parked Task, cleared by a human merging main in by hand).
//
// The vocabulary is exhaustive over one pass's outcomes for ONE merge request:
//
//	clean   the MIRROR said not-mergeable and the LIVE read disagreed. The
//	        mirror is written on MirrorCadence and is routinely minutes stale,
//	        so this is the EXPECTED steady state, not an anomaly.
//	dirty   confirmed DIRTY and deliberately not acted on: the Task is parked
//	        (there is exactly one way out of a park and this sweep is not it),
//	        or the rebase edge is not reachable from its state.
//	routed  confirmed DIRTY and handed to an agent at under-implementation.
//	capped  confirmed DIRTY and the conflict cycle ended at a PARK - the
//	        re-entry budget spent (merge-blocked), or part of mergeOrder
//	        already landed (merge-conflict).
//	error   the pass could not reach a verdict: the live read failed, or the
//	        forge credentials would not resolve, or a Task/MergeRequest List or
//	        the Repository lookup behind it failed, or the routing write did.
//	        The sweep FAILS OPEN and skips - one merge request on the per-item
//	        failures, the whole pass on the credential and List ones.
//
// There is no `repo` label: this counts sweep BEHAVIOUR, and the per-repo
// conflict story is already MergeConflictExitTotal's.
var ConflictSweepTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_conflict_sweep_total",
	Help: "Owned merge requests the G2 conflict sweep confirmed against the forge, by result (clean|dirty|routed|capped|error).",
}, []string{"result"})

// ClearMergeCursorStalled deletes every MergeCursorStalledSeconds series for a
// Task. Called when the Task leaves merging, for any reason.
func ClearMergeCursorStalled(task string) {
	MergeCursorStalledSeconds.DeletePartialMatch(prometheus.Labels{"task": task})
}

func init() {
	ctrlmetrics.Registry.MustRegister(UnexpectedMergeTotal, MergeCursorStalledSeconds,
		ReviewPostTotal, reviewFindingDegradedTotal, CIRedExitTotal, MergeConflictExitTotal,
		ConflictSweepTotal)
}
