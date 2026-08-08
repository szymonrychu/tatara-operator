package controller

import (
	"context"
	"errors"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// isPermanentTargetGone reports whether err is an SCM HTTPError meaning the
// target resource is permanently unreachable: 410 Gone (GitHub "This issue was
// deleted") or 404 Not Found. Retrying a write against such a target is futile,
// so a lifecycle reconcile must treat it as terminal (log + skip) instead of
// returning the error and letting controller-runtime requeue the same doomed
// write forever (issue #263: a deleted issue drove an unbounded add_label retry
// loop that amplified operator_scm_writes_total{result="error"} and fired the
// SCM write-failure-ratio alert). Transient 4xx (429/403 rate limits) and 5xx
// stay retryable and are NOT matched here.
func isPermanentTargetGone(err error) bool {
	var he *scm.HTTPError
	if errors.As(err, &he) {
		return he.Status == 404 || he.Status == 410
	}
	return false
}

// isShutdownCancellation reports whether err is the manager's OWN shutdown
// reaching a reconcile mid-flight, rather than a failure worth an ERROR line.
//
// Issue #538: controller-runtime logs every non-nil Reconcile error as
// ERROR msg="Reconciler error", and the Loki rule "Tatara operator error
// recurring" counts ERROR lines regardless of cause. On SIGTERM the manager
// cancels the reconcile context, every in-flight SCM/API call aborts with
// context.Canceled, and the reconciler returned it raw - so a rolling restart
// that cancelled >=2 in-flight calls deterministically fired the alert. It had
// fired 49 times. Nothing was ever wrong: the cancelled work is re-reconciled
// by the informer resync of whichever replica takes the lease next.
//
// BOTH HALVES ARE LOAD-BEARING and this is deliberately NOT a bare
// errors.Is(err, context.Canceled):
//
//   - The reconcile ctx must itself be cancelled. controller-runtime derives it
//     from the manager context and cancels it ONLY when the manager stops, so a
//     cancelled ctx IS the shutdown signal. Without this clause any per-call
//     cancellation (a caller-scoped context, an aborted sub-request) on a
//     perfectly healthy manager would be silently swallowed.
//   - The error must carry the cancellation. Without this clause a genuine
//     failure that merely happened to land during the shutdown window - an
//     etcd timeout, an SCM 500 - would be swallowed too.
//
// context.DeadlineExceeded is deliberately NOT matched: a timeout is a real
// failure whoever's clock it was.
func isShutdownCancellation(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		return false
	}
	return errors.Is(err, context.Canceled)
}

// classifyReconcileShutdown is the Reconcile-boundary classifier every
// reconciler in this package returns through. A shutdown cancellation is
// downgraded to (empty result, nil) plus one INFO line; everything else passes
// through untouched.
//
// The empty result is correct rather than merely convenient: a requeue asked for
// by a reconcile that is being cancelled is never serviced, because the work
// queue is shutting down with it.
func classifyReconcileShutdown(ctx context.Context, controller, name string,
	res ctrl.Result, err error) (ctrl.Result, error) {

	if !isShutdownCancellation(ctx, err) {
		return res, err
	}
	log.FromContext(ctx).Info("reconcile aborted by manager shutdown; not an error",
		"action", "reconcile_shutdown_cancel", "controller", controller,
		"resource_id", name, "cause", err.Error())
	return ctrl.Result{}, nil
}

// RecordSCM records the outcome of one SCM call on operator_scm_writes_total
// (+ operator_scm_request_errors_by_status_total on failure). m may be nil in
// tests. A permanently-gone target (404/410) is result="gone", not "error":
// a deleted issue is terminal, not a write failure, and counting it as an error
// inflated the write-failure-ratio alert against a single deleted issue (#268).
func RecordSCM(m *obs.OperatorMetrics, provider, verb string, err error) {
	if m == nil {
		return
	}
	if err == nil {
		m.SCMWrite(provider, verb, "ok")
		return
	}
	result := "error"
	if isPermanentTargetGone(err) {
		result = "gone"
	}
	m.SCMWrite(provider, verb, result)
	m.SCMRequestErrorByStatus(provider, verb, scm.ErrorStatus(err))
}

// lifecycleLabels returns the four managed phase labels (brainstorming/approved/
// implementation/declined), applying defaults when a field is empty.
func lifecycleLabels(s *tatarav1alpha1.ScmSpec) (brainstorming, approved, implementation, declined string) {
	brainstorming, approved, implementation, declined =
		"tatara-brainstorming", "tatara-approved", "tatara-implementation", "tatara-declined"
	if s == nil {
		return
	}
	if s.BrainstormingLabel != "" {
		brainstorming = s.BrainstormingLabel
	}
	if s.ApprovedLabel != "" {
		approved = s.ApprovedLabel
	}
	if s.ImplementationLabel != "" {
		implementation = s.ImplementationLabel
	}
	if s.DeclinedLabel != "" {
		declined = s.DeclinedLabel
	}
	return
}

// LifecycleLabels is the exported form of lifecycleLabels: the webhook's WS3
// trigger-label mint reads the approved/declined projection labels to exclude
// them, so a lifecycle-projection write can never self-trigger a mint.
func LifecycleLabels(s *tatarav1alpha1.ScmSpec) (brainstorming, approved, implementation, declined string) {
	return lifecycleLabels(s)
}

// incidentLabel returns the additive label for incident-originated proposals.
// It is NOT a managed phase label (never swept by setLifecycleLabel).
func incidentLabel(s *tatarav1alpha1.ScmSpec) string {
	if s != nil && s.IncidentLabel != "" {
		return s.IncidentLabel
	}
	return "tatara-incident"
}

// semver:* labels mark a PR's declared change significance for the push-CD
// cascade (cd-release keys the next tag off them). Additive palette, NOT phase
// labels: they MUST stay out of managedPhaseLabels/activePhaseLabels so
// setLifecycleLabel never strips them.
const (
	semverLabelMajor = "semver:major"
	semverLabelMinor = "semver:minor"
	semverLabelPatch = "semver:patch"
)

// managedLabelColors maps each managed tatara label (resolving any custom names
// from ScmSpec) to its hex color (6 digits, no '#'), for EnsureLabel.
func managedLabelColors(s *tatarav1alpha1.ScmSpec) map[string]string {
	b, a, i, d := lifecycleLabels(s)
	out := map[string]string{
		b:                "1d76db", // brainstorming - blue
		a:                "0e8a16", // approved - green
		i:                "fbca04", // implementation - yellow
		d:                "9e9e9e", // declined - gray
		incidentLabel(s): "d73a4a", // incident - red
	}
	// The semver palette is owned by the H.4 projection (semverLabelColors), which
	// EnsureLabels each level as it applies it. Folding the SAME table in here -
	// rather than restating the colours - is what stops the pre-coloured label and
	// the projected one drifting to two different colours.
	for label, color := range semverLabelColors {
		out[label] = color
	}
	return out
}

// NOTE: the former thirdPartyAuthor autoapprove tier (issue #56) and the label-
// driven approval it fed were both removed. Approval is now COMMENT-ONLY (C.6):
// there is no label -> status path and no Status.ApprovedByMaintainer field - a
// label is a one-way PROJECTION of Issue.Status.Status, never an input. TWO paths
// move an Issue to Status=approved, and each writes Issue.Status.Approval
// (ApprovalEvidence), never a label: (a) the AGENT-JUDGED CITATION GATE - the
// clarify agent reads the thread, decides for itself that a maintainer comment
// approves, and CITES it; the operator verifies only WHO wrote the cited comment
// and that the quote really occurs in the body it holds, never intent, and never
// recency (verifyOneIssue, approval_grammar.go). There is NO wordlist and no
// most-recent-comment rule; both were deleted with the approval wordlist. Recorded
// with the maintainer login + the consumed commentId; (b) auto-approve
// (autoApproveTataraProposals) - a bot-authored, tatara-proposed issue
// (tatara-proposed-by marker) under the per-project flag,
// where the brainstorm/incident investigation itself served as the review,
// recorded with Auto=true and the "<tatara:auto>" sentinel. Path (b) fires ONLY
// when NO maintainer has commented; a maintainer non-approval comment falls to
// path (a) and blocks. Author-based intake gating still lives in
// IsAllowedReporter/IsTrustedAuthor; neither releases implementation on its own.
