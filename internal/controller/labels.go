package controller

import (
	"errors"

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

// NOTE: the former thirdPartyAuthor autoapprove tier (issue #56) was removed
// when the maintainer-approval gate landed: third-party authorship is no
// longer a release signal by itself. Three paths now release a front-half
// issue to implement, all recorded on Status.ApprovedByMaintainer: (a) a
// MaintainerLogins member applying the approved label, (b) a verified
// maintainer conversational go-ahead, (c) auto-approve (item 4a) - a
// bot-authored, tatara-proposed issue under an explicit per-project flag,
// where the brainstorm/incident investigation itself served as the review.
// Author-based intake gating still lives in IsAllowedReporter/IsTrustedAuthor;
// neither releases implementation on its own.
