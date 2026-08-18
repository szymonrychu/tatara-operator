package obs

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// OwnershipFlipTotal counts MR ownership transitions (MR ownership design
// 2026-07-19), by direction and reason. Not to be confused with
// own_metrics.go, which covers controller owner-refs (contract B.2); this
// file is about who last pushed a MergeRequest's head (tatara vs. an
// external human), an unrelated notion of "ownership". Labels:
//
//	direction: to-tatara (external -> tatara, a gated takeover) or
//	           to-external (tatara -> external, an unattributable human push)
//	reason:    takeover (maintainer-gated comment) or external-push (head drift)
//
// The initial classification of a never-seen MR is NOT a flip and is not
// counted. Not on the tatara-observability allowlist yet - see the companion
// observability follow-up tracked in ROADMAP.md.
var OwnershipFlipTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_mr_ownership_flip_total",
	Help: "MergeRequest ownership flips by direction (to-tatara|to-external) and reason (takeover|external-push).",
}, []string{"direction", "reason"})

// OwnershipDeclineUpgradedTotal counts the ONE park upgrade the flip performs:
// a kind=takeover Task found already parked implement-declined when its merge
// request went back to the human, re-stamped ownership-lost so the re-take and
// DrainStandDownMerge can still reach it (#604 review, see
// stage.UpgradeDeclineToOwnershipLost).
//
// It is the ordinary stand-down arriving AGENT-FIRST: the pod's divergence
// signal is a local git call, the operator's flip waits on a webhook. This rate
// is the observable answer to "how often does that race land the wrong way
// round", which is otherwise invisible - both orders converge on the same final
// state by design, and only this counter distinguishes them. A counter with no
// labels needs no pre-seed: registering it exposes the zero baseline.
var OwnershipDeclineUpgradedTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "operator_mr_ownership_decline_upgraded_total",
	Help: "Takeover Tasks whose implement-declined park was upgraded to ownership-lost by a flip to external.",
})

// OwnershipHumanAssistTotal counts the head drifts that are NOT stand-downs: a
// commit from outside tatara landing on a merge request the platform bot itself
// AUTHORED. That is a human helping the bot's own pull request, not a human
// taking their own pull request back, and ReconcileOwnership retains ownership
// instead of flipping (see its bot-authored carve-out).
//
// It is the observable half of a decision that is otherwise invisible: the flip
// counter deliberately does NOT move here, so without this series a suppressed
// stand-down looks exactly like a head drift that never happened. A rising rate
// with stuck Tasks would mean the carve-out is too wide.
//
// IT CARRIES (repo, number) BECAUSE A LABEL-LESS COUNT CANNOT DRIVE AN
// INVESTIGATION. The event this counts is a foreign commit entering a branch the
// operator will merge unconditionally (mergeAllowedForOwnership accepts `tatara`
// outright), so "some assist happened somewhere" is not an answer - the alert
// that fires has to name the merge request to look at. Cardinality is bounded in
// practice by how rare the event is: it increments once per DISTINCT assisted
// head, on a merge request that already has a Task driving it, and the platform
// sees a handful a week. There is no pre-seed because the label values are not
// known until an assist happens; the series is absent, not zero, until then.
var OwnershipHumanAssistTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_mr_ownership_human_assist_total",
	Help: "Non-bot head drifts on bot-authored merge requests, where ownership is retained instead of standing down, by repo and merge request number.",
}, []string{"repo", "number"})

// OwnershipHumanAssist increments operator_mr_ownership_human_assist_total for
// one merge request.
func OwnershipHumanAssist(repo string, number int) {
	OwnershipHumanAssistTotal.WithLabelValues(repo, strconv.Itoa(number)).Inc()
}

// OwnershipHumanAssistCounter returns the counter for (repo, number) for tests.
func OwnershipHumanAssistCounter(repo string, number int) prometheus.Counter {
	return OwnershipHumanAssistTotal.WithLabelValues(repo, strconv.Itoa(number))
}

func init() {
	ctrlmetrics.Registry.MustRegister(OwnershipFlipTotal, OwnershipDeclineUpgradedTotal,
		OwnershipHumanAssistTotal)
	// Pre-seed the two real flip label sets so a healthy operator exposes a zero
	// baseline from startup (metric-wiring audit convention, issue #370).
	OwnershipFlipTotal.WithLabelValues("to-tatara", "takeover")
	OwnershipFlipTotal.WithLabelValues("to-external", "external-push")
}

// OwnershipFlip increments operator_mr_ownership_flip_total.
func OwnershipFlip(direction, reason string) {
	OwnershipFlipTotal.WithLabelValues(direction, reason).Inc()
}

// OwnershipFlipCounter returns the counter for (direction, reason) for tests.
func OwnershipFlipCounter(direction, reason string) prometheus.Counter {
	return OwnershipFlipTotal.WithLabelValues(direction, reason)
}
