package obs

import (
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

func init() {
	ctrlmetrics.Registry.MustRegister(OwnershipFlipTotal)
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
