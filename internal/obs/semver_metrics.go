package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// SemverLabelTotal counts projections of MergeRequest.status.significance onto
// the forge PR's semver:<level> label (contract H.4), by repo, level and result:
//
//	applied - the label was written: a first projection, or a RAISE that replaced
//	          the level it escalated from
//	error   - the forge write failed; the reconciler retries
//
// THE RELEASE TRAIN RUNS ON THIS LABEL. CI cuts the release tag from it.
var SemverLabelTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_semver_label_total",
	Help: "semver:<level> label projections onto a forge PR, by repo, level and result (contract H.4).",
}, []string{"repo", "level", "result"})

// SemverLabelMissingTotal counts MergeRequests the operator MERGED carrying NO
// declared change significance, by repo. Any non-zero value is a BROKEN RELEASE
// TRAIN: with no semver:<level> label, CI cuts no tag, nothing is published, no
// version pin propagates, tatara-helmfile applies nothing, deployedAt is never
// stamped, and the Task sits in deploying until its budget parks it.
//
// /outcome REQUIRES changeSignificance on action=submitted, so this counter can
// only move on an operator bug or a hand-mutated MergeRequest. It exists so that
// the wedge is VISIBLE instead of silent.
var SemverLabelMissingTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_semver_label_missing_total",
	Help: "MergeRequests merged with no declared change significance, by repo. Non-zero = CI cuts no release tag (contract H.4).",
}, []string{"repo"})

func init() {
	ctrlmetrics.Registry.MustRegister(SemverLabelTotal, SemverLabelMissingTotal)
}
