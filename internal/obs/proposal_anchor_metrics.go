// Copyright 2026 tatara authors.

package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// AnchorlessProposals is the count of OPEN, bot-authored Issue CRs whose body
// carries a valid tatara-proposed-by marker but whose Spec carries NO
// ProposalBodyHash - the auto-approve integrity anchor.
//
// THIS IS A STUCK COUNT, NOT AN ERROR RATE. Such an issue is permanently
// unworkable autonomously: ProposalBodyMatchesAnchor fails closed on an empty
// anchor so autoApproveApplies can never grant, and effectiveProposalKind refuses
// it too, so it does not even count against the backlog target whose slot it
// occupies. Nothing repairs it: the anchor is written once, at filing time, from
// the body the operator itself posted, and deriving one from the MIRRORED body
// would anchor the body to itself and delete the tamper guard.
//
// It is deliberately separate from operator_auto_approve_refused_total's
// axis="anchor-mismatch". That counter fires at the DECISION and folds two
// opposite causes together - a body that diverged (the guard working) and an
// anchor that was never written (this) - and it only fires for projects with the
// carve-out flag ON. This gauge is unconditional and is the one that answers "how
// many proposals are structurally stuck right now".
//
// A nonzero LEVEL is the alertable thing; a RISING level means a filing path
// regressed (restapi.mintIssueCR's adopt branch) and new proposals are joining
// the stuck set.
var AnchorlessProposals = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "operator_anchorless_proposals",
	Help: "Open bot-authored proposal issues carrying no auto-approve integrity anchor, by project. These can never auto-approve and never count toward the brainstorm target.",
}, []string{"project"})

func init() {
	ctrlmetrics.Registry.MustRegister(AnchorlessProposals)
}
