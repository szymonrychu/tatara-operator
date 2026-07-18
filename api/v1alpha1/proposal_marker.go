// Copyright 2026 tatara authors.

package v1alpha1

import "regexp"

// The provenance surface for autoApproveTataraProposals (project_types.go). A
// tatara-proposed issue - a brainstorm proposal or an incident tracker issue the
// operator filed under the bot account - carries an HTML-comment marker in its
// body naming WHICH self-proposal path produced it. The marker is the third,
// mandatory factor of the auto-approve carve-out (approval_grammar.go): flag on
// AND the issue is bot-authored AND it carries this marker. It is invisible in
// rendered markdown and round-trips verbatim through the SCM mirror, so it is a
// durable provenance signal, not a transient one.
const (
	// AutoApproveLogin is the sentinel Login stamped on ApprovalEvidence when the
	// autoApproveTataraProposals path grants approval (CommentID is then empty).
	// It is NOT a real forge account and can never be a maintainer, so it can
	// never satisfy the human approval grammar by accident.
	AutoApproveLogin = "<tatara:auto>"

	// ProposalKindBrainstorm marks an issue a brainstorm submit_outcome propose
	// filed. ProposalKindIncident marks an incident file_issue tracker issue.
	ProposalKindBrainstorm = "brainstorm"
	ProposalKindIncident   = "incident"
)

// proposalMarkerRe matches ONLY the two known proposal kinds. An unknown kind
// yields no match, so ProposalKindFromBody FAILS CLOSED: a marker naming a kind
// the operator does not mint cannot unlock auto-approve.
var proposalMarkerRe = regexp.MustCompile(`<!--\s*tatara-proposed-by:(brainstorm|incident)\s*-->`)

// ProposalMarker renders the provenance marker for a proposal kind.
func ProposalMarker(kind string) string {
	return "<!-- tatara-proposed-by:" + kind + " -->"
}

// ProposalKindFromBody returns the proposal kind named by the marker in body, or
// "" when no valid marker is present. It is the auto-approve carve-out's marker
// factor and MUST fail closed: an absent or malformed marker returns "".
func ProposalKindFromBody(body string) string {
	m := proposalMarkerRe.FindStringSubmatch(body)
	if m == nil {
		return ""
	}
	return m[1]
}

// StampProposalMarker prepends the provenance marker for kind to a proposed issue
// body, idempotently: a body that already carries a valid marker is returned
// unchanged. The marker leads the body so it survives a truncating mirror.
func StampProposalMarker(body, kind string) string {
	if ProposalKindFromBody(body) != "" {
		return body
	}
	return ProposalMarker(kind) + "\n\n" + body
}
