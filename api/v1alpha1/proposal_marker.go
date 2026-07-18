// Copyright 2026 tatara authors.

package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

// The provenance surface for autoApproveTataraProposals (project_types.go). A
// tatara-proposed issue - a brainstorm proposal or an incident tracker issue the
// operator filed under the bot account - carries an HTML-comment marker in its
// body naming WHICH self-proposal path produced it. The marker is provenance
// (kind detection, metric label) and NOTHING MORE: it lives in the forge-editable
// issue body, so it is not, and must not be, the integrity record.
//
// The integrity record is IssueSpec.ProposalBodyHash - a hash of the body as
// FILED, written ONCE by the operator at mintIssueCR time and never again. The
// mirror only ever writes Issue.Status (issue_types.go), so nothing SCM-side can
// reach the Spec anchor. autoApproveApplies compares the current mirrored
// Status.Body against that anchor: any divergence - a scope edit, a marker
// rewrite, even a recomputed-hash forgery attempt - fails to match, because the
// attacker cannot rewrite the CR Spec from the forge. This catches ALL
// post-filing body changes, not just the naive ones.
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
// the operator does not mint cannot supply provenance.
var proposalMarkerRe = regexp.MustCompile(`<!--\s*tatara-proposed-by:(brainstorm|incident)\s*-->`)

// proposalMarker renders the provenance marker for a kind.
func proposalMarker(kind string) string {
	return "<!-- tatara-proposed-by:" + kind + " -->"
}

// ProposalKindFromBody returns the proposal kind named by the marker in body, or
// "" when no valid marker is present. It supplies the auto-approve carve-out's
// provenance factor and MUST fail closed: an absent or malformed marker returns "".
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
	return proposalMarker(kind) + "\n\n" + body
}

// normalizeProposalContent canonicalises a proposal body before hashing: CRLF ->
// LF (absorbs SCM newline normalisation) and both-ends whitespace trim (absorbs
// cosmetic edits). It is deliberately whitespace-insensitive and content-
// SENSITIVE: reindentation or a trailing-newline change does not break the
// anchor, but any interior text edit does.
func normalizeProposalContent(s string) string {
	return strings.TrimSpace(strings.ReplaceAll(s, "\r\n", "\n"))
}

// ComputeProposalContentHash is the integrity anchor for a proposal body: the
// hash the operator writes to IssueSpec.ProposalBodyHash at filing time and
// re-derives from Status.Body at verification time.
func ComputeProposalContentHash(body string) string {
	sum := sha256.Sum256([]byte(normalizeProposalContent(body)))
	return hex.EncodeToString(sum[:])
}

// ProposalBodyMatchesAnchor reports whether body still matches the filing-time
// anchor (IssueSpec.ProposalBodyHash). It FAILS CLOSED: an empty anchor (a
// proposal filed by an older build that did not record one) or any content
// divergence returns false. The anchor is unforgeable from the forge because
// nothing SCM-side writes CR Spec.
func ProposalBodyMatchesAnchor(body, anchor string) bool {
	if anchor == "" {
		return false
	}
	return ComputeProposalContentHash(body) == anchor
}
