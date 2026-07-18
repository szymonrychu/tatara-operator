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
// body naming WHICH self-proposal path produced it AND a content fingerprint of
// the body at filing time. The marker is the third factor of the auto-approve
// carve-out (approval_grammar.go): flag on AND the issue is bot-authored AND it
// carries a marker whose fingerprint still matches the body. It is invisible in
// rendered markdown and round-trips verbatim through the SCM mirror, so it is a
// durable, tamper-evident provenance signal.
//
// WHY THE FINGERPRINT: auto-approve trusts that the body it releases is the body
// tatara proposed. Today that holds because Task.Spec.Goal is frozen at mint and
// issues.edited webhooks are ignored, so a human body edit is invisible to the
// operator. A parallel workstream WILL wire issue-edit body refresh, at which
// point a human could edit a bot proposal's scope and the mirror would carry the
// edit into Status.Body. The fingerprint makes that fail closed: any content edit
// since filing breaks the match and auto-approve refuses, so the edited scope is
// never silently released.
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

// proposalMarkerRe matches ONLY the two known proposal kinds; the fingerprint
// group is optional at the regex level so ProposalKindFromBody still detects a
// (malformed) fingerprint-less marker, but ProposalBodyMatchesFingerprint treats
// an absent fingerprint as a refusal. An unknown kind yields no match, so both
// helpers FAIL CLOSED: a marker naming a kind the operator does not mint, or one
// carrying no fingerprint, cannot unlock auto-approve.
var proposalMarkerRe = regexp.MustCompile(
	`<!--\s*tatara-proposed-by:(brainstorm|incident)(?:\s+content-sha256:([0-9a-f]{64}))?\s*-->`)

// proposalMarker renders the marker for a kind and content fingerprint.
func proposalMarker(kind, fingerprint string) string {
	return "<!-- tatara-proposed-by:" + kind + " content-sha256:" + fingerprint + " -->"
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

// proposalFingerprintFromBody returns the content fingerprint carried by the
// marker, or "" when the marker is absent or carries none.
func proposalFingerprintFromBody(body string) string {
	m := proposalMarkerRe.FindStringSubmatch(body)
	if m == nil {
		return ""
	}
	return m[2]
}

// normalizeProposalContent canonicalises a proposal body before fingerprinting:
// CRLF -> LF (absorbs SCM newline normalisation) and both-ends whitespace trim
// (absorbs the marker's own leading separator and cosmetic edits). It is
// deliberately whitespace-insensitive and content-SENSITIVE: reindentation or a
// trailing-newline change does not break the fingerprint, but any interior text
// edit does.
func normalizeProposalContent(s string) string {
	return strings.TrimSpace(strings.ReplaceAll(s, "\r\n", "\n"))
}

// ComputeProposalContentHash is the fingerprint of a proposal body's content.
func ComputeProposalContentHash(content string) string {
	sum := sha256.Sum256([]byte(normalizeProposalContent(content)))
	return hex.EncodeToString(sum[:])
}

// StampProposalMarker prepends the provenance marker (kind + content fingerprint)
// to a proposed issue body, idempotently: a body that already carries a valid
// marker is returned unchanged. The marker leads the body so it survives a
// truncating mirror. The fingerprint is computed over the body BEFORE the marker
// is prepended, so ProposalBodyMatchesFingerprint can recover it by stripping the
// marker back out.
func StampProposalMarker(body, kind string) string {
	if ProposalKindFromBody(body) != "" {
		return body
	}
	return proposalMarker(kind, ComputeProposalContentHash(body)) + "\n\n" + body
}

// ProposalBodyMatchesFingerprint reports whether body still matches the content
// fingerprint its marker carries - i.e. the body has not been edited since it was
// filed. It FAILS CLOSED: no marker, no fingerprint, or a mismatch all return
// false. Stripping the marker back out recovers the original content, which
// normalizeProposalContent then canonicalises identically on both the stamp and
// the verify side.
func ProposalBodyMatchesFingerprint(body string) bool {
	fp := proposalFingerprintFromBody(body)
	if fp == "" {
		return false
	}
	stripped := proposalMarkerRe.ReplaceAllString(body, "")
	return ComputeProposalContentHash(stripped) == fp
}
