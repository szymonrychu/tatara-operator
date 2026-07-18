// Copyright 2026 tatara authors.

package v1alpha1

import (
	"strings"
	"testing"
)

func TestProposalKindFromBody(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{"brainstorm marker", "<!-- tatara-proposed-by:brainstorm -->\n\nfix the reaper", ProposalKindBrainstorm},
		{"incident marker", "<!-- tatara-proposed-by:incident -->\n\nalert fired", ProposalKindIncident},
		{"marker with extra whitespace", "<!--   tatara-proposed-by:incident   -->\nbody", ProposalKindIncident},
		{"marker mid-body", "context\n<!-- tatara-proposed-by:brainstorm -->\nmore", ProposalKindBrainstorm},
		{"no marker", "just a plain human-filed issue body", ""},
		{"unknown kind fails closed", "<!-- tatara-proposed-by:followup -->\nbody", ""},
		{"empty kind fails closed", "<!-- tatara-proposed-by: -->\nbody", ""},
		{"empty body", "", ""},
		{"lookalike text is not the marker", "tatara-proposed-by:incident (no comment)", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ProposalKindFromBody(tc.body); got != tc.want {
				t.Fatalf("ProposalKindFromBody(%q) = %q, want %q", tc.body, got, tc.want)
			}
		})
	}
}

func TestStampProposalMarkerRoundTrips(t *testing.T) {
	body := "implement the widget"
	stamped := StampProposalMarker(body, ProposalKindBrainstorm)
	if got := ProposalKindFromBody(stamped); got != ProposalKindBrainstorm {
		t.Fatalf("stamped body kind = %q, want %q", got, ProposalKindBrainstorm)
	}
}

func TestStampProposalMarkerIsIdempotent(t *testing.T) {
	once := StampProposalMarker("body", ProposalKindIncident)
	twice := StampProposalMarker(once, ProposalKindIncident)
	if once != twice {
		t.Fatalf("re-stamping changed the body:\n%q\n%q", once, twice)
	}
}

func TestProposalBodyMatchesAnchor(t *testing.T) {
	stamped := StampProposalMarker("implement the widget the way we discussed", ProposalKindBrainstorm)
	anchor := ComputeProposalContentHash(stamped)

	if !ProposalBodyMatchesAnchor(stamped, anchor) {
		t.Fatal("an unmodified stamped body did not match its own anchor")
	}

	// A cosmetic edit (CRLF, trailing newline) must NOT break the match.
	crlf := strings.ReplaceAll(stamped, "\n", "\r\n") + "\r\n\r\n"
	if !ProposalBodyMatchesAnchor(crlf, anchor) {
		t.Fatal("a whitespace-only reformat broke the anchor (should be content-only)")
	}

	// A content edit (marker preserved) MUST break the match.
	edited := stamped + "\n\nand also delete the production database"
	if ProposalBodyMatchesAnchor(edited, anchor) {
		t.Fatal("an edited body still matched its filing-time anchor")
	}

	// A marker-rewrite attack that recomputes a hash of the edited body cannot
	// help: the anchor of record is the CR field, not anything in the body.
	if ProposalBodyMatchesAnchor(edited, ComputeProposalContentHash(edited)) == false {
		// (sanity: the edited body of course matches ITS OWN hash)
		t.Fatal("sanity check failed")
	}
	if ProposalBodyMatchesAnchor(edited, anchor) {
		t.Fatal("a recomputed body hash must not override the filing-time anchor")
	}

	// Empty anchor (older build filed no anchor) fails closed.
	if ProposalBodyMatchesAnchor(stamped, "") {
		t.Fatal("an empty anchor matched")
	}
}
