// Copyright 2026 tatara authors.

package v1alpha1

import "testing"

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
