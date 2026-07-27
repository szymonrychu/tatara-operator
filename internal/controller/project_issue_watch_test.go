package controller

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestProposalPendingChanged(t *testing.T) {
	tests := []struct {
		name     string
		old, new tatarav1alpha1.Issue
		want     bool
	}{
		{"approved leaves the pending set",
			iss("brainstorm", "open", "new", "r1"), iss("brainstorm", "open", "approved", "r1"), true},
		{"closed leaves the pending set",
			iss("brainstorm", "open", "new", "r1"), iss("brainstorm", "closed", "new", "r1"), true},
		{"rejected leaves the pending set",
			iss("brainstorm", "open", "new", "r1"), iss("brainstorm", "open", "rejected", "r1"), true},
		{"a mirror resync with no verdict change is not an event",
			iss("brainstorm", "open", "new", "r1"), iss("brainstorm", "open", "new", "r1"), false},
		{"a new proposal ENTERING the pending set is also an event (it changes the level)",
			iss("brainstorm", "closed", "new", "r1"), iss("brainstorm", "open", "new", "r1"), true},
		{"an incident issue is never a brainstorm event",
			iss("incident", "open", "new", "r1"), iss("incident", "closed", "new", "r1"), false},
		{"an unstamped issue that gets backfilled to brainstorm is an event",
			iss("", "open", "new", "r1"), iss("brainstorm", "open", "new", "r1"), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o, n := tc.old, tc.new
			if got := proposalPendingChanged(&o, &n); got != tc.want {
				t.Fatalf("proposalPendingChanged = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIssueToProject(t *testing.T) {
	in := iss("brainstorm", "open", "new", "r1")
	got := issueToProject(t.Context(), client.Object(&in))
	if len(got) != 1 {
		t.Fatalf("want 1 request, got %d", len(got))
	}
	if got[0].Name != "demo" || got[0].Namespace != "tatara" {
		t.Fatalf("request = %v, want tatara/demo", got[0])
	}
	// A non-proposal Issue must not wake the Project reconciler at all.
	other := iss("", "open", "new", "r1")
	if n := len(issueToProject(t.Context(), client.Object(&other))); n != 0 {
		t.Fatalf("want 0 requests for an unstamped issue, got %d", n)
	}
}
