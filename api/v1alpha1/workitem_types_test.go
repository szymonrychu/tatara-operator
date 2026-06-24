package v1alpha1

import (
	"encoding/json"
	"testing"
)

func TestWorkItemRef_Constants(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"RoleProposed", RoleProposed, "proposed"},
		{"RoleSource", RoleSource, "source"},
		{"RoleCloses", RoleCloses, "closes"},
		{"RoleOpenedPR", RoleOpenedPR, "openedPR"},
		{"RoleReviewed", RoleReviewed, "reviewed"},
		{"WorkItemIssue", WorkItemIssue, "issue"},
		{"WorkItemPR", WorkItemPR, "pr"},
		{"WIProposed", WIProposed, "proposed"},
		{"WIApproved", WIApproved, "approved"},
		{"WIDeclined", WIDeclined, "declined"},
		{"WIImplemented", WIImplemented, "implemented"},
		{"WIOpen", WIOpen, "open"},
		{"WIClosed", WIClosed, "closed"},
		{"WIMerged", WIMerged, "merged"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("want %q got %q", tc.want, tc.got)
			}
		})
	}
}

func TestWorkItemRef_OmitEmpty(t *testing.T) {
	ref := WorkItemRef{
		Provider: "github",
		Repo:     "o/r",
		Kind:     WorkItemIssue,
		Role:     RoleSource,
	}
	b, err := json.Marshal(ref)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	for _, key := range []string{`"number"`, `"state"`, `"headSHA"`, `"title"`, `"lastRefreshedAt"`} {
		if wiContains(s, key) {
			t.Errorf("zero-value field %s must be omitted, got: %s", key, s)
		}
	}
}

func wiContains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
