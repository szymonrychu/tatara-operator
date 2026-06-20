package v1alpha1

import (
	"encoding/json"
	"testing"
)

func TestNewFields_JSONRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		marshal func() ([]byte, error)
		want    string
	}{
		{
			name: "AgentSpec.Effort json key effort",
			marshal: func() ([]byte, error) {
				return json.Marshal(AgentSpec{Effort: "max"})
			},
			want: `"effort":"max"`,
		},
		{
			name: "TaskSource.Title json key title",
			marshal: func() ([]byte, error) {
				return json.Marshal(TaskSource{Provider: "github", IssueRef: "o/r#1", Title: "fix the thing"})
			},
			want: `"title":"fix the thing"`,
		},
		{
			name: "ProposedIssueSpec.SystemicID json key systemicId",
			marshal: func() ([]byte, error) {
				return json.Marshal(ProposedIssueSpec{RepositoryRef: "r", Title: "t", Body: "b", Kind: "bug", SystemicID: "abc123"})
			},
			want: `"systemicId":"abc123"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, err := tc.marshal()
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if !jsonContains(string(b), tc.want) {
				t.Fatalf("json %s does not contain %s", b, tc.want)
			}
		})
	}
}

func jsonContains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}
