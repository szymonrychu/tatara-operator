package v1alpha1

import (
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
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
			name: "LifecycleHooks json keys camelCase",
			marshal: func() ([]byte, error) {
				return json.Marshal(LifecycleHooks{
					PreClone:             "a",
					PostClone:            "b",
					ConversationStart:    "c",
					ConversationRestart:  "d",
					AgentTurnFinished:    "e",
					ConversationFinished: "f",
				})
			},
			want: `"preClone":"a","postClone":"b","conversationStart":"c","conversationRestart":"d","agentTurnFinished":"e","conversationFinished":"f"`,
		},
		{
			name: "AgentSpec.Hooks json key hooks",
			marshal: func() ([]byte, error) {
				return json.Marshal(AgentSpec{Hooks: &LifecycleHooks{PreClone: "x"}})
			},
			want: `"hooks":{"preClone":"x"}`,
		},
		{
			name: "AgentSpec.ExtraEnvs json key extraEnvs",
			marshal: func() ([]byte, error) {
				return json.Marshal(AgentSpec{ExtraEnvs: []corev1.EnvVar{{Name: "FOO", Value: "bar"}}})
			},
			want: `"extraEnvs":[{"name":"FOO","value":"bar"}]`,
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
			if !strings.Contains(string(b), tc.want) {
				t.Fatalf("json %s does not contain %s", b, tc.want)
			}
		})
	}
}
