package agent

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestModelForKind(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Agent.Model = "claude-opus-4-8"
	proj.Spec.Agent.ModelByKind = map[string]string{
		"review":      "claude-sonnet-5",
		"triageIssue": "claude-sonnet-5",
	}
	cases := []struct {
		name, kind, want string
	}{
		{"override present review", "review", "claude-sonnet-5"},
		{"override present triage", "triageIssue", "claude-sonnet-5"},
		{"override absent falls back", "implement", "claude-opus-4-8"},
		{"unknown kind falls back", "bogus", "claude-opus-4-8"},
		{"empty kind falls back", "", "claude-opus-4-8"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := modelForKind(proj, tc.kind); got != tc.want {
				t.Fatalf("modelForKind(%q) = %q, want %q", tc.kind, got, tc.want)
			}
		})
	}
}

func TestModelForKind_NilAndEmptyOverride(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Agent.Model = "claude-opus-4-8"
	if got := modelForKind(proj, "review"); got != "claude-opus-4-8" {
		t.Fatalf("nil map: modelForKind = %q, want claude-opus-4-8", got)
	}
	proj.Spec.Agent.ModelByKind = map[string]string{"review": ""}
	if got := modelForKind(proj, "review"); got != "claude-opus-4-8" {
		t.Fatalf("empty override treated as set: modelForKind = %q, want claude-opus-4-8", got)
	}
}

func TestEffortForKind(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Agent.Effort = "high"
	proj.Spec.Agent.EffortByKind = map[string]string{
		"review":      "medium",
		"triageIssue": "low",
	}
	cases := []struct {
		name, kind, want string
	}{
		{"override present review", "review", "medium"},
		{"override present triage", "triageIssue", "low"},
		{"override absent falls back", "implement", "high"},
		{"unknown kind falls back", "bogus", "high"},
		{"empty kind falls back", "", "high"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effortForKind(proj, tc.kind); got != tc.want {
				t.Fatalf("effortForKind(%q) = %q, want %q", tc.kind, got, tc.want)
			}
		})
	}
}

func TestEffortForKind_NilAndEmptyOverride(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Agent.Effort = "high"
	if got := effortForKind(proj, "review"); got != "high" {
		t.Fatalf("nil map: effortForKind = %q, want high", got)
	}
	proj.Spec.Agent.EffortByKind = map[string]string{"review": ""}
	if got := effortForKind(proj, "review"); got != "high" {
		t.Fatalf("empty override treated as set: effortForKind = %q, want high", got)
	}
}

func TestBuildPod_ModelEffortByKind(t *testing.T) {
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "tatara"},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "demo-scm",
			Agent: tatarav1alpha1.AgentSpec{
				Model:              "claude-opus-4-8",
				Effort:             "high",
				Image:              "wrapper:1",
				PermissionMode:     "bypassPermissions",
				TurnTimeoutSeconds: 1800,
				ModelByKind: map[string]string{
					"review":      "claude-sonnet-5",
					"triageIssue": "claude-sonnet-5",
				},
				EffortByKind: map[string]string{
					"review":      "medium",
					"triageIssue": "low",
				},
			},
		},
	}
	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "repo1", Namespace: "tatara"},
		Spec:       tatarav1alpha1.RepositorySpec{URL: "https://git/acme/repo1", DefaultBranch: "main"},
	}
	cfg := PodConfig{
		Namespace:           "tatara",
		CallbackURL:         "http://tatara-operator-internal.tatara.svc:8082",
		OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
		AnthropicSecretName: "anthropic",
		CLIOIDCSecretName:   "tatara-cli-oidc",
		OperatorURL:         "http://tatara-operator.tatara.svc:8080",
	}
	cases := []struct {
		kind, wantModel, wantEffort string
	}{
		{"review", "claude-sonnet-5", "medium"},
		{"triageIssue", "claude-sonnet-5", "low"},
		{"implement", "claude-opus-4-8", "high"},
		{"brainstorm", "claude-opus-4-8", "high"},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			task := &tatarav1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "task-1", Namespace: "tatara", UID: "uid-1"},
				Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "demo", RepositoryRef: "repo1", Goal: "g", Kind: tc.kind},
			}
			env := BuildPod(proj, repo, task, nil, "http://mem.tatara.svc:8080", cfg).Spec.Containers[0].Env
			model, ok := envValue(env, "MODEL")
			require.True(t, ok, "MODEL env present for kind %q", tc.kind)
			require.Equal(t, tc.wantModel, model, "MODEL for kind %q", tc.kind)
			effort, ok := envValue(env, "EFFORT")
			require.True(t, ok, "EFFORT env present for kind %q", tc.kind)
			require.Equal(t, tc.wantEffort, effort, "EFFORT for kind %q", tc.kind)
		})
	}
}
