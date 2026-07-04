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
			if got := modelForKind(proj, tc.kind, ""); got != tc.want {
				t.Fatalf("modelForKind(%q) = %q, want %q", tc.kind, got, tc.want)
			}
		})
	}
}

func TestModelForKind_NilAndEmptyOverride(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Agent.Model = "claude-opus-4-8"
	if got := modelForKind(proj, "review", ""); got != "claude-opus-4-8" {
		t.Fatalf("nil map: modelForKind = %q, want claude-opus-4-8", got)
	}
	proj.Spec.Agent.ModelByKind = map[string]string{"review": ""}
	if got := modelForKind(proj, "review", ""); got != "claude-opus-4-8" {
		t.Fatalf("empty override treated as set: modelForKind = %q, want claude-opus-4-8", got)
	}
}

func TestModelForKind_Exported(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Agent.Model = "claude-opus-4-8"
	proj.Spec.Agent.ModelByKind = map[string]string{"review": "claude-sonnet-5"}
	if got, want := ModelForKind(proj, "review", ""), modelForKind(proj, "review", ""); got != want {
		t.Fatalf("ModelForKind(review) = %q, want %q", got, want)
	}
	nilProj := &tatarav1alpha1.Project{}
	nilProj.Spec.Agent.Model = "claude-haiku-4-5"
	if got, want := ModelForKind(nilProj, "implement", ""), modelForKind(nilProj, "implement", ""); got != want {
		t.Fatalf("ModelForKind(implement, nil map) = %q, want %q", got, want)
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
			if got := effortForKind(proj, tc.kind, ""); got != tc.want {
				t.Fatalf("effortForKind(%q) = %q, want %q", tc.kind, got, tc.want)
			}
		})
	}
}

func TestEffortForKind_NilAndEmptyOverride(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Agent.Effort = "high"
	if got := effortForKind(proj, "review", ""); got != "high" {
		t.Fatalf("nil map: effortForKind = %q, want high", got)
	}
	proj.Spec.Agent.EffortByKind = map[string]string{"review": ""}
	if got := effortForKind(proj, "review", ""); got != "high" {
		t.Fatalf("empty override treated as set: effortForKind = %q, want high", got)
	}
}

// TestModelForKind_HealthCheckActivity asserts a healthCheck-activity task
// (Kind=brainstorm) resolves against the "healthCheck" pseudo-key in
// ModelByKind/EffortByKind before falling back to the "brainstorm" kind entry
// or the project default, splitting healthCheck's tier from brainstorm's.
func TestModelForKind_HealthCheckActivity(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Agent.Model = "claude-opus-4-8"
	proj.Spec.Agent.Effort = "high"
	proj.Spec.Agent.ModelByKind = map[string]string{
		"healthCheck": "claude-sonnet-5",
		"brainstorm":  "claude-opus-4-8",
	}
	proj.Spec.Agent.EffortByKind = map[string]string{
		"healthCheck": "medium",
		"brainstorm":  "high",
	}

	if got := modelForKind(proj, "brainstorm", "healthCheck"); got != "claude-sonnet-5" {
		t.Fatalf("modelForKind(brainstorm, healthCheck) = %q, want claude-sonnet-5", got)
	}
	if got := effortForKind(proj, "brainstorm", "healthCheck"); got != "medium" {
		t.Fatalf("effortForKind(brainstorm, healthCheck) = %q, want medium", got)
	}

	// Plain brainstorm (no healthCheck activity) still resolves to the
	// brainstorm entry.
	if got := modelForKind(proj, "brainstorm", ""); got != "claude-opus-4-8" {
		t.Fatalf("modelForKind(brainstorm, \"\") = %q, want claude-opus-4-8", got)
	}
	if got := effortForKind(proj, "brainstorm", ""); got != "high" {
		t.Fatalf("effortForKind(brainstorm, \"\") = %q, want high", got)
	}
}

// TestModelForKind_HealthCheckActivity_FallsBackToKind asserts that when no
// "healthCheck" pseudo-key override is set, a healthCheck-activity task falls
// back to the "brainstorm" kind entry (not directly to the project default),
// matching P0's original kept-on-Opus behavior for unconfigured projects.
func TestModelForKind_HealthCheckActivity_FallsBackToKind(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Agent.Model = "claude-opus-4-8"
	proj.Spec.Agent.Effort = "high"
	proj.Spec.Agent.ModelByKind = map[string]string{"brainstorm": "claude-opus-4-8"}
	proj.Spec.Agent.EffortByKind = map[string]string{"brainstorm": "high"}

	if got := modelForKind(proj, "brainstorm", "healthCheck"); got != "claude-opus-4-8" {
		t.Fatalf("modelForKind(brainstorm, healthCheck) fallback = %q, want claude-opus-4-8", got)
	}
	if got := effortForKind(proj, "brainstorm", "healthCheck"); got != "high" {
		t.Fatalf("effortForKind(brainstorm, healthCheck) fallback = %q, want high", got)
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
					"healthCheck": "claude-sonnet-5",
				},
				EffortByKind: map[string]string{
					"review":      "medium",
					"triageIssue": "low",
					"healthCheck": "medium",
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
		name, kind, activity, wantModel, wantEffort string
	}{
		{"review", "review", "", "claude-sonnet-5", "medium"},
		{"triageIssue", "triageIssue", "", "claude-sonnet-5", "low"},
		{"implement", "implement", "", "claude-opus-4-8", "high"},
		{"brainstorm", "brainstorm", "", "claude-opus-4-8", "high"},
		{"healthCheck activity", "brainstorm", "healthCheck", "claude-sonnet-5", "medium"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &tatarav1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "task-1", Namespace: "tatara", UID: "uid-1"},
				Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "demo", RepositoryRef: "repo1", Goal: "g", Kind: tc.kind},
			}
			if tc.activity != "" {
				task.Labels = map[string]string{tatarav1alpha1.LabelActivity: tc.activity}
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
