package agent

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// TestModelForKindOnRepo_HelmfileFloor asserts the tier-revert self-heal floor:
// a reasoning-kind task targeting the terminal tatara-helmfile repo (the flow
// that FIXES a bad tier) is pinned to its locked opus default even when the
// project's ModelByKind downgraded or broke that kind. Component-repo tiering is
// unaffected. documentation/refine (cheap kinds) stay tierable everywhere.
func TestModelForKindOnRepo_HelmfileFloor(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Agent.Model = "claude-opus-5"
	proj.Spec.Agent.ModelByKind = map[string]string{
		"review":        "claude-sonnet-5",  // a valid downgrade (tier experiment)
		"implement":     "not-a-real-model", // a BROKEN override
		"documentation": "claude-sonnet-5",
	}
	cases := []struct {
		name, kind, activity, repo, want string
	}{
		// The self-heal review of the revert MR must run on opus, not the
		// downgraded tier being reverted.
		{"review floored on helmfile", "review", "", "tatara-helmfile", "claude-opus-5"},
		// A broken override for implement must not break the fix flow.
		{"broken implement floored on helmfile", "implement", "", "tatara-helmfile", "claude-opus-5"},
		// Component-repo review still tiers down (the experiment works normally).
		{"review tiers on component repo", "review", "", "tatara-operator", "claude-sonnet-5"},
		{"broken implement on component repo not floored", "implement", "", "tatara-operator", "not-a-real-model"},
		// Cheap kinds stay tierable even on helmfile.
		{"documentation not floored on helmfile", "documentation", "", "tatara-helmfile", "claude-sonnet-5"},
		// healthCheck (brainstorm-kind recurring classification) stays tierable on helmfile.
		{"healthCheck exempt on helmfile", "brainstorm", "healthCheck", "tatara-helmfile", "claude-opus-5"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := modelForKindOnRepo(proj, tc.kind, tc.activity, tc.repo); got != tc.want {
				t.Fatalf("modelForKindOnRepo(%q,%q,%q) = %q, want %q", tc.kind, tc.activity, tc.repo, got, tc.want)
			}
		})
	}
}

// TestModelForKindOnRepo_HealthCheckTierableOnHelmfile confirms a healthCheck
// override still applies on helmfile (the floor exempts healthCheck).
func TestModelForKindOnRepo_HealthCheckTierableOnHelmfile(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Agent.ModelByKind = map[string]string{"healthCheck": "claude-sonnet-5"}
	if got := modelForKindOnRepo(proj, "brainstorm", "healthCheck", "tatara-helmfile"); got != "claude-sonnet-5" {
		t.Fatalf("healthCheck on helmfile = %q, want claude-sonnet-5 (exempt from floor)", got)
	}
}

// TestBuildPod_HelmfileReviewFloored asserts the MODEL env floors to opus for a
// review task whose repo is tatara-helmfile, even with a downgrading ModelByKind.
func TestBuildPod_HelmfileReviewFloored(t *testing.T) {
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "tatara"},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "demo-scm",
			Agent: tatarav1alpha1.AgentSpec{
				Model:              "claude-opus-5",
				Effort:             "high",
				Image:              "wrapper:1",
				PermissionMode:     "bypassPermissions",
				TurnTimeoutSeconds: 1800,
				ModelByKind:        map[string]string{"review": "claude-sonnet-5"},
			},
		},
	}
	helmfileRepo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-helmfile", Namespace: "tatara"},
		Spec:       tatarav1alpha1.RepositorySpec{URL: "https://github.com/szymonrychu/tatara-helmfile", DefaultBranch: "main"},
	}
	componentRepo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "repo1", Namespace: "tatara"},
		Spec:       tatarav1alpha1.RepositorySpec{URL: "https://github.com/szymonrychu/tatara-operator", DefaultBranch: "main"},
	}
	cfg := PodConfig{
		Namespace:           "tatara",
		CallbackURL:         "http://tatara-operator-internal.tatara.svc:8082",
		OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
		AnthropicSecretName: "anthropic",
		CLIOIDCSecretName:   "tatara-cli-oidc",
		OperatorURL:         "http://tatara-operator.tatara.svc:8080",
	}
	mk := func(repo *tatarav1alpha1.Repository) string {
		task := &tatarav1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: "task-1", Namespace: "tatara", UID: "uid-1"},
			Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "demo", RepositoryRef: repo.Name, Goal: "g", Kind: "review"},
		}
		env := BuildPod(proj, repo, task, nil, "http://mem.tatara.svc:8080", cfg).Spec.Containers[0].Env
		m, _ := envValue(env, "MODEL")
		return m
	}
	if got := mk(helmfileRepo); got != "claude-opus-5" {
		t.Fatalf("BuildPod MODEL for review on tatara-helmfile = %q, want claude-opus-5 (floored)", got)
	}
	if got := mk(componentRepo); got != "claude-sonnet-5" {
		t.Fatalf("BuildPod MODEL for review on component repo = %q, want claude-sonnet-5 (tiered)", got)
	}
}
