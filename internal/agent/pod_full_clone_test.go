package agent_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// TestBuildPod_FullCloneEnv checks that TATARA_WORKSPACE_FULL_CLONE is "true"
// for project-scoped kinds (repo==nil) and "" for repo-scoped kinds (repo!=nil).
// Mirrors the TATARA_SKILL_PROFILE env test pattern.
func TestBuildPod_FullCloneEnv(t *testing.T) {
	cases := []struct {
		name    string
		kind    string
		nilRepo bool
		wantVal string
	}{
		// project-scoped: no primary repo -> full clone
		{"brainstorm_project_scoped", "brainstorm", true, "true"},
		{"incident_project_scoped", "incident", true, "true"},
		{"refine_project_scoped", "refine", true, "true"},
		// repo-scoped: primary repo present -> not "true"
		{"implement_repo_scoped", "implement", false, ""},
		{"review_repo_scoped", "review", false, ""},
		{"triageIssue_repo_scoped", "triageIssue", false, ""},
		{"issueLifecycle_repo_scoped", "issueLifecycle", false, ""},
	}

	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "tatara"},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "demo-scm",
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1",
				PermissionMode: "bypassPermissions", TurnTimeoutSeconds: 1800,
			},
		},
	}
	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "repo1", Namespace: "tatara"},
		Spec:       tatarav1alpha1.RepositorySpec{URL: "https://git/acme/repo1", DefaultBranch: "main"},
	}
	cfg := agent.PodConfig{
		Namespace:           "tatara",
		CallbackURL:         "http://tatara-operator-internal.tatara.svc:8082",
		OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
		AnthropicSecretName: "anthropic",
		CLIOIDCSecretName:   "tatara-cli-oidc",
		OperatorURL:         "http://tatara-operator.tatara.svc:8080",
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &tatarav1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "task-fc", Namespace: "tatara", UID: "uid-fc"},
				Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "demo", Goal: "g", Kind: tc.kind},
			}
			var repoArg *tatarav1alpha1.Repository
			if !tc.nilRepo {
				repoArg = repo
				task.Spec.RepositoryRef = "repo1"
			}
			c := agent.BuildPod(proj, repoArg, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]

			val, found := envValue(c, "TATARA_WORKSPACE_FULL_CLONE")
			require.True(t, found, "TATARA_WORKSPACE_FULL_CLONE must be present in env for kind %q", tc.kind)
			require.Equal(t, tc.wantVal, val,
				"TATARA_WORKSPACE_FULL_CLONE value for kind %q (nilRepo=%v)", tc.kind, tc.nilRepo)
		})
	}
}
