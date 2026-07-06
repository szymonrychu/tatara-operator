package agent_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// buildTestProjectScopedPod builds a project-scoped pod (repo==nil) with at
// least two repos, matching the brainstorm/incident/refine project-scoped shape.
func buildTestProjectScopedPod(t *testing.T, kind string) *corev1.Pod {
	t.Helper()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "testproj", Namespace: "tatara"},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "testproj-scm",
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1",
				PermissionMode: "bypassPermissions", TurnTimeoutSeconds: 1800,
			},
		},
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-bs", Namespace: "tatara", UID: "uid-bs"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "testproj", Goal: "g", Kind: kind},
	}
	allRepos := []tatarav1alpha1.Repository{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "repo-a"},
			Spec:       tatarav1alpha1.RepositorySpec{URL: "https://github.com/org/repo-a", DefaultBranch: "main"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "repo-b"},
			Spec:       tatarav1alpha1.RepositorySpec{URL: "https://github.com/org/repo-b", DefaultBranch: "main"},
		},
	}
	cfg := agent.PodConfig{
		Namespace:           "tatara",
		CallbackURL:         "http://tatara-operator-internal.tatara.svc:8082",
		OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
		AnthropicSecretName: "anthropic",
		CLIOIDCSecretName:   "tatara-cli-oidc",
		OperatorURL:         "http://tatara-operator.tatara.svc:8080",
	}
	return agent.BuildPod(proj, nil, task, allRepos, testMemoryEndpoint, cfg)
}

// envMap converts a pod's container env to a string->string map for easy
// lookup. Only plain Value entries are included (no ValueFrom).
func envMap(t *testing.T, pod *corev1.Pod) map[string]string {
	t.Helper()
	m := make(map[string]string)
	for _, e := range pod.Spec.Containers[0].Env {
		m[e.Name] = e.Value
	}
	return m
}

func TestBuildPod_BrainstormShallowClone(t *testing.T) {
	cases := []struct {
		kind          string
		wantFullClone string
	}{
		{"brainstorm", ""},   // depth-1
		{"incident", "true"}, // full clone
		{"refine", "true"},   // full clone
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			pod := buildTestProjectScopedPod(t, tc.kind)
			env := envMap(t, pod)
			assert.Equal(t, tc.wantFullClone, env["TATARA_WORKSPACE_FULL_CLONE"], "kind=%q", tc.kind)
			assert.NotEmpty(t, env["TATARA_REPOS"], "brainstorm must still receive the full repo list")
		})
	}
}
