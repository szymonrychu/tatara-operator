package agent_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

const testMemoryEndpoint = "http://mem-demo.tatara.svc:8080"

func sampleInputs() (*tatarav1alpha1.Project, *tatarav1alpha1.Repository, *tatarav1alpha1.Task, agent.PodConfig) {
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
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-7", Namespace: "tatara", UID: "uid-task-7"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "demo", RepositoryRef: "repo1", Goal: "g"},
	}
	cfg := agent.PodConfig{
		Namespace:           "tatara",
		CallbackURL:         "http://tatara-operator-internal.tatara.svc:8082",
		OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
		AnthropicSecretName: "anthropic",
		CLIOIDCSecretName:   "tatara-cli-oidc",
		ImagePullSecret:     "regcred",
	}
	return proj, repo, task, cfg
}

func TestBuildPod_ImagePullSecrets(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	ips := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.ImagePullSecrets
	require.Equal(t, []corev1.LocalObjectReference{{Name: "regcred"}}, ips)

	cfg.ImagePullSecret = ""
	require.Empty(t, agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.ImagePullSecrets)
}

func envValue(c corev1.Container, name string) (string, bool) {
	for _, e := range c.Env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

func envSecretRef(c corev1.Container, name string) (*corev1.SecretKeySelector, bool) {
	for _, e := range c.Env {
		if e.Name == name && e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
			return e.ValueFrom.SecretKeyRef, true
		}
	}
	return nil, false
}

func TestBuildPod_NameAndImageAndOwner(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)

	require.Equal(t, agent.PodName(task), pod.Name)
	require.Equal(t, "tatara", pod.Namespace)
	require.Len(t, pod.Spec.Containers, 1)
	require.Equal(t, "wrapper:1", pod.Spec.Containers[0].Image)

	require.Len(t, pod.OwnerReferences, 1)
	or := pod.OwnerReferences[0]
	require.Equal(t, "Task", or.Kind)
	require.Equal(t, "task-7", or.Name)
	require.Equal(t, "uid-task-7", string(or.UID))
	require.True(t, or.Controller != nil && *or.Controller)
}

func TestBuildPod_PlainEnv(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]

	checks := map[string]string{
		"REPO_URL":             "https://git/acme/repo1",
		"REPO_BRANCH":          "main",
		"MODEL":                "claude-x",
		"PERMISSION_MODE":      "bypassPermissions",
		"TURN_TIMEOUT_SECONDS": "1800",
		"DEFAULT_CALLBACK_URL": "http://tatara-operator-internal.tatara.svc:8082/internal/turn-complete",
		"TATARA_TASK":          "task-7",
		"TATARA_PROJECT":       "demo",
		"TASK_BRANCH":          "tatara/task-task-7",
		"OIDC_ISSUER":          "https://keycloak.tatara.svc/realms/master",
		"OIDC_AUDIENCE":        "tatara-claude-code-wrapper",
		"TATARA_MEMORY_URL":    "http://mem-demo.tatara.svc:8080",
	}
	for k, want := range checks {
		got, ok := envValue(c, k)
		require.True(t, ok, "env %s missing", k)
		require.Equal(t, want, got, "env %s", k)
	}
}

func TestBuildPod_SecretEnv(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]

	ant, ok := envSecretRef(c, "CLAUDE_CODE_OAUTH_TOKEN")
	require.True(t, ok)
	require.Equal(t, "anthropic", ant.Name)
	require.Equal(t, "oauth-token", ant.Key)

	git, ok := envSecretRef(c, "GIT_TOKEN")
	require.True(t, ok)
	require.Equal(t, "demo-scm", git.Name)
	require.Equal(t, "token", git.Key)

	cid, ok := envSecretRef(c, "CLI_OIDC_CLIENT_ID")
	require.True(t, ok)
	require.Equal(t, "tatara-cli-oidc", cid.Name)
	require.Equal(t, "client-id", cid.Key)

	csec, ok := envSecretRef(c, "CLI_OIDC_CLIENT_SECRET")
	require.True(t, ok)
	require.Equal(t, "tatara-cli-oidc", csec.Name)
	require.Equal(t, "client-secret", csec.Key)
}

// TestBuildPod_CallbackURLFromConfig asserts that DEFAULT_CALLBACK_URL is
// derived from PodConfig.CallbackURL (not a bind address) and that a trailing
// slash in CallbackURL is stripped before appending the path.
func TestBuildPod_CallbackURLFromConfig(t *testing.T) {
	proj, repo, task, _ := sampleInputs()
	cfg := agent.PodConfig{
		Namespace:           "tatara",
		CallbackURL:         "http://tatara-operator-internal.tatara.svc:8082/",
		AnthropicSecretName: "anthropic",
		CLIOIDCSecretName:   "tatara-cli-oidc",
	}
	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]
	got, ok := envValue(c, "DEFAULT_CALLBACK_URL")
	require.True(t, ok)
	require.Equal(t, "http://tatara-operator-internal.tatara.svc:8082/internal/turn-complete", got)
}

func TestBuildPod_PortAndReadiness(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]
	require.Len(t, c.Ports, 1)
	require.Equal(t, int32(8080), c.Ports[0].ContainerPort)
	require.NotNil(t, c.ReadinessProbe)
	require.Equal(t, "/readyz", c.ReadinessProbe.HTTPGet.Path)
}

func TestBuildService_MatchesPod(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	svc := agent.BuildService(proj, repo, task, cfg)
	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)

	require.Equal(t, pod.Name, svc.Name) // service name == pod name
	require.Equal(t, "tatara", svc.Namespace)
	require.Equal(t, pod.Labels, svc.Spec.Selector)
	require.Len(t, svc.Spec.Ports, 1)
	require.Equal(t, int32(8080), svc.Spec.Ports[0].Port)

	require.Len(t, svc.OwnerReferences, 1)
	require.Equal(t, "Task", svc.OwnerReferences[0].Kind)
}

func TestBuildPod_MemoryEndpointEnv(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	const ep = "http://mem-other.tatara.svc:8080"
	c := agent.BuildPod(proj, repo, task, nil, ep, cfg).Spec.Containers[0]
	got, ok := envValue(c, "TATARA_MEMORY_URL")
	require.True(t, ok, "TATARA_MEMORY_URL missing")
	require.Equal(t, ep, got)
}

func TestBuildPod_SetsTataraRepos(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	repos := []tatarav1alpha1.Repository{
		{ObjectMeta: metav1.ObjectMeta{Name: "repo1"}, Spec: tatarav1alpha1.RepositorySpec{URL: "https://git/acme/repo1", DefaultBranch: "main"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "repo2"}, Spec: tatarav1alpha1.RepositorySpec{URL: "https://git/acme/repo2", DefaultBranch: "dev"}},
	}
	c := agent.BuildPod(proj, repo, task, repos, testMemoryEndpoint, cfg).Spec.Containers[0]
	v, ok := envValue(c, "TATARA_REPOS")
	require.True(t, ok)
	var got []map[string]string
	require.NoError(t, json.Unmarshal([]byte(v), &got))
	require.Equal(t, "repo1", got[0]["name"]) // primary (the task's repo) first
	require.Equal(t, "https://git/acme/repo2", got[1]["url"])
	require.Equal(t, "dev", got[1]["branch"])
}
