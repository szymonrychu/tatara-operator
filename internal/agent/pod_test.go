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
		OperatorURL:         "http://tatara-operator.tatara.svc:8080",
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
		"OPERATOR_PUSH_URL":    "http://tatara-operator-internal.tatara.svc:8082/internal/metrics/push",
		"RUN_ID":               "wrapper-task-7",
		"POD_NAME":             "wrapper-task-7",
		"TATARA_TASK":          "task-7",
		"TATARA_PROJECT":       "demo",
		"TASK_BRANCH":          "tatara/task-task-7",
		"OIDC_ISSUER":          "https://keycloak.tatara.svc/realms/master",
		"OIDC_AUDIENCE":        "tatara-claude-code-wrapper",
		"TATARA_MEMORY_URL":    "http://mem-demo.tatara.svc:8080",
		"TATARA_OPERATOR_URL":  "http://tatara-operator.tatara.svc:8080",
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

func TestBuildPodName(t *testing.T) {
	cases := []struct {
		name     string
		project  string
		provider string
		repoRef  string
		kind     string
		labels   map[string]string
		source   *tatarav1alpha1.TaskSource
		want     string
	}{
		{
			name:    "github issue",
			project: "tatara", provider: "github", repoRef: "tatara-operator", kind: "issueLifecycle",
			source: &tatarav1alpha1.TaskSource{Number: 23, IsPR: false},
			want:   "tatara-tatara-gh-tatara-operator-issue-23",
		},
		{
			name:    "gitlab mr",
			project: "tatara", provider: "gitlab", repoRef: "tatara-cli", kind: "issueLifecycle",
			source: &tatarav1alpha1.TaskSource{Number: 7, IsPR: true},
			want:   "tatara-tatara-gl-tatara-cli-mr-7",
		},
		{
			name:    "github scan (no source)",
			project: "tatara", provider: "github", repoRef: "tatara-operator", kind: "implement",
			source: nil,
			want:   "tatara-tatara-gh-tatara-operator-scan",
		},
		{
			name:    "github brainstorm",
			project: "tatara", provider: "github", repoRef: "tatara-operator", kind: "brainstorm",
			source: nil,
			want:   "tatara-tatara-gh-tatara-operator-brainstorm",
		},
		{
			name:    "project board issue drops repo",
			project: "tatara", provider: "github", repoRef: "", kind: "issueLifecycle",
			source: &tatarav1alpha1.TaskSource{Number: 5, IsPR: false},
			want:   "tatara-tatara-gh-issue-5",
		},
		{
			name:    "project board brainstorm drops repo",
			project: "tatara", provider: "gitlab", repoRef: "", kind: "brainstorm",
			source: nil,
			want:   "tatara-tatara-gl-brainstorm",
		},
		{
			name:    "github healthCheck disambiguated by activity label",
			project: "tatara", provider: "github", repoRef: "tatara-operator", kind: "brainstorm",
			labels: map[string]string{tatarav1alpha1.LabelActivity: "healthCheck"},
			source: nil,
			want:   "tatara-tatara-gh-tatara-operator-healthcheck",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &tatarav1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Labels: tc.labels},
				Spec:       tatarav1alpha1.TaskSpec{Kind: tc.kind, Source: tc.source},
			}
			require.Equal(t, tc.want, agent.BuildPodName(tc.project, tc.provider, tc.repoRef, task))
		})
	}
}

func TestStampPodName_AndPodNameFallback(t *testing.T) {
	// No annotation: PodName falls back to wrapper-<name>.
	legacy := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "task-7"}}
	require.Equal(t, "wrapper-task-7", agent.PodName(legacy))

	// Stamped: PodName returns the descriptive name, and BuildPod/BuildService
	// adopt it.
	proj, repo, task, cfg := sampleInputs()
	task.Spec.Kind = "issueLifecycle"
	task.Spec.Source = &tatarav1alpha1.TaskSource{Provider: "github", Number: 42, IsPR: false}
	agent.StampPodName(task, "demo", "github", "repo1")
	require.Equal(t, "tatara-demo-gh-repo1-issue-42", agent.PodName(task))
	require.Equal(t, agent.PodName(task), agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Name)
	require.Equal(t, agent.PodName(task), agent.BuildService(proj, repo, task, cfg).Name)
}

func TestBuildPodName_SanitizesAndCaps(t *testing.T) {
	task := &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{Kind: "brainstorm"}}
	got := agent.BuildPodName("My Proj", "github", "Weird/Repo Name", task)
	require.Equal(t, "tatara-my-proj-gh-weird-repo-name-brainstorm", got)

	long := agent.BuildPodName("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "github", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", task)
	require.LessOrEqual(t, len(long), 63)
}

// TestBuildPod_Resources asserts that a PodConfig with resource requests/limits
// wires them into the wrapper container.
func TestBuildPod_Resources(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	cfg.CPURequest = "100m"
	cfg.CPULimit = "500m"
	cfg.MemoryRequest = "128Mi"
	cfg.MemoryLimit = "512Mi"
	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]
	require.Equal(t, "100m", c.Resources.Requests.Cpu().String())
	require.Equal(t, "500m", c.Resources.Limits.Cpu().String())
	require.Equal(t, "128Mi", c.Resources.Requests.Memory().String())
	require.Equal(t, "512Mi", c.Resources.Limits.Memory().String())
}

// TestBuildPod_ResourcesEmpty asserts that an empty PodConfig produces no
// resource requirements (zero value), preserving backward compatibility.
func TestBuildPod_ResourcesEmpty(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	cfg.CPURequest = ""
	cfg.CPULimit = ""
	cfg.MemoryRequest = ""
	cfg.MemoryLimit = ""
	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]
	require.True(t, c.Resources.Requests.Cpu().IsZero())
	require.True(t, c.Resources.Limits.Cpu().IsZero())
}

// TestBuildPod_ResourcesMalformedNoPanic asserts that a malformed resource
// quantity does not panic the reconcile hot path (resource.MustParse would).
// The malformed dimension is dropped; valid dimensions still apply.
func TestBuildPod_ResourcesMalformedNoPanic(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	cfg.CPURequest = "1OO m" // typo: letter O, embedded space - invalid
	cfg.MemoryRequest = "128Mi"
	require.NotPanics(t, func() {
		c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]
		require.True(t, c.Resources.Requests.Cpu().IsZero(), "malformed cpu should be dropped")
		require.Equal(t, "128Mi", c.Resources.Requests.Memory().String())
	})
}

// TestValidatePodResourceQuantities flags malformed quantities at config load.
func TestValidatePodResourceQuantities(t *testing.T) {
	_, _, _, cfg := sampleInputs()
	cfg.CPURequest = "100m"
	cfg.MemoryLimit = "512Mi"
	require.NoError(t, agent.ValidatePodResourceQuantities(cfg))

	bad := cfg
	bad.CPULimit = "not-a-qty"
	require.ErrorContains(t, agent.ValidatePodResourceQuantities(bad), "cpuLimit")

	empty := agent.PodConfig{}
	require.NoError(t, agent.ValidatePodResourceQuantities(empty), "empty scalars are valid")
}

// TestBuildPod_SecurityContext asserts that the container gets a restrictive
// securityContext when RunAsNonRoot is enabled in PodConfig.
func TestBuildPod_SecurityContext(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	cfg.RunAsNonRoot = true
	uid := int64(65534)
	cfg.RunAsUser = &uid
	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
	sc := pod.Spec.Containers[0].SecurityContext
	require.NotNil(t, sc)
	require.True(t, *sc.RunAsNonRoot)
	require.Equal(t, uid, *sc.RunAsUser)
}

// TestBuildPod_Tolerations asserts that Tolerations in PodConfig propagate to
// the PodSpec.
func TestBuildPod_Tolerations(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	cfg.Tolerations = []corev1.Toleration{{Key: "node-role.kubernetes.io/control-plane", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}
	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
	require.Len(t, pod.Spec.Tolerations, 1)
	require.Equal(t, "node-role.kubernetes.io/control-plane", pod.Spec.Tolerations[0].Key)
}

// TestBuildPod_NodeSelector asserts that NodeSelector in PodConfig propagates
// to the PodSpec.
func TestBuildPod_NodeSelector(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	cfg.NodeSelector = map[string]string{"kubernetes.io/os": "linux"}
	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
	require.Equal(t, map[string]string{"kubernetes.io/os": "linux"}, pod.Spec.NodeSelector)
}

// TestValidatePodSecretRefs returns an error when ScmSecretRef is empty.
func TestValidatePodSecretRefs(t *testing.T) {
	proj, _, _, cfg := sampleInputs()

	// All set: no error.
	require.NoError(t, agent.ValidatePodSecretRefs(proj, cfg))

	// Missing ScmSecretRef.
	projNoSCM := *proj
	projNoSCM.Spec.ScmSecretRef = ""
	require.ErrorContains(t, agent.ValidatePodSecretRefs(&projNoSCM, cfg), "ScmSecretRef")

	// Missing AnthropicSecretName.
	cfgNoAnt := cfg
	cfgNoAnt.AnthropicSecretName = ""
	require.ErrorContains(t, agent.ValidatePodSecretRefs(proj, cfgNoAnt), "AnthropicSecretName")

	// Missing CLIOIDCSecretName.
	cfgNoCLI := cfg
	cfgNoCLI.CLIOIDCSecretName = ""
	require.ErrorContains(t, agent.ValidatePodSecretRefs(proj, cfgNoCLI), "CLIOIDCSecretName")
}

func TestBuildPodEgressLabel(t *testing.T) {
	cases := []struct {
		name    string
		sources string
		want    bool
	}{
		{"internet present", "docs,memory,internet", true},
		{"internet absent", "docs,memory", false},
		{"no annotation", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &tatarav1alpha1.Task{}
			task.Name = "bs"
			task.Spec.Kind = "brainstorm"
			if tc.sources != "" {
				task.Annotations = map[string]string{"tatara.dev/brainstorm-sources": tc.sources}
			}
			proj := &tatarav1alpha1.Project{}
			repo := &tatarav1alpha1.Repository{}
			pod := agent.BuildPod(proj, repo, task, nil, "http://mem", agent.PodConfig{Namespace: "tatara"})
			_, has := pod.Labels["tatara.io/egress"]
			if has != tc.want {
				t.Fatalf("egress label present=%v, want %v (labels=%+v)", has, tc.want, pod.Labels)
			}
			if tc.want && pod.Labels["tatara.io/egress"] != "internet" {
				t.Fatalf("egress label value = %q, want internet", pod.Labels["tatara.io/egress"])
			}
		})
	}
}
