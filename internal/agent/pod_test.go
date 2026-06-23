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
		"RUN_ID":               "wrapper-task-7-0",
		"POD_NAME":             "wrapper-task-7",
		"TATARA_TASK":          "task-7",
		"TATARA_PROJECT":       "demo",
		"TASK_BRANCH":          "tatara/task-task-7",
		"OIDC_ISSUER":          "https://keycloak.tatara.svc/realms/master",
		"OIDC_AUDIENCE":        "tatara-claude-code-wrapper",
		"TATARA_MEMORY_URL":    "http://mem-demo.tatara.svc:8080",
		"TATARA_OPERATOR_URL":  "http://tatara-operator.tatara.svc:8080",
		"TATARA_CHAT_URL":      "http://chat-demo.tatara.svc:8080",
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

// TestBuildPod_FSGroup asserts that FSGroup in PodConfig propagates to a
// pod-level SecurityContext so mounted-volume ownership matches the runtime UID.
func TestBuildPod_FSGroup(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	fsg := int64(65532)
	cfg.FSGroup = &fsg
	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
	require.NotNil(t, pod.Spec.SecurityContext)
	require.Equal(t, fsg, *pod.Spec.SecurityContext.FSGroup)
}

// TestBuildPod_NoPodSecurityContextWhenUnset asserts that an unset FSGroup
// leaves the pod-level SecurityContext nil (no constraint).
func TestBuildPod_NoPodSecurityContextWhenUnset(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	cfg.FSGroup = nil
	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
	require.Nil(t, pod.Spec.SecurityContext)
}

// TestBuildPod_Affinity asserts that Affinity in PodConfig propagates to the
// PodSpec.
func TestBuildPod_Affinity(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	cfg.Affinity = &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key: "disktype", Operator: corev1.NodeSelectorOpIn, Values: []string{"ssd"},
					}},
				}},
			},
		},
	}
	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
	require.NotNil(t, pod.Spec.Affinity)
	require.NotNil(t, pod.Spec.Affinity.NodeAffinity)
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

// TestBuildPod_ChatURL asserts that BuildPod injects TATARA_CHAT_URL pointing at
// the in-cluster chat service for the project so agent chat tools do not fall
// through to the public ingress default.
func TestBuildPod_ChatURL(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]
	got, ok := envValue(c, "TATARA_CHAT_URL")
	require.True(t, ok, "TATARA_CHAT_URL missing from pod env")
	require.Equal(t, "http://chat-demo.tatara.svc:8080", got)
}

// TestValidatePodSecurityContext rejects RunAsNonRoot=true without RunAsUser.
// Defence-in-depth mirror of ValidatePodSecretRefs: the operator fails fast at
// config load so kubelet CreateContainerConfigError never fires per-spawn.
func TestValidatePodSecurityContext(t *testing.T) {
	_, _, _, cfg := sampleInputs()

	// Neither flag: no error (nil SecurityContext path).
	require.NoError(t, agent.ValidatePodSecurityContext(cfg))

	// RunAsUser set without RunAsNonRoot: fine.
	uid := int64(65534)
	cfgUser := cfg
	cfgUser.RunAsUser = &uid
	require.NoError(t, agent.ValidatePodSecurityContext(cfgUser))

	// RunAsNonRoot=true with RunAsUser set: valid contract.
	cfgBoth := cfg
	cfgBoth.RunAsNonRoot = true
	cfgBoth.RunAsUser = &uid
	require.NoError(t, agent.ValidatePodSecurityContext(cfgBoth))

	// RunAsNonRoot=true without RunAsUser: must error (unsatisfiable kubelet contract).
	cfgNoUser := cfg
	cfgNoUser.RunAsNonRoot = true
	cfgNoUser.RunAsUser = nil
	err := agent.ValidatePodSecurityContext(cfgNoUser)
	require.Error(t, err)
	require.Contains(t, err.Error(), "RunAsNonRoot")
	require.Contains(t, err.Error(), "RunAsUser")
}

// TestBuildPod_RunIDUniqPerAttempt asserts that RUN_ID encodes the RunAttempt
// counter so successive boot-crash respawns of the same Task store push-metrics
// under distinct keys (POD_NAME remains stable for service addressing).
func TestBuildPod_RunIDUniqPerAttempt(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()

	// Attempt 0 (first spawn).
	c0 := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]
	runID0, ok := envValue(c0, "RUN_ID")
	require.True(t, ok, "RUN_ID missing")
	podName0, _ := envValue(c0, "POD_NAME")
	require.Equal(t, "wrapper-task-7-0", runID0)
	require.Equal(t, "wrapper-task-7", podName0, "POD_NAME must stay stable")

	// Attempt 1 (first boot-crash respawn).
	cfg1 := cfg
	cfg1.RunAttempt = 1
	c1 := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg1).Spec.Containers[0]
	runID1, _ := envValue(c1, "RUN_ID")
	podName1, _ := envValue(c1, "POD_NAME")
	require.Equal(t, "wrapper-task-7-1", runID1)
	require.Equal(t, "wrapper-task-7", podName1, "POD_NAME must stay stable across respawns")

	// Each attempt produces a distinct RUN_ID.
	require.NotEqual(t, runID0, runID1, "RUN_ID must differ between respawns")
}

// TestBuildPod_CallbackHMACSecretViaSecretKeyRef asserts that when a callback
// HMAC secret name is configured, CALLBACK_HMAC_SECRET is injected as a
// SecretKeyRef (never a literal Pod-spec value) so the shared secret never lands
// in the Pod object / etcd in plaintext, matching every other agent secret.
func TestBuildPod_CallbackHMACSecretViaSecretKeyRef(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()

	// Not configured: CALLBACK_HMAC_SECRET must be absent entirely.
	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]
	if _, ok := envSecretRef(c, "CALLBACK_HMAC_SECRET"); ok {
		t.Fatal("CALLBACK_HMAC_SECRET injected without a configured secret name")
	}
	if v, ok := envValue(c, "CALLBACK_HMAC_SECRET"); ok {
		t.Fatalf("CALLBACK_HMAC_SECRET present as a literal value %q without a secret name", v)
	}

	// Configured: must be a SecretKeyRef, never a literal Value.
	cfg.CallbackHMACSecretName = "tatara-callback-hmac"
	c = agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]
	if v, ok := envValue(c, "CALLBACK_HMAC_SECRET"); ok && v != "" {
		t.Fatalf("CALLBACK_HMAC_SECRET leaked as a literal Pod-spec value %q; must be a SecretKeyRef", v)
	}
	ref, ok := envSecretRef(c, "CALLBACK_HMAC_SECRET")
	require.True(t, ok, "CALLBACK_HMAC_SECRET must be a SecretKeyRef when a secret name is configured")
	require.Equal(t, "tatara-callback-hmac", ref.Name)
	require.Equal(t, agent.CallbackHMACSecretKey, ref.Key)
}

// TestBuildPodEgressLabel_UsesSharedConst asserts that the brainstorm-sources
// annotation key in the egress-label gate is the same literal as
// tatarav1alpha1.AnnBrainstormSources, so the two sites cannot silently drift.
func TestBuildPodEgressLabel_UsesSharedConst(t *testing.T) {
	require.Equal(t, "tatara.dev/brainstorm-sources", tatarav1alpha1.AnnBrainstormSources,
		"AnnBrainstormSources value changed; update pod.go and projectscan.go")

	task := &tatarav1alpha1.Task{}
	task.Name = "bs"
	task.Spec.Kind = "brainstorm"
	task.Annotations = map[string]string{tatarav1alpha1.AnnBrainstormSources: "internet"}
	proj := &tatarav1alpha1.Project{}
	repo := &tatarav1alpha1.Repository{}
	pod := agent.BuildPod(proj, repo, task, nil, "http://mem", agent.PodConfig{Namespace: "tatara"})
	require.Equal(t, "internet", pod.Labels["tatara.io/egress"],
		"egress label not set when annotation key matches AnnBrainstormSources")
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
				task.Annotations = map[string]string{tatarav1alpha1.AnnBrainstormSources: tc.sources}
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

func TestBuildPod_HookEnvs(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	proj.Spec.Agent.Hooks = &tatarav1alpha1.LifecycleHooks{
		PreClone:             "echo pre",
		PostClone:            "echo post",
		ConversationStart:    "echo start",
		ConversationRestart:  "echo restart",
		AgentTurnFinished:    "echo turn",
		ConversationFinished: "echo finished",
	}
	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]
	for k, want := range map[string]string{
		"HOOK_PRE_CLONE":             "echo pre",
		"HOOK_POST_CLONE":            "echo post",
		"HOOK_CONVERSATION_START":    "echo start",
		"HOOK_CONVERSATION_RESTART":  "echo restart",
		"HOOK_AGENT_TURN_FINISHED":   "echo turn",
		"HOOK_CONVERSATION_FINISHED": "echo finished",
	} {
		got, ok := envValue(c, k)
		require.Truef(t, ok, "hook env %s missing", k)
		require.Equalf(t, want, got, "hook env %s", k)
	}
}

func TestBuildPod_HookEnvs_OnlyNonEmptyEmitted(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	// Only one hook set: the other five must not appear as env vars.
	proj.Spec.Agent.Hooks = &tatarav1alpha1.LifecycleHooks{PreClone: "echo pre"}
	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]
	got, ok := envValue(c, "HOOK_PRE_CLONE")
	require.True(t, ok)
	require.Equal(t, "echo pre", got)
	for _, k := range []string{
		"HOOK_POST_CLONE", "HOOK_CONVERSATION_START", "HOOK_CONVERSATION_RESTART",
		"HOOK_AGENT_TURN_FINISHED", "HOOK_CONVERSATION_FINISHED",
	} {
		_, ok := envValue(c, k)
		require.Falsef(t, ok, "unset hook %s must not be emitted", k)
	}
}

func TestBuildPod_NoHooks_NoHookEnvs(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]
	for _, e := range c.Env {
		require.NotContains(t, e.Name, "HOOK_", "no HOOK_* env when Hooks is nil")
	}
}

func TestBuildPod_ExtraEnvsAndEnvFrom(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	proj.Spec.Agent.ExtraEnvs = []corev1.EnvVar{{Name: "FOO", Value: "bar"}}
	proj.Spec.Agent.ExtraEnvsFrom = []corev1.EnvFromSource{
		{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "cm"}}},
	}
	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]

	got, ok := envValue(c, "FOO")
	require.True(t, ok)
	require.Equal(t, "bar", got)
	require.Equal(t, proj.Spec.Agent.ExtraEnvsFrom, c.EnvFrom)
}

func TestBuildPod_ExtraEnvsAppendedLast(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	// An extra env named like a required operator var must not shadow it: the
	// operator's value must still be the first occurrence in the list.
	proj.Spec.Agent.ExtraEnvs = []corev1.EnvVar{{Name: "TATARA_TASK", Value: "hijacked"}}
	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]
	got, ok := envValue(c, "TATARA_TASK")
	require.True(t, ok)
	require.Equal(t, task.Name, got, "operator value must win (appears before the extra)")
}

func TestBuildPod_ExtraVolumesMountsSidecarsInit(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	proj.Spec.Agent.ExtraVolumeMounts = []corev1.VolumeMount{{Name: "vol", MountPath: "/data"}}
	proj.Spec.Agent.ExtraVolumes = []corev1.Volume{{Name: "vol"}}
	proj.Spec.Agent.ExtraSidecarContainers = []corev1.Container{{Name: "sidecar", Image: "busybox"}}
	proj.Spec.Agent.ExtraInitContainers = []corev1.Container{{Name: "init", Image: "busybox"}}

	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)

	require.Equal(t, proj.Spec.Agent.ExtraVolumes, pod.Spec.Volumes)
	require.Equal(t, proj.Spec.Agent.ExtraInitContainers, pod.Spec.InitContainers)
	require.Equal(t, proj.Spec.Agent.ExtraVolumeMounts, pod.Spec.Containers[0].VolumeMounts)

	// Wrapper stays first; sidecar is appended after it.
	require.Len(t, pod.Spec.Containers, 2)
	require.Equal(t, "wrapper", pod.Spec.Containers[0].Name)
	require.Equal(t, "sidecar", pod.Spec.Containers[1].Name)
}

func TestBuildPod_NoExtras_EmptyByDefault(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
	require.Empty(t, pod.Spec.Volumes)
	require.Empty(t, pod.Spec.InitContainers)
	require.Empty(t, pod.Spec.Containers[0].EnvFrom)
	require.Empty(t, pod.Spec.Containers[0].VolumeMounts)
	require.Len(t, pod.Spec.Containers, 1, "no sidecars by default")
}

func TestConversationKey(t *testing.T) {
	// issue-numbered task.
	issue := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-issue"},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "tatara", RepositoryRef: "tatara-operator",
			Source: &tatarav1alpha1.TaskSource{Number: 114},
		},
	}
	require.Equal(t, "tatara/tatara-operator/issue-114.jsonl", agent.ConversationKey(issue))

	// PR-numbered task.
	pr := issue.DeepCopy()
	pr.Spec.Source.IsPR = true
	require.Equal(t, "tatara/tatara-operator/pr-114.jsonl", agent.ConversationKey(pr))

	// No source -> keyed by task name.
	noSrc := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "brainstorm-x"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "tatara"},
	}
	require.Equal(t, "tatara/task-brainstorm-x.jsonl", agent.ConversationKey(noSrc))

	// Status override wins (e.g. a forked key, subtask 8).
	forked := issue.DeepCopy()
	forked.Status.ConversationObjectKey = "tatara/forked/issue-200.jsonl"
	require.Equal(t, "tatara/forked/issue-200.jsonl", agent.ConversationKey(forked))
}

func TestBuildPod_S3DisabledByDefault(t *testing.T) {
	proj, repo, task, cfg := sampleInputs() // no S3Bucket
	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]
	for _, k := range []string{"S3_BUCKET", "S3_ENDPOINT", "CONVERSATION_OBJECT_KEY", "AWS_ACCESS_KEY_ID"} {
		_, ok := envValue(c, k)
		_, okSecret := envSecretRef(c, k)
		require.False(t, ok || okSecret, "%s must NOT be set when no S3 bucket configured", k)
	}
}

func TestBuildPod_S3EnabledInjectsEnvCredsAndKey(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	cfg.S3Endpoint = "http://rook-ceph-rgw.tatara.svc"
	cfg.S3Bucket = "tatara-conversations"
	cfg.S3Region = "us-east-1"
	cfg.S3KeyPrefix = "conv"
	cfg.S3ForcePathStyle = true
	cfg.S3SecretName = "tatara-s3"
	task.Spec.Source = &tatarav1alpha1.TaskSource{Number: 114}

	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]
	requireEnv := func(name, want string) {
		got, ok := envValue(c, name)
		require.True(t, ok, "%s must be set", name)
		require.Equal(t, want, got)
	}
	requireEnv("S3_ENDPOINT", "http://rook-ceph-rgw.tatara.svc")
	requireEnv("S3_BUCKET", "tatara-conversations")
	requireEnv("S3_REGION", "us-east-1")
	requireEnv("S3_KEY_PREFIX", "conv")
	requireEnv("S3_FORCE_PATH_STYLE", "true")
	requireEnv("CONVERSATION_OBJECT_KEY", "demo/repo1/issue-114.jsonl")

	// CONVERSATION_SESSION_ID only once Status.SessionID is recorded.
	_, hasSID := envValue(c, "CONVERSATION_SESSION_ID")
	require.False(t, hasSID, "no session id env until a prior run recorded it")

	// AWS creds via SecretKeyRef, not literal env.
	ref, ok := envSecretRef(c, "AWS_ACCESS_KEY_ID")
	require.True(t, ok)
	require.Equal(t, "tatara-s3", ref.Name)
	require.Equal(t, "AWS_ACCESS_KEY_ID", ref.Key)
	_, ok = envSecretRef(c, "AWS_SECRET_ACCESS_KEY")
	require.True(t, ok)
}

func TestBuildPod_S3SessionIDReplay(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	cfg.S3Bucket = "tatara-conversations"
	task.Status.SessionID = "sid-abc"
	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]
	got, ok := envValue(c, "CONVERSATION_SESSION_ID")
	require.True(t, ok, "CONVERSATION_SESSION_ID must be set when Status.SessionID is recorded")
	require.Equal(t, "sid-abc", got)
}

func TestBuildPod_S3NoCredsWhenSecretEmpty(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	cfg.S3Bucket = "tatara-conversations" // no S3SecretName -> default cred chain (IRSA)
	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]
	_, ok := envSecretRef(c, "AWS_ACCESS_KEY_ID")
	require.False(t, ok, "no AWS creds injected when S3SecretName is empty")
	_, ok = envValue(c, "S3_BUCKET")
	require.True(t, ok, "S3 config still injected for the IRSA path")
}
