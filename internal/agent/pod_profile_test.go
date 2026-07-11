package agent

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// TestToolProfileForKind asserts the mapping from CRD Kind to TATARA_TOOL_PROFILE value.
func TestToolProfileForKind(t *testing.T) {
	cases := []struct {
		kind string
		want string
	}{
		{"implement", "implement"},
		{"review", "review"},
		{"clarify", "clarify"},
		{"triageIssue", "triage"},
		{"brainstorm", "brainstorm"},
		{"issueLifecycle", "lifecycle"},
		{"incident", "incident"},
		{"refine", "refine"},
		{"documentation", "documentation"},
		{"selfImprove", ""}, // selfImprove removed; dormant CRD enum value maps to fail-open
		{"", ""},            // unknown/empty -> fail-open
		{"unknown", ""},     // unknown kind -> fail-open
		{"healthCheck", ""}, // not a real Kind; brainstorm shares Kind=brainstorm
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			require.Equal(t, tc.want, toolProfileForKind(tc.kind))
		})
	}
}

// TestToolProfileForKind_AllActiveCRDKinds asserts that every active Kind value
// in the CRD enum maps to a non-empty profile, so a future new kind fails this
// test until it is added to toolProfileForKind. The enum is:
//
//	implement;review;selfImprove;triageIssue;brainstorm;issueLifecycle;incident;healthCheck;refine
//
// (from +kubebuilder:validation:Enum on TaskSpec.Kind)
//
// healthCheck is omitted from the loop below: it is a vestigial enum alias and
// runtime healthCheck tasks set Kind=brainstorm, so toolProfileForKind
// ("healthCheck") is intentionally "" (fail-open never reached at runtime).
// selfImprove is omitted: it is a dormant enum value kept only to avoid
// rejecting stored terminal selfImprove CRs; its profile is intentionally "".
func TestToolProfileForKind_AllActiveCRDKinds(t *testing.T) {
	// The CRD enum is the single source of truth; update here when it changes.
	crdKinds := []string{
		"implement",
		"review",
		"clarify",
		"triageIssue",
		"brainstorm",
		"issueLifecycle",
		"incident",
		"refine",
		"documentation",
	}
	for _, kind := range crdKinds {
		t.Run(kind, func(t *testing.T) {
			profile := toolProfileForKind(kind)
			require.NotEmpty(t, profile, "CRD kind %q must map to a non-empty profile in toolProfileForKind", kind)
		})
	}
}

// TestBuildPod_ToolProfileEnv asserts that BuildPod injects TATARA_TOOL_PROFILE
// with the correct value for each task kind.
func TestBuildPod_ToolProfileEnv(t *testing.T) {
	cases := []struct {
		kind    string
		profile string
	}{
		{"implement", "implement"},
		{"review", "review"},
		{"triageIssue", "triage"},
		{"brainstorm", "brainstorm"},
		{"issueLifecycle", "lifecycle"},
		{"incident", "incident"},
		{"refine", "refine"},
		{"selfImprove", ""}, // selfImprove removed; dormant CRD enum value maps to fail-open
		{"", ""},            // unset -> empty (fail-open)
		{"unknown", ""},     // unknown -> empty (fail-open)
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
	cfg := PodConfig{
		Namespace:           "tatara",
		CallbackURL:         "http://tatara-operator-internal.tatara.svc:8082",
		OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
		AnthropicSecretName: "anthropic",
		CLIOIDCSecretName:   "tatara-cli-oidc",
		OperatorURL:         "http://tatara-operator.tatara.svc:8080",
	}

	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			task := &tatarav1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "task-1", Namespace: "tatara", UID: "uid-1"},
				Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "demo", RepositoryRef: "repo1", Goal: "g", Kind: tc.kind},
			}
			c := BuildPod(proj, repo, task, nil, "http://mem.tatara.svc:8080", cfg).Spec.Containers[0]
			// Find the env var in the list.
			var profileVal string
			var found bool
			for _, e := range c.Env {
				if e.Name == "TATARA_TOOL_PROFILE" {
					profileVal = e.Value
					found = true
					break
				}
			}
			require.True(t, found, "TATARA_TOOL_PROFILE must be present in pod env for kind %q", tc.kind)
			require.Equal(t, tc.profile, profileVal, "TATARA_TOOL_PROFILE value for kind %q", tc.kind)
		})
	}
}

// TestBuildPod_ToolProfileHealthCheck asserts that a healthCheck task
// (which uses Kind=brainstorm) maps to profile "brainstorm".
func TestBuildPod_ToolProfileHealthCheck(t *testing.T) {
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
	cfg := PodConfig{
		Namespace:           "tatara",
		CallbackURL:         "http://tatara-operator-internal.tatara.svc:8082",
		OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
		AnthropicSecretName: "anthropic",
		CLIOIDCSecretName:   "tatara-cli-oidc",
		OperatorURL:         "http://tatara-operator.tatara.svc:8080",
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "hc-1",
			Namespace: "tatara",
			UID:       "uid-hc-1",
			Labels:    map[string]string{tatarav1alpha1.LabelActivity: "healthCheck"},
		},
		Spec: tatarav1alpha1.TaskSpec{ProjectRef: "demo", Goal: "health check", Kind: "brainstorm"},
	}
	// Project-scoped (no repo, no repositoryRef).
	c := BuildPod(proj, nil, task, nil, "http://mem.tatara.svc:8080", cfg).Spec.Containers[0]
	var profileVal string
	for _, e := range c.Env {
		if e.Name == "TATARA_TOOL_PROFILE" {
			profileVal = e.Value
			break
		}
	}
	require.Equal(t, "brainstorm", profileVal, "healthCheck (Kind=brainstorm) must map to profile brainstorm")
}

// TestBuildPod_ToolProfileBeforeExtraEnvs asserts that TATARA_TOOL_PROFILE
// appears in the env list BEFORE any ExtraEnvs, making the operator-set
// profile authoritative. Because the first occurrence wins in a Pod env list,
// a stray ExtraEnvs entry with the same name is silently ignored and cannot
// shadow the operator-set value.
//
// Concretely: if the operator sets ExtraEnvs=[{TATARA_TOOL_PROFILE, "override"}],
// that entry must appear AFTER the operator-injected value in the env list
// (i.e. operator-set profile index < extra's index) and the extra is ignored.
func TestBuildPod_ToolProfileBeforeExtraEnvs(t *testing.T) {
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "tatara"},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "demo-scm",
			Agent: tatarav1alpha1.AgentSpec{
				Model:              "claude-x",
				Image:              "wrapper:1",
				PermissionMode:     "bypassPermissions",
				TurnTimeoutSeconds: 1800,
				ExtraEnvs: []corev1.EnvVar{
					{Name: "TATARA_TOOL_PROFILE", Value: "override"},
				},
			},
		},
	}
	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "repo1"},
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
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-2", Namespace: "tatara", UID: "uid-2"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "demo", RepositoryRef: "repo1", Goal: "g", Kind: "implement"},
	}
	c := BuildPod(proj, repo, task, nil, "http://mem.tatara.svc:8080", cfg).Spec.Containers[0]

	// Find the indices of TATARA_TOOL_PROFILE occurrences.
	profileIdx := -1
	overrideIdx := -1
	for i, e := range c.Env {
		if e.Name == "TATARA_TOOL_PROFILE" {
			if profileIdx == -1 {
				profileIdx = i
			} else {
				overrideIdx = i
			}
		}
	}
	require.NotEqual(t, -1, profileIdx, "TATARA_TOOL_PROFILE (operator-set) must be present")
	require.NotEqual(t, -1, overrideIdx, "TATARA_TOOL_PROFILE (ExtraEnvs override) must be present")
	require.Less(t, profileIdx, overrideIdx,
		"operator-set TATARA_TOOL_PROFILE must appear before ExtraEnvs in the env list")

	// The first occurrence is the operator value ("implement" for kind=implement).
	require.Equal(t, "implement", c.Env[profileIdx].Value, "operator-set profile value must be the kind's profile")
}
