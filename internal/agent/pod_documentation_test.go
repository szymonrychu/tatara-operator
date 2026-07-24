package agent

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// TestModelForKind_DocumentationDefaultsToSonnet5 asserts the locked model
// choice for the documentation kind (claude-sonnet-5) applies regardless of
// the project's general Model, unless the project explicitly overrides it via
// ModelByKind.
func TestModelForKind_DocumentationDefaultsToSonnet5(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Agent.Model = "claude-opus-5"
	require.Equal(t, "claude-sonnet-5", modelForKind(proj, "documentation", ""))

	proj.Spec.Agent.ModelByKind = map[string]string{"documentation": "claude-opus-5"}
	require.Equal(t, "claude-opus-5", modelForKind(proj, "documentation", ""),
		"explicit ModelByKind override must still win over the locked default")
}

// TestBranchKind_Documentation asserts the branch/title prefix is "docs".
func TestBranchKind_Documentation(t *testing.T) {
	task := &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{Kind: "documentation"}}
	require.Equal(t, "docs", branchKind(task))
}

// TestTaskBranch_DocumentationUsesSourceHeadSHA asserts a documentation Task
// (SHA-keyed, no Source.Number) branches as tatara/docs-<short-sha> instead of
// the generic tatara/task-<name> fallback, so concurrent doc Tasks are
// distinguishable.
func TestTaskBranch_DocumentationUsesSourceHeadSHA(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "documentation-abc123",
			Annotations: map[string]string{tatarav1alpha1.AnnSourceHeadSHA: "deadbeef1234"},
		},
		Spec: tatarav1alpha1.TaskSpec{Kind: "documentation"},
	}
	require.Equal(t, "tatara/docs-deadbee", TaskBranch(task))
}

// TestTaskBranch_DocumentationFallsBackWithoutSHA asserts the generic
// tatara/task-<name> fallback still applies when the head-SHA annotation is
// absent (defensive; should not happen in practice).
func TestTaskBranch_DocumentationFallsBackWithoutSHA(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "documentation-abc123"},
		Spec:       tatarav1alpha1.TaskSpec{Kind: "documentation"},
	}
	require.Equal(t, "tatara/task-documentation-abc123", TaskBranch(task))
}

// TestPodNameSuffix_Documentation asserts the pod-name suffix is SHA-derived
// so concurrent documentation Tasks (SHA-keyed, no issue/PR Number) do not
// collide on the pod-name slot.
func TestPodNameSuffix_Documentation(t *testing.T) {
	withSHA := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{tatarav1alpha1.AnnSourceHeadSHA: "deadbeef1234"}},
		Spec:       tatarav1alpha1.TaskSpec{Kind: "documentation"},
	}
	require.Equal(t, "docs-deadbee", podNameSuffix(withSHA))

	withoutSHA := &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{Kind: "documentation"}}
	require.Equal(t, "docs", podNameSuffix(withoutSHA))
}

// TestBuildPod_Documentation asserts the full per-kind wiring for a
// documentation Task: model=claude-sonnet-5, both tool/skill profiles=
// "documentation", and the source annotations land on the pod env.
func TestBuildPod_Documentation(t *testing.T) {
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "tatara"},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "demo-scm",
			Agent: tatarav1alpha1.AgentSpec{
				Model:              "claude-opus-5",
				Image:              "wrapper:1",
				PermissionMode:     "bypassPermissions",
				TurnTimeoutSeconds: 1800,
			},
		},
	}
	docsRepo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "tatara-documentation", Namespace: "tatara"},
		Spec:       tatarav1alpha1.RepositorySpec{URL: "https://git/acme/tatara-documentation", DefaultBranch: "main"},
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "documentation-abc123", Namespace: "tatara", UID: "uid-doc-1",
			Annotations: map[string]string{
				tatarav1alpha1.AnnSourceRepo:    "https://git/acme/tatara-cli",
				tatarav1alpha1.AnnSourceBaseSHA: "0000000",
				tatarav1alpha1.AnnSourceHeadSHA: "deadbeef1234",
			},
		},
		Spec: tatarav1alpha1.TaskSpec{ProjectRef: "demo", RepositoryRef: "tatara-documentation", Goal: "g", Kind: "documentation"},
	}
	cfg := PodConfig{
		Namespace:           "tatara",
		CallbackURL:         "http://tatara-operator-internal.tatara.svc:8082",
		OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
		AnthropicSecretName: "anthropic",
		CLIOIDCSecretName:   "tatara-cli-oidc",
		OperatorURL:         "http://tatara-operator.tatara.svc:8080",
	}
	env := BuildPod(proj, docsRepo, task, nil, "http://mem.tatara.svc:8080", cfg).Spec.Containers[0].Env

	model, ok := envValue(env, "MODEL")
	require.True(t, ok)
	require.Equal(t, "claude-sonnet-5", model)

	toolProfile, ok := envValue(env, "TATARA_TOOL_PROFILE")
	require.True(t, ok)
	require.Equal(t, "documentation", toolProfile)

	skillProfile, ok := envValue(env, "TATARA_SKILL_PROFILE")
	require.True(t, ok)
	require.Equal(t, "documentation", skillProfile)

	sourceRepo, ok := envValue(env, "TATARA_SOURCE_REPO")
	require.True(t, ok)
	require.Equal(t, "https://git/acme/tatara-cli", sourceRepo)

	baseSHA, ok := envValue(env, "TATARA_SOURCE_BASE_SHA")
	require.True(t, ok)
	require.Equal(t, "0000000", baseSHA)

	headSHA, ok := envValue(env, "TATARA_SOURCE_HEAD_SHA")
	require.True(t, ok)
	require.Equal(t, "deadbeef1234", headSHA)
}

// TestBuildPod_SourceAnnotationsOnlyForDocumentation asserts the
// TATARA_SOURCE_* env vars are only injected for the documentation kind, not
// leaked onto unrelated kinds that happen to carry stray annotations.
func TestBuildPod_SourceAnnotationsOnlyForDocumentation(t *testing.T) {
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
		ObjectMeta: metav1.ObjectMeta{
			Name: "task-1", Namespace: "tatara", UID: "uid-1",
			Annotations: map[string]string{tatarav1alpha1.AnnSourceHeadSHA: "deadbeef1234"},
		},
		Spec: tatarav1alpha1.TaskSpec{ProjectRef: "demo", RepositoryRef: "repo1", Goal: "g", Kind: "implement"},
	}
	env := BuildPod(proj, repo, task, nil, "http://mem.tatara.svc:8080", cfg).Spec.Containers[0].Env
	_, ok := envValue(env, "TATARA_SOURCE_HEAD_SHA")
	require.False(t, ok, "TATARA_SOURCE_HEAD_SHA must not be injected for non-documentation kinds")
}
