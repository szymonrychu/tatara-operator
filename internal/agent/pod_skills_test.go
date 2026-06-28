package agent

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// TestSkillProfileForKind asserts the mapping from CRD Kind to TATARA_SKILL_PROFILE value.
func TestSkillProfileForKind(t *testing.T) {
	cases := []struct {
		kind string
		want string
	}{
		{"implement", "implement"},
		{"review", "review"},
		{"triageIssue", "triage"},
		{"brainstorm", "brainstorm"},
		{"issueLifecycle", "lifecycle"},
		{"incident", "incident"},
		{"selfImprove", "selfImprove"},
		{"healthCheck", ""}, // healthCheck uses Kind=brainstorm at the task level; unknown here -> fail-open
		{"refine", ""},      // refine -> fail-open
		{"", ""},            // empty -> fail-open
		{"unknown", ""},     // unknown -> fail-open
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			require.Equal(t, tc.want, skillProfileForKind(tc.kind))
		})
	}
}

// TestSkillProfileForKind_AllCRDKinds asserts that every Kind in the CRD enum maps
// to a non-empty skill profile, so a future new kind fails this test until added.
func TestSkillProfileForKind_AllCRDKinds(t *testing.T) {
	crdKinds := []string{
		"implement",
		"review",
		"selfImprove",
		"triageIssue",
		"brainstorm",
		"issueLifecycle",
		"incident",
	}
	for _, kind := range crdKinds {
		t.Run(kind, func(t *testing.T) {
			profile := skillProfileForKind(kind)
			require.NotEmpty(t, profile, "CRD kind %q must map to a non-empty skill profile", kind)
		})
	}
}

func testSkillsProj(extraEnvs ...corev1.EnvVar) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "tatara"},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "demo-scm",
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1",
				PermissionMode: "bypassPermissions", TurnTimeoutSeconds: 1800,
				ExtraEnvs: extraEnvs,
			},
		},
	}
}

func testSkillsRepo() *tatarav1alpha1.Repository {
	return &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "repo1", Namespace: "tatara"},
		Spec:       tatarav1alpha1.RepositorySpec{URL: "https://git/acme/repo1", DefaultBranch: "main"},
	}
}

func testSkillsCfg() PodConfig {
	return PodConfig{
		Namespace:           "tatara",
		CallbackURL:         "http://tatara-operator-internal.tatara.svc:8082",
		OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
		AnthropicSecretName: "anthropic",
		CLIOIDCSecretName:   "tatara-cli-oidc",
		OperatorURL:         "http://tatara-operator.tatara.svc:8080",
	}
}

func findEnvVar(envs []corev1.EnvVar, name string) (corev1.EnvVar, bool) {
	for _, e := range envs {
		if e.Name == name {
			return e, true
		}
	}
	return corev1.EnvVar{}, false
}

func envIndex(envs []corev1.EnvVar, name string) int {
	for i, e := range envs {
		if e.Name == name {
			return i
		}
	}
	return -1
}

// TestBuildPod_SkillProfileEnv asserts TATARA_SKILL_PROFILE is set correctly per kind.
func TestBuildPod_SkillProfileEnv(t *testing.T) {
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
		{"selfImprove", "selfImprove"},
		{"", ""},        // unknown -> fail-open
		{"unknown", ""}, // unknown -> fail-open
	}

	proj := testSkillsProj()
	repo := testSkillsRepo()
	cfg := testSkillsCfg()

	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			task := &tatarav1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "task-1", Namespace: "tatara", UID: "uid-1"},
				Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "demo", RepositoryRef: "repo1", Goal: "g", Kind: tc.kind},
			}
			c := BuildPod(proj, repo, task, nil, "http://mem.tatara.svc:8080", cfg).Spec.Containers[0]
			v, found := findEnvVar(c.Env, "TATARA_SKILL_PROFILE")
			require.True(t, found, "TATARA_SKILL_PROFILE must be present for kind %q", tc.kind)
			require.Equal(t, tc.profile, v.Value, "TATARA_SKILL_PROFILE for kind %q", tc.kind)
		})
	}
}

// TestBuildPod_SkillsRepoPresent asserts TATARA_SKILLS_REPO is always set to the default URL.
func TestBuildPod_SkillsRepoPresent(t *testing.T) {
	proj := testSkillsProj()
	repo := testSkillsRepo()
	cfg := testSkillsCfg()
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-1", Namespace: "tatara", UID: "uid-1"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "demo", RepositoryRef: "repo1", Goal: "g", Kind: "implement"},
	}
	c := BuildPod(proj, repo, task, nil, "http://mem.tatara.svc:8080", cfg).Spec.Containers[0]
	v, found := findEnvVar(c.Env, "TATARA_SKILLS_REPO")
	require.True(t, found, "TATARA_SKILLS_REPO must be present")
	require.Equal(t, skillsRepoDefault, v.Value)
}

// TestBuildPod_SkillsRefDefaultsToMain asserts TATARA_SKILLS_REF = "main" when SkillsRef is empty.
func TestBuildPod_SkillsRefDefaultsToMain(t *testing.T) {
	proj := testSkillsProj()
	repo := testSkillsRepo()
	cfg := testSkillsCfg()
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-1", Namespace: "tatara", UID: "uid-1"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "demo", RepositoryRef: "repo1", Goal: "g", Kind: "implement"},
	}
	c := BuildPod(proj, repo, task, nil, "http://mem.tatara.svc:8080", cfg).Spec.Containers[0]
	v, found := findEnvVar(c.Env, "TATARA_SKILLS_REF")
	require.True(t, found, "TATARA_SKILLS_REF must be present")
	require.Equal(t, "main", v.Value)
}

// TestBuildPod_SkillsRefCustom asserts TATARA_SKILLS_REF = the project's SkillsRef when set.
func TestBuildPod_SkillsRefCustom(t *testing.T) {
	proj := testSkillsProj()
	proj.Spec.Agent.SkillsRef = "v2.1.0"
	repo := testSkillsRepo()
	cfg := testSkillsCfg()
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-1", Namespace: "tatara", UID: "uid-1"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "demo", RepositoryRef: "repo1", Goal: "g", Kind: "implement"},
	}
	c := BuildPod(proj, repo, task, nil, "http://mem.tatara.svc:8080", cfg).Spec.Containers[0]
	v, found := findEnvVar(c.Env, "TATARA_SKILLS_REF")
	require.True(t, found, "TATARA_SKILLS_REF must be present")
	require.Equal(t, "v2.1.0", v.Value)
}

// TestBuildPod_SkillEnvsBeforeExtraEnvs asserts that TATARA_SKILL_PROFILE, TATARA_SKILLS_REPO,
// and TATARA_SKILLS_REF all appear before any ExtraEnvs, making operator-set values authoritative.
func TestBuildPod_SkillEnvsBeforeExtraEnvs(t *testing.T) {
	proj := testSkillsProj(
		corev1.EnvVar{Name: "TATARA_SKILL_PROFILE", Value: "override"},
		corev1.EnvVar{Name: "TATARA_SKILLS_REPO", Value: "https://example.com/override"},
		corev1.EnvVar{Name: "TATARA_SKILLS_REF", Value: "override-ref"},
	)
	repo := testSkillsRepo()
	cfg := testSkillsCfg()
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-2", Namespace: "tatara", UID: "uid-2"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "demo", RepositoryRef: "repo1", Goal: "g", Kind: "implement"},
	}
	c := BuildPod(proj, repo, task, nil, "http://mem.tatara.svc:8080", cfg).Spec.Containers[0]

	profileIdx := envIndex(c.Env, "TATARA_SKILL_PROFILE")
	repoIdx := envIndex(c.Env, "TATARA_SKILLS_REPO")
	refIdx := envIndex(c.Env, "TATARA_SKILLS_REF")
	require.NotEqual(t, -1, profileIdx, "TATARA_SKILL_PROFILE must be in env")
	require.NotEqual(t, -1, repoIdx, "TATARA_SKILLS_REPO must be in env")
	require.NotEqual(t, -1, refIdx, "TATARA_SKILLS_REF must be in env")

	// Find ExtraEnvs override indices (second occurrences).
	profileOverrideIdx := -1
	repoOverrideIdx := -1
	refOverrideIdx := -1
	seenProfile, seenRepo, seenRef := false, false, false
	for i, e := range c.Env {
		switch e.Name {
		case "TATARA_SKILL_PROFILE":
			if seenProfile {
				profileOverrideIdx = i
			}
			seenProfile = true
		case "TATARA_SKILLS_REPO":
			if seenRepo {
				repoOverrideIdx = i
			}
			seenRepo = true
		case "TATARA_SKILLS_REF":
			if seenRef {
				refOverrideIdx = i
			}
			seenRef = true
		}
	}
	require.NotEqual(t, -1, profileOverrideIdx, "ExtraEnvs TATARA_SKILL_PROFILE override must appear")
	require.NotEqual(t, -1, repoOverrideIdx, "ExtraEnvs TATARA_SKILLS_REPO override must appear")
	require.NotEqual(t, -1, refOverrideIdx, "ExtraEnvs TATARA_SKILLS_REF override must appear")

	require.Less(t, profileIdx, profileOverrideIdx,
		"operator TATARA_SKILL_PROFILE must precede ExtraEnvs override")
	require.Less(t, repoIdx, repoOverrideIdx,
		"operator TATARA_SKILLS_REPO must precede ExtraEnvs override")
	require.Less(t, refIdx, refOverrideIdx,
		"operator TATARA_SKILLS_REF must precede ExtraEnvs override")

	// The first occurrence is the operator value.
	require.Equal(t, "implement", c.Env[profileIdx].Value, "operator TATARA_SKILL_PROFILE must be 'implement' for kind=implement")
	require.Equal(t, skillsRepoDefault, c.Env[repoIdx].Value, "operator TATARA_SKILLS_REPO must be the default URL")
	require.Equal(t, "main", c.Env[refIdx].Value, "operator TATARA_SKILLS_REF must be 'main' when SkillsRef empty")
}
