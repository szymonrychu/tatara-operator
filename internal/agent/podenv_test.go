package agent_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

func g9EnvMap(vars []corev1.EnvVar) map[string]string {
	m := make(map[string]string, len(vars))
	for _, e := range vars {
		if _, seen := m[e.Name]; !seen { // first occurrence wins, as in a Pod
			m[e.Name] = e.Value
		}
	}
	return m
}

// TestAgentEnv_G9Contract pins the contract G.9 env block, key for key. The
// three profile-bearing keys are the day-one fleet wedge: they are keyed on the
// AGENT kind (status.agentKind), not on spec.Kind, and the cli fails CLOSED on an
// unknown key - no submit_outcome, no terminal outcome tool at all.
func TestAgentEnv_G9Contract(t *testing.T) {
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "tatara"},
		Spec:       tatarav1alpha1.ProjectSpec{AgentPodTTLSeconds: 3600},
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-7", Namespace: "tatara"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "demo", RepositoryRef: "repo1", Kind: "clarify"},
		Status: tatarav1alpha1.TaskStatus{
			Stage:     tatarav1alpha1.StageImplementing,
			AgentKind: "implement",
		},
	}

	env := g9EnvMap(agent.AgentEnv(proj, task))

	require.Equal(t, "task-7", env["TATARA_TASK"])
	require.Equal(t, "demo", env["TATARA_PROJECT"])
	// CHANGED semantics: TATARA_KIND is the AGENT kind, not spec.Kind.
	require.Equal(t, "implement", env["TATARA_KIND"])
	require.Equal(t, "implement", env["TATARA_TOOL_PROFILE"])
	require.Equal(t, "implement", env["TATARA_SKILL_PROFILE"])
	// Narrowed: empty except for the documentation agent.
	require.Equal(t, "", env["TATARA_REPO"])
	require.Equal(t, agent.TaskBranch(task), env["TASK_BRANCH"])
	require.Equal(t, "3600", env["AGENT_POD_TTL_SECONDS"])
	require.Equal(t, "2", env["TATARA_CONTRACT_VERSION"])
}

// TestAgentEnv_RepoOnlyForDocumentation: TATARA_REPO is the Repository CR name,
// and under the stage machine it is set for the documentation agent ALONE. Every
// other agent kind is project-scoped and sees the repo set via TATARA_REPOS.
func TestAgentEnv_RepoOnlyForDocumentation(t *testing.T) {
	proj := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}
	for _, tc := range []struct {
		agentKind string
		stage     string
		want      string
	}{
		{"documentation", tatarav1alpha1.StageDocumenting, "repo1"},
		{"implement", tatarav1alpha1.StageImplementing, ""},
		{"review", tatarav1alpha1.StageReviewing, ""},
		{"clarify", tatarav1alpha1.StageClarifying, ""},
		{"brainstorm", tatarav1alpha1.StageBrainstorming, ""},
		{"incident", tatarav1alpha1.StageInvestigating, ""},
		{"refine", tatarav1alpha1.StageRefining, ""},
	} {
		t.Run(tc.agentKind, func(t *testing.T) {
			task := &tatarav1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "t"},
				Spec:       tatarav1alpha1.TaskSpec{RepositoryRef: "repo1"},
				Status:     tatarav1alpha1.TaskStatus{Stage: tc.stage, AgentKind: tc.agentKind},
			}
			require.Equal(t, tc.want, g9EnvMap(agent.AgentEnv(proj, task))["TATARA_REPO"])
		})
	}
}

// TestAgentEnv_AllSevenAgentKindsResolveAProfile is the day-one wedge guard.
// The seven agent kinds MUST each resolve to a non-empty profile - INCLUDING
// clarify. resolveProfile fails CLOSED: an unknown key means the cli serves only
// the always-on tool set and never registers submit_outcome, so the pod lists 74
// tools, may call 4 of them, and has no terminal outcome tool at all.
func TestAgentEnv_AllSevenAgentKindsResolveAProfile(t *testing.T) {
	proj := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}
	kinds := []string{"brainstorm", "incident", "clarify", "implement", "review", "refine", "documentation"}
	require.Len(t, kinds, 7)
	for _, k := range kinds {
		t.Run(k, func(t *testing.T) {
			task := &tatarav1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "t"},
				Status:     tatarav1alpha1.TaskStatus{Stage: tatarav1alpha1.StageImplementing, AgentKind: k},
			}
			env := g9EnvMap(agent.AgentEnv(proj, task))
			require.Equal(t, k, env["TATARA_TOOL_PROFILE"], "agent kind %q must resolve a tool profile", k)
			require.Equal(t, k, env["TATARA_SKILL_PROFILE"], "agent kind %q must resolve a skill profile", k)
		})
	}
}

// TestAgentEnv_LegacyTaskFallsBackToSpecKind: a phase-driven Task carries no
// stage and no agentKind. It must keep the pre-redesign behaviour exactly - the
// two models coexist until the cutover.
func TestAgentEnv_LegacyTaskFallsBackToSpecKind(t *testing.T) {
	proj := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t"},
		Spec:       tatarav1alpha1.TaskSpec{Kind: "review", RepositoryRef: "repo1"},
	}
	env := g9EnvMap(agent.AgentEnv(proj, task))
	require.Equal(t, "review", env["TATARA_KIND"])
	require.Equal(t, "review", env["TATARA_TOOL_PROFILE"])
	require.Equal(t, "repo1", env["TATARA_REPO"], "legacy Tasks keep the un-narrowed TATARA_REPO")
}

// TestBuildPod_CarriesG9Block: BuildPod must actually emit the block.
func TestBuildPod_CarriesG9Block(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	proj.Spec.AgentPodTTLSeconds = 7200
	task.Status.Stage = tatarav1alpha1.StageImplementing
	task.Status.AgentKind = "implement"

	env := g9EnvMap(agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0].Env)

	require.Equal(t, "implement", env["TATARA_KIND"])
	require.Equal(t, "implement", env["TATARA_TOOL_PROFILE"])
	require.Equal(t, "implement", env["TATARA_SKILL_PROFILE"])
	require.Equal(t, "7200", env["AGENT_POD_TTL_SECONDS"])
	require.Equal(t, "2", env["TATARA_CONTRACT_VERSION"])
}

// TestBuildPod_ProfileEnvBeatsExtraEnvs: the operator-set profile keys must win
// over a stray ExtraEnvs duplicate. First occurrence wins in a Pod env list.
func TestBuildPod_ProfileEnvBeatsExtraEnvs(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	task.Status.Stage = tatarav1alpha1.StageReviewing
	task.Status.AgentKind = "review"
	proj.Spec.Agent.ExtraEnvs = []corev1.EnvVar{
		{Name: "TATARA_TOOL_PROFILE", Value: "hijacked"},
		{Name: "TATARA_CONTRACT_VERSION", Value: "1"},
	}
	env := g9EnvMap(agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0].Env)
	require.Equal(t, "review", env["TATARA_TOOL_PROFILE"])
	require.Equal(t, "2", env["TATARA_CONTRACT_VERSION"])
}
