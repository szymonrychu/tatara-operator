package agent_test

// TDD: project-scoped pod tests (written before implementation).
// These tests define behavior for BuildPod/BuildPodName/StampPodName
// when repo==nil (brainstorm/healthCheck project-scoped tasks).

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// TestBuildPod_nilRepo_omitsRepoURLBranch: BuildPod with nil repo must not set
// REPO_URL or REPO_BRANCH env vars.
func TestBuildPod_nilRepo_omitsRepoURLBranch(t *testing.T) {
	proj, _, task, cfg := sampleInputs()
	task.Spec.Kind = "brainstorm"
	task.Spec.RepositoryRef = ""

	allRepos := []tatarav1alpha1.Repository{
		{ObjectMeta: metav1.ObjectMeta{Name: "repo1"}, Spec: tatarav1alpha1.RepositorySpec{URL: "https://git/acme/repo1", DefaultBranch: "main"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "repo2"}, Spec: tatarav1alpha1.RepositorySpec{URL: "https://git/acme/repo2", DefaultBranch: "dev"}},
	}

	pod := agent.BuildPod(proj, nil, task, allRepos, testMemoryEndpoint, cfg)
	c := pod.Spec.Containers[0]

	_, hasRepoURL := envValue(c, "REPO_URL")
	require.False(t, hasRepoURL, "REPO_URL must be absent when repo==nil")

	_, hasRepoBranch := envValue(c, "REPO_BRANCH")
	require.False(t, hasRepoBranch, "REPO_BRANCH must be absent when repo==nil")
}

// TestBuildPod_nilRepo_setsTATARA_REPOS: BuildPod with nil repo must set
// TATARA_REPOS to all repos sorted by name (no primary-first reordering).
func TestBuildPod_nilRepo_setsTATARA_REPOS(t *testing.T) {
	proj, _, task, cfg := sampleInputs()
	task.Spec.Kind = "brainstorm"
	task.Spec.RepositoryRef = ""

	allRepos := []tatarav1alpha1.Repository{
		{ObjectMeta: metav1.ObjectMeta{Name: "zzz-repo"}, Spec: tatarav1alpha1.RepositorySpec{URL: "https://git/acme/zzz", DefaultBranch: "main"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "aaa-repo"}, Spec: tatarav1alpha1.RepositorySpec{URL: "https://git/acme/aaa", DefaultBranch: "dev"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "mmm-repo"}, Spec: tatarav1alpha1.RepositorySpec{URL: "https://git/acme/mmm", DefaultBranch: "main"}},
	}

	pod := agent.BuildPod(proj, nil, task, allRepos, testMemoryEndpoint, cfg)
	c := pod.Spec.Containers[0]

	v, ok := envValue(c, "TATARA_REPOS")
	require.True(t, ok, "TATARA_REPOS must be present when repo==nil and allRepos non-empty")

	var got []map[string]string
	require.NoError(t, json.Unmarshal([]byte(v), &got))
	require.Len(t, got, 3)
	// Must be sorted by name.
	require.Equal(t, "aaa-repo", got[0]["name"])
	require.Equal(t, "mmm-repo", got[1]["name"])
	require.Equal(t, "zzz-repo", got[2]["name"])
}

// TestBuildPodName_emptyRepoRef_isProjectScoped: BuildPodName with empty repoRef
// produces a project-scoped name (no repo segment).
func TestBuildPodName_emptyRepoRef_isProjectScoped(t *testing.T) {
	task := &tatarav1alpha1.Task{
		Spec: tatarav1alpha1.TaskSpec{Kind: "brainstorm"},
	}
	got := agent.BuildPodName("myproj", "github", "", task)
	// Must be: tatara-myproj-gh-brainstorm (no repo segment)
	require.Equal(t, "tatara-myproj-gh-brainstorm", got)
}

// TestBuildPodName_emptyRepoRef_healthcheck: healthCheck (activity label) with
// empty repoRef gets project-scoped healthcheck name.
func TestBuildPodName_emptyRepoRef_healthcheck(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{tatarav1alpha1.LabelActivity: "healthCheck"},
		},
		Spec: tatarav1alpha1.TaskSpec{Kind: "brainstorm"},
	}
	got := agent.BuildPodName("myproj", "github", "", task)
	require.Equal(t, "tatara-myproj-gh-healthcheck", got)
}

// TestStampPodName_emptyRepoRef: StampPodName with empty repoRef stamps the
// project-scoped pod name (no repo segment).
func TestStampPodName_emptyRepoRef(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "bs-proj-1"},
		Spec:       tatarav1alpha1.TaskSpec{Kind: "brainstorm", ProjectRef: "myproj"},
	}
	agent.StampPodName(task, "myproj", "github", "")
	require.Equal(t, "tatara-myproj-gh-brainstorm", agent.PodName(task))
}

// TestBuildPod_nilRepo_stillSetsOtherEnvs: other env vars (MODEL, TASK_BRANCH,
// etc.) are still set correctly when repo==nil.
func TestBuildPod_nilRepo_stillSetsOtherEnvs(t *testing.T) {
	proj, _, task, cfg := sampleInputs()
	task.Spec.Kind = "brainstorm"
	task.Spec.RepositoryRef = ""
	agent.StampPodName(task, "demo", "github", "")

	pod := agent.BuildPod(proj, nil, task, nil, testMemoryEndpoint, cfg)
	c := pod.Spec.Containers[0]

	model, ok := envValue(c, "MODEL")
	require.True(t, ok)
	// brainstorm carries a locked per-kind model tier (kindDefaultModel=opus)
	// that overrides the project-wide Agent.Model, matching documentation's lock.
	require.Equal(t, "claude-opus-4-8", model)

	proj2, ok2 := envValue(c, "TATARA_PROJECT")
	require.True(t, ok2)
	require.Equal(t, "demo", proj2)
}
