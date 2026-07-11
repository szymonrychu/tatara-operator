package agent_test

// TDD: Phase 5, Task 16 - clone scope + prompt context from the ledger.
// Tests written BEFORE implementation; they FAIL until pod.go is updated.

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// TestBuildPod_LedgerCloneScope: when the Task has WorkItems spanning two repos,
// TATARA_REPOS is filtered to those two repos (matching by owner/repo slug) rather
// than returning all project repos.
func TestBuildPod_LedgerCloneScope(t *testing.T) {
	proj, primaryRepo, task, cfg := sampleInputs()

	// Give the primary repo a URL that parses to owner/repo slug "acme/repo1".
	primaryRepo.Spec.URL = "https://github.com/acme/repo1.git"

	// A second repo not in the ledger.
	extraRepo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "extra-repo"},
		Spec:       tatarav1alpha1.RepositorySpec{URL: "https://github.com/acme/extra.git", DefaultBranch: "main"},
	}

	// Ledger spans only repo1 and repo2, not extra.
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "acme/repo1", Number: 5, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
		{Provider: "github", Repo: "acme/repo2", Number: 2, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleCloses, State: tatarav1alpha1.WIOpen},
	}

	repo2 := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "repo2"},
		Spec:       tatarav1alpha1.RepositorySpec{URL: "https://github.com/acme/repo2.git", DefaultBranch: "main"},
	}

	allRepos := []tatarav1alpha1.Repository{*primaryRepo, *repo2, *extraRepo}

	pod := agent.BuildPod(proj, primaryRepo, task, allRepos, testMemoryEndpoint, cfg)
	c := pod.Spec.Containers[0]

	reposVal, ok := envValue(c, "TATARA_REPOS")
	require.True(t, ok, "TATARA_REPOS must be present")

	var repos []map[string]string
	require.NoError(t, json.Unmarshal([]byte(reposVal), &repos))

	// Must include repo1 and repo2, must NOT include extra.
	names := make([]string, 0, len(repos))
	for _, r := range repos {
		names = append(names, r["name"])
	}
	require.Contains(t, names, "repo1", "primary repo must be in TATARA_REPOS")
	require.Contains(t, names, "repo2", "ledger-spanned repo2 must be in TATARA_REPOS")
	require.NotContains(t, names, "extra-repo", "extra repo not in ledger must be excluded")
}

// TestBuildPod_UmbrellaKind_ClonesAllProjectRepos is the U-B regression: an
// umbrella kind (clarify/implement/review) must clone ALL enrolled project repos
// even when its ledger only spans the source repo, so the umbrella agent gets
// every repo at once. A narrow ledger must NOT shrink the clone set.
func TestBuildPod_UmbrellaKind_ClonesAllProjectRepos(t *testing.T) {
	for _, kind := range []string{"implement", "review", "clarify"} {
		t.Run(kind, func(t *testing.T) {
			proj, _, task, cfg := sampleInputs()
			task.Spec.Kind = kind
			task.Spec.RepositoryRef = ""

			// Ledger spans only repo1, but the project enrolls three repos.
			task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
				{Provider: "github", Repo: "acme/repo1", Number: 5, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
			}
			allRepos := []tatarav1alpha1.Repository{
				{ObjectMeta: metav1.ObjectMeta{Name: "repo1"}, Spec: tatarav1alpha1.RepositorySpec{URL: "https://github.com/acme/repo1.git", DefaultBranch: "main"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "repo2"}, Spec: tatarav1alpha1.RepositorySpec{URL: "https://github.com/acme/repo2.git", DefaultBranch: "main"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "repo3"}, Spec: tatarav1alpha1.RepositorySpec{URL: "https://github.com/acme/repo3.git", DefaultBranch: "main"}},
			}

			pod := agent.BuildPod(proj, nil, task, allRepos, testMemoryEndpoint, cfg)
			c := pod.Spec.Containers[0]

			reposVal, ok := envValue(c, "TATARA_REPOS")
			require.True(t, ok, "TATARA_REPOS must be present")
			var repos []map[string]string
			require.NoError(t, json.Unmarshal([]byte(reposVal), &repos))
			names := make([]string, 0, len(repos))
			for _, r := range repos {
				names = append(names, r["name"])
			}
			require.Len(t, repos, 3, "umbrella kind must clone ALL project repos, not just the ledger scope")
			require.Contains(t, names, "repo1")
			require.Contains(t, names, "repo2")
			require.Contains(t, names, "repo3")

			// Full-clone is on for a project-scoped umbrella (repo==nil, not brainstorm).
			fc, _ := envValue(c, "TATARA_WORKSPACE_FULL_CLONE")
			require.Equal(t, "true", fc, "umbrella kind must full-clone")
		})
	}
}

// TestBuildPod_LedgerCloneScope_EmptyLedger: when the ledger is empty, fall back
// to the full project repo list (backward compatibility).
func TestBuildPod_LedgerCloneScope_EmptyLedger(t *testing.T) {
	proj, primaryRepo, task, cfg := sampleInputs()
	primaryRepo.Spec.URL = "https://github.com/acme/r1.git"

	// No WorkItems -> backward-compat fallback: all repos.
	task.Status.WorkItems = nil

	allRepos := []tatarav1alpha1.Repository{
		*primaryRepo,
		{ObjectMeta: metav1.ObjectMeta{Name: "r2"}, Spec: tatarav1alpha1.RepositorySpec{URL: "https://github.com/acme/r2.git", DefaultBranch: "main"}},
	}

	pod := agent.BuildPod(proj, primaryRepo, task, allRepos, testMemoryEndpoint, cfg)
	c := pod.Spec.Containers[0]

	reposVal, ok := envValue(c, "TATARA_REPOS")
	require.True(t, ok, "TATARA_REPOS must be present")

	var repos []map[string]string
	require.NoError(t, json.Unmarshal([]byte(reposVal), &repos))
	require.Len(t, repos, 2, "empty ledger must fall back to all repos")
}

// TestBuildPod_LedgerCloneScope_NoSilentFallbackOnUnparseableURL: when at least
// one repo matches the ledger scope, a sibling repo whose URL cannot be parsed to
// the ledger slug (e.g. an SSH form) must be EXCLUDED, not trigger a silent
// fall-back to the full unfiltered project list.
func TestBuildPod_LedgerCloneScope_NoSilentFallbackOnUnparseableURL(t *testing.T) {
	proj, primaryRepo, task, cfg := sampleInputs()
	primaryRepo.Spec.URL = "https://github.com/acme/repo1.git"

	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "acme/repo1", Number: 5, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
	}

	// Sibling repo with an SSH-form URL that the canonical parser rejects.
	sshRepo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "ssh-repo"},
		Spec:       tatarav1alpha1.RepositorySpec{URL: "git@github.com:acme/sshonly.git", DefaultBranch: "main"},
	}
	allRepos := []tatarav1alpha1.Repository{*primaryRepo, *sshRepo}

	pod := agent.BuildPod(proj, primaryRepo, task, allRepos, testMemoryEndpoint, cfg)
	c := pod.Spec.Containers[0]

	reposVal, ok := envValue(c, "TATARA_REPOS")
	require.True(t, ok)
	var repos []map[string]string
	require.NoError(t, json.Unmarshal([]byte(reposVal), &repos))
	names := make([]string, 0, len(repos))
	for _, r := range repos {
		names = append(names, r["name"])
	}
	require.Contains(t, names, "repo1")
	require.NotContains(t, names, "ssh-repo", "unparseable sibling must be excluded, not trigger a full-list fallback")
}

// TestBuildPod_WorkItemContext: when the Task has open WorkItems, TATARA_WORK_ITEMS
// env must be set to a non-empty context string containing open issue/MR refs.
func TestBuildPod_WorkItemContext(t *testing.T) {
	proj, primaryRepo, task, cfg := sampleInputs()

	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "acme/repo1", Number: 5, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen, Title: "Fix the bug"},
		{Provider: "github", Repo: "acme/repo2", Number: 3, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleCloses, State: tatarav1alpha1.WIOpen, Title: "Related cleanup"},
		{Provider: "github", Repo: "acme/repo1", Number: 10, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIMerged},
	}

	pod := agent.BuildPod(proj, primaryRepo, task, nil, testMemoryEndpoint, cfg)
	c := pod.Spec.Containers[0]

	v, ok := envValue(c, "TATARA_WORK_ITEMS")
	require.True(t, ok, "TATARA_WORK_ITEMS must be present when WorkItems non-empty")
	require.NotEmpty(t, v)

	// Must contain open issue refs.
	require.True(t, strings.Contains(v, "acme/repo1#5") || strings.Contains(v, "5"), "must include source issue ref")
	require.True(t, strings.Contains(v, "acme/repo2#3") || strings.Contains(v, "3"), "must include closes issue ref")
	// Must note open state for open items.
	require.Contains(t, v, "open", "must include state for open items")
}

// TestBuildPod_WorkItemContext_EmptyWhenNoItems: when WorkItems is empty,
// TATARA_WORK_ITEMS must be absent (no empty env injection).
func TestBuildPod_WorkItemContext_EmptyWhenNoItems(t *testing.T) {
	proj, primaryRepo, task, cfg := sampleInputs()
	task.Status.WorkItems = nil

	pod := agent.BuildPod(proj, primaryRepo, task, nil, testMemoryEndpoint, cfg)
	c := pod.Spec.Containers[0]

	_, ok := envValue(c, "TATARA_WORK_ITEMS")
	require.False(t, ok, "TATARA_WORK_ITEMS must be absent when WorkItems empty")
}

// TestBuildPod_ReposInScopeUnioned: a cross-repo task declares Spec.ReposInScope
// (Repository CR names) for repos NOT yet in the ledger (they only enter WorkItems
// via the Phase-3 backstop after PRs exist). Those repos must still be cloned, so
// the cross-repo edit is not silently lost.
func TestBuildPod_ReposInScopeUnioned(t *testing.T) {
	proj, primaryRepo, task, cfg := sampleInputs()
	primaryRepo.Spec.URL = "https://github.com/acme/repo1.git"

	// Ledger spans only the source repo (secondary repos not yet seeded).
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "acme/repo1", Number: 5, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
	}
	// Declarative cross-repo scope names a Repository CR (by NAME, not slug).
	task.Spec.ReposInScope = []string{"repoB"}

	repoB := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "repoB"},
		Spec:       tatarav1alpha1.RepositorySpec{URL: "https://github.com/acme/other.git", DefaultBranch: "main"},
	}
	extraRepo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "extra-repo"},
		Spec:       tatarav1alpha1.RepositorySpec{URL: "https://github.com/acme/extra.git", DefaultBranch: "main"},
	}

	allRepos := []tatarav1alpha1.Repository{*primaryRepo, *repoB, *extraRepo}

	pod := agent.BuildPod(proj, primaryRepo, task, allRepos, testMemoryEndpoint, cfg)
	c := pod.Spec.Containers[0]

	reposVal, ok := envValue(c, "TATARA_REPOS")
	require.True(t, ok, "TATARA_REPOS must be present")

	var repos []map[string]string
	require.NoError(t, json.Unmarshal([]byte(reposVal), &repos))
	names := make([]string, 0, len(repos))
	for _, r := range repos {
		names = append(names, r["name"])
	}
	require.Contains(t, names, "repo1", "primary repo must be in TATARA_REPOS")
	require.Contains(t, names, "repoB", "ReposInScope repo must be cloned even when absent from the ledger")
	require.NotContains(t, names, "extra-repo", "repo neither in ledger nor in ReposInScope must be excluded")
}

// TestTaskReposInScope_Dedup: TaskReposInScope returns sorted, deduplicated slugs.
func TestTaskReposInScope_Dedup(t *testing.T) {
	task := &tatarav1alpha1.Task{
		Spec: tatarav1alpha1.TaskSpec{
			Source: &tatarav1alpha1.TaskSource{IssueRef: "org/repo1#5"},
		},
		Status: tatarav1alpha1.TaskStatus{
			WorkItems: []tatarav1alpha1.WorkItemRef{
				{Repo: "org/repo2", Number: 1, Kind: tatarav1alpha1.WorkItemIssue},
				{Repo: "org/repo1", Number: 5, Kind: tatarav1alpha1.WorkItemIssue},
				{Repo: "org/repo2", Number: 2, Kind: tatarav1alpha1.WorkItemPR}, // dup
			},
		},
	}
	got := tatarav1alpha1.TaskReposInScope(task)
	require.Equal(t, []string{"org/repo1", "org/repo2"}, got)
}
