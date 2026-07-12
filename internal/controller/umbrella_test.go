// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ---- fakes -------------------------------------------------------------

// umbrellaFakeReader implements scm.SCMReader + scm.PRCommentLister for the
// umbrella refresh/thread tests. Keyed by "owner/repo".
type umbrellaFakeReader struct {
	openIssues map[string][]scm.IssueRef
	openPRs    map[string][]scm.PRRef
	issueBody  map[string]string             // "owner/repo#n" -> body
	ci         map[string]string             // sha -> status
	issueCmts  map[string][]scm.IssueComment // "owner/repo#n" -> thread
	prCmts     map[string][]scm.IssueComment // "owner/repo#n" -> thread
}

func (f *umbrellaFakeReader) ListOpenIssues(_ context.Context, owner, repo string) ([]scm.IssueRef, error) {
	return f.openIssues[owner+"/"+repo], nil
}
func (f *umbrellaFakeReader) ListOpenPRs(_ context.Context, owner, repo string) ([]scm.PRRef, error) {
	return f.openPRs[owner+"/"+repo], nil
}
func (f *umbrellaFakeReader) ListBoardItems(context.Context, scm.BoardRef) ([]scm.BoardItem, error) {
	return nil, nil
}
func (f *umbrellaFakeReader) GetCommitCIStatus(_ context.Context, _, _, sha string) (string, error) {
	return f.ci[sha], nil
}
func (f *umbrellaFakeReader) ListIssueComments(_ context.Context, owner, repo string, number int) ([]scm.IssueComment, error) {
	return f.issueCmts[refKey(owner+"/"+repo, number)], nil
}
func (f *umbrellaFakeReader) ListPRComments(_ context.Context, owner, repo string, number int) ([]scm.IssueComment, error) {
	return f.prCmts[refKey(owner+"/"+repo, number)], nil
}
func (f *umbrellaFakeReader) GetIssue(_ context.Context, owner, repo string, number int) (scm.IssueContent, error) {
	return scm.IssueContent{Title: "t", Body: f.issueBody[refKey(owner+"/"+repo, number)]}, nil
}
func (f *umbrellaFakeReader) GetDefaultBranchHeadSHA(context.Context, string, string) (string, error) {
	return "", nil
}
func (f *umbrellaFakeReader) ListClosedIssues(context.Context, string, string, time.Time) ([]scm.IssueRef, error) {
	return nil, nil
}
func (f *umbrellaFakeReader) ListCommits(context.Context, string, string, time.Time) ([]scm.CommitRef, error) {
	return nil, nil
}

type umbrellaFakeMerger struct{ state scm.MergeState }

func (m umbrellaFakeMerger) GetMergeState(context.Context, string, string, int) (scm.MergeState, error) {
	return m.state, nil
}

func refKey(repo string, n int) string {
	return repo + "#" + strconv.Itoa(n)
}

// ---- buildUmbrellaPrompt ----------------------------------------------

func TestBuildUmbrellaPrompt_RendersAllSections(t *testing.T) {
	task := &tatarav1alpha1.Task{}
	task.Spec.Goal = "Ship cross-repo feature"
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/a", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen,
			Body: "Issue A body", Labels: []string{"tatara-implementation", "bug"}},
		{Provider: "gitlab", Repo: "o/b", Number: 42, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen,
			Body: "PR B body", Labels: []string{"tatara-approved"},
			HeadBranch: "tatara/task-x", CIStatus: "success", Mergeable: "clean"},
	}
	repos := []umbrellaRepo{
		{Slug: "o/a", Name: "repo-a", URL: "https://github.com/o/a", DefaultBranch: "main"},
		{Slug: "o/b", Name: "repo-b", URL: "https://github.com/o/b", DefaultBranch: "master"},
	}
	threads := map[string][]scm.IssueComment{
		"o/a#7":  {{Author: "alice", Body: "please do X"}},
		"o/b!42": {{Author: "bob", Body: "LGTM"}},
	}
	out := buildUmbrellaPrompt(task, repos, threads, "Do the clarify thing.", nil)

	require.Contains(t, out, "# Umbrella task: Ship cross-repo feature")
	// Repos in scope + per-repo checkout instructions.
	require.Contains(t, out, "## Repos in scope")
	require.Contains(t, out, "o/a")
	require.Contains(t, out, "https://github.com/o/a")
	require.Contains(t, out, "main")
	require.Contains(t, out, "/workspace/repo-a")
	require.Contains(t, out, "master")
	// Issues section: body + thread + state + labels + repo.
	require.Contains(t, out, "## Issues")
	require.Contains(t, out, "### o/a#7")
	require.Contains(t, out, "Issue A body")
	require.Contains(t, out, "tatara-implementation")
	require.Contains(t, out, "**alice**: please do X")
	// MR section: description + branch + CI + mergeable + state + thread.
	require.Contains(t, out, "## Merge requests")
	require.Contains(t, out, "### o/b!42")
	require.Contains(t, out, "PR B body")
	require.Contains(t, out, "branch:tatara/task-x")
	require.Contains(t, out, "CI:success")
	require.Contains(t, out, "mergeable:clean")
	require.Contains(t, out, "**bob**: LGTM")
	// Task goal tail.
	require.Contains(t, out, "## Your task")
	require.Contains(t, out, "Do the clarify thing.")
}

func TestBuildUmbrellaPrompt_PerMemberThreadBudget(t *testing.T) {
	task := &tatarav1alpha1.Task{}
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Repo: "o/a", Number: 1, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
	}
	long := strings.Repeat("x", triageCommentCharBudget+5000)
	threads := map[string][]scm.IssueComment{
		"o/a#1": {{Author: "a", Body: long}},
	}
	out := buildUmbrellaPrompt(task, nil, threads, "goal", nil)
	require.Less(t, len(out), len(long), "per-member thread must be char-budgeted")
}

func TestBuildUmbrellaPrompt_RendersSystemicSiblings(t *testing.T) {
	task := &tatarav1alpha1.Task{}
	task.Spec.Goal = "Ship the design"
	siblings := []systemicSiblingInfo{
		{Ref: "o/r1#7", Found: true, Locked: true, Phase: "locked"},
		{Ref: "o/r2#9", Found: true, Locked: false, Phase: "open"},
		{Ref: "o/r3#3", Found: false},
	}
	out := buildUmbrellaPrompt(task, nil, nil, "goal", siblings)

	require.Contains(t, out, "## Related systemic-group issues")
	require.Contains(t, out, "o/r1#7: implementation-locked")
	require.Contains(t, out, "o/r2#9: still open")
	require.Contains(t, out, "o/r3#3: not yet tracked")
}

// ---- refreshUmbrellaMembers -------------------------------------------

func TestRefreshUmbrellaMembers_MergesFreshState(t *testing.T) {
	task := &tatarav1alpha1.Task{}
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/a", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
		{Provider: "github", Repo: "o/b", Number: 42, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadSHA: "sha1"},
	}
	reader := &umbrellaFakeReader{
		openIssues: map[string][]scm.IssueRef{
			"o/a": {{Repo: "o/a", Number: 7, State: "open", Labels: []string{"tatara-implementation"}}},
		},
		openPRs: map[string][]scm.PRRef{
			"o/b": {{Repo: "o/b", Number: 42, HeadSHA: "sha2", HeadBranch: "feat/x", Body: "pr body", Labels: []string{"tatara-approved"}}},
		},
		issueBody: map[string]string{"o/a#7": "issue body"},
		ci:        map[string]string{"sha2": "success"},
	}
	merger := umbrellaFakeMerger{state: scm.MergeStateClean}
	slugToURL := map[string]string{"o/a": "https://github.com/o/a", "o/b": "https://github.com/o/b"}

	changed := refreshUmbrellaMembers(context.Background(), reader, merger, slugToURL, "tok", task, time.Minute, time.Now())
	require.True(t, changed)

	iss := task.Status.WorkItems[0]
	require.Equal(t, "issue body", iss.Body)
	require.Equal(t, []string{"tatara-implementation"}, iss.Labels)
	require.NotNil(t, iss.LastRefreshedAt)

	pr := task.Status.WorkItems[1]
	require.Equal(t, "pr body", pr.Body)
	require.Equal(t, "feat/x", pr.HeadBranch)
	require.Equal(t, "sha2", pr.HeadSHA)
	require.Equal(t, "success", pr.CIStatus)
	require.Equal(t, string(scm.MergeStateClean), pr.Mergeable)
	require.Equal(t, []string{"tatara-approved"}, pr.Labels)
	require.NotNil(t, pr.LastRefreshedAt)
}

func TestRefreshUmbrellaMembers_TTLGatedSkipsFresh(t *testing.T) {
	now := time.Now()
	fresh := metav1.NewTime(now.Add(-10 * time.Second))
	task := &tatarav1alpha1.Task{}
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/a", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen, LastRefreshedAt: &fresh},
	}
	reader := &umbrellaFakeReader{
		openIssues: map[string][]scm.IssueRef{"o/a": {{Repo: "o/a", Number: 7, State: "open"}}},
		issueBody:  map[string]string{"o/a#7": "should not be fetched"},
	}
	changed := refreshUmbrellaMembers(context.Background(), reader, nil, nil, "tok", task, time.Minute, now)
	require.False(t, changed)
	require.Empty(t, task.Status.WorkItems[0].Body, "fresh member within TTL must not be re-polled")
}

func TestRefreshUmbrellaMembers_ClosedPRAndIssue(t *testing.T) {
	task := &tatarav1alpha1.Task{}
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/a", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
		{Provider: "github", Repo: "o/a", Number: 9, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen},
	}
	// Empty open lists: both are gone from SCM -> closed.
	reader := &umbrellaFakeReader{openIssues: map[string][]scm.IssueRef{}, openPRs: map[string][]scm.PRRef{}}
	changed := refreshUmbrellaMembers(context.Background(), reader, nil, nil, "tok", task, time.Minute, time.Now())
	require.True(t, changed)
	require.Equal(t, tatarav1alpha1.WIClosed, task.Status.WorkItems[0].State)
	require.Equal(t, tatarav1alpha1.WIClosed, task.Status.WorkItems[1].State)
}

// ---- buildUmbrellaPromptFor (live assembly wiring) --------------------

func TestBuildUmbrellaPromptFor_AssemblesLiveBundle(t *testing.T) {
	ctx := context.Background()
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	reader := &umbrellaFakeReader{
		openIssues: map[string][]scm.IssueRef{
			"o/r": {{Repo: "o/r", Number: 5, State: "open", Labels: []string{"tatara-implementation"}}},
		},
		issueBody: map[string]string{"o/r#5": "the live issue body"},
		issueCmts: map[string][]scm.IssueComment{"o/r#5": {{Author: "carol", Body: "context comment"}}},
	}
	r.ReaderFor = func(string, string) (scm.SCMReader, error) { return reader, nil }

	task := seedWritebackKindTask(t, "umbrella-live", "umbrella-proj", "umbrella-repo", "umbrella-sec",
		tatarav1alpha1.TaskSpec{
			Kind:   "clarify",
			Goal:   "Clarify the thing",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5},
		}, &tatarav1alpha1.ScmSpec{Provider: "github", BotLogin: "tatara-bot"})
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 5, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
	}
	require.NoError(t, k8sClient.Status().Update(ctx, task))

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "umbrella-proj"}, &proj))

	out := r.buildUmbrellaPromptFor(ctx, &proj, task, "GOAL-TAIL-MARKER")
	require.Contains(t, out, "## Repos in scope")
	require.Contains(t, out, "o/r")
	require.Contains(t, out, "## Issues")
	require.Contains(t, out, "### o/r#5")
	require.Contains(t, out, "the live issue body")
	require.Contains(t, out, "**carol**: context comment")
	require.Contains(t, out, "GOAL-TAIL-MARKER")
}

func TestBuildUmbrellaPromptFor_IncludesLockedSystemicSibling(t *testing.T) {
	ctx := context.Background()
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	reader := &umbrellaFakeReader{
		openIssues: map[string][]scm.IssueRef{
			"o/r": {{Repo: "o/r", Number: 5, State: "open"}},
		},
		issueBody: map[string]string{"o/r#5": "the live issue body"},
	}
	r.ReaderFor = func(string, string) (scm.SCMReader, error) { return reader, nil }

	task := seedWritebackKindTask(t, "umbrella-sibs", "umbrella-sibs-proj", "umbrella-sibs-repo", "umbrella-sibs-sec",
		tatarav1alpha1.TaskSpec{
			Kind:   "clarify",
			Goal:   "Clarify the thing",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5},
			SystemicGroup: &tatarav1alpha1.SystemicGroup{
				SystemicID:       "sg1",
				SameRepoSiblings: []int{7},
			},
		}, &tatarav1alpha1.ScmSpec{Provider: "github", BotLogin: "tatara-bot"})
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 5, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
	}
	require.NoError(t, k8sClient.Status().Update(ctx, task))

	// Seed the sibling's own Task, already implementation-locked. Created
	// directly (not via seedWritebackKindTask) since the lead task above
	// already seeded the shared project/repo/secret and re-seeding them
	// under the same names would collide on Create.
	sibling := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "umbrella-sibs-sib", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    "umbrella-sibs-proj",
			RepositoryRef: "umbrella-sibs-repo",
			Kind:          "clarify",
			Source:        &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#7", Number: 7},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, sibling))
	sibling.Status.ImplementationLocked = true
	require.NoError(t, k8sClient.Status().Update(ctx, sibling))

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "umbrella-sibs-proj"}, &proj))

	out := r.buildUmbrellaPromptFor(ctx, &proj, task, "GOAL-TAIL-MARKER")
	require.Contains(t, out, "## Related systemic-group issues")
	require.Contains(t, out, "o/r#7: implementation-locked")
}

// ---- umbrellaLinkedIssue (source-issue <-> PR linkage) -----------------

func TestUmbrellaLinkedIssue_ResolvesSourceForPR(t *testing.T) {
	task := &tatarav1alpha1.Task{}
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Repo: "o/a", Number: 7, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
		{Repo: "o/a", Number: 42, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen},
	}
	require.Equal(t, 7, umbrellaLinkedIssue(task, "o/a"))
	require.Equal(t, 0, umbrellaLinkedIssue(task, "o/other"))
}
