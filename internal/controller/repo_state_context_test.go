package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// capturingCIReader records every GetCommitCIStatus call (owner, repo, sha).
type capturingCIReader struct {
	fakeRdr
	prs     []scm.PRRef
	calls   []ciCall
	results map[string]string // sha -> status
}

type ciCall struct {
	owner, repo, sha string
}

func (c *capturingCIReader) ListOpenPRs(_ context.Context, _, _ string) ([]scm.PRRef, error) {
	return c.prs, nil
}

func (c *capturingCIReader) GetCommitCIStatus(_ context.Context, owner, repo, sha string) (string, error) {
	c.calls = append(c.calls, ciCall{owner: owner, repo: repo, sha: sha})
	if c.results != nil {
		if st, ok := c.results[sha]; ok {
			return st, nil
		}
	}
	return "", nil
}

// fakeRdr is a minimal SCMReader for repo-state-context tests. It returns no
// comments (so botCommentedOnIssue -> false) and empty CI statuses.
type fakeRdr struct{}

func (fakeRdr) ListOpenPRs(context.Context, string, string) ([]scm.PRRef, error) {
	return nil, nil
}
func (fakeRdr) ListOpenIssues(context.Context, string, string) ([]scm.IssueRef, error) {
	return nil, nil
}
func (fakeRdr) ListBoardItems(context.Context, scm.BoardRef) ([]scm.BoardItem, error) {
	return nil, nil
}
func (fakeRdr) GetCommitCIStatus(context.Context, string, string, string) (string, error) {
	return "", nil
}
func (fakeRdr) ListIssueComments(context.Context, string, string, int) ([]scm.IssueComment, error) {
	return nil, nil
}
func (fakeRdr) GetIssue(context.Context, string, string, int) (scm.IssueContent, error) {
	return scm.IssueContent{}, nil
}
func (fakeRdr) GetDefaultBranchHeadSHA(context.Context, string, string) (string, error) {
	return "", nil
}
func (fakeRdr) ListClosedIssues(context.Context, string, string, time.Time) ([]scm.IssueRef, error) {
	return nil, nil
}
func (fakeRdr) ListCommits(context.Context, string, string, time.Time) ([]scm.CommitRef, error) {
	return nil, nil
}

func TestBuildRepoStateContext_Blocks(t *testing.T) {
	r := newScanReconciler(&fakeReader{})
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{Provider: "github", BotLogin: "bot"}
	repos := []tatarav1alpha1.Repository{
		{Spec: tatarav1alpha1.RepositorySpec{URL: "https://github.com/o/r"}},
	}
	issues := map[string][]scm.IssueRef{
		"o/r": {{Repo: "o/r", Number: 1, Title: "an open issue", Labels: []string{"bug"}}},
	}
	prs := map[string][]scm.PRRef{
		"o/r": {{Repo: "o/r", Number: 9, Body: "Add metrics"}},
	}
	prCI := map[string]map[int]string{
		"o/r": {9: "failure"},
	}
	mainCI := map[string]string{
		"o/r": "success",
	}
	got := r.buildRepoStateContext(context.Background(), proj, fakeRdr{}, issues, prs, prCI, mainCI, repos)
	for _, want := range []string{
		"ISSUES:", "o/r#1 [bug] an open issue",
		"OPEN MRs:", "o/r#9 [ci:failure]",
		"MAIN HEALTH:", "o/r main CI: success",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("context missing %q\n---\n%s", want, got)
		}
	}
}

func TestBuildRepoStateContext_GitLabSeparator(t *testing.T) {
	r := newScanReconciler(&fakeReader{})
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{Provider: "gitlab", BotLogin: "bot"}
	repos := []tatarav1alpha1.Repository{
		{Spec: tatarav1alpha1.RepositorySpec{URL: "https://gitlab.com/o/r"}},
	}
	issues := map[string][]scm.IssueRef{}
	prs := map[string][]scm.PRRef{
		"o/r": {{Repo: "o/r", Number: 9, Body: "add feature"}},
	}
	prCI := map[string]map[int]string{
		"o/r": {9: "success"},
	}
	mainCI := map[string]string{"o/r": "success"}
	got := r.buildRepoStateContext(context.Background(), proj, fakeRdr{}, issues, prs, prCI, mainCI, repos)
	if !strings.Contains(got, "o/r!9") {
		t.Fatalf("gitlab MR should use ! separator, got:\n%s", got)
	}
}

func TestBuildRepoStateContext_MissingMainCI(t *testing.T) {
	r := newScanReconciler(&fakeReader{})
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{Provider: "github", BotLogin: "bot"}
	repos := []tatarav1alpha1.Repository{
		{Spec: tatarav1alpha1.RepositorySpec{URL: "https://github.com/o/r"}},
	}
	got := r.buildRepoStateContext(context.Background(), proj, fakeRdr{}, nil, nil, nil, nil, repos)
	if !strings.Contains(got, "o/r main CI: unknown") {
		t.Fatalf("missing main CI entry should degrade to 'unknown', got:\n%s", got)
	}
}

func TestBuildRepoStateContext_IssuesCap(t *testing.T) {
	r := newScanReconciler(&fakeReader{})
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{Provider: "github", BotLogin: "bot"}
	repos := []tatarav1alpha1.Repository{
		{Spec: tatarav1alpha1.RepositorySpec{URL: "https://github.com/o/r"}},
	}
	var issues []scm.IssueRef
	for i := 1; i <= 65; i++ {
		issues = append(issues, scm.IssueRef{Repo: "o/r", Number: i, Title: "issue title"})
	}
	issuesBySlug := map[string][]scm.IssueRef{"o/r": issues}
	got := r.buildRepoStateContext(context.Background(), proj, fakeRdr{}, issuesBySlug, nil, nil, nil, repos)
	if !strings.Contains(got, "(+5 more omitted)") {
		t.Fatalf("expected cap notice (+5 more omitted), got:\n%s", got)
	}
}

// TestGatherRepoCIState_GitLab_PRCIUsesFullProjectPath asserts that for GitLab
// repos the GetCommitCIStatus calls for PR-CI use the full URL-derived project
// path (e.g. "mygroup/myproject") not just the first path segment ("mygroup").
// This is the same fix already applied for main-CI (lifecycle.go); this test
// covers the PR-CI path that was previously broken.
func TestGatherRepoCIState_GitLab_PRCIUsesFullProjectPath(t *testing.T) {
	const headSHA = "abc123"
	// GitLab URL with group/project: OwnerRepo returns ("mygroup","myproject"),
	// but GitLabProjectPath returns "mygroup/myproject". Before the fix, PR-CI
	// called GetCommitCIStatus(ctx, "mygroup", "myproject", sha) which would
	// resolve the wrong /projects/mygroup/... API path; after the fix it must
	// call GetCommitCIStatus(ctx, "mygroup/myproject", "", sha).
	cap := &capturingCIReader{
		prs:     []scm.PRRef{{Repo: "mygroup/myproject", Number: 7, HeadSHA: headSHA}},
		results: map[string]string{headSHA: "success"},
	}
	r := newScanReconciler(&fakeReader{})
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{Provider: "gitlab"}
	repos := []tatarav1alpha1.Repository{
		{Spec: tatarav1alpha1.RepositorySpec{URL: "https://gitlab.com/mygroup/myproject"}},
	}
	prsBySlug, prCIBySlug, _ := r.gatherRepoCIState(context.Background(), proj, cap, repos, "test")

	// Expect one PR in the slug.
	slug := "mygroup/myproject"
	if len(prsBySlug[slug]) != 1 {
		t.Fatalf("want 1 PR for slug %s, got %d", slug, len(prsBySlug[slug]))
	}

	// All GetCommitCIStatus calls must use the full project path "mygroup/myproject",
	// not just the first segment "mygroup".
	if len(cap.calls) == 0 {
		t.Fatal("no GetCommitCIStatus calls recorded; PR had a HeadSHA so at least one was expected")
	}
	for _, call := range cap.calls {
		if call.owner != "mygroup/myproject" {
			t.Errorf("GetCommitCIStatus called with owner=%q, want full path %q", call.owner, "mygroup/myproject")
		}
		if call.repo != "" {
			t.Errorf("GetCommitCIStatus called with repo=%q, want empty string (gitlab uses owner for full path)", call.repo)
		}
	}

	// PR CI status must reflect what the fake returned (not "unknown" degradation).
	ciMap := prCIBySlug[slug]
	if ciMap == nil {
		t.Fatal("prCIBySlug missing slug entry")
	}
	if st := ciMap[7]; st != "success" {
		t.Errorf("PR 7 CI status = %q, want %q", st, "success")
	}
}

// TestBuildRepoStateContext_EmptyPRBody_NoPlaceholder asserts that a PR with an
// empty body produces an empty title in the OPEN MRs block rather than the
// "tatara automated change" placeholder from firstLine.
func TestBuildRepoStateContext_EmptyPRBody_NoPlaceholder(t *testing.T) {
	r := newScanReconciler(&fakeReader{})
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{Provider: "github", BotLogin: "bot"}
	repos := []tatarav1alpha1.Repository{
		{Spec: tatarav1alpha1.RepositorySpec{URL: "https://github.com/o/r"}},
	}
	prs := map[string][]scm.PRRef{
		"o/r": {{Repo: "o/r", Number: 3, Body: ""}},
	}
	got := r.buildRepoStateContext(context.Background(), proj, fakeRdr{}, nil, prs, nil, nil, repos)
	if strings.Contains(got, "tatara automated change") {
		t.Fatalf("empty PR body must not produce placeholder text; got:\n%s", got)
	}
	if !strings.Contains(got, "o/r#3") {
		t.Fatalf("PR line for o/r#3 missing; got:\n%s", got)
	}
}
