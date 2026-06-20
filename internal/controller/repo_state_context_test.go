package controller

import (
	"context"
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

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
