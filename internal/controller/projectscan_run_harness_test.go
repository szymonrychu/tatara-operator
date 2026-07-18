package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

type fakeReader struct {
	prs      []scm.PRRef
	issues   []scm.IssueRef
	board    []scm.BoardItem
	prErr    error
	comments []scm.IssueComment
	// commentCalls counts ListIssueComments invocations, for tests asserting the
	// per-cycle comment cache dedupes repeated gate reads of the same issue.
	commentCalls int
}

func (f *fakeReader) ListOpenPRs(context.Context, string, string) ([]scm.PRRef, error) {
	return f.prs, f.prErr
}
func (f *fakeReader) ListOpenIssues(context.Context, string, string) ([]scm.IssueRef, error) {
	return f.issues, nil
}
func (f *fakeReader) ListBoardItems(context.Context, scm.BoardRef) ([]scm.BoardItem, error) {
	return f.board, nil
}
func (f *fakeReader) GetCommitCIStatus(context.Context, string, string, string) (string, error) {
	return "", nil
}
func (f *fakeReader) ListIssueComments(context.Context, string, string, int) ([]scm.IssueComment, error) {
	f.commentCalls++
	return f.comments, nil
}
func (f *fakeReader) GetIssue(context.Context, string, string, int) (scm.IssueContent, error) {
	return scm.IssueContent{}, nil
}
func (f *fakeReader) GetDefaultBranchHeadSHA(context.Context, string, string) (string, error) {
	return "", nil
}
func (f *fakeReader) ListClosedIssues(context.Context, string, string, time.Time) ([]scm.IssueRef, error) {
	return nil, nil
}
func (f *fakeReader) ListCommits(context.Context, string, string, time.Time) ([]scm.CommitRef, error) {
	return nil, nil
}

func seedScanProject(t *testing.T, name string, cron *tatarav1alpha1.ScmCron) (*tatarav1alpha1.Project, *tatarav1alpha1.Repository) {
	t.Helper()
	ctx := context.Background()
	mkSecret(t, name+"-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{}
	proj.Name = name
	proj.Namespace = testNS
	proj.Spec.ScmSecretRef = name + "-scm"
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot", PriorityLabel: "tatara/priority", Cron: cron}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	repo := &tatarav1alpha1.Repository{}
	repo.Name = name + "-repo"
	repo.Namespace = testNS
	repo.Spec = tatarav1alpha1.RepositorySpec{ProjectRef: name, URL: "https://github.com/o/r.git", DefaultBranch: "main", ReingestSchedule: "0 6 * * *"}
	if err := k8sClient.Create(ctx, repo); err != nil {
		t.Fatalf("create repo: %v", err)
	}
	return proj, repo
}

func listScanTasks(t *testing.T, project string) []tatarav1alpha1.Task {
	t.Helper()
	var list tatarav1alpha1.TaskList
	if err := k8sClient.List(context.Background(), &list); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	var out []tatarav1alpha1.Task
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef == project {
			out = append(out, list.Items[i])
		}
	}
	return out
}

func mkScanRepo(t *testing.T, project, name, url string) tatarav1alpha1.Repository {
	t.Helper()
	rp := &tatarav1alpha1.Repository{}
	rp.Name = name
	rp.Namespace = testNS
	rp.Spec = tatarav1alpha1.RepositorySpec{ProjectRef: project, URL: url, DefaultBranch: "main", ReingestSchedule: "0 6 * * *"}
	if err := k8sClient.Create(context.Background(), rp); err != nil {
		t.Fatalf("create repo %s: %v", name, err)
	}
	return *rp
}

func listScanQEs(t *testing.T, project string) []tatarav1alpha1.QueuedEvent {
	t.Helper()
	var list tatarav1alpha1.QueuedEventList
	if err := k8sClient.List(context.Background(), &list); err != nil {
		t.Fatalf("list QEs: %v", err)
	}
	var out []tatarav1alpha1.QueuedEvent
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef == project {
			out = append(out, list.Items[i])
		}
	}
	return out
}

func listBrainstormQEs(t *testing.T, project string) []tatarav1alpha1.QueuedEvent {
	t.Helper()
	qes := listScanQEs(t, project)
	var out []tatarav1alpha1.QueuedEvent
	for _, qe := range qes {
		if qe.Spec.Payload.Labels[labelActivity] == "brainstorm" {
			out = append(out, qe)
		}
	}
	return out
}

func labelsMatch(pairs []*dto.LabelPair, want map[string]string) bool {
	got := map[string]string{}
	for _, p := range pairs {
		got[p.GetName()] = p.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

// perRepoFakeReader allows scripting per-repo issues and errors for brainstorm
// and backstop tests. It falls through to the base fakeReader for PRs/board.
type perRepoFakeReader struct {
	fakeReader
	// issuesByRepo maps "owner/repo" -> issues to return.
	issuesByRepo map[string][]scm.IssueRef
	// errRepos is the set of "owner/repo" slugs that return an error.
	errRepos map[string]bool
}

func (f *perRepoFakeReader) ListOpenIssues(_ context.Context, owner, repo string) ([]scm.IssueRef, error) {
	slug := owner + "/" + repo
	if f.errRepos[slug] {
		return nil, fmt.Errorf("fake error for %s", slug)
	}
	if iss, ok := f.issuesByRepo[slug]; ok {
		return iss, nil
	}
	return nil, nil
}

// seedBrainstormProject creates a Project with ApprovalLabel set and a brainstorm
// cron, plus the requested repositories (by slug "owner/repo").
func seedBrainstormProject(t *testing.T, name string, repoSlugs []string, maxOpenProposals int) (*tatarav1alpha1.Project, []tatarav1alpha1.Repository) {
	t.Helper()
	ctx := context.Background()
	mkSecret(t, name+"-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	cron := &tatarav1alpha1.ScmCron{
		Brainstorm: tatarav1alpha1.BrainstormActivity{
			Enabled:          true,
			Schedule:         "0 * * * *",
			MaxOpenProposals: maxOpenProposals,
		},
	}
	proj := &tatarav1alpha1.Project{}
	proj.Name = name
	proj.Namespace = testNS
	proj.Spec.ScmSecretRef = name + "-scm"
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{
		Provider: "github",
		Owner:    "o",
		BotLogin: "tatara-bot",
		Cron:     cron,
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	var repos []tatarav1alpha1.Repository
	for _, slug := range repoSlugs {
		repoName := name + "-" + strings.ReplaceAll(slug, "/", "-")
		rp := &tatarav1alpha1.Repository{}
		rp.Name = repoName
		rp.Namespace = testNS
		rp.Spec = tatarav1alpha1.RepositorySpec{
			ProjectRef:       name,
			URL:              "https://github.com/" + slug + ".git",
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		}
		if err := k8sClient.Create(ctx, rp); err != nil {
			t.Fatalf("create repo %s: %v", slug, err)
		}
		repos = append(repos, *rp)
	}
	return proj, repos
}

// listBrainstormTasks returns brainstorm tasks for the given project.
func listBrainstormTasks(t *testing.T, project string) []tatarav1alpha1.Task {
	t.Helper()
	tasks := listScanTasks(t, project)
	var out []tatarav1alpha1.Task
	for _, tk := range tasks {
		if tk.Labels[labelActivity] == "brainstorm" {
			out = append(out, tk)
		}
	}
	return out
}

// newScanReconciler is the shared ProjectReconciler used by the scan tests.
func newScanReconciler(reader scm.SCMReader) *ProjectReconciler {
	r := newProjectReconciler()
	r.Seq = &queue.SeqSource{Client: k8sClient, Namespace: testNS}
	r.ReaderFor = func(string, string) (scm.SCMReader, error) { return reader, nil }
	return r
}
