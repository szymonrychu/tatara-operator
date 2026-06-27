package restapi_test

// TDD tests for B1 (GET /projects/{p}/issues), B2 (close/edit/create), B3 (GET /projects/{p}/commits).

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/restapi"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// issuesFakeReader implements scm.SCMReader.
type issuesFakeReader struct {
	openIssues   map[string][]scm.IssueRef  // "owner/repo" -> []IssueRef
	closedIssues map[string][]scm.IssueRef  // "owner/repo" -> []IssueRef
	commits      map[string][]scm.CommitRef // "owner/repo" -> []CommitRef
}

func (f *issuesFakeReader) ListOpenIssues(_ context.Context, owner, repo string) ([]scm.IssueRef, error) {
	return f.openIssues[owner+"/"+repo], nil
}
func (f *issuesFakeReader) ListClosedIssues(_ context.Context, owner, repo string, _ time.Time) ([]scm.IssueRef, error) {
	return f.closedIssues[owner+"/"+repo], nil
}
func (f *issuesFakeReader) ListCommits(_ context.Context, owner, repo string, _ time.Time) ([]scm.CommitRef, error) {
	return f.commits[owner+"/"+repo], nil
}
func (f *issuesFakeReader) ListOpenPRs(_ context.Context, _, _ string) ([]scm.PRRef, error) {
	return nil, nil
}
func (f *issuesFakeReader) ListBoardItems(_ context.Context, _ scm.BoardRef) ([]scm.BoardItem, error) {
	return nil, nil
}
func (f *issuesFakeReader) GetCommitCIStatus(_ context.Context, _, _, _ string) (string, error) {
	return "", nil
}
func (f *issuesFakeReader) ListIssueComments(_ context.Context, _, _ string, _ int) ([]scm.IssueComment, error) {
	return nil, nil
}
func (f *issuesFakeReader) GetIssue(_ context.Context, _, _ string, _ int) (scm.IssueContent, error) {
	return scm.IssueContent{}, nil
}
func (f *issuesFakeReader) GetDefaultBranchHeadSHA(_ context.Context, _, _ string) (string, error) {
	return "", nil
}

// issuesFakeWriter records SCM write calls.
type issuesFakeWriter struct {
	scm.SCMWriter
	closedRepo    string
	closedNumber  int
	closedComment string
	editedRepo    string
	editedNumber  int
	editedReq     scm.EditIssueReq
	createdRepo   string
	createdReq    scm.IssueReq
}

func (f *issuesFakeWriter) CloseIssue(_ context.Context, _, repo string, number int, comment string) error {
	f.closedRepo = repo
	f.closedNumber = number
	f.closedComment = comment
	return nil
}
func (f *issuesFakeWriter) EditIssue(_ context.Context, _, repo string, number int, req scm.EditIssueReq) error {
	f.editedRepo = repo
	f.editedNumber = number
	f.editedReq = req
	return nil
}
func (f *issuesFakeWriter) CreateIssue(_ context.Context, repoURL, _ string, req scm.IssueReq) (scm.CreatedIssue, error) {
	f.createdRepo = repoURL
	f.createdReq = req
	return scm.CreatedIssue{Ref: "o/r#99", URL: "https://github.com/o/r/issues/99"}, nil
}
func (f *issuesFakeWriter) Comment(_ context.Context, _, _, _ string) error { return nil }

// buildRouterWithSCMIssues builds a router with both reader and writer injected.
func buildRouterWithSCMIssues(t *testing.T, reader scm.SCMReader, writer scm.SCMWriter, objs ...client.Object) *chi.Mux {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&tatarav1alpha1.Project{}, &tatarav1alpha1.Repository{}, &tatarav1alpha1.Task{}).
		Build()
	s := restapi.NewServer(restapi.Config{
		Client:    fc,
		Namespace: "tatara",
		SCMFor:    func(_ string) (scm.SCMWriter, error) { return writer, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return reader, nil },
	})
	r := chi.NewRouter()
	s.Mount(r, nil)
	return r
}

// seed objects for issue handler tests.
func issueProject(name string) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: name + "-scm",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "bot"},
		},
	}
}

func issueRepo(name, projectRef, url string) *tatarav1alpha1.Repository {
	return &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: projectRef, URL: url},
	}
}

func issueSecret(name string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Data:       map[string][]byte{"token": []byte("tok")},
	}
}

// B1: GET /projects/{p}/issues

func TestListProjectIssues_AggregatesOpenAndClosed(t *testing.T) {
	reader := &issuesFakeReader{
		openIssues: map[string][]scm.IssueRef{
			"o/repo-a": {{Repo: "o/repo-a", Number: 1, Title: "open issue", State: "open"}},
		},
		closedIssues: map[string][]scm.IssueRef{
			"o/repo-b": {{Repo: "o/repo-b", Number: 2, Title: "closed issue", State: "closed"}},
		},
	}
	r := buildRouterWithSCMIssues(t, reader, &issuesFakeWriter{},
		issueProject("proj"),
		issueRepo("repo-a", "proj", "https://github.com/o/repo-a.git"),
		issueRepo("repo-b", "proj", "https://github.com/o/repo-b.git"),
		issueSecret("proj-scm"),
	)

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/projects/proj/issues", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var out struct {
		Issues []struct {
			Number int    `json:"number"`
			Title  string `json:"title"`
			State  string `json:"state"`
		} `json:"issues"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Len(t, out.Issues, 2)
}

func TestListProjectIssues_ExcludesPRs(t *testing.T) {
	reader := &issuesFakeReader{
		openIssues: map[string][]scm.IssueRef{
			"o/repo-a": {
				{Repo: "o/repo-a", Number: 1, Title: "real issue", IsPR: false},
				{Repo: "o/repo-a", Number: 2, Title: "a pr", IsPR: true},
			},
		},
	}
	r := buildRouterWithSCMIssues(t, reader, &issuesFakeWriter{},
		issueProject("proj2"),
		issueRepo("repo-a2", "proj2", "https://github.com/o/repo-a.git"),
		issueSecret("proj2-scm"),
	)

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/projects/proj2/issues", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var out struct {
		Issues []struct{ Number int } `json:"issues"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Len(t, out.Issues, 1)
	require.Equal(t, 1, out.Issues[0].Number)
}

func TestListProjectIssues_ProjectNotFound(t *testing.T) {
	r := buildRouterWithSCMIssues(t, &issuesFakeReader{}, &issuesFakeWriter{})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/projects/missing/issues", nil))
	require.Equal(t, http.StatusNotFound, w.Code)
}

// B2: close / edit / create

func TestCloseProjectIssue_RequiresCommentAndRepoInProject(t *testing.T) {
	writer := &issuesFakeWriter{}
	r := buildRouterWithSCMIssues(t, &issuesFakeReader{}, writer,
		issueProject("p3"),
		issueRepo("r3", "p3", "https://github.com/o/r3.git"),
		issueSecret("p3-scm"),
	)

	// Successful close.
	body := `{"comment":"duplicate of o/r#1"}`
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/projects/p3/issues/o/r3/7/close", strings.NewReader(body)))
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "o/r3", writer.closedRepo)
	require.Equal(t, 7, writer.closedNumber)

	// Missing comment -> 400.
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, httptest.NewRequest(http.MethodPost, "/projects/p3/issues/o/r3/7/close", strings.NewReader(`{"comment":""}`)))
	require.Equal(t, http.StatusBadRequest, w2.Code)

	// Repo not in project -> 400.
	w3 := httptest.NewRecorder()
	r.ServeHTTP(w3, httptest.NewRequest(http.MethodPost, "/projects/p3/issues/o/other/7/close", strings.NewReader(body)))
	require.Equal(t, http.StatusBadRequest, w3.Code)
}

func TestEditProjectIssue_PatchesProvided(t *testing.T) {
	writer := &issuesFakeWriter{}
	r := buildRouterWithSCMIssues(t, &issuesFakeReader{}, writer,
		issueProject("p4"),
		issueRepo("r4", "p4", "https://github.com/o/r4.git"),
		issueSecret("p4-scm"),
	)

	body := `{"body":"narrowed scope"}`
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPatch, "/projects/p4/issues/o/r4/7", strings.NewReader(body)))
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "o/r4", writer.editedRepo)
	require.Equal(t, 7, writer.editedNumber)
	require.Nil(t, writer.editedReq.Title, "title should not be set")
	require.NotNil(t, writer.editedReq.Body)
	require.Equal(t, "narrowed scope", *writer.editedReq.Body)
}

func TestCreateProjectIssue_Splits(t *testing.T) {
	writer := &issuesFakeWriter{}
	r := buildRouterWithSCMIssues(t, &issuesFakeReader{}, writer,
		issueProject("p5"),
		issueRepo("r5", "p5", "https://github.com/o/r5.git"),
		issueSecret("p5-scm"),
	)

	body := `{"title":"child issue","body":"implements part of #5"}`
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/projects/p5/issues/o/r5", strings.NewReader(body)))
	require.Equal(t, http.StatusCreated, w.Code)
	require.Equal(t, "https://github.com/o/r5.git", writer.createdRepo)
	require.Equal(t, "child issue", writer.createdReq.Title)
}

// B3: GET /projects/{p}/commits

func TestListProjectCommits_AggregatesAcrossRepos(t *testing.T) {
	reader := &issuesFakeReader{
		commits: map[string][]scm.CommitRef{
			"o/repo-a": {{SHA: "abc123", Message: "feat: thing", Author: "bot", Date: time.Now()}},
			"o/repo-b": {{SHA: "def456", Message: "fix: other", Author: "bot", Date: time.Now()}},
		},
	}
	r := buildRouterWithSCMIssues(t, reader, &issuesFakeWriter{},
		issueProject("projc"),
		issueRepo("repo-ca", "projc", "https://github.com/o/repo-a.git"),
		issueRepo("repo-cb", "projc", "https://github.com/o/repo-b.git"),
		issueSecret("projc-scm"),
	)

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/projects/projc/commits?sinceDays=30", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var out struct {
		Commits []struct {
			SHA  string `json:"sha"`
			Repo string `json:"repo"`
		} `json:"commits"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Len(t, out.Commits, 2)
}
