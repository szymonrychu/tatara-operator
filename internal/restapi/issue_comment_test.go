package restapi_test

// TDD tests for PART B: POST /projects/{p}/issue-comment
// Failing until the handler + SCMFor wiring is implemented.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/restapi"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// fakeWriter captures Comment calls.
type fakeWriter struct {
	mu         sync.Mutex
	comments   []capturedComment
	commentErr error
}

type capturedComment struct {
	Token    string
	IssueRef string
	Body     string
}

func (f *fakeWriter) Comment(_ context.Context, token, issueRef, body string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.commentErr != nil {
		return f.commentErr
	}
	f.comments = append(f.comments, capturedComment{Token: token, IssueRef: issueRef, Body: body})
	return nil
}
func (f *fakeWriter) OpenChange(_ context.Context, _, _, _, _, _, _ string) (string, error) {
	return "", nil
}
func (f *fakeWriter) CreateIssue(_ context.Context, _, _ string, _ scm.IssueReq) (scm.CreatedIssue, error) {
	return scm.CreatedIssue{}, nil
}
func (f *fakeWriter) AddLabel(_ context.Context, _, _, _ string) error    { return nil }
func (f *fakeWriter) RemoveLabel(_ context.Context, _, _, _ string) error { return nil }
func (f *fakeWriter) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	return scm.PRState{}, nil
}
func (f *fakeWriter) Approve(_ context.Context, _, _ string, _ int, _ string) error { return nil }
func (f *fakeWriter) RequestChanges(_ context.Context, _, _ string, _ int, _ string) error {
	return nil
}
func (f *fakeWriter) Suggest(_ context.Context, _, _ string, _ int, _ []scm.Suggestion) error {
	return nil
}
func (f *fakeWriter) Merge(_ context.Context, _, _ string, _ int, _ string) (string, error) {
	return "", nil
}
func (f *fakeWriter) ClosePR(_ context.Context, _, _ string, _ int, _ string) error { return nil }
func (f *fakeWriter) AddBoardItem(_ context.Context, _ string, _ scm.BoardRef, _ string) error {
	return nil
}
func (f *fakeWriter) SetBoardColumn(_ context.Context, _ string, _ scm.BoardRef, _, _ string) error {
	return nil
}
func (f *fakeWriter) CloseIssue(_ context.Context, _, _ string, _ int, _ string) error { return nil }

// buildRouterWithSCM builds a chi router with an SCMFor factory injected into the Server.
func buildRouterWithSCM(t *testing.T, writer scm.SCMWriter, objs ...client.Object) *chi.Mux {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&tatarav1alpha1.Project{}, &tatarav1alpha1.Repository{},
			&tatarav1alpha1.Task{}, &tatarav1alpha1.Subtask{}).
		Build()
	s := restapi.NewServer(restapi.Config{
		Client:    fc,
		Namespace: "tatara",
		SCMFor: func(_ string) (scm.SCMWriter, error) {
			return writer, nil
		},
	})
	r := chi.NewRouter()
	s.Mount(r, nil)
	return r
}

// projectWithSCM creates a Project CRD with an SCM spec pointing at a secret.
func projectWithSCM(name, secretName string) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec: tatarav1alpha1.ProjectSpec{
			TriggerLabel:       "tatara",
			MaxConcurrentTasks: 3,
			ScmSecretRef:       secretName,
			Scm: &tatarav1alpha1.ScmSpec{
				Provider: "github",
				Owner:    "o",
			},
		},
	}
}

// scmSecret creates a Secret with a "token" key.
func scmSecret(name, token string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Data:       map[string][]byte{"token": []byte(token)},
	}
}

// repoForProject creates a Repository CRD for a given project.
func repoForProject(name, projectRef, url string) *tatarav1alpha1.Repository {
	return &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:    projectRef,
			URL:           url,
			DefaultBranch: "main",
			IngestEnabled: boolPtrH(true),
		},
	}
}

func TestCommentOnIssue_PostsComment(t *testing.T) {
	writer := &fakeWriter{}
	proj := projectWithSCM("proj1", "proj1-scm")
	secret := scmSecret("proj1-scm", "mytoken")
	repo := repoForProject("proj1-repo", "proj1", "https://github.com/owner/myrepo.git")

	r := buildRouterWithSCM(t, writer, proj, secret, repo)

	body := strings.NewReader(`{"repo":"owner/myrepo","number":42,"body":"looks good to me"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/proj1/issue-comment", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

	writer.mu.Lock()
	defer writer.mu.Unlock()
	require.Len(t, writer.comments, 1)
	require.Equal(t, "owner/myrepo#42", writer.comments[0].IssueRef)
	require.Equal(t, "looks good to me", writer.comments[0].Body)
	require.Equal(t, "mytoken", writer.comments[0].Token)
}

func TestCommentOnIssue_MissingBody(t *testing.T) {
	writer := &fakeWriter{}
	proj := projectWithSCM("proj2", "proj2-scm")
	secret := scmSecret("proj2-scm", "tok2")
	repo := repoForProject("proj2-repo", "proj2", "https://github.com/o/r.git")

	r := buildRouterWithSCM(t, writer, proj, secret, repo)

	body := strings.NewReader(`{"repo":"o/r","number":5}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/proj2/issue-comment", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCommentOnIssue_ZeroNumber(t *testing.T) {
	writer := &fakeWriter{}
	proj := projectWithSCM("proj3", "proj3-scm")
	secret := scmSecret("proj3-scm", "tok3")
	repo := repoForProject("proj3-repo", "proj3", "https://github.com/o/r.git")

	r := buildRouterWithSCM(t, writer, proj, secret, repo)

	body := strings.NewReader(`{"repo":"o/r","number":0,"body":"hi"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/proj3/issue-comment", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCommentOnIssue_ProjectNotFound(t *testing.T) {
	writer := &fakeWriter{}
	r := buildRouterWithSCM(t, writer)

	body := strings.NewReader(`{"repo":"o/r","number":1,"body":"hi"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/missing/issue-comment", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestCommentOnIssue_UnknownRepo(t *testing.T) {
	writer := &fakeWriter{}
	proj := projectWithSCM("proj4", "proj4-scm")
	secret := scmSecret("proj4-scm", "tok4")
	// No repository registered for proj4.

	r := buildRouterWithSCM(t, writer, proj, secret)

	body := strings.NewReader(`{"repo":"o/unknown","number":1,"body":"hi"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/proj4/issue-comment", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestCommentOnIssue_LogsAction(t *testing.T) {
	writer := &fakeWriter{}
	proj := projectWithSCM("proj5", "proj5-scm")
	secret := scmSecret("proj5-scm", "tok5")
	repo := repoForProject("proj5-repo", "proj5", "https://github.com/org/svc.git")

	r := buildRouterWithSCM(t, writer, proj, secret, repo)

	body := strings.NewReader(`{"repo":"org/svc","number":99,"body":"test comment"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/proj5/issue-comment", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, "ok", resp["status"])
}

// --- Finding 8: project with no SCM provider returns 409 not 500 ---

func projectWithoutSCM(name string) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec: tatarav1alpha1.ProjectSpec{
			TriggerLabel:       "tatara",
			MaxConcurrentTasks: 3,
			ScmSecretRef:       name + "-scm",
		},
	}
}

func TestCommentOnIssue_NoSCMProvider_Returns409(t *testing.T) {
	writer := &fakeWriter{}
	proj := projectWithoutSCM("proj6")
	secret := scmSecret("proj6-scm", "tok6")
	repo := repoForProject("proj6-repo", "proj6", "https://github.com/o/r.git")

	r := buildRouterWithSCM(t, writer, proj, secret, repo)

	body := strings.NewReader(`{"repo":"o/r","number":1,"body":"hi"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/proj6/issue-comment", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusConflict, w.Code)
	require.Contains(t, w.Body.String(), "no scm provider")
}

// --- Finding 4/12: SCM comment metric is recorded ---

func buildRouterWithSCMAndMetrics(t *testing.T, writer scm.SCMWriter, m *obs.OperatorMetrics, objs ...client.Object) *chi.Mux {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&tatarav1alpha1.Project{}, &tatarav1alpha1.Repository{},
			&tatarav1alpha1.Task{}, &tatarav1alpha1.Subtask{}).
		Build()
	s := restapi.NewServer(restapi.Config{
		Client:    fc,
		Namespace: "tatara",
		SCMFor: func(_ string) (scm.SCMWriter, error) {
			return writer, nil
		},
		Metrics: m,
	})
	r := chi.NewRouter()
	s.Mount(r, nil)
	return r
}

func TestCommentOnIssue_RecordsSCMWriteMetric(t *testing.T) {
	writer := &fakeWriter{}
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	proj := projectWithSCM("proj7", "proj7-scm")
	secret := scmSecret("proj7-scm", "tok7")
	repo := repoForProject("proj7-repo", "proj7", "https://github.com/o/repo.git")

	r := buildRouterWithSCMAndMetrics(t, writer, m, proj, secret, repo)

	body := strings.NewReader(`{"repo":"o/repo","number":10,"body":"metric test"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/proj7/issue-comment", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	// Confirm metric was incremented for provider=github, verb=comment, result=ok.
	ctr := m.SCMWriteCounter("github", "comment", "ok")
	require.NotNil(t, ctr)
	var metric dto.Metric
	require.NoError(t, ctr.Write(&metric))
	require.EqualValues(t, 1, metric.GetCounter().GetValue())
}

func TestCommentOnIssue_RecordsSCMWriteMetricOnError(t *testing.T) {
	writer := &fakeWriter{commentErr: fmt.Errorf("scm unavailable")}
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	proj := projectWithSCM("proj8", "proj8-scm")
	secret := scmSecret("proj8-scm", "tok8")
	repo := repoForProject("proj8-repo", "proj8", "https://github.com/o/repo2.git")

	r := buildRouterWithSCMAndMetrics(t, writer, m, proj, secret, repo)

	body := strings.NewReader(`{"repo":"o/repo2","number":11,"body":"will fail"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/proj8/issue-comment", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	ctr := m.SCMWriteCounter("github", "comment", "error")
	require.NotNil(t, ctr)
	var metric dto.Metric
	require.NoError(t, ctr.Write(&metric))
	require.EqualValues(t, 1, metric.GetCounter().GetValue())
}
