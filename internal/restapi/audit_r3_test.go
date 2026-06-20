// Package restapi_test - TDD tests for audit-2026-06-15 round-3 defects.
package restapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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
	"github.com/szymonrychu/tatara-operator/internal/auth"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/restapi"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// --- Finding 1: commentOnIssue must check verified-subject (like all mutating handlers) ---

func TestCommentOnIssue_EmptySubjectRejected(t *testing.T) {
	writer := &fakeWriter{}
	proj := projectWithSCM("proj-authz", "proj-authz-scm")
	secret := scmSecret("proj-authz-scm", "tok")
	repo := repoForProject("proj-authz-repo", "proj-authz", "https://github.com/o/r.git")

	r := buildRouterWithSCM(t, writer, proj, secret, repo)

	body := strings.NewReader(`{"repo":"o/r","number":1,"body":"hi"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/proj-authz/issue-comment", body)
	req.Header.Set("Content-Type", "application/json")
	ctx := auth.ContextWithClaims(req.Context(), &auth.Claims{Subject: ""})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req.WithContext(ctx))
	require.Equal(t, http.StatusForbidden, w.Code)
}

func TestCommentOnIssue_ValidSubjectAllowed(t *testing.T) {
	writer := &fakeWriter{}
	proj := projectWithSCM("proj-authz2", "proj-authz2-scm")
	secret := scmSecret("proj-authz2-scm", "tok")
	repo := repoForProject("proj-authz2-repo", "proj-authz2", "https://github.com/o/r.git")

	r := buildRouterWithSCM(t, writer, proj, secret, repo)

	body := strings.NewReader(`{"repo":"o/r","number":1,"body":"hi"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/proj-authz2/issue-comment", body)
	req.Header.Set("Content-Type", "application/json")
	ctx := auth.ContextWithClaims(req.Context(), &auth.Claims{Subject: "svc-account"})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req.WithContext(ctx))
	require.Equal(t, http.StatusOK, w.Code)
}

// --- Finding 2: patchSubtask must check verified-subject ---

func TestPatchSubtask_EmptySubjectRejected(t *testing.T) {
	r := buildRouter(t, subtask("st-authz", "t1", 1))
	req := httptest.NewRequest(http.MethodPatch, "/subtasks/st-authz", strings.NewReader(`{"phase":"Done"}`))
	req.Header.Set("Content-Type", "application/json")
	ctx := auth.ContextWithClaims(req.Context(), &auth.Claims{Subject: ""})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req.WithContext(ctx))
	require.Equal(t, http.StatusForbidden, w.Code)
}

func TestPatchSubtask_ValidSubjectAllowed(t *testing.T) {
	r := buildRouter(t, subtask("st-authz2", "t1", 1))
	req := httptest.NewRequest(http.MethodPatch, "/subtasks/st-authz2", strings.NewReader(`{"phase":"Done"}`))
	req.Header.Set("Content-Type", "application/json")
	ctx := auth.ContextWithClaims(req.Context(), &auth.Claims{Subject: "svc"})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req.WithContext(ctx))
	require.Equal(t, http.StatusOK, w.Code)
}

// --- Finding 3: decodeJSON must return 413 for oversized body, not 400 ---

func TestDecodeJSON_OversizedBody_Returns413(t *testing.T) {
	bigVal := strings.Repeat("a", 2*1024*1024)
	payload := `{"resultSummary":"` + bigVal + `"}`
	r := buildRouter(t, task("t3-big", "alpha"))
	req := httptest.NewRequest(http.MethodPatch, "/tasks/t3-big", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
	require.Contains(t, w.Body.String(), "request body too large")
}

func TestDecodeJSON_InvalidJSON_Returns400WithGenericMessage(t *testing.T) {
	r := buildRouter(t, task("t3-bad", "alpha"))
	req := httptest.NewRequest(http.MethodPatch, "/tasks/t3-bad", strings.NewReader(`not-json`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "invalid JSON body")
	require.NotContains(t, w.Body.String(), "invalid character")
}

// --- Finding 4: REST metrics recorded for mutating handlers ---

func buildRouterWithMetrics(t *testing.T, m *obs.OperatorMetrics, objs ...client.Object) *chi.Mux {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&tatarav1alpha1.Project{}, &tatarav1alpha1.Repository{},
			&tatarav1alpha1.Task{}, &tatarav1alpha1.Subtask{}).
		Build()
	s := restapi.NewServer(restapi.Config{Client: fc, Namespace: "tatara", Metrics: m})
	r := chi.NewRouter()
	s.Mount(r, nil)
	return r
}

func counterVal(t *testing.T, ctr prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, ctr.Write(&m))
	return m.GetCounter().GetValue()
}

func TestPatchTask_RecordsMetric(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := buildRouterWithMetrics(t, m, task("tm1", "alpha"))
	req := httptest.NewRequest(http.MethodPatch, "/tasks/tm1", strings.NewReader(`{"resultSummary":"x"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.EqualValues(t, 1, counterVal(t, m.RESTRequestsCounter("patch_task", "ok")))
}

func TestCreateSubtask_RecordsMetric(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := buildRouterWithMetrics(t, m, task("tm2", "alpha"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/tm2/subtasks", strings.NewReader(`{"title":"x"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusCreated, w.Code)
	require.EqualValues(t, 1, counterVal(t, m.RESTRequestsCounter("create_subtask", "ok")))
}

func TestReviewVerdict_RecordsMetric(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := buildRouterWithMetrics(t, m, taskWithKind("tm3", "alpha", "review"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/tm3/review", strings.NewReader(`{"decision":"approve"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.EqualValues(t, 1, counterVal(t, m.RESTRequestsCounter("review_verdict", "ok")))
}

func TestPROutcome_RecordsMetric(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := buildRouterWithMetrics(t, m, taskWithKind("tm4", "alpha", "selfImprove"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/tm4/pr-outcome", strings.NewReader(`{"action":"merge"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.EqualValues(t, 1, counterVal(t, m.RESTRequestsCounter("pr_outcome", "ok")))
}

func TestIssueOutcome_RecordsMetric(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := buildRouterWithMetrics(t, m, taskWithKind("tm5", "alpha", "triageIssue"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/tm5/issue-outcome", strings.NewReader(`{"action":"implement"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.EqualValues(t, 1, counterVal(t, m.RESTRequestsCounter("issue_outcome", "ok")))
}

func TestImplementOutcome_RecordsMetric(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := buildRouterWithMetrics(t, m, taskWithKind("tm6", "alpha", "issueLifecycle"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/tm6/implement-outcome", strings.NewReader(`{"action":"declined","reason":"done"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.EqualValues(t, 1, counterVal(t, m.RESTRequestsCounter("implement_outcome", "ok")))
}

func TestPostComment_RecordsMetric(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	tk := taskWithSource("tm7", "alpha", "owner/repo#5")
	r := buildRouterWithMetrics(t, m, tk)
	req := httptest.NewRequest(http.MethodPost, "/tasks/tm7/comment", strings.NewReader(`{"body":"hi"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.EqualValues(t, 1, counterVal(t, m.RESTRequestsCounter("post_comment", "ok")))
}

func TestChangeSummary_RecordsMetric(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := buildRouterWithMetrics(t, m, task("tm8", "alpha"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/tm8/change-summary", strings.NewReader(`{"prTitle":"fix(scan): retry flaky push events on CI timeout","prBody":"y","deliveredScope":"z"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.EqualValues(t, 1, counterVal(t, m.RESTRequestsCounter("change_summary", "ok")))
}

func TestHandover_RecordsMetric(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := buildRouterWithMetrics(t, m, task("tm9", "alpha"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/tm9/handover", strings.NewReader(`{"handover":"notes"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.EqualValues(t, 1, counterVal(t, m.RESTRequestsCounter("handover", "ok")))
}

func TestPatchSubtask_RecordsMetric(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := buildRouterWithMetrics(t, m, subtask("stm1", "t1", 1))
	req := httptest.NewRequest(http.MethodPatch, "/subtasks/stm1", strings.NewReader(`{"phase":"Done"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.EqualValues(t, 1, counterVal(t, m.RESTRequestsCounter("patch_subtask", "ok")))
}

// --- Finding 6: commentOnIssue must fail fast when token is empty ---

func buildRouterWithSCMObjs(t *testing.T, writer scm.SCMWriter, objs ...client.Object) *chi.Mux {
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
		SCMFor:    func(_ string) (scm.SCMWriter, error) { return writer, nil },
	})
	r := chi.NewRouter()
	s.Mount(r, nil)
	return r
}

func TestCommentOnIssue_EmptyToken_Returns500(t *testing.T) {
	writer := &fakeWriter{}
	proj := projectWithSCM("proj-emptytok", "proj-emptytok-scm")
	// Secret exists but has no "token" key.
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "proj-emptytok-scm", Namespace: "tatara"},
		Data:       map[string][]byte{},
	}
	repo := repoForProject("proj-emptytok-repo", "proj-emptytok", "https://github.com/o/r.git")

	r := buildRouterWithSCMObjs(t, writer, proj, secret, repo)

	body := strings.NewReader(`{"repo":"o/r","number":1,"body":"hi"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/proj-emptytok/issue-comment", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusInternalServerError, w.Code)
	// The writer must NOT have been called with an empty token.
	writer.mu.Lock()
	defer writer.mu.Unlock()
	require.Empty(t, writer.comments, "SCM must not be called with empty token")
}

// --- Finding 9: patchTask must reject writes to terminal tasks ---

func buildRouterWithTaskStatus(t *testing.T, tk *tatarav1alpha1.Task) *chi.Mux {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tk).
		WithStatusSubresource(&tatarav1alpha1.Task{}).
		Build()
	s := restapi.NewServer(restapi.Config{Client: fc, Namespace: "tatara"})
	r := chi.NewRouter()
	s.Mount(r, nil)
	return r
}

func taskWithPhase(name, projectRef, phase string) *tatarav1alpha1.Task {
	tk := task(name, projectRef)
	tk.Status.Phase = phase
	return tk
}

func TestPatchTask_TerminalSucceeded_Returns409(t *testing.T) {
	tk := taskWithPhase("t-term1", "alpha", "Succeeded")
	r := buildRouterWithTaskStatus(t, tk)
	req := httptest.NewRequest(http.MethodPatch, "/tasks/t-term1", strings.NewReader(`{"resultSummary":"late"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
}

func TestPatchTask_TerminalFailed_Returns409(t *testing.T) {
	tk := taskWithPhase("t-term2", "alpha", "Failed")
	r := buildRouterWithTaskStatus(t, tk)
	req := httptest.NewRequest(http.MethodPatch, "/tasks/t-term2", strings.NewReader(`{"resultSummary":"late"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
}

func TestPatchTask_NonTerminal_Allowed(t *testing.T) {
	tk := taskWithPhase("t-term3", "alpha", "Running")
	r := buildRouterWithTaskStatus(t, tk)
	req := httptest.NewRequest(http.MethodPatch, "/tasks/t-term3", strings.NewReader(`{"resultSummary":"progress"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}

// --- Finding 11: proposeIssue must use REST metric, not ScanTaskCreated ---

func TestProposeIssue_RecordsRESTMetricNotScanMetric(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	proj := project("pm1")
	repo := repository("pm1-repo", "pm1")
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(proj, repo).
		WithStatusSubresource(&tatarav1alpha1.Project{}, &tatarav1alpha1.Repository{}, &tatarav1alpha1.Task{}).
		Build()
	s := restapi.NewServer(restapi.Config{Client: fc, Namespace: "tatara", Metrics: m})
	r := chi.NewRouter()
	s.Mount(r, nil)

	body := strings.NewReader(`{"repositoryRef":"pm1-repo","title":"Add systemic correlation labels to brainstorm proposals","body":"B","kind":"bug"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/pm1/issues", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusCreated, w.Code)

	// REST metric must be incremented.
	require.EqualValues(t, 1, counterVal(t, m.RESTRequestsCounter("propose_issue", "ok")))
}

// --- Finding 12: createSubtask must use GenerateName not timestamp suffix ---

func TestCreateSubtask_UsesGenerateName(t *testing.T) {
	r := buildRouter(t, task("tgn1", "alpha"))
	body := strings.NewReader(`{"title":"step1","order":1}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/tgn1/subtasks", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusCreated, w.Code)

	var out restapi.SubtaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Contains(t, out.Name, "tgn1-st-")
	// A raw nanosecond suffix would be 19 digits; GenerateName suffix is 5 alphanum chars.
	suffix := out.Name[len("tgn1-st-"):]
	require.NotEmpty(t, suffix)
	// Nanosecond timestamps are all digits and long (>10). GenerateName suffix is short alphanum.
	allDigits := true
	for _, c := range suffix {
		if c < '0' || c > '9' {
			allDigits = false
			break
		}
	}
	require.False(t, allDigits, "name suffix should not be a pure numeric timestamp: %s", out.Name)
}

// --- Finding 13: postComment must cap PendingComments length ---

func TestPostComment_CapPendingComments(t *testing.T) {
	tk := taskWithSource("tc-cap", "alpha", "owner/repo#1")
	r := buildRouter(t, tk)
	var lastCode int
	// Post enough comments to hit the cap (cap is expected to be ~20).
	for i := 0; i < 25; i++ {
		req := httptest.NewRequest(http.MethodPost, "/tasks/tc-cap/comment",
			strings.NewReader(`{"body":"comment"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		lastCode = w.Code
		if w.Code == http.StatusConflict {
			break
		}
	}
	require.Equal(t, http.StatusConflict, lastCode, "expected 409 when PendingComments cap is reached")
}

// --- Finding 14: reviewVerdict/prOutcome/issueOutcome/implementOutcome - no post-loop kind re-check ---

func TestReviewVerdict_KindCheckBeforeLoop(t *testing.T) {
	r := buildRouter(t, taskWithKind("t14a", "alpha", "implement"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/t14a/review", strings.NewReader(`{"decision":"approve"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
}

func TestPROutcome_KindCheckBeforeLoop(t *testing.T) {
	r := buildRouter(t, taskWithKind("t14b", "alpha", "review"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/t14b/pr-outcome", strings.NewReader(`{"action":"merge"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
}
