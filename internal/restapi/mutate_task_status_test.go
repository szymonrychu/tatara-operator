package restapi_test

// S26 regression test: pins the metric name, error text, log fields and
// success metric of each of the 5 handlers folded into mutateTaskStatus
// (reviewVerdict, prOutcome, implementOutcome, brainstormOutcome,
// issueOutcome) so the shared-skeleton extraction cannot silently drift any
// of these per-handler contracts (the prior audit f31ffdb already fixed this
// exact class of regression once).

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/restapi"
)

// buildRouterWithLogAndMetrics is like buildRouterWithMetrics (audit_r3_test.go)
// but also captures JSON log lines into logBuf so tests can assert on
// structured log fields.
func buildRouterWithLogAndMetrics(t *testing.T, m *obs.OperatorMetrics, logBuf *bytes.Buffer, objs ...client.Object) *chi.Mux {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&tatarav1alpha1.Project{}, &tatarav1alpha1.Repository{},
			&tatarav1alpha1.Task{}, &tatarav1alpha1.Subtask{}).
		Build()
	logger := slog.New(slog.NewJSONHandler(logBuf, nil))
	s := restapi.NewServer(restapi.Config{Client: fc, Namespace: "tatara", Metrics: m, Logger: logger})
	r := chi.NewRouter()
	s.Mount(r, nil)
	return r
}

func lastLogLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.NotEmpty(t, lines)
	last := lines[len(lines)-1]
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(last), &m))
	return m
}

func counterValMTS(t *testing.T, ctr prometheus.Counter) float64 {
	t.Helper()
	return counterVal(t, ctr)
}

func TestMutateTaskStatus_ReviewVerdict_Contract(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	var logBuf bytes.Buffer
	r := buildRouterWithLogAndMetrics(t, m, &logBuf, taskWithKind("t1", "alpha", "review"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/review", strings.NewReader(`{"decision":"approve"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.EqualValues(t, 1, counterValMTS(t, m.RESTRequestsCounter("review_verdict", "ok")))
	fields := lastLogLine(t, &logBuf)
	require.Equal(t, "restapi: reviewVerdict", fields["msg"])
	require.Equal(t, "review_verdict", fields["action"])
	require.Equal(t, "t1", fields["resource_id"])
	require.Equal(t, "approve", fields["decision"])
}

func TestMutateTaskStatus_ReviewVerdict_WrongKindErrorText(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "implement"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/review", strings.NewReader(`{"decision":"approve"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
	var out map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, "review verdict only applies to a review task", out["error"])
}

func TestMutateTaskStatus_PROutcome_Contract(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	var logBuf bytes.Buffer
	r := buildRouterWithLogAndMetrics(t, m, &logBuf, taskWithKind("t1", "alpha", "selfImprove"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/pr-outcome", strings.NewReader(`{"action":"merge"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.EqualValues(t, 1, counterValMTS(t, m.RESTRequestsCounter("pr_outcome", "ok")))
	fields := lastLogLine(t, &logBuf)
	require.Equal(t, "restapi: prOutcome", fields["msg"])
	require.Equal(t, "pr_outcome", fields["action"])
	require.Equal(t, "merge", fields["pr_action"])
}

func TestMutateTaskStatus_PROutcome_WrongKindErrorText(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "review"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/pr-outcome", strings.NewReader(`{"action":"merge"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
	var out map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, "pr outcome only applies to a selfImprove task", out["error"])
}

func TestMutateTaskStatus_ImplementOutcome_Contract(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	var logBuf bytes.Buffer
	r := buildRouterWithLogAndMetrics(t, m, &logBuf, taskWithKind("t1", "alpha", "issueLifecycle"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/implement-outcome", strings.NewReader(`{"action":"declined","reason":"not needed"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.EqualValues(t, 1, counterValMTS(t, m.RESTRequestsCounter("implement_outcome", "ok")))
	fields := lastLogLine(t, &logBuf)
	require.Equal(t, "restapi: implementOutcome", fields["msg"])
	require.Equal(t, "implement_outcome", fields["action"])
	require.Equal(t, "declined", fields["impl_action"])
}

func TestMutateTaskStatus_ImplementOutcome_WrongKindErrorText(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "implement"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/implement-outcome", strings.NewReader(`{"action":"declined","reason":"not needed"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
	var out map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, "implement outcome only applies to an issueLifecycle task", out["error"])
}

func TestMutateTaskStatus_BrainstormOutcome_Contract(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	var logBuf bytes.Buffer
	r := buildRouterWithLogAndMetrics(t, m, &logBuf, taskWithKind("t1", "alpha", "brainstorm"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/brainstorm-outcome", strings.NewReader(`{"action":"none","reason":"nothing novel"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.EqualValues(t, 1, counterValMTS(t, m.RESTRequestsCounter("brainstorm_outcome", "ok")))
	fields := lastLogLine(t, &logBuf)
	require.Equal(t, "restapi: brainstormOutcome", fields["msg"])
	require.Equal(t, "brainstorm_outcome", fields["action"])
	require.NotContains(t, fields, "impl_action")
}

func TestMutateTaskStatus_BrainstormOutcome_WrongKindErrorText(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "issueLifecycle"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/brainstorm-outcome", strings.NewReader(`{"action":"none","reason":"nothing novel"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
	var out map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, "brainstorm outcome only applies to a brainstorm task", out["error"])
}

// TestMutateTaskStatus_IssueOutcome_OnSuccessHookSurvives pins the one
// divergent piece of the 5 folded handlers: issueOutcome's onSuccess hook
// must still call s.metrics.IssueOutcome(action) (IssueOutcomeTotal), in
// addition to the generic RecordRESTRequest metric all 5 share.
func TestMutateTaskStatus_IssueOutcome_OnSuccessHookSurvives(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	var logBuf bytes.Buffer
	r := buildRouterWithLogAndMetrics(t, m, &logBuf, taskWithKind("t1", "alpha", "triageIssue"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/issue-outcome", strings.NewReader(`{"action":"implement"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.EqualValues(t, 1, counterValMTS(t, m.RESTRequestsCounter("issue_outcome", "ok")))
	require.EqualValues(t, 1, counterValMTS(t, m.IssueOutcomeTotal("implement")))
	fields := lastLogLine(t, &logBuf)
	require.Equal(t, "restapi: issueOutcome", fields["msg"])
	require.Equal(t, "issue_outcome", fields["action"])
	require.Equal(t, "implement", fields["issue_action"])
}

func TestMutateTaskStatus_IssueOutcome_WrongKindErrorText(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "review"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/issue-outcome", strings.NewReader(`{"action":"implement"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
	var out map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, "issue outcome only applies to a triageIssue or issueLifecycle task", out["error"])
}
