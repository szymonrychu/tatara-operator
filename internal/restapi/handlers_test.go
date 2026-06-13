package restapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/restapi"
)

func buildRouter(t *testing.T, objs ...client.Object) *chi.Mux {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&tatarav1alpha1.Project{}, &tatarav1alpha1.Repository{},
			&tatarav1alpha1.Task{}, &tatarav1alpha1.Subtask{}).
		Build()
	s := restapi.NewServer(restapi.Config{Client: fc, Namespace: "tatara"})
	r := chi.NewRouter()
	s.Mount(r, nil)
	return r
}

func project(name string) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec:       tatarav1alpha1.ProjectSpec{TriggerLabel: "tatara", MaxConcurrentTasks: 3},
	}
}

func TestListProjects(t *testing.T) {
	r := buildRouter(t, project("alpha"), project("beta"))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/projects", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var out []restapi.ProjectDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Len(t, out, 2)
}

func TestGetProject(t *testing.T) {
	r := buildRouter(t, project("alpha"))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/projects/alpha", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.ProjectDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, "alpha", out.Name)
}

func TestGetProject_NotFound(t *testing.T) {
	r := buildRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/projects/missing", nil))
	require.Equal(t, http.StatusNotFound, w.Code)
}

func repository(name, projectRef string) *tatarav1alpha1.Repository {
	return &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: projectRef, URL: "https://git/" + name, DefaultBranch: "main", IngestEnabled: true},
	}
}

func task(name, projectRef string) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: projectRef, RepositoryRef: "repo", Goal: "g"},
	}
}

func TestListRepositories_FilteredByProject(t *testing.T) {
	r := buildRouter(t, repository("r1", "alpha"), repository("r2", "alpha"), repository("r3", "beta"))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/projects/alpha/repositories", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var out []restapi.RepositoryDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Len(t, out, 2)
	for _, d := range out {
		require.Equal(t, "alpha", d.ProjectRef)
	}
}

func TestListTasks_FilteredByProject(t *testing.T) {
	r := buildRouter(t, task("t1", "alpha"), task("t2", "beta"))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/projects/alpha/tasks", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var out []restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Len(t, out, 1)
	require.Equal(t, "t1", out[0].Name)
}

func TestGetTask(t *testing.T) {
	r := buildRouter(t, task("t1", "alpha"))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/tasks/t1", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, "t1", out.Name)
}

func TestPatchTask_SetsResultSummaryAndCondition(t *testing.T) {
	r := buildRouter(t, task("t1", "alpha"))
	body := strings.NewReader(`{"resultSummary":"halfway","note":"cloned repo"}`)
	req := httptest.NewRequest(http.MethodPatch, "/tasks/t1", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, "halfway", out.Status.ResultSummary)
	require.NotEmpty(t, out.Status.Conditions)
	require.Equal(t, "AgentNote", out.Status.Conditions[len(out.Status.Conditions)-1].Type)
	require.Equal(t, "cloned repo", out.Status.Conditions[len(out.Status.Conditions)-1].Message)
}

func TestPatchTask_NotFound(t *testing.T) {
	r := buildRouter(t)
	req := httptest.NewRequest(http.MethodPatch, "/tasks/nope", strings.NewReader(`{"resultSummary":"x"}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
}

func taskWithKind(name, projectRef, kind string) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: projectRef, RepositoryRef: "repo", Goal: "g", Kind: kind},
	}
}

func subtask(name, taskRef string, order int) *tatarav1alpha1.Subtask {
	return &tatarav1alpha1.Subtask{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec:       tatarav1alpha1.SubtaskSpec{TaskRef: taskRef, Title: name, Order: order},
	}
}

func TestListSubtasks_SortedByOrder(t *testing.T) {
	r := buildRouter(t, subtask("b", "t1", 2), subtask("a", "t1", 1), subtask("z", "t2", 1))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/tasks/t1/subtasks", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var out []restapi.SubtaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Len(t, out, 2)
	require.Equal(t, 1, out[0].Order)
	require.Equal(t, 2, out[1].Order)
}

func TestCreateSubtask_OwnerRefAndTaskRef(t *testing.T) {
	r := buildRouter(t, task("t1", "alpha"))
	body := strings.NewReader(`{"title":"write tests","detail":"unit","order":1}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/subtasks", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusCreated, w.Code)
	var out restapi.SubtaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, "t1", out.TaskRef)
	require.Equal(t, "write tests", out.Title)
	require.NotEmpty(t, out.Name)
}

func TestCreateSubtask_TaskNotFound(t *testing.T) {
	r := buildRouter(t)
	req := httptest.NewRequest(http.MethodPost, "/tasks/nope/subtasks", strings.NewReader(`{"title":"x"}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestCreateSubtask_MissingTitle(t *testing.T) {
	r := buildRouter(t, task("t1", "alpha"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/subtasks", strings.NewReader(`{"detail":"d"}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestPatchSubtask_MarksDone(t *testing.T) {
	r := buildRouter(t, subtask("s1", "t1", 1))
	body := strings.NewReader(`{"phase":"Done","result":"all green","turnId":"turn-7"}`)
	req := httptest.NewRequest(http.MethodPatch, "/subtasks/s1", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.SubtaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, "Done", out.Status.Phase)
	require.Equal(t, "all green", out.Status.Result)
	require.Equal(t, "turn-7", out.Status.TurnID)
}

func TestPatchSubtask_NotFound(t *testing.T) {
	r := buildRouter(t)
	req := httptest.NewRequest(http.MethodPatch, "/subtasks/none", strings.NewReader(`{"phase":"Done"}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
}

// --- Task 11: propose_issue / review_verdict / pr_outcome ---

func TestProposeIssue(t *testing.T) {
	r := buildRouter(t, project("alpha"))
	body := strings.NewReader(`{"repositoryRef":"repo-a","title":"Fix login","body":"login broken","kind":"bug"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/alpha/issues", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusCreated, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, "implement", out.Kind)
	require.True(t, out.ApprovalRequired)
	require.Equal(t, "alpha", out.ProjectRef)
	require.Equal(t, "repo-a", out.RepositoryRef)
	require.Equal(t, "AwaitingApproval", out.Status.Phase)
	require.NotEmpty(t, out.Name)
	found := false
	for _, c := range out.Status.Conditions {
		if c.Type == "ApprovalApproved" && c.Status == metav1.ConditionFalse {
			found = true
		}
	}
	require.True(t, found, "ApprovalApproved=False condition not set")
}

func TestProposeIssue_MissingFields(t *testing.T) {
	r := buildRouter(t, project("alpha"))
	body := strings.NewReader(`{"title":"T"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/alpha/issues", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestProposeIssue_ProjectNotFound(t *testing.T) {
	r := buildRouter(t)
	body := strings.NewReader(`{"repositoryRef":"r","title":"T","body":"B","kind":"bug"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/missing/issues", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestReviewVerdict(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "review"))
	body := strings.NewReader(`{"decision":"approve","body":"lgtm","suggestions":[{"path":"a.go","line":12,"body":"x"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/review", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.NotNil(t, out.Status.ReviewVerdict)
	require.Equal(t, "approve", out.Status.ReviewVerdict.Decision)
	require.Equal(t, "lgtm", out.Status.ReviewVerdict.Body)
	require.Len(t, out.Status.ReviewVerdict.Suggestions, 1)
}

func TestReviewVerdict_MissingDecision(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "review"))
	body := strings.NewReader(`{"body":"lgtm"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/review", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestReviewVerdict_InvalidDecision(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "review"))
	body := strings.NewReader(`{"decision":"reject"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/review", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestReviewVerdict_WrongKind(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "selfImprove"))
	body := strings.NewReader(`{"decision":"approve"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/review", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
}

func TestReviewVerdict_TaskNotFound(t *testing.T) {
	r := buildRouter(t)
	body := strings.NewReader(`{"decision":"approve"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/missing/review", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestPROutcome(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "selfImprove"))
	body := strings.NewReader(`{"action":"merge","reason":"green"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/pr-outcome", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.NotNil(t, out.Status.PROutcome)
	require.Equal(t, "merge", out.Status.PROutcome.Action)
	require.Equal(t, "green", out.Status.PROutcome.Reason)
}

func TestPROutcome_MissingAction(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "selfImprove"))
	body := strings.NewReader(`{"reason":"x"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/pr-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestPROutcome_InvalidAction(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "selfImprove"))
	body := strings.NewReader(`{"action":"rebase"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/pr-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestPROutcome_WrongKind(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "review"))
	body := strings.NewReader(`{"action":"merge"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/pr-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
}

func TestPROutcome_TaskNotFound(t *testing.T) {
	r := buildRouter(t)
	body := strings.NewReader(`{"action":"merge"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/missing/pr-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestIssueOutcome(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "triageIssue"))
	body := strings.NewReader(`{"action":"close","comment":"out of scope"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/issue-outcome", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.NotNil(t, out.Status.IssueOutcome)
	require.Equal(t, "close", out.Status.IssueOutcome.Action)
	require.Equal(t, "out of scope", out.Status.IssueOutcome.Comment)
}

func TestIssueOutcome_Implement(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "triageIssue"))
	body := strings.NewReader(`{"action":"implement"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/issue-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestIssueOutcome_DiscussLifecycle(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "issueLifecycle"))
	body := strings.NewReader(`{"action":"discuss","comment":"need details: which repo?"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/issue-outcome", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.NotNil(t, out.Status.IssueOutcome)
	require.Equal(t, "discuss", out.Status.IssueOutcome.Action)
}

func TestIssueOutcome_DiscussRequiresComment(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "issueLifecycle"))
	body := strings.NewReader(`{"action":"discuss"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/issue-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestIssueOutcome_MissingAction(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "triageIssue"))
	body := strings.NewReader(`{"comment":"x"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/issue-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestIssueOutcome_InvalidAction(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "triageIssue"))
	body := strings.NewReader(`{"action":"merge"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/issue-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestIssueOutcome_CloseRequiresComment(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "triageIssue"))
	body := strings.NewReader(`{"action":"close"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/issue-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestIssueOutcome_WrongKind(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "review"))
	body := strings.NewReader(`{"action":"implement"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/issue-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
}

func TestIssueOutcome_TaskNotFound(t *testing.T) {
	r := buildRouter(t)
	body := strings.NewReader(`{"action":"implement"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/missing/issue-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
}

// --- M4 Task 2: POST /tasks/{t}/change-summary ---

func TestChangeSummary_WritesAllFields(t *testing.T) {
	r := buildRouter(t, task("t1", "alpha"))
	body := strings.NewReader(`{"prTitle":"feat: add login","prBody":"Implements login flow","deliveredScope":"login endpoint","remainingScope":"logout endpoint"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/change-summary", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.NotNil(t, out.Status.ChangeSummary)
	require.Equal(t, "feat: add login", out.Status.ChangeSummary.PRTitle)
	require.Equal(t, "Implements login flow", out.Status.ChangeSummary.PRBody)
	require.Equal(t, "login endpoint", out.Status.ChangeSummary.DeliveredScope)
	require.Equal(t, "logout endpoint", out.Status.ChangeSummary.RemainingScope)
}

func TestChangeSummary_WritesWithoutRemainingScope(t *testing.T) {
	r := buildRouter(t, task("t2", "alpha"))
	body := strings.NewReader(`{"prTitle":"fix: close bug","prBody":"Fixes bug","deliveredScope":"bug fixed"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t2/change-summary", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.NotNil(t, out.Status.ChangeSummary)
	require.Equal(t, "bug fixed", out.Status.ChangeSummary.DeliveredScope)
	require.Equal(t, "", out.Status.ChangeSummary.RemainingScope)
}

func TestChangeSummary_TaskNotFound(t *testing.T) {
	r := buildRouter(t)
	body := strings.NewReader(`{"prTitle":"x","prBody":"y","deliveredScope":"z"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/missing/change-summary", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestChangeSummary_InvalidBody(t *testing.T) {
	r := buildRouter(t, task("t3", "alpha"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/t3/change-summary", strings.NewReader(`not-json`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}
