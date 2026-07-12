package restapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/restapi"
)

func boolPtrH(v bool) *bool { return &v }

func buildRouter(t *testing.T, objs ...client.Object) *chi.Mux {
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
		Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: projectRef, URL: "https://git/" + name, DefaultBranch: "main", IngestEnabled: boolPtrH(true)},
	}
}

func task(name, projectRef string) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: projectRef, RepositoryRef: "repo", Goal: "g"},
	}
}

func TestListRepositories_FilteredByProject(t *testing.T) {
	r := buildRouter(t, project("alpha"), project("beta"), repository("r1", "alpha"), repository("r2", "alpha"), repository("r3", "beta"))
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

func TestListRepositories_ProjectNotFound(t *testing.T) {
	r := buildRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/projects/missing/repositories", nil))
	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestListTasks_FilteredByProject(t *testing.T) {
	r := buildRouter(t, project("alpha"), project("beta"), task("t1", "alpha"), task("t2", "beta"))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/projects/alpha/tasks", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var out []restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Len(t, out, 1)
	require.Equal(t, "t1", out[0].Name)
}

func TestListTasks_ProjectNotFound(t *testing.T) {
	r := buildRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/projects/missing/tasks", nil))
	require.Equal(t, http.StatusNotFound, w.Code)
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

func TestCreateSubtask_AddsRollupEntry(t *testing.T) {
	r := buildRouter(t, task("t1", "alpha"))
	body := strings.NewReader(`{"title":"write tests","detail":"unit","order":1}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/subtasks", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusCreated, w.Code)

	req2 := httptest.NewRequest(http.MethodGet, "/tasks/t1", nil)
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)
	require.Equal(t, http.StatusOK, w2.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w2.Body.Bytes(), &out))
	require.Len(t, out.Status.Subtasks, 1)
	require.Equal(t, "write tests", out.Status.Subtasks[0].Title)
	require.Equal(t, "Pending", out.Status.Subtasks[0].Phase)
	require.Equal(t, 1, out.Status.Subtasks[0].Order)
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

// TestPatchSubtask_NormalizesPhaseCase covers the issue #174 minor bug: a
// lowercase "done" phase used to be written straight to the CRD status and
// rejected by its enum schema. It must now be canonicalized to "Done".
func TestPatchSubtask_NormalizesPhaseCase(t *testing.T) {
	r := buildRouter(t, subtask("s1", "t1", 1))
	body := strings.NewReader(`{"phase":"done"}`)
	req := httptest.NewRequest(http.MethodPatch, "/subtasks/s1", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.SubtaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, "Done", out.Status.Phase)
}

// TestPatchSubtask_RejectsInvalidPhase ensures an unrecognized phase returns a
// clear 400 at the REST boundary rather than an opaque CRD admission 500.
func TestPatchSubtask_RejectsInvalidPhase(t *testing.T) {
	r := buildRouter(t, subtask("s1", "t1", 1))
	body := strings.NewReader(`{"phase":"finished"}`)
	req := httptest.NewRequest(http.MethodPatch, "/subtasks/s1", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// --- Task 11: propose_issue / review_verdict / pr_outcome ---

func TestProposeIssue(t *testing.T) {
	// repo-a must exist and belong to "alpha" for the repository validation to pass.
	repo := repository("repo-a", "alpha")
	r := buildRouter(t, project("alpha"), repo)
	body := strings.NewReader(`{"repositoryRef":"repo-a","title":"fix(auth): login broken on OIDC redirect","body":"login broken","kind":"bug"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/alpha/issues", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusCreated, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, "implement", out.Kind)
	require.False(t, out.ApprovalRequired)
	require.Equal(t, "alpha", out.ProjectRef)
	require.Equal(t, "repo-a", out.RepositoryRef)
	require.Empty(t, out.Status.Phase) // starts Pending; controller completes it to Succeeded
	require.NotEmpty(t, out.Name)
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
	body := strings.NewReader(`{"repositoryRef":"r","title":"fix(auth): login broken on OIDC redirect","body":"B","kind":"bug"}`)
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

func TestReviewVerdict_CarriesSemver(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "review"))
	body := strings.NewReader(`{"decision":"approve","semver":[{"repo":"o/r","number":9,"level":"minor"},{"repo":"o/r2","number":21,"level":"major"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/review", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.NotNil(t, out.Status.ReviewVerdict)
	require.Len(t, out.Status.ReviewVerdict.Semver, 2)
	require.Equal(t, "o/r", out.Status.ReviewVerdict.Semver[0].Repo)
	require.Equal(t, 9, out.Status.ReviewVerdict.Semver[0].Number)
	require.Equal(t, "minor", out.Status.ReviewVerdict.Semver[0].Level)
	require.Equal(t, "major", out.Status.ReviewVerdict.Semver[1].Level)
}

func TestReviewVerdict_InvalidSemverLevel(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "review"))
	body := strings.NewReader(`{"decision":"approve","semver":[{"repo":"o/r","number":9,"level":"nope"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/review", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
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
	r := buildRouter(t, taskWithKind("t1", "alpha", "implement"))
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
	r := buildRouter(t, taskWithKind("t1", "alpha", "issueLifecycle"))
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
	r := buildRouter(t, taskWithKind("t1", "alpha", "issueLifecycle"))
	body := strings.NewReader(`{"reason":"x"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/pr-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestPROutcome_InvalidAction(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "issueLifecycle"))
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

func TestIssueOutcome_Clarify(t *testing.T) {
	// A clarify umbrella pod is instructed (clarifyGoalTail) to call issue_outcome
	// as its REQUIRED terminal; finishClarify consumes Status.IssueOutcome. The
	// handler must admit kind=clarify or every clarify->implement handoff 409s.
	r := buildRouter(t, taskWithKind("t1", "alpha", "clarify"))
	body := strings.NewReader(`{"action":"implement"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/issue-outcome", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.NotNil(t, out.Status.IssueOutcome)
	require.Equal(t, "implement", out.Status.IssueOutcome.Action)
}

func TestIssueOutcome_LockedFieldAccepted(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "clarify"))
	body := strings.NewReader(`{"action":"implement","locked":true}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/issue-outcome", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.NotNil(t, out.Status.IssueOutcome)
	require.True(t, out.Status.IssueOutcome.Locked)
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

// --- POST /tasks/{t}/implement-outcome ---

func TestImplementOutcome_Writes(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "issueLifecycle"))
	body := strings.NewReader(`{"action":"declined","reason":"already fixed in PR #5"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/implement-outcome", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.NotNil(t, out.Status.ImplementOutcome)
	require.Equal(t, "declined", out.Status.ImplementOutcome.Action)
	require.Equal(t, "already fixed in PR #5", out.Status.ImplementOutcome.Reason)
}

func TestImplementOutcome_MissingAction(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "issueLifecycle"))
	body := strings.NewReader(`{"reason":"some reason"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/implement-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestImplementOutcome_InvalidAction(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "issueLifecycle"))
	body := strings.NewReader(`{"action":"implement","reason":"x"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/implement-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestImplementOutcome_EmptyReason(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "issueLifecycle"))
	body := strings.NewReader(`{"action":"declined","reason":""}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/implement-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestImplementOutcome_MissingReason(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "issueLifecycle"))
	body := strings.NewReader(`{"action":"declined"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/implement-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestImplementOutcome_DiscreteImplement(t *testing.T) {
	// The discrete implement umbrella kind runs tatara-implement-workflow, whose
	// terminal (per tatara-mcp-scm-lifecycle) is decline_implementation /
	// already_done -> implement_outcome. The handler must admit kind=implement.
	r := buildRouter(t, taskWithKind("t1", "alpha", "implement"))
	body := strings.NewReader(`{"action":"declined","reason":"out of scope after investigation"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/implement-outcome", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.NotNil(t, out.Status.ImplementOutcome)
	require.Equal(t, "declined", out.Status.ImplementOutcome.Action)
	require.Equal(t, "out of scope after investigation", out.Status.ImplementOutcome.Reason)
}

func TestImplementOutcome_WrongKind(t *testing.T) {
	// review has no implement_outcome tool in its profile; it must still 409.
	r := buildRouter(t, taskWithKind("t1", "alpha", "review"))
	body := strings.NewReader(`{"action":"declined","reason":"not needed"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/implement-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
}

func TestImplementOutcome_TaskNotFound(t *testing.T) {
	r := buildRouter(t)
	body := strings.NewReader(`{"action":"declined","reason":"not needed"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/missing/implement-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestImplementOutcome_AlreadyDoneWrites(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "issueLifecycle"))
	body := strings.NewReader(`{"action":"already_done","reason":"already committed on the shared branch"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/implement-outcome", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.NotNil(t, out.Status.ImplementOutcome)
	require.Equal(t, "already_done", out.Status.ImplementOutcome.Action)
	require.Equal(t, "already committed on the shared branch", out.Status.ImplementOutcome.Reason)
}

func TestImplementOutcome_AlreadyDoneEmptyReason(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "issueLifecycle"))
	body := strings.NewReader(`{"action":"already_done","reason":"   "}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/implement-outcome", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// --- POST /tasks/{t}/brainstorm-outcome ---

func TestBrainstormOutcomeRecordsNone(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "brainstorm"))
	body := strings.NewReader(`{"action":"none","reason":"nothing novel"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/brainstorm-outcome", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.NotNil(t, out.Status.BrainstormOutcome)
	require.Equal(t, "none", out.Status.BrainstormOutcome.Action)
	require.Equal(t, "nothing novel", out.Status.BrainstormOutcome.Reason)
}

func TestBrainstormOutcomeRejectsEmptyReason(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "brainstorm"))
	body := strings.NewReader(`{"action":"none"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/brainstorm-outcome", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestBrainstormOutcomeRejectsNonBrainstormTask(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "issueLifecycle"))
	body := strings.NewReader(`{"action":"none","reason":"nothing novel"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/brainstorm-outcome", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
}

// --- M4 Task 2: POST /tasks/{t}/change-summary ---

func TestChangeSummary_WritesAllFields(t *testing.T) {
	r := buildRouter(t, task("t1", "alpha"))
	body := strings.NewReader(`{"prTitle":"feat: add login","prBody":"Implements login flow","deliveredScope":"login endpoint","remainingScope":"logout endpoint","changeSignificance":"minor"}`)
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
	require.Equal(t, "minor", out.Status.ChangeSummary.Significance)
}

func TestChangeSummary_WritesWithoutRemainingScope(t *testing.T) {
	r := buildRouter(t, task("t2", "alpha"))
	body := strings.NewReader(`{"prTitle":"fix: close bug","prBody":"Fixes bug","deliveredScope":"bug fixed","changeSignificance":"patch"}`)
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
	require.Equal(t, "patch", out.Status.ChangeSummary.Significance)
}

func TestChangeSummary_TaskNotFound(t *testing.T) {
	r := buildRouter(t)
	body := strings.NewReader(`{"prTitle":"fix(auth): login broken on OIDC redirect","prBody":"y","deliveredScope":"z","changeSignificance":"patch"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/missing/change-summary", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
}

// changeSignificance is REQUIRED (D2): a missing or out-of-enum value is a 400,
// the belt-and-suspenders REST gate behind the cli's required MCP field.
func TestChangeSummary_SignificanceRequired(t *testing.T) {
	r := buildRouter(t, task("t1", "alpha"))
	body := strings.NewReader(`{"prTitle":"feat: add login","prBody":"b","deliveredScope":"s"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/change-summary", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code, "body: %s", w.Body.String())
}

func TestChangeSummary_SignificanceInvalidRejected(t *testing.T) {
	r := buildRouter(t, task("t1", "alpha"))
	body := strings.NewReader(`{"prTitle":"feat: add login","prBody":"b","deliveredScope":"s","changeSignificance":"breaking"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/change-summary", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code, "body: %s", w.Body.String())
}

func TestChangeSummary_SignificanceMajorAccepted(t *testing.T) {
	r := buildRouter(t, task("t1", "alpha"))
	body := strings.NewReader(`{"prTitle":"feat!: drop v1 API","prBody":"b","deliveredScope":"s","changeSignificance":"major"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/change-summary", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.NotNil(t, out.Status.ChangeSummary)
	require.Equal(t, "major", out.Status.ChangeSummary.Significance)
}

func TestChangeSummary_InvalidBody(t *testing.T) {
	r := buildRouter(t, task("t3", "alpha"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/t3/change-summary", strings.NewReader(`not-json`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// --- POST /tasks/{t}/comment ---

func taskWithSource(name, projectRef, issueRef string) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    projectRef,
			RepositoryRef: "repo",
			Goal:          "g",
			Kind:          "issueLifecycle",
			Source:        &tatarav1alpha1.TaskSource{IssueRef: issueRef},
		},
	}
}

func TestPostComment_QueuesComment(t *testing.T) {
	tk := taskWithSource("tc1", "alpha", "owner/repo#5")
	r := buildRouter(t, tk)
	body := strings.NewReader(`{"body":"hello"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/tc1/comment", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	// Re-read via a second GET to confirm PendingComments persisted
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/tasks/tc1", nil))
	// The handler returns the updated task; verify comment in Status via re-GET
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/tasks/tc1", nil))
	var got restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w2.Body.Bytes(), &got))
	require.Contains(t, got.Status.PendingComments, "hello")
}

func TestPostComment_EmptyBodyRejected(t *testing.T) {
	tk := taskWithSource("tc2", "alpha", "owner/repo#5")
	r := buildRouter(t, tk)
	body := strings.NewReader(`{"body":""}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/tc2/comment", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestPostComment_NoSourceConflict(t *testing.T) {
	tk := taskWithKind("tc3", "alpha", "issueLifecycle")
	r := buildRouter(t, tk)
	body := strings.NewReader(`{"body":"hello"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/tc3/comment", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
}

func TestPostComment_WrongKindRejected(t *testing.T) {
	tk := taskWithKind("tc4", "alpha", "review")
	tk.Spec.Source = &tatarav1alpha1.TaskSource{IssueRef: "owner/repo#9"}
	r := buildRouter(t, tk)
	body := strings.NewReader(`{"body":"hello"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/tc4/comment", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
	require.Contains(t, w.Body.String(), "issueLifecycle")
}

func TestPostComment_TaskNotFound(t *testing.T) {
	r := buildRouter(t)
	body := strings.NewReader(`{"body":"hello"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/missing/comment", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
}

// --- Finding 2: issueOutcome with plan field ---

func TestIssueOutcome_PlanFieldAccepted(t *testing.T) {
	r := buildRouter(t, taskWithKind("t1", "alpha", "triageIssue"))
	body := strings.NewReader(`{"action":"implement","plan":"implement login flow via OAuth2"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/issue-outcome", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.NotNil(t, out.Status.IssueOutcome)
	require.Equal(t, "implement", out.Status.IssueOutcome.Action)
	require.Equal(t, "implement login flow via OAuth2", out.Status.IssueOutcome.Plan)
}

// --- Finding 6: changeSummary with mostProblematic field ---

func TestChangeSummary_MostProblematicAccepted(t *testing.T) {
	r := buildRouter(t, task("t1", "alpha"))
	body := strings.NewReader(`{"prTitle":"feat(auth): implement OAuth2 login flow","prBody":"body","deliveredScope":"login","mostProblematic":"token refresh","changeSignificance":"minor"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t1/change-summary", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.NotNil(t, out.Status.ChangeSummary)
	require.Equal(t, "token refresh", out.Status.ChangeSummary.MostProblematic)
}

// --- Findings 9/11: proposeIssue kind enum validation ---

func TestProposeIssue_InvalidKindRejected(t *testing.T) {
	r := buildRouter(t, project("alpha"), repository("repo-a", "alpha"))
	body := strings.NewReader(`{"repositoryRef":"repo-a","title":"T","body":"B","kind":"feature"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/alpha/issues", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "bug or improvement")
}

func TestProposeIssue_BugKindAccepted(t *testing.T) {
	r := buildRouter(t, project("alpha"), repository("repo-a", "alpha"))
	body := strings.NewReader(`{"repositoryRef":"repo-a","title":"fix(auth): login broken on OIDC redirect","body":"B","kind":"bug"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/alpha/issues", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusCreated, w.Code)
}

func TestProposeIssue_ImprovementKindAccepted(t *testing.T) {
	r := buildRouter(t, project("alpha"), repository("repo-a", "alpha"))
	body := strings.NewReader(`{"repositoryRef":"repo-a","title":"Add retry logic for flaky CI push events","body":"B","kind":"improvement"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/alpha/issues", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusCreated, w.Code)
}
