package restapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
			&tatarav1alpha1.Task{}).
		Build()
	s := restapi.NewServer(restapi.Config{Client: fc, Namespace: "tatara"})
	r := chi.NewRouter()
	s.Mount(r, nil)
	return r
}

func project(name string) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec:       tatarav1alpha1.ProjectSpec{TriggerLabel: "tatara"},
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
