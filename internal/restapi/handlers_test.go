package restapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
