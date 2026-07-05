package restapi_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/go-chi/chi/v5"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/restapi"
)

func buildRouterForPropose(t *testing.T, objs ...client.Object) *chi.Mux {
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

func proposeProject(name string) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec: tatarav1alpha1.ProjectSpec{
			TriggerLabel:       "tatara",
			MaxConcurrentTasks: 3,
		},
	}
}

func proposeRepo(name, projectRef string) *tatarav1alpha1.Repository {
	return &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:    projectRef,
			URL:           "https://github.com/o/r.git",
			DefaultBranch: "main",
			IngestEnabled: boolPtrH(true),
		},
	}
}

func TestProposeIssue_WeakTitleRejected(t *testing.T) {
	proj := proposeProject("p")
	repo := proposeRepo("r", "p")

	tests := []struct {
		name       string
		title      string
		wantStatus int
	}{
		{"weak bare go rejected", "Go", http.StatusBadRequest},
		{"empty rejected", "", http.StatusBadRequest},
		{"good accepted", "Add systemic correlation labels to proposals", http.StatusCreated},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := buildRouterForPropose(t, proj, repo)
			body := fmt.Sprintf(`{"repositoryRef":"r","title":%q,"body":"a real body here","kind":"bug"}`, tc.title)
			req := httptest.NewRequest(http.MethodPost, "/projects/p/issues", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			srv.ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
		})
	}
}

func TestProposeIssue_SystemicIDThreaded(t *testing.T) {
	proj := proposeProject("psys")
	repo := proposeRepo("rsys", "psys")
	srv := buildRouterForPropose(t, proj, repo)

	body := `{"repositoryRef":"rsys","title":"Add systemic correlation labels to proposals","body":"body details here","kind":"bug","systemicId":"grp1"}`
	req := httptest.NewRequest(http.MethodPost, "/projects/psys/issues", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())

	// Decode the created task DTO and check the ProposedIssue.SystemicID.
	var resp restapi.TaskDTO
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	// The task DTO exposes ProposedIssue; verify systemicId was threaded.
	require.Equal(t, "grp1", resp.ProposedIssue.SystemicID, "systemicId must be threaded through")
}

func TestChangeSummary_WeakPRTitleRejected(t *testing.T) {
	tk := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "tatara"},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    "p",
			RepositoryRef: "r",
			Goal:          "some goal",
		},
	}
	srv := buildRouterForPropose(t, proposeProject("p"), proposeRepo("r", "p"), tk)

	tests := []struct {
		name       string
		prTitle    string
		wantStatus int
	}{
		{"weak bare go rejected", "Go", http.StatusBadRequest},
		{"absent prTitle allowed", "", http.StatusOK},
		{"good prTitle allowed", "fix(scan): retry flaky push events on CI timeout", http.StatusOK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"prTitle":%q,"prBody":"some body","changeSignificance":"patch"}`, tc.prTitle)
			req := httptest.NewRequest(http.MethodPost, "/tasks/t1/change-summary", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			srv.ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
		})
	}
}
