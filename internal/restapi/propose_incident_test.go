package restapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/restapi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const incidentTestNS = "tatara"

func buildIncidentTestRouter(t *testing.T, objs ...client.Object) (*chi.Mux, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&tatarav1alpha1.Project{}, &tatarav1alpha1.Repository{},
			&tatarav1alpha1.Task{}, &tatarav1alpha1.Subtask{}).
		Build()
	s := restapi.NewServer(restapi.Config{Client: fc, Namespace: incidentTestNS})
	r := chi.NewRouter()
	s.Mount(r, nil)
	return r, fc
}

func incidentProject(name string) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: incidentTestNS},
		Spec: tatarav1alpha1.ProjectSpec{
			TriggerLabel:       "tatara",
			MaxConcurrentTasks: 3,
			Scm: &tatarav1alpha1.ScmSpec{
				Provider: "github", Owner: "o", BotLogin: "tatara-bot",
			},
		},
	}
}

func incidentRepo(name, projectRef string) *tatarav1alpha1.Repository {
	return &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: incidentTestNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: projectRef, URL: "https://github.com/o/r.git",
			DefaultBranch: "main", IngestEnabled: boolPtrH(true),
		},
	}
}

func inflightIncidentTask(name, projectRef string) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: incidentTestNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: projectRef, Kind: "incident", Goal: "investigate"},
	}
}

func postProposeIncident(t *testing.T, r *chi.Mux, proj, repo string) *httptest.ResponseRecorder {
	t.Helper()
	body := strings.NewReader(`{"repositoryRef":"` + repo + `","title":"Fix the broken thing in module X","body":"details here","kind":"bug"}`)
	req := httptest.NewRequest(http.MethodPost, "/projects/"+proj+"/issues", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func newestProposalTask(t *testing.T, fc client.Client, proj string) *tatarav1alpha1.Task {
	t.Helper()
	var list tatarav1alpha1.TaskList
	require.NoError(t, fc.List(context.Background(), &list, client.InNamespace(incidentTestNS)))
	var found *tatarav1alpha1.Task
	for i := range list.Items {
		tk := &list.Items[i]
		if tk.Spec.ProjectRef == proj && tk.Spec.ProposedIssue != nil {
			if found == nil || tk.CreationTimestamp.After(found.CreationTimestamp.Time) {
				found = tk
			}
		}
	}
	require.NotNil(t, found, "no proposal task found for project %q", proj)
	return found
}

func TestProposeIssue_StampsIncidentWhenIncidentInflight(t *testing.T) {
	proj := incidentProject("inc-proj")
	repo := incidentRepo("inc-repo", "inc-proj")
	incTask := inflightIncidentTask("inc-task-1", "inc-proj")

	r, fc := buildIncidentTestRouter(t, proj, repo, incTask)
	rec := postProposeIncident(t, r, "inc-proj", "inc-repo")
	require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())

	task := newestProposalTask(t, fc, "inc-proj")
	require.NotNil(t, task.Spec.ProposedIssue, "ProposedIssue must be set")
	require.True(t, task.Spec.ProposedIssue.Incident, "incident-inflight proposal must set ProposedIssue.Incident=true")
}

func TestProposeIssue_NoIncidentWhenIncidentTaskParked(t *testing.T) {
	proj := incidentProject("parked-proj")
	repo := incidentRepo("parked-repo", "parked-proj")
	// Incident tasks leave Phase empty for their whole life and signal
	// completion via LifecycleState (Done/Stopped/Parked). A parked incident
	// task is terminal and must NOT stamp a later proposal as an incident.
	incTask := inflightIncidentTask("inc-task-parked", "parked-proj")
	incTask.Status.LifecycleState = "Parked"

	r, fc := buildIncidentTestRouter(t, proj, repo, incTask)
	rec := postProposeIncident(t, r, "parked-proj", "parked-repo")
	require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())

	task := newestProposalTask(t, fc, "parked-proj")
	if task.Spec.ProposedIssue != nil && task.Spec.ProposedIssue.Incident {
		t.Fatalf("parked (terminal) incident task must NOT set Incident")
	}
}

func TestProposeIssue_NoIncidentWhenOnlyBrainstormInflight(t *testing.T) {
	proj := incidentProject("bs-proj")
	repo := incidentRepo("bs-repo", "bs-proj")
	bsTask := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "bs-task-1", Namespace: incidentTestNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "bs-proj", Kind: "brainstorm", Goal: "brainstorm"},
	}

	r, fc := buildIncidentTestRouter(t, proj, repo, bsTask)
	rec := postProposeIncident(t, r, "bs-proj", "bs-repo")
	require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())

	task := newestProposalTask(t, fc, "bs-proj")
	if task.Spec.ProposedIssue != nil && task.Spec.ProposedIssue.Incident {
		t.Fatalf("brainstorm-only proposal must NOT set Incident")
	}
}
