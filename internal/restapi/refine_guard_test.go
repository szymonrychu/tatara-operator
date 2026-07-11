package restapi_test

// #3: refine's close_issue/edit_issue must hard-refuse (409) when a non-terminal
// Task is actively working that issue, so a refiner cannot close or rewrite an
// issue an implement/review/clarify pod is mid-flight on. A terminal task (or no
// task) still lets the legit close of a delivered/duplicate issue through.

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func liveIssueTask(name, project, issueRef string, number int, phase string) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: project, Kind: "issueLifecycle", Goal: "g",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: issueRef, Number: number},
		},
		Status: tatarav1alpha1.TaskStatus{Phase: phase},
	}
}

func TestCloseProjectIssue_RefusedWhenLiveTask(t *testing.T) {
	writer := &issuesFakeWriter{}
	r := buildRouterWithSCMIssues(t, &issuesFakeReader{}, writer,
		issueProject("pg"),
		issueRepo("rg", "pg", "https://github.com/o/rg.git"),
		issueSecret("pg-scm"),
		liveIssueTask("live-1", "pg", "o/rg#7", 7, "Running"),
	)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/projects/pg/issues/o/rg/7/close", strings.NewReader(`{"comment":"dup"}`)))
	require.Equal(t, http.StatusConflict, w.Code, "close must be refused while a live task works the issue")
	require.Equal(t, "", writer.closedRepo, "CloseIssue must NOT be called")
}

func TestEditProjectIssue_RefusedWhenLiveTask(t *testing.T) {
	writer := &issuesFakeWriter{}
	r := buildRouterWithSCMIssues(t, &issuesFakeReader{}, writer,
		issueProject("pge"),
		issueRepo("rge", "pge", "https://github.com/o/rge.git"),
		issueSecret("pge-scm"),
		liveIssueTask("live-2", "pge", "o/rge#7", 7, "Running"),
	)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPatch, "/projects/pge/issues/o/rge/7", strings.NewReader(`{"body":"rewrite"}`)))
	require.Equal(t, http.StatusConflict, w.Code, "edit must be refused while a live task works the issue")
	require.Equal(t, "", writer.editedRepo, "EditIssue must NOT be called")
}

func TestCloseProjectIssue_AllowedWhenTaskTerminal(t *testing.T) {
	writer := &issuesFakeWriter{}
	// A Succeeded task for the SAME issue must not block a legit close of a
	// delivered/duplicate issue.
	r := buildRouterWithSCMIssues(t, &issuesFakeReader{}, writer,
		issueProject("pgt"),
		issueRepo("rgt", "pgt", "https://github.com/o/rgt.git"),
		issueSecret("pgt-scm"),
		liveIssueTask("done-1", "pgt", "o/rgt#7", 7, "Succeeded"),
	)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/projects/pgt/issues/o/rgt/7/close", strings.NewReader(`{"comment":"delivered"}`)))
	require.Equal(t, http.StatusOK, w.Code, "close of an issue with only a terminal task still works")
	require.Equal(t, "o/rgt", writer.closedRepo)
}

func TestCloseProjectIssue_AllowedWhenLiveTaskIsDifferentIssue(t *testing.T) {
	writer := &issuesFakeWriter{}
	r := buildRouterWithSCMIssues(t, &issuesFakeReader{}, writer,
		issueProject("pgd"),
		issueRepo("rgd", "pgd", "https://github.com/o/rgd.git"),
		issueSecret("pgd-scm"),
		liveIssueTask("live-3", "pgd", "o/rgd#8", 8, "Running"), // different number
	)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/projects/pgd/issues/o/rgd/7/close", strings.NewReader(`{"comment":"dup"}`)))
	require.Equal(t, http.StatusOK, w.Code, "a live task on a DIFFERENT issue must not block this close")
	require.Equal(t, 7, writer.closedNumber)
}
