package controller

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

func adoptedAssignmentTask() *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "mt-u-charts-41", Namespace: "default"},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "test-project", RepositoryRef: "charts", Kind: "upgrade", Goal: "g",
			Source: &tatarav1alpha1.TaskSource{IsPR: true, Number: 41},
		},
	}
}

// The upgrade assignment now serves TWO shapes and must keep both. The cron
// shape discovers and claims; the adopted shape has neither to do.
func TestAssignment_UpgradeNamesBothShapes(t *testing.T) {
	got := assignmentFor(stage.AgentUpgrade, adoptedAssignmentTask(), &tatarav1alpha1.Project{}, false)
	for _, want := range []string{
		"EXACTLY ONE",              // the cron shape survives
		"task_context(index=true)", // the cron shape survives
		"already open",             // the adopted shape is named
		"merge request body",       // where the changelog lives
		"do not open a new merge request",
		"requested changes", // the adopted upgrade turn's only entry
	} {
		if !strings.Contains(got, want) {
			t.Errorf("upgrade assignment missing %q", want)
		}
	}
}

// The review agent meets an adopted merge request FIRST, and two things about
// it are outside everything it has been told before: the changelog lives in the
// description rather than the diff, and approving MERGES a third party's merge
// request instead of parking.
func TestAssignment_ReviewNamesTheAdoptedMergeRequest(t *testing.T) {
	got := assignmentFor(stage.AgentReview, adoptedAssignmentTask(), &tatarav1alpha1.Project{}, false)
	for _, want := range []string{
		"read the diff",       // the existing instruction survives
		"DESCRIPTION",         // the body is named as a review input
		"changelog",           //
		"approving MERGES it", // the consequence, stated
		"scm_read",            // the recovery when the body is elided
		`truncated="true"`,    // and the marker that says WHICH empty body was elided
	} {
		if !strings.Contains(got, want) {
			t.Errorf("review assignment missing %q", want)
		}
	}
}
