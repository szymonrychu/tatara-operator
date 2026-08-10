package controller

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/promptguidance"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// Every agent kind writes forge-visible text (issue comments, MR bodies,
// clarify/discuss messages). Issue #506: these were too verbose for a senior
// dev/devops audience. The turn-0 directive must carry concision guidance for
// every kind, the same way PlatformProblemGuidance does.
func TestAssignmentFor_ConcisenessGuidance(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-concise"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Kind: "implement", Goal: "g"},
	}

	for _, kind := range []string{
		stage.AgentBrainstorm, stage.AgentIncident,
		stage.AgentRefine, stage.AgentImplement, stage.AgentReview, stage.AgentDocumentation,
	} {
		got := assignmentFor(kind, task, &tatarav1alpha1.Project{}, false)
		if !strings.Contains(got, promptguidance.ConcisenessGuidance) {
			t.Fatalf("kind %q: ConcisenessGuidance missing from assignment:\n%s", kind, got)
		}
	}
}
