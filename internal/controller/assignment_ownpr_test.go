package controller

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/promptguidance"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

func ownPRTask() *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "test-task", Namespace: "default"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "test-project", Kind: "implement", Goal: "g"},
	}
}

// PR B2, THE ANTI-DRIFT PIN. Before this, the implement job text ran from the
// gate to submit_outcome without the word CI in it once, so an agent that pushed
// and submitted in the same second was doing exactly what it had been told. Each
// clause below is load-bearing: dropping any one of them puts the operator's B1
// refusal in front of an agent that was never told the rule it broke.
func TestAssignmentImplementRequiresTheOwnYourPRLoop(t *testing.T) {
	got := assignmentFor(stage.AgentImplement, ownPRTask(), &tatarav1alpha1.Project{}, false)

	for _, want := range []string{
		// The three axes, in the same vocabulary the refusal uses.
		"`scm_read(kind=\"ci\"",
		"GREEN at your pushed head",
		"Every review finding on the PR is answered IN CODE",
		"merges cleanly into its base",
		// The bound, and what happens past it. Without this half, "wait for green"
		// reads as "wait indefinitely" and idles a pod for a terraform queue.
		"BOUND YOUR WAIT AT ROUGHLY 20 MINUTES",
		"Submitting on a still-running pipeline is CORRECT",
		// The refusal, stated so a 409 reads as a normal result rather than a
		// tool failure the agent gives up on.
		"the operator refuses that outcome, your turn does not end",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the implement job text no longer requires the loop: missing %q", want)
		}
	}
}

// THE REVIEW AGENT IS TOLD THE SAME REFUSAL EXISTS. An approve on a red head is
// refused by the operator, and a reviewer that does not know that spends its
// turn discovering it.
func TestAssignmentReviewRequiresReadingThePipeline(t *testing.T) {
	got := assignmentFor(stage.AgentReview, ownPRTask(), &tatarav1alpha1.Project{}, false)

	for _, want := range []string{
		"`scm_read(kind=\"ci\"",
		"A RED check is a finding",
		"an approve on a red head is REFUSED",
		"Checks still RUNNING are not a finding",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the review job text no longer requires reading the pipeline: missing %q", want)
		}
	}
}

// THE TOOL NAME IS `scm_read`, NOT `scm_read_ci`. ci is a KIND argument to it
// (restapi/server.go route 13). This is the one that would have shipped: the
// route is logged as action=scm_read_ci and the plan for this change named
// `scm_read_ci` throughout, and neither of those is a tool an agent can call.
func TestOwnYourPRRuleNamesOnlyRealTools(t *testing.T) {
	for _, kind := range []string{stage.AgentImplement, stage.AgentReview} {
		got := assignmentFor(kind, ownPRTask(), &tatarav1alpha1.Project{}, false)
		if bad := promptguidance.UnknownToolNames(got); len(bad) > 0 {
			t.Fatalf("%s names tools that do not exist: %v", kind, bad)
		}
		if strings.Contains(got, "scm_read_ci") {
			t.Fatalf("%s names scm_read_ci, which is a LOG ACTION, not a tool", kind)
		}
	}
}
