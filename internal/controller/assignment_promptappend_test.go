package controller

import (
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestAssignmentFor_PromptAppendByKind verifies the append block orders the
// built-in job text before the wildcard entry before the kind-specific entry,
// and that an empty Project appends nothing.
func TestAssignmentFor_PromptAppendByKind(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "test-task"},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "test-project",
			Kind:       "review",
			Goal:       "test goal",
		},
	}

	proj := &tatarav1alpha1.Project{}
	proj.Spec.Agent.PromptAppendByKind = map[string]string{
		"*":      "WILDCARD_TEXT",
		"review": "REVIEW_TEXT",
	}

	got := assignmentFor(stage.AgentReview, task, proj)

	jobIdx := strings.Index(got, "## Your job")
	wildcardIdx := strings.Index(got, "WILDCARD_TEXT")
	reviewIdx := strings.Index(got, "REVIEW_TEXT")
	if jobIdx == -1 || wildcardIdx == -1 || reviewIdx == -1 {
		t.Fatalf("missing expected sections: %q", got)
	}
	if jobIdx >= wildcardIdx || wildcardIdx >= reviewIdx {
		t.Fatalf("wrong order: job=%d wildcard=%d review=%d\n%s", jobIdx, wildcardIdx, reviewIdx, got)
	}

	emptyProj := &tatarav1alpha1.Project{}
	gotEmpty := assignmentFor(stage.AgentReview, task, emptyProj)
	if strings.Contains(gotEmpty, "WILDCARD_TEXT") || strings.Contains(gotEmpty, "REVIEW_TEXT") {
		t.Fatalf("empty project must append nothing: %q", gotEmpty)
	}
}
