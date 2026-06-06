package v1alpha1_test

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestTaskFields(t *testing.T) {
	task := v1alpha1.Task{
		Spec: v1alpha1.TaskSpec{
			ProjectRef:    "p",
			RepositoryRef: "r",
			Goal:          "do the thing",
			Source: &v1alpha1.TaskSource{
				Provider: "github",
				IssueRef: "owner/repo#123",
				URL:      "https://github.com/owner/repo/issues/123",
			},
			MaxTurns: 25,
		},
		Status: v1alpha1.TaskStatus{
			Phase:          "Running",
			PodName:        "task-p-1",
			TurnsCompleted: 4,
			PrURL:          "https://github.com/owner/repo/pull/5",
			ResultSummary:  "opened PR",
		},
	}
	if task.Spec.Source.Provider != "github" {
		t.Fatalf("Source.Provider = %q, want github", task.Spec.Source.Provider)
	}
	if task.Status.TurnsCompleted != 4 {
		t.Fatalf("TurnsCompleted = %d, want 4", task.Status.TurnsCompleted)
	}
}

func TestSubtaskFields(t *testing.T) {
	s := v1alpha1.Subtask{
		Spec: v1alpha1.SubtaskSpec{
			TaskRef: "task-p-1",
			Title:   "write test",
			Detail:  "add the failing test",
			Order:   1,
		},
		Status: v1alpha1.SubtaskStatus{
			Phase:  "Done",
			TurnID: "turn-abc",
			Result: "test added",
		},
	}
	if s.Spec.Order != 1 {
		t.Fatalf("Order = %d, want 1", s.Spec.Order)
	}
	if s.Status.TurnID != "turn-abc" {
		t.Fatalf("TurnID = %q, want turn-abc", s.Status.TurnID)
	}
}

func TestTaskAndSubtaskRegisteredInScheme(t *testing.T) {
	sch := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(sch); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	for _, kind := range []string{"Task", "Subtask"} {
		if !sch.Recognizes(v1alpha1.GroupVersion.WithKind(kind)) {
			t.Fatalf("%s kind not recognized by scheme", kind)
		}
	}
}
