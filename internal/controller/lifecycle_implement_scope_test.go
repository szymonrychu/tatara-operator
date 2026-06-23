package controller

import (
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestImplementPromptReposInScope(t *testing.T) {
	t.Run("absent field omits the spans-repos block", func(t *testing.T) {
		task := &tatarav1alpha1.Task{
			Spec: tatarav1alpha1.TaskSpec{
				ProjectRef:    "proj",
				RepositoryRef: "tatara-helmfile",
				Goal:          "fix #8",
			},
		}
		task.Name = "scan-x"
		got := implementPrompt(task)
		if strings.Contains(got, "spans repos") {
			t.Fatalf("single-repo task must not mention spans repos; got:\n%s", got)
		}
	})

	t.Run("populated field injects every in-scope repo", func(t *testing.T) {
		task := &tatarav1alpha1.Task{
			Spec: tatarav1alpha1.TaskSpec{
				ProjectRef:    "proj",
				RepositoryRef: "tatara-helmfile",
				Goal:          "fix #8",
				ReposInScope:  []string{"tatara-helmfile", "terraform", "ansible"},
			},
		}
		task.Name = "scan-x"
		got := implementPrompt(task)
		if !strings.Contains(got, "This issue spans repos: tatara-helmfile, terraform, ansible") {
			t.Fatalf("missing spans-repos line; got:\n%s", got)
		}
		if !strings.Contains(got, "Edit and push every repo you change") {
			t.Fatalf("missing edit-and-push directive; got:\n%s", got)
		}
	})
}
