package agent

import (
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func taskWith(name, kind string, num int, title string, isPR bool) *tatarav1alpha1.Task {
	t := &tatarav1alpha1.Task{}
	t.Name = name
	t.Spec.Kind = kind
	if num > 0 || title != "" {
		t.Spec.Source = &tatarav1alpha1.TaskSource{Number: num, Title: title, IsPR: isPR}
	}
	return t
}

func TestTaskBranch(t *testing.T) {
	tests := []struct {
		name string
		task *tatarav1alpha1.Task
		want string
	}{
		{"pr review is chore", taskWith("scan-def", "review", 7, "Review: add metrics", true), "tatara/chore-7-review-add-metrics"},
		{"no number falls back", taskWith("brainstorm-xyz", "brainstorm", 0, "", false), "tatara/task-brainstorm-xyz"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := TaskBranch(tc.task)
			if tc.want != "" && got != tc.want {
				t.Fatalf("TaskBranch = %q, want %q", got, tc.want)
			}
			if len(got) > 63 {
				t.Fatalf("branch %q exceeds 63 chars", got)
			}
			if !strings.HasPrefix(got, "tatara/") {
				t.Fatalf("branch %q missing tatara/ prefix", got)
			}
		})
	}
}

func TestSlugifyTitle(t *testing.T) {
	tests := []struct{ in, want string }{
		{"Fix flaky CI on push events", "fix-flaky-ci-on-push-events"},
		{"  Trim   Spaces  ", "trim-spaces"},
		{"Special!@#chars$%^here", "special-chars-here"},
		{"", ""},
		{strings.Repeat("a", 60), strings.Repeat("a", 40)},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			if got := slugifyTitle(tc.in); got != tc.want {
				t.Fatalf("slugifyTitle(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestBranchKind(t *testing.T) {
	tests := []struct {
		name string
		task *tatarav1alpha1.Task
		want string
	}{
		{"implement is feat", taskWith("b", "implement", 1, "x", false), "feat"},
		{"review is chore", taskWith("c", "review", 1, "x", true), "chore"},
		{"brainstorm is chore", taskWith("d", "brainstorm", 0, "", false), "chore"},
		{"incident is fix", taskWith("e", "incident", 0, "", false), "fix"},
		{"documentation is docs", taskWith("f", "documentation", 0, "", false), "docs"},
		{"clarify is chore", taskWith("g", "clarify", 0, "", false), "chore"},
		{"refine is chore", taskWith("h", "refine", 0, "", false), "chore"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := branchKind(tc.task); got != tc.want {
				t.Fatalf("branchKind = %q, want %q", got, tc.want)
			}
		})
	}
}
