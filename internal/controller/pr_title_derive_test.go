package controller

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestDerivePRTitle(t *testing.T) {
	mk := func(kind, srcTitle, csTitle, goal string) *tatarav1alpha1.Task {
		ta := &tatarav1alpha1.Task{}
		ta.Spec.Kind = kind
		ta.Spec.Goal = goal
		if srcTitle != "" {
			ta.Spec.Source = &tatarav1alpha1.TaskSource{Title: srcTitle}
		}
		if csTitle != "" {
			ta.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{PRTitle: csTitle}
		}
		return ta
	}
	tests := []struct {
		name string
		task *tatarav1alpha1.Task
		want string
	}{
		{"strong changesummary wins", mk("issueLifecycle", "Fix flaky CI", "fix(ci): retry flaky push checks", "body line"), "fix(ci): retry flaky push checks"},
		{"weak changesummary derives", mk("issueLifecycle", "Add main-branch CI health survey", "Go", "body line"), "fix(repo): Add main-branch CI health survey"},
		{"absent changesummary derives", mk("implement", "Thread systemicId through propose_issue", "", "issue body first line"), "feat(repo): Thread systemicId through propose_issue"},
		{"no source title falls to goal-ish but not weak", mk("issueLifecycle", "", "", "Make the brainstorm survey live state"), "fix(repo): Make the brainstorm survey live state"},
		{"documentation kind titles as docs", mk("documentation", "", "", "Update docs for the merged diff"), "docs(repo): Update docs for the merged diff"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := derivePRTitle(tc.task, "repo"); got != tc.want {
				t.Fatalf("derivePRTitle = %q, want %q", got, tc.want)
			}
		})
	}
}
