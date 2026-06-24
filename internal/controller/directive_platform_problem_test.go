package controller

import (
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// TestDirectivesIncludePlatformProblemGuidance asserts that every agent kind's
// turn-0 directive contains the platform-problem self-report instruction.
func TestDirectivesIncludePlatformProblemGuidance(t *testing.T) {
	const wantTool = "report_internal_issue"
	const wantBlock = "platform or tooling failure"

	task := &tatarav1alpha1.Task{}
	task.Name = "task-test"
	task.Spec.ProjectRef = "proj"
	task.Spec.Goal = "do the thing"
	task.Spec.Kind = "issueLifecycle"

	cases := []struct {
		name string
		text string
	}{
		{"plan", planTurnText("goal", "branch/task", "proj", "task-test")},
		{"review", reviewText("Review PR", "proj", "task-test")},
		{"triagePhaseGuidance", lifecyclePhaseGuidance("Triage")},
		{"implementPhaseGuidance", lifecyclePhaseGuidance("Implement")},
		{"brainstorm", brainstormGoalProject([]string{"o/a", "o/b"}, "STATE", "")},
		{"healthCheck", healthCheckGoalProject([]string{"o/a"}, "STATE", "")},
		{"implementPrompt", implementPrompt(task)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(tc.text, wantTool) {
				t.Errorf("directive %q must instruct self-report via %q", tc.name, wantTool)
			}
			if !strings.Contains(tc.text, wantBlock) {
				t.Errorf("directive %q missing platform-problem block", tc.name)
			}
		})
	}
}
