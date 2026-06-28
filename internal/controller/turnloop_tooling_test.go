package controller

import (
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// toolingSubstr is a unique prefix shared by toolingNoteGuidance used in
// proposer-agent prompts (brainstorm, healthCheck, refine, incident).
const toolingNoteSubstr = "## Tooling you needed"

// toolingConsumeSubstr is a unique prefix shared by toolingConsumeGuidance
// used in implementer-agent prompts (planTurnText / implementPrompt).
const toolingConsumeSubstr = "## Tooling from the issue"

// TestToolingNoteGuidance_InBrainstormGoal checks that brainstormGoalProject
// contains the toolingNoteGuidance block so proposer agents know to fold
// any extra mise tooling into the issue they file.
func TestToolingNoteGuidance_InBrainstormGoal(t *testing.T) {
	cases := []struct {
		name     string
		slugs    []string
		ctx      string
		guidance string
	}{
		{"no_guidance", []string{"o/a", "o/b"}, "STATE", ""},
		{"with_guidance", []string{"o/a"}, "STATE", "some charter"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := brainstormGoalProject(tc.slugs, tc.ctx, tc.guidance)
			if !strings.Contains(g, toolingNoteSubstr) {
				t.Errorf("brainstorm goal missing tooling-note guidance (%q):\n%s", toolingNoteSubstr, g)
			}
		})
	}
}

// TestToolingNoteGuidance_InHealthCheckGoal checks that healthCheckGoalProject
// contains the toolingNoteGuidance block.
func TestToolingNoteGuidance_InHealthCheckGoal(t *testing.T) {
	cases := []struct {
		name     string
		slugs    []string
		ctx      string
		guidance string
	}{
		{"no_guidance", []string{"o/a"}, "STATE", ""},
		{"with_guidance", []string{"o/a"}, "STATE", "some charter"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := healthCheckGoalProject(tc.slugs, tc.ctx, tc.guidance)
			if !strings.Contains(g, toolingNoteSubstr) {
				t.Errorf("healthCheck goal missing tooling-note guidance (%q):\n%s", toolingNoteSubstr, g)
			}
		})
	}
}

// TestToolingConsumeGuidance_InPlanTurnText checks that planTurnText (the
// implement-agent turn-0 base) contains toolingConsumeGuidance.
func TestToolingConsumeGuidance_InPlanTurnText(t *testing.T) {
	cases := []struct {
		goal, branch, project, task string
	}{
		{"ship the feature", "tatara/task-abc", "proj1", "task-abc"},
		{"fix a typo", "tatara/task-xyz", "proj2", "task-xyz"},
	}
	for _, tc := range cases {
		t.Run(tc.task, func(t *testing.T) {
			txt := planTurnText(tc.goal, tc.branch, tc.project, tc.task)
			if !strings.Contains(txt, toolingConsumeSubstr) {
				t.Errorf("planTurnText missing tooling-consume guidance (%q):\n%s", toolingConsumeSubstr, txt)
			}
		})
	}
}

// TestToolingConsumeGuidance_InImplementPrompt checks that implementPrompt
// (which is built on planTurnText) also contains toolingConsumeGuidance.
func TestToolingConsumeGuidance_InImplementPrompt(t *testing.T) {
	task := &tatarav1alpha1.Task{
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "proj",
			Goal:       "implement the thing",
			Kind:       "issueLifecycle",
		},
	}
	task.Name = "task-impl"
	got := implementPrompt(task)
	if !strings.Contains(got, toolingConsumeSubstr) {
		t.Errorf("implementPrompt missing tooling-consume guidance (%q):\n%s", toolingConsumeSubstr, got)
	}
}
