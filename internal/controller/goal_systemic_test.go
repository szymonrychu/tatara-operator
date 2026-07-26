package controller

import (
	"strings"
	"testing"
)

func TestBrainstormGoalProject_SystemicMandate(t *testing.T) {
	goal := brainstormGoalProject([]string{"o/a", "o/b"}, "ISSUES:\no/a#1 [bug] x\nOPEN MRs:\no/a#2 [ci:failure] y\nMAIN HEALTH:\no/a main CI: failure", "", 3)
	for _, want := range []string{
		"systemic", "subagent", "MAIN HEALTH:", "OPEN MRs:", "skip_research",
	} {
		if !strings.Contains(goal, want) {
			t.Fatalf("goal missing %q", want)
		}
	}
	// C4: nothing writes a tatara/systemic-* label and proposalPayload has no
	// systemicId field, so the goal must not ask the agent to generate one.
	if strings.Contains(goal, "systemicId") {
		t.Fatalf("goal still names systemicId, which submit_outcome no longer accepts")
	}
	if strings.Contains(goal, "Exactly one action per run") {
		t.Fatalf("stale single-action clause still present")
	}
	if strings.Contains(goal, "comment_on_issue") {
		t.Fatalf("brainstorm goal must NOT contain comment_on_issue (path-2 dropped)")
	}
}
