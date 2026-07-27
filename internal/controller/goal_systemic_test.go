package controller

import (
	"strings"
	"testing"
)

// The goal used to carry a SYSTEMIC MANDATE ("prefer a pattern spanning >=2
// repositories... dispatch one parallel subagent per repository") whose only
// execution path was the ACTION RULE's `systemicId` fan-out. Nothing writes a
// tatara/systemic-* label and proposalPayload has no systemicId field, so that
// fan-out was removed and the mandate went with it: a mandate with no way to
// act on it just made the goal contradict itself two paragraphs later. This is
// the guard that neither comes back.
func TestBrainstormGoalProject_NoSystemicRouting(t *testing.T) {
	goal := brainstormGoalProject([]string{"o/a", "o/b"}, "ISSUES:\no/a#1 [bug] x\nOPEN MRs:\no/a#2 [ci:failure] y\nMAIN HEALTH:\no/a main CI: failure", "", 3)
	for _, want := range []string{"MAIN HEALTH:", "OPEN MRs:", "action=skip"} {
		if !strings.Contains(goal, want) {
			t.Fatalf("goal missing %q", want)
		}
	}
	for _, unwanted := range []string{"SYSTEMIC MANDATE", "systemicId", "subagent"} {
		if strings.Contains(goal, unwanted) {
			t.Fatalf("goal still carries retired systemic routing: %q", unwanted)
		}
	}
	if strings.Contains(goal, "Exactly one action per run") {
		t.Fatalf("stale single-action clause still present")
	}
	if strings.Contains(goal, "comment_on_issue") {
		t.Fatalf("brainstorm goal must NOT contain comment_on_issue (path-2 dropped)")
	}
}
