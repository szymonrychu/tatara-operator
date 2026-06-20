package controller

import (
	"strings"
	"testing"
)

func TestBrainstormGoalProject_SystemicMandate(t *testing.T) {
	goal := brainstormGoalProject([]string{"o/a", "o/b"}, "ISSUES:\no/a#1 [bug] x\nOPEN MRs:\no/a#2 [ci:failure] y\nMAIN HEALTH:\no/a main CI: failure")
	for _, want := range []string{
		"systemic", "subagent", "Workflow", "systemicId", "MAIN HEALTH:", "OPEN MRs:", "[bot-engaged]",
	} {
		if !strings.Contains(goal, want) {
			t.Fatalf("goal missing %q", want)
		}
	}
	if strings.Contains(goal, "Exactly one action per run") {
		t.Fatalf("stale single-action clause still present")
	}
}

func TestHealthCheckGoalProject_SystemicMandate(t *testing.T) {
	goal := healthCheckGoalProject([]string{"o/a"}, "ISSUES:\nMAIN HEALTH:\no/a main CI: success")
	for _, want := range []string{"systemic", "subagent", "tatara-health-check", "systemicId"} {
		if !strings.Contains(goal, want) {
			t.Fatalf("goal missing %q", want)
		}
	}
}
