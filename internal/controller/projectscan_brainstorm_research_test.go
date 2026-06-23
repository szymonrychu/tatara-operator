package controller

import (
	"strings"
	"testing"
)

// TestBrainstormGoalNamesResearchSkillAndADR verifies that the brainstorm goal
// instructs the agent to invoke tatara-deep-architectural-research for net-new
// structural opportunities and frames the output as a long-lived ADR/RFC artifact
// that is human-gated before any implementation.
func TestBrainstormGoalNamesResearchSkillAndADR(t *testing.T) {
	g := brainstormGoalProject([]string{"o/a", "o/b"}, "STATE", "")

	// Must name the architectural-research skill.
	if !strings.Contains(g, "tatara-deep-architectural-research") {
		t.Fatal("brainstorm goal must name tatara-deep-architectural-research skill")
	}

	// Must signal that its output is a long-lived artifact (ADR/RFC).
	for _, want := range []string{"ADR", "championed", "human"} {
		if !strings.Contains(g, want) {
			t.Fatalf("brainstorm goal missing architectural-research intent keyword %q", want)
		}
	}

	// Must name the skip_research tool (not skip_brainstorm).
	if !strings.Contains(g, "skip_research") {
		t.Fatal("brainstorm goal must name skip_research for the early-exit tool")
	}
}

// TestBrainstormGoalNoDanglingSkipBrainstorm is the regression guard that
// confirms the dangling skip_brainstorm prompt token has been removed.
// This test FAILS on the unmodified codebase.
func TestBrainstormGoalNoDanglingSkipBrainstorm(t *testing.T) {
	g := brainstormGoalProject([]string{"o/a", "o/b"}, "STATE", "")
	if strings.Contains(g, "skip_brainstorm") {
		t.Fatal("brainstorm goal must NOT contain dangling skip_brainstorm token; use skip_research instead")
	}
}
