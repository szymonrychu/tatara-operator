package controller

import (
	"strings"
	"testing"
)

// TestBrainstormGoalNamesCodeQualitySkillAndGrounding verifies that the brainstorm goal
// instructs the agent to use the code-quality skill backed by real on-disk code and
// the code-graph MCP tools, not just the deep-research/ADR path.
func TestBrainstormGoalNamesCodeQualitySkillAndGrounding(t *testing.T) {
	g := brainstormGoalProject([]string{"o/a", "o/b"}, "STATE", "")

	// Must name the code-quality proposal skill.
	if !strings.Contains(g, "tatara-code-quality-proposal") {
		t.Fatal("brainstorm goal must name tatara-code-quality-proposal skill")
	}

	// Must ground proposals in real code: on-disk clones and code-graph tools.
	for _, want := range []string{"workspace/", "code_search", "simplification", "robustness"} {
		if !strings.Contains(g, want) {
			t.Fatalf("brainstorm goal missing code-quality grounding keyword %q", want)
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
