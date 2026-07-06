package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBrainstormGoalProject_CodeQualityGrounding(t *testing.T) {
	goal := brainstormGoalProject([]string{"szymonrychu/tatara-operator", "szymonrychu/tatara-cli"}, "ISSUES: none", "")
	for _, want := range []string{
		"workspace/",     // on-disk clone layout
		"code_search",    // code-graph grounding
		"simplification", // target
		"robustness",     // target
		"propose_issue",  // read-only proposer contract
		"skip_research",  // early exit preserved
	} {
		assert.Contains(t, goal, want, "goal must mention %q", want)
	}
	assert.Contains(t, goal, "tatara-operator", "must name the target repos")
}
