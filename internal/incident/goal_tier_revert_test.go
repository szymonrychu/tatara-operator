package incident

import (
	"strings"
	"testing"
)

func TestGoalTierRevert(t *testing.T) {
	g := GoalTierRevert("tatara", "review", "claude-sonnet-5")
	for _, kw := range []string{
		"review",
		"claude-sonnet-5",
		"tatara",
		"claude-opus-5",
		"values/project-tatara/common.yaml",
		"agent.modelByKind",
		"agent.effortByKind",
		"tatara-helmfile",
		"open one MR",
		"do not merge",
	} {
		if !strings.Contains(strings.ToLower(g), strings.ToLower(kw)) {
			t.Fatalf("tier-revert goal missing %q:\n%s", kw, g)
		}
	}
}
