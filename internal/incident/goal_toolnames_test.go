package incident

import (
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/promptguidance"
)

// Every tool an agent goal names must exist. The bug this guards against is
// concrete: the project brainstorm goal instructed agents to call
// `list_handoffs` and `get_handoff` for months. Neither tool has ever existed;
// agents silently fell back to task_list and a narrowed scope propagated
// cycle to cycle. See internal/promptguidance/toolnames.go.
func TestIncidentGoalsNameOnlyRealTools(t *testing.T) {
	const alertCtx = "group=g status=firing labels: alertname=X"
	slugs := []string{"o/a", "o/b"}

	for name, goal := range map[string]string{
		"GoalProject":    GoalProject(alertCtx, slugs),
		"GoalEscalation": GoalEscalation(alertCtx, slugs, "o/a#7", 3),
		"GoalTierRevert": GoalTierRevert("mtg", "brainstorm", "claude-sonnet-4-6"),
	} {
		t.Run(name, func(t *testing.T) {
			if bad := promptguidance.UnknownToolNames(goal); len(bad) > 0 {
				t.Fatalf("%s names tools that do not exist: %v\n"+
					"Add the tool to promptguidance.AgentVisibleTools if tatara-cli really "+
					"serves it, otherwise fix the prose to name the tool that does.", name, bad)
			}
		})
	}
}
