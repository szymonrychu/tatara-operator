package refine

import (
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/promptguidance"
)

// KNOWN GAP, stated so nobody reads this pass as a clean bill of health:
// GoalProject's prose instructs the agent to call list_issues, list_commits,
// close_issue, edit_issue, submit_turn and exit_plan_mode. None of those exist
// (the first four are in tatara-cli's TestReapedToolsAreGone), and the goal
// never names submit_outcome, which is how a refine stage actually ends. They
// are NOT backticked, so UnknownToolNames cannot see them and this test passes
// vacuously today. Rewriting that prose needs a decision about which real call
// shapes replace them (scm_read? issue_write?) and is tracked separately. This
// test still earns its place: it catches the next backticked name to drift.
func TestRefineGoalNamesOnlyRealTools(t *testing.T) {
	goal := GoalProject([]string{"o/a", "o/b"}, 14)
	if bad := promptguidance.UnknownToolNames(goal); len(bad) > 0 {
		t.Fatalf("refine.GoalProject names tools that do not exist: %v", bad)
	}
}
