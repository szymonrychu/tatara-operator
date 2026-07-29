package refine

import (
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/promptguidance"
)

// GoalProject used to instruct the agent to call list_issues, list_commits,
// close_issue, edit_issue, submit_turn and exit_plan_mode - none of which
// exist (the first four are reaped by tatara-cli's TestReapedToolsAreGone),
// and it never named submit_outcome, which is how a refine task actually
// ends. Those names were never backticked, so UnknownToolNames could not see
// them and this test passed vacuously. GoalProject now names only real tools
// (scm_read, issue_write, task_list, submit_outcome), backticked so this test
// actually guards them: the next name to drift here fails the build.
func TestRefineGoalNamesOnlyRealTools(t *testing.T) {
	goal := GoalProject([]string{"o/a", "o/b"}, 14)
	if bad := promptguidance.UnknownToolNames(goal); len(bad) > 0 {
		t.Fatalf("refine.GoalProject names tools that do not exist: %v", bad)
	}
}
