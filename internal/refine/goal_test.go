package refine_test

import (
	"strings"
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/refine"
)

func TestGoalProject_MentionsActionsAndScope(t *testing.T) {
	g := refine.GoalProject([]string{"szymonrychu/tatara-operator", "szymonrychu/tatara-cli"}, 30)
	for _, want := range []string{
		"list_issues", "list_commits", "close_issue", "edit_issue", "create_issue",
		"duplicate", "already", "followup", "commit", "30",
		"tatara-operator", "tatara-cli",
	} {
		if !strings.Contains(g, want) {
			t.Fatalf("goal missing %q", want)
		}
	}
}
