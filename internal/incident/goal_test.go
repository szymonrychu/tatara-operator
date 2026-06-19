package incident

import (
	"strings"
	"testing"
)

func TestGoalProject(t *testing.T) {
	g := GoalProject("groupKey=abc status=firing commonLabels={alertname=HighCPU}", []string{"o/api", "o/web"})
	for _, kw := range []string{"o/api", "o/web", "groupKey=abc", "grafana", "propose_issue", "read-only"} {
		if !strings.Contains(g, kw) {
			t.Fatalf("incident goal missing %q:\n%s", kw, g)
		}
	}
	// Must forbid remediation/write actions.
	if !strings.Contains(g, "Do NOT") || !strings.Contains(strings.ToLower(g), "remediat") {
		t.Fatalf("incident goal must forbid remediation:\n%s", g)
	}
}
