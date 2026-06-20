// Package titlecheck provides title-quality validation for proposed issues and PRs.
package titlecheck

import (
	"fmt"
	"strings"
)

// denylist are bare tokens that, alone, make a useless title.
var denylist = map[string]bool{
	"go": true, "update": true, "fix": true, "change": true, "wip": true,
	"misc": true, "chore": true, "stuff": true, "changes": true, "updates": true,
	"python": true, "golang": true, "helm": true, "docker": true, "ci": true,
}

// Weak reports whether a proposed issue/PR title is too weak to emit and,
// when weak, a one-line guidance message the agent can act on that same turn.
func Weak(s string) (bool, string) {
	t := strings.TrimSpace(s)
	if t == "" {
		return true, "title is empty: provide a descriptive, conventional title (e.g. 'fix(scope): concrete change')"
	}
	if len(t) < 12 {
		return true, fmt.Sprintf("title %q is too short (<12 chars): describe the concrete change", t)
	}
	if len(strings.Fields(t)) <= 2 {
		return true, fmt.Sprintf("title %q has too few words (<=2): name the component and the change", t)
	}
	if denylist[strings.ToLower(t)] {
		return true, fmt.Sprintf("title %q is a bare token: describe the concrete change instead", t)
	}
	return false, ""
}
