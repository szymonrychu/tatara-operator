// Package titlecheck provides title-quality validation for proposed issues and PRs.
package titlecheck

import (
	"fmt"
	"strings"
)

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
	return false, ""
}
