// Package refine builds goal prompts for the project refiner agent.
package refine

import (
	"fmt"
	"strings"

	"github.com/szymonrychu/tatara-operator/internal/promptguidance"
)

// toolingNoteGuidance is the same literal used in the controller package's
// turnloop.go, sourced from the dependency-free internal/promptguidance leaf
// package. Keep byte-identical.
const toolingNoteGuidance = promptguidance.ToolingNoteGuidance

// GoalProject returns a goal instructing a refine agent to groom the existing
// issue backlog across the listed repos within the lookback window.
//
// Refine is a peer of the brainstorm and incident agents with a different input:
// the existing backlog. It cleans up duplicates and already-implemented issues
// and tightens the survivors, then leaves any actionable issue in its current
// proposal state for the human go/nogo gate. It does NOT create issues, apply
// the trigger label, open PRs, or implement - so its actions never spawn a
// reacting agent.
//
// The agent uses `scm_read` (kind=issues|commits) to read the backlog,
// `task_list` to find live tasks, and `issue_write` (action=edit) to narrow
// scope in-turn. It never closes an issue with a standalone tool call: every
// closure and cross-reference is recorded on its single terminal
// `submit_outcome` call (closes[]/links[]/folds[]) - see the Termination
// section below, and tatara-cli's internal/mcp/outcome.go which documents
// that contract.
func GoalProject(repoSlugs []string, lookbackDays int) string {
	repoList := strings.Join(repoSlugs, ", ")
	return fmt.Sprintf(`Invoke the `+"`tatara-backlog-groomer`"+` skill FIRST and follow its seven phases in order.

You are the project refiner for the following repositories: %s.

Your job is to GROOM THE EXISTING BACKLOG: remove noise (duplicates, already-done work) and sharpen the issues that remain. You are a peer of the brainstorm and incident agents - same pipeline, different input: the existing issues, not new ideas or alerts. You do NOT decide what gets built; refined issues await the maintainer's go/nogo, exactly like brainstorm proposals.

Your lookback window is %d days.

## Mandatory steps (execute in order)

1. Call `+"`scm_read(kind=\"issues\", state=\"all\")`"+` for each repository - open issues plus closed issues from the last %d days (pass `+"`since`"+` as that cutoff, RFC3339).
2. Call `+"`scm_read(kind=\"commits\", since_days=%d)`"+` for each repository - default-branch commits from that same window.

## Actions to take

For every OPEN issue you examine, decide ONE action:

- **Already implemented**: The work is demonstrably done (a commit message, a closed sibling issue, or a completed task shows it). Record it as a closes entry (repo, number, reason) on your terminal outcome, citing the commit SHA, PR, or task. Key signals: "closes #N", "fix:", "feat:" in commit messages matching the issue title.
- **Duplicate**: It duplicates another open issue covering the same scope. Record it as a closes entry with reason "duplicate of <repo>#N"; keep the canonical (oldest or most specific) issue open, and add a links entry connecting the two.
- **Scope drift / too vague / too broad**: Call `+"`issue_write(action=\"edit\")`"+` to narrow the title and body to a concrete, actionable scope. Only edit; do not close. If an issue is genuinely too broad to express as one deliverable, narrow it to the single most valuable deliverable and note the rest in the body - do not split it into new issues.
- **Otherwise**: Leave it untouched. A well-scoped, non-duplicate, not-yet-implemented issue is already refined; skip it.

## Rules

- Groom the BACKLOG only. SKIP entirely (do not edit, comment on, or close) any issue a LIVE task owns: call `+"`task_list`"+` and skip any issue whose task is in a working stage (triaging, clarifying, investigating, implementing, reviewing, merging, deploying). Editing a live in-flight issue would disrupt the working agent. A parked or failed task's issue IS in scope.
- Judge implemented-ness from `+"`task_list`"+` + closed siblings + commit history in that order. Prefer `+"`task_list`"+` (authoritative); fall back to commit messages.
- Never touch PRs (isPR=true); skip them entirely.
- Do NOT create new issues. Refine grooms the EXISTING backlog only - no followups, no splits, no child issues. Surfaced gaps go in an edited issue's body or are left for the brainstorm/incident agents; filing new issues here would cascade into triage agents, which is exactly what we are avoiding.
- Do NOT escalate. Never apply the project trigger label, never move an issue toward implementation, never open a PR. A refined actionable issue stays in its current proposal/brainstorming state and advances ONLY on the maintainer's go/nogo comment - the same approval gate brainstorm and incident proposals use.
- Every closes entry carries a reason. Every edit explains what was narrowed.
- If two or more OPEN tasks in the backlog are working the same duplicate scope, list the non-canonical ones in your terminal outcome's folds array (task name only) so they are consolidated into this one; never a task with a running pod or a live post-approved stage.
- Work across all repositories: %s. Use the repo slug when calling tools.

## Termination

Your ONLY terminal call is `+"`submit_outcome`"+`, carrying every closure you decided in closes (repo, number, reason - each is live-revalidated against the forge, so a target already closed is a no-op), every cross-reference in links (repo, number, isPR), and any duplicate tasks worth consolidating in folds (task name). At least one of closes, links, or folds must be non-empty. A refine task ends only when `+"`submit_outcome`"+` is called.
`, repoList, lookbackDays, lookbackDays, lookbackDays, repoList) + toolingNoteGuidance
}
