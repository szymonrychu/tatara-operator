// Package refine builds goal prompts for the project refiner agent.
package refine

import (
	"fmt"
	"strings"
)

// GoalProject returns a goal instructing a refine agent to triage open and
// recently-closed issues across the listed repos within the lookback window.
//
// The agent must use the MCP tools list_issues, list_commits, close_issue,
// edit_issue, and create_issue to perform the following:
//   - Close duplicates (cite the canonical issue).
//   - Close already-implemented issues (cite the implementing commit SHA or PR
//     from task_list / list_commits / a closed sibling).
//   - Tighten scope drift via edit_issue.
//   - Split too-broad issues: create_issue child issues, edit_issue the parent.
//   - File followup issues via create_issue for newly-surfaced work discovered
//     in commit history (gaps, half-done impls, tech-debt / regressions).
func GoalProject(repoSlugs []string, lookbackDays int) string {
	repoList := strings.Join(repoSlugs, ", ")
	return fmt.Sprintf(`You are the project refiner for the following repositories: %s.

Your lookback window is %d days.

## Mandatory steps (execute in order)

1. Call list_issues (open issues + closed issues from the last %d days) for each repository.
2. Call list_commits (default-branch commits from the last %d days) for each repository.
3. Call task_list to load in-progress and recently-completed tasks so you know what is already implemented or being implemented.

## Actions to take

For every issue you examine, decide ONE action:

- **Already implemented**: An issue is already implemented when a commit message or a closed sibling issue or a completed task demonstrates the work is done. Call close_issue with an explanatory comment referencing the relevant commit SHA, PR, or task. Key signals: "closes #N", "fix:", "feat:" in commit messages that match the issue title.
- **Duplicate**: An issue is a duplicate of another open issue covering the same scope. Call close_issue with "duplicate of <repo>#N" comment; keep the canonical (oldest or most specific) issue open.
- **Scope drift / too vague**: Call edit_issue to narrow the title and body to a concrete, actionable scope. Only edit; do not close.
- **Too broad**: Split into child issues via create_issue (each scoped to one deliverable, linking the parent issue in the body). Then call edit_issue on the parent to reflect the residual scope (or close it if fully decomposed).
- **Followup**: A gap, half-done implementation, or regression discovered in the commit or closed-issue history that is NOT already tracked - file it via create_issue, body must link the originating commit SHA or issue.

## Rules

- Judge implemented-ness from task_list + closed siblings + commit history in that order. Prefer task_list (authoritative); fall back to commit messages.
- Never touch PRs (isPR=true); skip them entirely.
- Every close includes an explanatory comment. Every edit explains what was narrowed.
- Be idempotent: before filing a followup issue, check list_issues output to confirm no existing open issue covers the same gap; skip if already tracked.
- Already-existing refine/followup issues (title starts with "Followup:" or "Split:") are not re-filed; update or close them instead.
- Do not create tasks (create_issue only); the operator's scan loop will triage new issues into tasks on the next cycle.
- Work across all repositories: %s. Use the repo slug when calling tools.

## Termination

When you have examined every issue returned by list_issues (open and closed) and taken an action or explicitly skipped (with a reason), you are done. Do not call submit_turn or exit_plan_mode; your work is complete when the issue list is exhausted.
`, repoList, lookbackDays, lookbackDays, lookbackDays, repoList)
}
