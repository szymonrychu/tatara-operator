package controller

import (
	"fmt"
	"sort"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// planTurnText is the turn-0 prompt: the goal plus the instruction to
// decompose the work into Subtasks via the subtask MCP tool, and the
// branch directive so the agent knows where to push its work.
func planTurnText(goal, branch, project, task string) string {
	return fmt.Sprintf(
		"You are working on Task `%s` in Project `%s`. "+
			"Use the tatara MCP tools with task=`%s` (and project=`%s`).\n\n"+
			"%s\n\n"+
			"All Project repos are cloned under `/workspace/<name>` (primary: this task's repo). "+
			"Make changes in whatever repos the issue requires; each repo you change is committed and "+
			"pushed to `%s` and gets its own PR.\n\n"+
			"If this objective is small enough to finish in one turn, implement it directly now - "+
			"edit the files in the working tree. If it needs several steps, decompose it into ordered "+
			"Subtasks via subtask_create(task=`%s`, ...), one per concrete step, which are executed in "+
			"later turns.\n\n"+
			"Your changes are committed and pushed to the git branch `%s` automatically at the end of each "+
			"turn (the branch is created from the default branch for you). NEVER commit or push to the "+
			"default branch directly.",
		task, project, task, project, goal, branch, task, branch)
}

// nextPendingSubtask returns the lowest-order Pending subtask, if any.
func nextPendingSubtask(subs []tatarav1alpha1.Subtask) (*tatarav1alpha1.Subtask, bool) {
	pending := make([]tatarav1alpha1.Subtask, 0, len(subs))
	for i := range subs {
		if subs[i].Status.Phase == "Pending" || subs[i].Status.Phase == "" {
			pending = append(pending, subs[i])
		}
	}
	if len(pending) == 0 {
		return nil, false
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].Spec.Order < pending[j].Spec.Order })
	out := pending[0]
	return &out, true
}

// lifecycleTriageText is the turn-0 prompt for the Triage state. The agent
// reads the issue body, consults docs/code/memory via tatara MCP tools, and
// decides by calling the issue_outcome MCP tool with action close (with a
// comment), discuss (with questions as the comment), or implement.
func lifecycleTriageText(task *tatarav1alpha1.Task) string {
	issueRef := ""
	issueURL := ""
	if task.Spec.Source != nil {
		issueRef = task.Spec.Source.IssueRef
		issueURL = task.Spec.Source.URL
	}
	return fmt.Sprintf(
		"You are the tatara lifecycle agent performing Triage for issue %s (%s).\n\n"+
			"Issue body:\n%s\n\n"+
			"Your job:\n"+
			"1. Read the issue carefully.\n"+
			"2. Use tatara MCP tools (memory, code search, docs) to understand the codebase and "+
			"determine whether this issue should be implemented, needs more discussion, or should be closed.\n"+
			"3. Call the `issue_outcome` MCP tool with one of:\n"+
			"   - action=implement  (ready to work on)\n"+
			"   - action=discuss    (needs clarification; supply your questions as `comment`)\n"+
			"   - action=close      (out of scope / duplicate / not actionable; supply reason as `comment`)\n\n"+
			"You MUST call issue_outcome before finishing. Do not open PRs or make code changes in this turn.",
		issueRef, issueURL, task.Spec.Goal)
}

// turnText is the prompt for executing one Subtask.
func turnText(sub tatarav1alpha1.Subtask, branch, task string) string {
	return fmt.Sprintf("(task=`%s`) Subtask: %s\n\n%s\n\nCommit and push your work to branch `%s`.",
		task, sub.Spec.Title, sub.Spec.Detail, branch)
}
