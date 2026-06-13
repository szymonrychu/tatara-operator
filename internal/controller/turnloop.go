package controller

import (
	"fmt"
	"sort"
	"strings"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

const (
	triageCommentCap        = 20   // max comments included in triage prompt
	triageCommentCharBudget = 8000 // max chars of comment thread
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
func lifecycleTriageText(task *tatarav1alpha1.Task, title, body string) string {
	issueRef := ""
	issueURL := ""
	if task.Spec.Source != nil {
		issueRef = task.Spec.Source.IssueRef
		issueURL = task.Spec.Source.URL
	}
	if title == "" {
		title = "(title unavailable)"
	}
	if body == "" {
		body = "(body unavailable)"
	}
	return fmt.Sprintf(
		"You are the tatara lifecycle agent performing Triage for issue %s (%s).\n\n"+
			"Issue title: %s\n"+
			"Issue body:\n%s\n\n"+
			"Your job:\n"+
			"1. Read the issue AND the full conversation thread carefully.\n"+
			"2. Use tatara MCP tools (memory, code search, docs) to understand the codebase.\n"+
			"3. Decide the outcome by interpreting the human's intent in the thread:\n"+
			"   - A human approval / go-ahead -> action=implement.\n"+
			"   - A human decline, or duplicate / out-of-scope / not-actionable -> action=close (supply the reason as `comment`).\n"+
			"   - Still under discussion or needing the human -> action=discuss (supply your questions as `comment`).\n"+
			"4. IMPORTANT: if THIS issue was opened by tatara itself (a tatara idea), emit action=implement "+
			"ONLY if a human has posted an approval comment in the thread; otherwise emit action=discuss and wait.\n"+
			"5. Call the `issue_outcome` MCP tool with your chosen action.\n\n"+
			"You MUST call issue_outcome before finishing. Do not open PRs or make code changes in this turn.",
		issueRef, issueURL, title, body)
}

// buildTriagePrompt constructs the turn-0 prompt for the Triage state. When
// comments are non-nil it appends a "## Conversation thread" block (capped to
// the most-recent triageCommentCap comments and triageCommentCharBudget chars)
// so a fresh pod has full context. When comments is empty the prompt equals
// lifecycleTriageText(task, title, body).
func buildTriagePrompt(task *tatarav1alpha1.Task, title, body string, comments []scm.IssueComment) string {
	base := lifecycleTriageText(task, title, body)
	if len(comments) == 0 {
		return base
	}
	// Cap to most-recent N comments.
	if len(comments) > triageCommentCap {
		comments = comments[len(comments)-triageCommentCap:]
	}
	var sb strings.Builder
	sb.WriteString("\n\n## Conversation thread\n")
	for _, c := range comments {
		line := fmt.Sprintf("**%s**: %s\n", c.Author, c.Body)
		sb.WriteString(line)
	}
	thread := sb.String()
	// Apply char budget: truncate from the front (oldest) if over budget.
	if len(thread) > triageCommentCharBudget {
		thread = thread[len(thread)-triageCommentCharBudget:]
	}
	return base + thread
}

// turnText is the prompt for executing one Subtask.
func turnText(sub tatarav1alpha1.Subtask, branch, task string) string {
	return fmt.Sprintf("(task=`%s`) Subtask: %s\n\n%s\n\nCommit and push your work to branch `%s`.",
		task, sub.Spec.Title, sub.Spec.Detail, branch)
}
