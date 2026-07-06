package controller

import (
	"fmt"
	"sort"
	"strings"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/promptguidance"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

const (
	triageCommentCap        = 20   // max comments included in triage prompt
	triageCommentCharBudget = 8000 // max chars of comment thread
)

// platformProblemGuidance is appended to every agent turn-0 directive. A
// platform/tooling failure (MCP error, missing access, a tatara tool failing)
// is self-reported via report_internal_issue, never filed as a tracker issue.
// Defined in internal/promptguidance (a dependency-free leaf package) so
// incident and refine can import the same literal without an import cycle.
const platformProblemGuidance = promptguidance.PlatformProblemGuidance

// toolingNoteGuidance is appended to proposer-agent prompts (brainstorm,
// healthCheck, refine, incident). It instructs the agent to fold any mise
// tooling it needed into the issue it files, so the implementer can add it to
// the repo's .mise.toml.
const toolingNoteGuidance = promptguidance.ToolingNoteGuidance

// toolingConsumeGuidance is appended to implementer-agent prompts. It
// instructs the agent to pick up any '## Tooling' section from the issue body
// and add each tool to the repo's .mise.toml as part of the implementation.
const toolingConsumeGuidance = promptguidance.ToolingConsumeGuidance

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
		task, project, task, project, goal, branch, task, branch) + platformProblemGuidance + toolingConsumeGuidance
}

// lifecyclePhaseGuidance returns a "## Lifecycle phase" block telling the agent
// which lifecycle phase it is running in and what the transient workspace
// guarantees are. The workspace is rebuilt by git clone+checkout on every run,
// so the agent must know which of its outputs survive to the next run:
//   - comment phases (Triage, Conversation): file edits are discarded; only the
//     issue/MR conversation (comments, the issue_outcome decision) is durable.
//   - implementation phases (Implement, MRCI, Merge, MainCI): changes committed
//     and pushed to the task branch are restored on the next run.
func lifecyclePhaseBlock(state string) string {
	durable := "Only what you post to the issue/MR conversation (comments, the issue_outcome decision) survives to the next run. Any file edits you make in this workspace are discarded and will NOT be restored."
	switch state {
	case "Implement", "MRCI", "Merge", "MainCI":
		durable = "Changes you commit and push to the task branch ARE restored on the next run (the workspace is re-cloned and the branch checked out). Uncommitted file edits are discarded."
	}
	return fmt.Sprintf(
		"\n\n## Lifecycle phase: %s\n"+
			"This issue is handled as a multi-phase conversation and you are currently in the %s phase. "+
			"The workspace is transient: it is rebuilt by git clone+checkout on every run and nothing on disk carries over between runs by itself. "+
			"%s",
		state, state, durable)
}

// lifecyclePhaseGuidance is lifecyclePhaseBlock plus platformProblemGuidance, for
// callers (lifecycleTriageText) that do not already carry the platform guidance.
func lifecyclePhaseGuidance(state string) string {
	return lifecyclePhaseBlock(state) + platformProblemGuidance
}

// reviewText is the turn-0 prompt for a kind=review Task (MR/PR review, issue
// #114 decision 4). The PR head is checked out in the workspace so the agent can
// review AND test it; it MUST submit a verdict via review_verdict and never
// pushes (the review pod has no push branch).
func reviewText(goal, project, task string) string {
	return fmt.Sprintf(
		"You are working on Task `%s` in Project `%s`. "+
			"Use the tatara MCP tools with task=`%s` (and project=`%s`).\n\n"+
			"%s\n\n"+
			"This is an MR/PR REVIEW. The pull request's head branch is already checked out "+
			"in the workspace under `/workspace/<owner>/<repo>`, so you can read the diff AND "+
			"actually run it. Your job:\n"+
			"1. Review the change for correctness, security, and quality.\n"+
			"2. TEST it: build it and run the repo's tests/linters where present; note what you ran and the result.\n"+
			"3. Submit your verdict with the `review_verdict` MCP tool - this is REQUIRED before you finish.\n\n"+
			"Do NOT commit, push, or open a PR: the workspace is transient and read-only for review, and nothing "+
			"you change on disk is kept. Communicate only through the review verdict.",
		task, project, task, project, goal) + "\n\n" + skillsDirective("review") + platformProblemGuidance
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
			"Invoke the `tatara-research-followup` skill, which defines how to research the codebase and decide the outcome.\n\n"+
			"Your job:\n"+
			"1. Read the issue AND the full conversation thread carefully.\n"+
			"2. Use tatara MCP tools (memory, code search, docs) to understand the codebase.\n"+
			"3. Decide the outcome by interpreting the human's intent in the thread:\n"+
			"   - A human approval / go-ahead -> action=implement.\n"+
			"   - A human decline, or duplicate / out-of-scope / not-actionable -> action=close (supply the reason as `comment`).\n"+
			"   - Still under discussion or needing the human -> action=discuss (supply your questions as `comment`).\n"+
			"4. IMPORTANT: if THIS issue was opened by tatara itself (a tatara idea), emit action=implement "+
			"ONLY if a human has posted an approval comment in the thread. "+
			"If no human has commented yet, emit action=discuss with comment=\"\" (empty) - "+
			"the operator will NOT post a comment in this case; do NOT use the comment tool to post one either.\n"+
			"5. Call the `issue_outcome` MCP tool with your chosen action.\n\n"+
			"You MUST call issue_outcome before finishing. Do not open PRs or make code changes in this turn.",
		issueRef, issueURL, title, body) + lifecyclePhaseGuidance("Triage")
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

// requiredSkillsForKind returns the skill names an agent must invoke this turn.
// Returns nil for kinds with no required skills (fail-open).
func requiredSkillsForKind(kind string) []string {
	switch kind {
	case "implement":
		return []string{"tatara-implement-workflow", "test-driven-development"}
	case "review":
		return []string{"tatara-review-checklist"}
	case "triageIssue":
		return []string{"tatara-triage-judgment"}
	case "brainstorm":
		return []string{"tatara-brainstorm-guardrails"}
	case "issueLifecycle":
		return []string{"tatara-implement-workflow", "tatara-review-checklist"}
	case "incident":
		return []string{"tatara-incident-investigation", "systematic-debugging"}
	case "selfImprove":
		return []string{"tatara-deep-architectural-research"}
	case "documentation":
		return []string{"tatara-documentation-workflow"}
	default:
		return nil
	}
}

// isReferenceKind reports whether kind uses advisory "Consult" wording (REFERENCE
// skills per Phase-2 contract) rather than mandatory "Required/Invoke" wording.
func isReferenceKind(kind string) bool {
	return kind == "brainstorm" || kind == "triageIssue"
}

// skillsDirective builds the required-skills line for the given kind. Returns ""
// when no skills are mapped (empty kind, unknown kind, etc.).
func skillsDirective(kind string) string {
	skills := requiredSkillsForKind(kind)
	if len(skills) == 0 {
		return ""
	}
	names := strings.Join(skills, ", ")
	if isReferenceKind(kind) {
		return "Consult these skills this turn: " + names + "."
	}
	return "Required skills this turn: " + names + ". Invoke each before acting."
}

// turnText is the prompt for executing one Subtask.
func turnText(sub tatarav1alpha1.Subtask, branch, task string) string {
	return fmt.Sprintf("(task=`%s`) Subtask: %s\n\n%s\n\nCommit and push your work to branch `%s`.",
		task, sub.Spec.Title, sub.Spec.Detail, branch)
}
