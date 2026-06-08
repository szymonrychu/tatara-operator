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
			"If this objective is small enough to finish in one turn, implement it directly now - "+
			"edit the files in the working tree. If it needs several steps, decompose it into ordered "+
			"Subtasks via subtask_create(task=`%s`, ...), one per concrete step, which are executed in "+
			"later turns.\n\n"+
			"Your changes are committed and pushed to the git branch `%s` automatically at the end of each "+
			"turn (the branch is created from the default branch for you). NEVER commit or push to the "+
			"default branch directly.",
		task, project, task, project, goal, task, branch)
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

// turnText is the prompt for executing one Subtask.
func turnText(sub tatarav1alpha1.Subtask, branch, task string) string {
	return fmt.Sprintf("(task=`%s`) Subtask: %s\n\n%s\n\nCommit and push your work to branch `%s`.",
		task, sub.Spec.Title, sub.Spec.Detail, branch)
}
