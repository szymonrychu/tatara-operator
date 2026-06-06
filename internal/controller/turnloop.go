package controller

import (
	"fmt"
	"sort"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// planTurnText is the turn-0 prompt: the goal plus the instruction to
// decompose the work into Subtasks via the subtask MCP tool, and the
// branch directive so the agent knows where to push its work.
func planTurnText(goal, branch string) string {
	return fmt.Sprintf(
		"%s\n\nDecompose this objective into ordered Subtasks via the subtask MCP tool "+
			"(subtask_create), one per concrete step. Do not start implementation in this turn.\n\n"+
			"All work for this task MUST be committed and pushed to the git branch `%s` "+
			"(create it from the default branch at the start). NEVER commit or push to the default branch directly. "+
			"Push your branch before the task ends.",
		goal, branch)
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
func turnText(sub tatarav1alpha1.Subtask, branch string) string {
	return fmt.Sprintf("Subtask: %s\n\n%s\n\nCommit and push your work to branch `%s`.",
		sub.Spec.Title, sub.Spec.Detail, branch)
}
