package controller

import (
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ResolveCommentAgentKind answers "which agent kind wrote this comment" from the
// operator's OWN write ledger: Comment.AgentKind, stamped when the operator
// posted the comment and keyed by the forge's ExternalID.
//
// It FAILS CLOSED. An unknown ExternalID, a comment the operator did not write,
// or a bot comment predating the ledger all return "", and "" never triggers
// anything (see CrossKindTriggers). That is the correct direction to fail: a
// missed trigger costs responsiveness, a wrong one costs an agent loop.
func ResolveCommentAgentKind(obj client.Object, externalID string) string {
	if externalID == "" {
		return ""
	}
	var comments []tatarav1alpha1.Comment
	switch o := obj.(type) {
	case *tatarav1alpha1.Issue:
		comments = o.Status.Comments
	case *tatarav1alpha1.MergeRequest:
		comments = o.Status.Comments
	default:
		return ""
	}
	for i := range comments {
		if comments[i].ExternalID == externalID {
			return comments[i].AgentKind
		}
	}
	return ""
}

// conversationalParkReasons are the parked reasons that still count as a LIVE
// conversational state (decision D2): a comment on one of them starts work,
// because a human is being waited on and the thread is not settled. Every other
// park reason is a dead end that a comment resumes through the WS3-I4 fresh
// re-mint, never through a smuggled trigger.
var conversationalParkReasons = map[string]bool{
	stage.ReasonAwaitingHuman:      true,
	stage.ReasonIdentityUnverified: true,
}

// ReactingAgentKind is the agent kind a comment on this Task would wake, or ""
// when the Task is not in a live conversational state.
//
// Decision D2: agent work starts only for clarifying, reviewing, conversing, or
// a park that is still waiting on a human. A comment on a settled, delivered,
// merging or closed Task is mirrored and queued and starts nothing.
func ReactingAgentKind(task *tatarav1alpha1.Task) string {
	if task == nil {
		return ""
	}
	switch task.Status.Stage {
	case tatarav1alpha1.StageClarifying, tatarav1alpha1.StageReviewing, tatarav1alpha1.StageConversing:
		return stage.AgentKindFor(task.Status.Stage)
	case tatarav1alpha1.StageParked:
		if conversationalParkReasons[task.Status.StageReason] {
			// A park that is waiting on a human resumes into a conversation, and
			// conversing runs the clarify agent (F.2).
			return stage.AgentKindFor(tatarav1alpha1.StageConversing)
		}
		return ""
	default:
		return ""
	}
}

// CrossKindTriggers reports whether an agent-authored comment may start work.
//
// It may, and only, when the reacting agent kind DIFFERS from the authoring one.
// Review's comment wakes implement; implement's own comment never wakes
// implement. Self-loops become impossible BY CONSTRUCTION rather than by a
// filter that can be got wrong - which is what the 2026-06 reactivation loop
// (40+ duplicate bot comments in production) was.
//
// An empty kind on EITHER side never triggers: an unresolved author is a comment
// whose provenance the operator cannot vouch for, and an unresolved reacting kind
// is a Task that is not in a live conversational state.
func CrossKindTriggers(authorKind, reactingKind string) bool {
	if authorKind == "" || reactingKind == "" {
		return false
	}
	return authorKind != reactingKind
}
