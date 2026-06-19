package controller

// Tests for FIX: a blank/whitespace-only discuss comment must NOT be posted.
// Root cause of the tatara-operator#74/#75 422 loop: finishTriage defaults to
// action=discuss with an empty comment when the agent never calls issue_outcome
// (outcome==nil), and the agent can also return a whitespace-only comment.
// Posting that blank body to GitHub yields "422 Body cannot be blank", which
// returns an error before enterConversation runs, so the task is stuck in Triage
// and the reconcile retries forever. The fix skips the post when the body is
// blank, letting the discuss arm proceed to Conversation.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

// seedDiscussTaskWithOutcome seeds a Succeeded triage task whose IssueOutcome is
// set to the provided value (nil allowed, to model the agent never calling
// issue_outcome).
func seedDiscussTaskWithOutcome(t *testing.T, suffix string, outcome *tatarav1alpha1.IssueOutcome) (*tatarav1alpha1.Task, *tatarav1alpha1.Project) {
	t.Helper()
	_, task, _ := seedLabelTask(t, "bc-"+suffix, nil)
	ctx := context.Background()
	var fresh tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &fresh))
	fresh.Status.Phase = "Succeeded"
	fresh.Status.IssueOutcome = outcome
	require.NoError(t, k8sClient.Status().Update(ctx, &fresh))

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))
	return getTaskByName(t, task.Name), &proj
}

// TestFinishTriage_HumanFiled_Discuss_WhitespaceComment_SkipsPost verifies that a
// human-filed issue (silence gate does NOT apply) with action=discuss and a
// whitespace-only comment does NOT post a comment and still enters Conversation.
func TestFinishTriage_HumanFiled_Discuss_WhitespaceComment_SkipsPost(t *testing.T) {
	task, proj := seedDiscussTaskWithOutcome(t, "ws",
		&tatarav1alpha1.IssueOutcome{Action: "discuss", Comment: "   "})

	rdr := &discussSilenceReader{
		// No marker -> human-filed -> silence gate inactive -> would normally post.
		issueBody: "I want a new feature",
		comments:  nil,
	}
	w := &commentCapturingWriter{}
	r := reconcilerFor(w, rdr)

	_, err := r.finishTriage(context.Background(), proj, task)
	require.NoError(t, err)

	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.LifecycleState,
		"blank discuss comment must still enter Conversation")

	w.mu.Lock()
	posted := len(w.commentBodies)
	w.mu.Unlock()
	require.Zero(t, posted,
		"whitespace-only discuss comment must NOT be posted; got: %v", w.commentBodies)
}

// TestFinishTriage_HumanFiled_NilOutcome_SkipsPost verifies that when the agent
// never calls issue_outcome (outcome==nil) finishTriage defaults to discuss with
// an empty comment, which must NOT be posted, and the task enters Conversation.
func TestFinishTriage_HumanFiled_NilOutcome_SkipsPost(t *testing.T) {
	task, proj := seedDiscussTaskWithOutcome(t, "nil", nil)

	rdr := &discussSilenceReader{
		issueBody: "I want a new feature",
		comments:  nil,
	}
	w := &commentCapturingWriter{}
	r := reconcilerFor(w, rdr)

	_, err := r.finishTriage(context.Background(), proj, task)
	require.NoError(t, err)

	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.LifecycleState,
		"inconclusive triage (nil outcome) must enter Conversation")

	w.mu.Lock()
	posted := len(w.commentBodies)
	w.mu.Unlock()
	require.Zero(t, posted,
		"empty default discuss comment must NOT be posted; got: %v", w.commentBodies)
}
