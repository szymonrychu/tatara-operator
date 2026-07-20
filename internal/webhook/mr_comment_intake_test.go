package webhook_test

// OP6: EnsureTaskForMRComment - the MR arm of the general comment->task intake
// pipeline (user invariant: every human comment yields a Task update or
// creation). TestOrphanPRComment_MintsReviewTask (primary_mint_test.go) covers
// the "no mirror at all" stub-mint path; the tests here cover the mirror-based
// paths: an unowned open mirror mints and delivers, a closed/merged mirror
// mints nothing, a bot-authored comment mints nothing, and an already-owned
// mirror mints nothing but still runs the ordinary pending-event delivery -
// mirroring primary_mint_test.go's TestOrphanComment_UnownedMirror_MintsTask /
// TestOwnedMirrorComment_NoMint_PendingPathRuns for the ISSUE arm.

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
)

// unownedMR builds a MergeRequest mirror CR with no controller owner, matching
// primary_mint_test.go's `unowned` Issue fixture pattern.
func unownedMR(repoName, projName string, number int, state string) *tatarav1.MergeRequest {
	n := strconv.Itoa(number)
	return &tatarav1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1.MergeRequestName(repoName, number), Namespace: ns},
		Spec: tatarav1.MergeRequestSpec{
			RepositoryRef: repoName, ProjectRef: projName, Number: number,
			URL: "https://github.com/o/r/pull/" + n,
		},
		Status: tatarav1.MergeRequestStatus{Author: "octocat", State: state, HeadBranch: "renovate/foo", HeadSHA: "sha-" + n},
	}
}

// A comment lands on an MR whose mirror CR exists but carries no controller
// owner and is OPEN: EnsureTaskForMRComment mints a review Task, the mint
// controller-owns the mirror, and the ordinary pending-event path delivers the
// mr_comment onto it in the SAME request.
func TestOrphanMRComment_UnownedOpenMirror_MintsAndDelivers(t *testing.T) {
	const secretVal = "whsec-mroc1"
	const repoName = "repo-mroc1"
	const projName = "mrocp1"
	mr := unownedMR(repoName, projName, 50, "open")
	c := seedClient(t,
		projectWithReporters(projName, "mrocp1-scm", "tatara", "tatara-bot", nil),
		secret("mrocp1-scm", secretVal),
		repository(repoName, projName, "https://github.com/o/r.git", "main"),
		mr,
	)
	h, _ := newServer(t, c)

	postIssueComment(t, h, projName, secretVal, prCommentBodyOn(50, "alice"))

	tasks := allTasks(t, c, projName)
	require.Len(t, tasks, 1, "an orphan open MR comment must mint a review task (no more F5-1 drop)")
	require.Equal(t, "review", tasks[0].Spec.Kind)
	require.Len(t, tasks[0].Status.PendingEvents, 1, "the mr_comment must be delivered to the minted task")

	var got tatarav1.MergeRequest
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: mr.Name}, &got))
	owner, ok := own.ControllerOwner(&got)
	require.True(t, ok)
	require.Equal(t, tasks[0].Name, owner)
}

// A comment lands on an MR whose mirror is unowned but CLOSED/MERGED: the
// closed/merged MR gate refuses to mint, and no Task exists to deliver to.
func TestMRComment_ClosedMirror_NoMint(t *testing.T) {
	const secretVal = "whsec-mroc2"
	const repoName = "repo-mroc2"
	const projName = "mrocp2"
	mr := unownedMR(repoName, projName, 51, "merged")
	c := seedClient(t,
		projectWithReporters(projName, "mrocp2-scm", "tatara", "tatara-bot", nil),
		secret("mrocp2-scm", secretVal),
		repository(repoName, projName, "https://github.com/o/r.git", "main"),
		mr,
	)
	h, _ := newServer(t, c)

	postIssueComment(t, h, projName, secretVal, prCommentBodyOn(51, "alice"))

	require.Empty(t, allTasks(t, c, projName), "a comment on a closed/merged MR must not mint a task")
}

// A bot-authored comment on an unowned open MR mints nothing - the top-of-
// handler isBotActor self-loop guard returns before EnsureTaskForMRComment is
// ever reached, same gate every other webhook mint path applies.
func TestMRComment_BotAuthor_NoMint(t *testing.T) {
	const secretVal = "whsec-mroc3"
	const repoName = "repo-mroc3"
	const projName = "mrocp3"
	mr := unownedMR(repoName, projName, 52, "open")
	c := seedClient(t,
		projectWithReporters(projName, "mrocp3-scm", "tatara", "tatara-bot", nil),
		secret("mrocp3-scm", secretVal),
		repository(repoName, projName, "https://github.com/o/r.git", "main"),
		mr,
	)
	h, _ := newServer(t, c)

	postIssueComment(t, h, projName, secretVal, prCommentBodyOn(52, "tatara-bot"))

	require.Empty(t, allTasks(t, c, projName), "a bot-authored comment must never mint a task")
}

// A comment lands on an MR mirror that IS owned by an existing Task:
// EnsureTaskForMRComment returns the existing owner unchanged (no second
// mint), and the ordinary pending-event path still queues the comment onto it.
func TestOwnedMRComment_NoMint_PendingPathRuns(t *testing.T) {
	const secretVal = "whsec-mroc4"
	const repoName = "repo-mroc4"
	const projName = "mrocp4"
	task := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "existing-review-task-mroc4", Namespace: ns},
		Spec:       tatarav1.TaskSpec{Kind: "review", ProjectRef: projName, Goal: "g"},
	}
	owned := unownedMR(repoName, projName, 53, "open")
	own.AddPlainOwner(owned, task)
	require.NoError(t, own.HandOverController(owned, nil, task))
	c := seedClient(t,
		projectWithReporters(projName, "mrocp4-scm", "tatara", "tatara-bot", nil),
		secret("mrocp4-scm", secretVal),
		repository(repoName, projName, "https://github.com/o/r.git", "main"),
		task, owned,
	)
	h, _ := newServer(t, c)

	postIssueComment(t, h, projName, secretVal, prCommentBodyOn(53, "alice"))

	require.Len(t, allTasks(t, c, projName), 1, "an owned mirror must not mint a second task")

	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: task.Name}, &got))
	require.Len(t, got.Status.PendingEvents, 1, "the pending path must still queue the comment on the owning task")
}
