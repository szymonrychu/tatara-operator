package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// resumeWriter is a fake forge for the WS3-I4 resume tests: it records ClosePR
// and RemoveLabel (the only two forge writes the resume path makes).
type resumeWriter struct {
	scm.SCMWriter
	closed  []int
	removed []string
}

func (w *resumeWriter) ClosePR(_ context.Context, _, _ string, number int, _ string) error {
	w.closed = append(w.closed, number)
	return nil
}

func (w *resumeWriter) RemoveLabel(_ context.Context, _, issueRef, label string) error {
	w.removed = append(w.removed, issueRef+"|"+label)
	return nil
}

// botMR builds an OPEN bot PR mirror owned by taskName, on the task/<taskName>
// head branch, so ourMR matches it.
func botMR(name, taskName, repoRef string, number int) *tatarav1alpha1.MergeRequest {
	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.MergeRequestSpec{RepositoryRef: repoRef, Number: number, ProjectRef: "resume"},
	}
	mr.Status.State = "open"
	mr.Status.Author = "tatara-bot"
	mr.Status.HeadBranch = TaskBranchPrefix + taskName
	owner := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: taskName}}
	own.AddPlainOwner(mr, owner)
	if err := own.HandOverController(mr, nil, owner); err != nil {
		panic(err)
	}
	return mr
}

// TestResumeNoReentryPark_ClosesPRSeversAndReadoptsActive is the WS3-I4 core: a
// human reply to a parked(review-loop-exhausted) Task closes its bot PR, severs
// (orphans) the issue, and the re-mint lands ACTIVE (triaging) off humanHasLastWord.
func TestResumeNoReentryPark_ClosesPRSeversAndReadoptsActive(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("resume")
	repo := reapRepo("resume", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	oldName := tatarav1alpha1.IntakeTaskName("resume", SweepIssueKind, "tatara-operator", 1)
	issName := tatarav1alpha1.IssueName("tatara-operator", 1)
	mrName := tatarav1alpha1.MergeRequestName("tatara-operator", 42)

	old := reapTask("resume", oldName, "clarify", tatarav1alpha1.StageParked,
		stage.ReasonReviewLoopExhausted, time.Now().Add(-time.Hour))
	old.Status.IssueRefs = []string{issName}
	old.Status.MRRefs = []string{mrName}
	old.Status.PendingEvents = []tatarav1alpha1.TaskEvent{
		{At: metav1.Now(), Kind: "issue_comment", Author: "maintainer", Body: "please continue"},
	}
	iss := ownedIssue(issName, 1, old, tatarav1alpha1.IssueStatus{
		State:    "open",
		Comments: []tatarav1alpha1.Comment{{ExternalID: "c1", Author: "maintainer", Body: "please continue"}},
	})
	mr := botMR(mrName, oldName, "tatara-operator", 42)

	c := newMirrorClient(t, proj, repo, reapSecret(), old, iss, mr)
	w := &resumeWriter{}
	r := reapReconciler(c, w)

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	// The old task's bot PR is closed.
	require.Contains(t, w.closed, 42, "the old task's bot PR must be closed")
	// The old stale-terminal task is deleted on the re-mint name collision.
	_, ok := mustGetTask(t, c, oldName)
	require.False(t, ok, "the old stale-terminal task is deleted on the re-mint collision")
	// The issue is now an ownerless orphan (sever dropped the ref).
	got := mustGetIssue(t, c, issName)
	_, owned := own.ControllerOwner(got)
	require.False(t, owned, "the issue is orphaned by the sever")

	// Simulate the next sweep pass re-minting the orphan issue: it must land ACTIVE
	// (triaging) via humanHasLastWord, NOT parked, and carry no tatara-parked.
	task2, created, err := r.minter().MintForItem(ctx, proj, repo, forgeItemFromMirror(got), false, nil)
	require.NoError(t, err)
	require.True(t, created, "one reply -> one fresh clarify task")
	require.Equal(t, tatarav1alpha1.StageTriaging, task2.Spec.InitialStage, "humanHasLastWord -> active, not parked")
}

// TestResumeNoReentryPark_ReentryReasonUntouched: a parked(awaiting-human) Task
// has an F.6 re-entry rule (driveUnparks owns it), so the I4 driver must skip it
// entirely - no sever, no PR close.
func TestResumeNoReentryPark_ReentryReasonUntouched(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("resume")
	repo := reapRepo("resume", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	issName := tatarav1alpha1.IssueName("tatara-operator", 1)

	task := reapTask("resume", "aw-task", "clarify", tatarav1alpha1.StageParked,
		stage.ReasonAwaitingHuman, time.Now().Add(-time.Hour))
	task.Status.IssueRefs = []string{issName}
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{
		{At: metav1.Now(), Kind: "issue_comment", Author: "maintainer", Body: "go ahead"},
	}
	iss := ownedIssue(issName, 1, task, tatarav1alpha1.IssueStatus{State: "open"})
	c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
	w := &resumeWriter{} // no ClosePR expected
	r := reapReconciler(c, w)

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	require.Empty(t, w.closed, "an awaiting-human park has an F.6 re-entry: the I4 driver must not touch it")
	gotIss := mustGetIssue(t, c, issName)
	_, owned := own.ControllerOwner(gotIss)
	require.True(t, owned, "the issue must NOT be severed from a re-entryable park")
}

// --- WS3-I5: deploy-timeout comment ----------------------------------------

func deployTimeoutTask(name string, issName string) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", RepositoryRef: "tatara-operator"},
		Status: tatarav1alpha1.TaskStatus{
			Stage: tatarav1alpha1.StageParked, StageReason: stage.ReasonDeployTimeout,
			IssueRefs: []string{issName}, DeployReentries: 1,
		},
	}
}

func TestEnqueueDeployTimeoutComment_FirstOnly(t *testing.T) {
	ctx := context.Background()
	proj := mirrorProject("tatara-bot")
	issName := tatarav1alpha1.IssueName("tatara-operator", 1)
	task := deployTimeoutTask("t-deploy", issName)
	iss := ownedIssue(issName, 1, task, tatarav1alpha1.IssueStatus{State: "open"})
	mr := ownedMR(tatarav1alpha1.MergeRequestName("tatara-operator", 42), "t-deploy", "tatara-operator", 42)
	mr.Status.State = "merged" // merged but not deployed
	c := newMirrorClient(t, proj, task, iss, mr)
	r := &TaskReconciler{Client: c}
	mrs := []tatarav1alpha1.MergeRequest{*mr}
	now := time.Now()

	require.NoError(t, r.enqueueDeployTimeoutComment(ctx, proj, task, mrs, now))
	got := getIssueCR(t, c, issName)
	require.Len(t, got.Status.PendingComments, 1)
	require.NotNil(t, got.Status.LastDeployTimeoutCommentAt)
	require.Contains(t, got.Status.PendingComments[0].Body, "tatara-operator")

	// A second timeout retry must NOT enqueue a duplicate (its own cooldown marker).
	require.NoError(t, r.enqueueDeployTimeoutComment(ctx, proj, task, mrs, now.Add(time.Hour)))
	got2 := getIssueCR(t, c, issName)
	require.Len(t, got2.Status.PendingComments, 1, "own cooldown: no duplicate on the deploy-timeout retry")
}

// TestEnqueueDeployTimeoutComment_DoesNotClobberRefireCooldown proves the DISTINCT
// marker: an issue that is also an incident tracker keeps its LastRefireCommentAt.
func TestEnqueueDeployTimeoutComment_DoesNotClobberRefireCooldown(t *testing.T) {
	ctx := context.Background()
	proj := mirrorProject("tatara-bot")
	issName := tatarav1alpha1.IssueName("tatara-operator", 1)
	task := deployTimeoutTask("t-deploy2", issName)
	refireAt := metav1.NewTime(time.Now().Add(-time.Minute))
	iss := ownedIssue(issName, 1, task, tatarav1alpha1.IssueStatus{
		State: "open", RefireCount: 3, LastRefireCommentAt: &refireAt,
	})
	mr := ownedMR(tatarav1alpha1.MergeRequestName("tatara-operator", 42), "t-deploy2", "tatara-operator", 42)
	mr.Status.State = "merged"
	c := newMirrorClient(t, proj, task, iss, mr)
	r := &TaskReconciler{Client: c}

	require.NoError(t, r.enqueueDeployTimeoutComment(ctx, proj, task, []tatarav1alpha1.MergeRequest{*mr}, time.Now()))
	got := getIssueCR(t, c, issName)
	require.NotNil(t, got.Status.LastDeployTimeoutCommentAt, "the deploy-timeout marker is set")
	require.Equal(t, refireAt.Unix(), got.Status.LastRefireCommentAt.Unix(), "the incident-refire cooldown must be untouched")
}
