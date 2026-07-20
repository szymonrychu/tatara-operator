package webhook

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

const peRepoURL = "https://github.com/o/r.git"

func peMR(number int, task *tatarav1.Task, status tatarav1.MergeRequestStatus) *tatarav1.MergeRequest {
	mr := &tatarav1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1.MergeRequestName("pe-repo", number), Namespace: peNS},
		Spec:       tatarav1.MergeRequestSpec{RepositoryRef: "pe-repo", Number: number, ProjectRef: "pe-proj", URL: "u"},
		Status:     status,
	}
	if task != nil {
		own.AddPlainOwner(mr, task)
		if err := own.HandOverController(mr, nil, task); err != nil {
			panic(err)
		}
	}
	return mr
}

// --- WS3-I2: issue edited ---------------------------------------------------

func TestIssueEdited_BodyChange_MirrorsAndQueuesEvent(t *testing.T) {
	task := peTask("t-edit", tatarav1.StageParked, "awaiting-human", tatarav1.IssueName("pe-repo", 7))
	iss := peIssue(7, task)
	iss.Status.Body, iss.Status.Title = "old body", "old title"
	proj := peProject("tatara-bot", "maintainer")
	c := peClient(t, proj, peRepo(), task, iss)
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{
		Kind: "issue", Action: "edited", Number: 7, Repo: peRepoURL, IssueRef: "o/r#7",
		Body: "new scope body", Title: "new title", ActorLogin: "alice",
	}
	w := httptest.NewRecorder()
	s.handleIssueEdited(context.Background(), w, "github", *proj, ev)
	require.Equal(t, 202, w.Code)

	got := getPEIssue(t, c, iss.Name)
	require.Equal(t, "new scope body", got.Status.Body)
	require.Equal(t, "new title", got.Status.Title)
	gotTask := getPETask(t, c, task.Name)
	require.Len(t, gotTask.Status.PendingEvents, 1)
	require.Equal(t, "issue_edited", gotTask.Status.PendingEvents[0].Kind)
}

func TestIssueEdited_NoBodyChange_NoEvent(t *testing.T) {
	task := peTask("t-noedit", tatarav1.StageParked, "awaiting-human", tatarav1.IssueName("pe-repo", 7))
	iss := peIssue(7, task)
	iss.Status.Body, iss.Status.Title = "same", "same"
	proj := peProject("tatara-bot", "maintainer")
	c := peClient(t, proj, peRepo(), task, iss)
	s := peServer(c, &stubSpiller{}, nil)

	// A label-only update: identical body/title.
	ev := scm.WebhookEvent{Kind: "issue", Action: "labeled", Number: 7, Repo: peRepoURL, Body: "same", Title: "same", ActorLogin: "alice", ChangedLabel: "bug"}
	w := httptest.NewRecorder()
	s.handleIssueEdited(context.Background(), w, "github", *proj, ev)

	require.Empty(t, getPETask(t, c, task.Name).Status.PendingEvents, "a label-only edit must not queue an issue_edited event")
}

// TestIssueEdited_CombinedLabelEdit_ViaRouter proves the labeled path runs the
// body diff (a combined body+label edit surfaces as GitLab labeled).
func TestIssueEdited_CombinedLabelEdit_ViaRouter(t *testing.T) {
	task := peTask("t-combined", tatarav1.StageParked, "awaiting-human", tatarav1.IssueName("pe-repo", 7))
	iss := peIssue(7, task)
	iss.Status.Body = "old"
	proj := peProject("tatara-bot", "maintainer") // no TriggerLabel -> no mint side effect
	c := peClient(t, proj, peRepo(), task, iss)
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{Kind: "issue", Action: "labeled", Number: 7, Repo: peRepoURL, Body: "new body", Title: "", ActorLogin: "alice", ChangedLabel: "ops"}
	w := httptest.NewRecorder()
	s.handleForgeItem(context.Background(), w, "github", *proj, ev)

	require.Len(t, getPETask(t, c, task.Name).Status.PendingEvents, 1, "the labeled path must still fold a body change into an event")
}

// --- WS3 trigger-label mint -------------------------------------------------

func triggerProject() *tatarav1.Project {
	p := peProject("tatara-bot", "maintainer")
	p.Spec.TriggerLabel = "tatara"
	return p
}

func taskCount(t *testing.T, c client.Client) int {
	t.Helper()
	var tl tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tl, client.InNamespace(peNS)))
	return len(tl.Items)
}

func TestTriggerLabelMint_OrphanIssueMints(t *testing.T) {
	proj := triggerProject()
	c := peClient(t, proj, peRepo(), peSecret("pe-proj-scm", "tok")) // no Issue CR -> orphan
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{Kind: "issue", Action: "labeled", Number: 7, Repo: peRepoURL, ChangedLabel: "tatara", ActorLogin: "alice", Title: "t", Body: "b"}
	s.maybeTriggerLabelMint(context.Background(), "github", proj, ev)
	require.Equal(t, 1, taskCount(t, c), "the configured trigger label on an orphan issue mints a Task")
}

func TestTriggerLabelMint_NonTriggerLabel_NoMint(t *testing.T) {
	proj := triggerProject()
	c := peClient(t, proj, peRepo(), peSecret("pe-proj-scm", "tok"))
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{Kind: "issue", Action: "labeled", Number: 7, Repo: peRepoURL, ChangedLabel: "bug", ActorLogin: "alice"}
	s.maybeTriggerLabelMint(context.Background(), "github", proj, ev)
	require.Equal(t, 0, taskCount(t, c))
}

func TestTriggerLabelMint_ProjectionLabel_NoMint(t *testing.T) {
	proj := triggerProject()
	proj.Spec.TriggerLabel = "tatara-approved" // trigger set to the projection label
	c := peClient(t, proj, peRepo(), peSecret("pe-proj-scm", "tok"))
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{Kind: "issue", Action: "labeled", Number: 7, Repo: peRepoURL, ChangedLabel: "tatara-approved", ActorLogin: "alice"}
	s.maybeTriggerLabelMint(context.Background(), "github", proj, ev)
	require.Equal(t, 0, taskCount(t, c), "an approved/declined projection label must never self-trigger a mint")
}

func TestTriggerLabelMint_BotActor_NoMint(t *testing.T) {
	proj := triggerProject()
	c := peClient(t, proj, peRepo(), peSecret("pe-proj-scm", "tok"))
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{Kind: "issue", Action: "labeled", Number: 7, Repo: peRepoURL, ChangedLabel: "tatara", ActorLogin: "tatara-bot"}
	s.maybeTriggerLabelMint(context.Background(), "github", proj, ev)
	require.Equal(t, 0, taskCount(t, c))
}

func TestTriggerLabelMint_AlreadyOwned_NoMint(t *testing.T) {
	proj := triggerProject()
	task := peTask("t-own", tatarav1.StageClarifying, "", tatarav1.IssueName("pe-repo", 7))
	iss := peIssue(7, task) // owned -> not an orphan
	c := peClient(t, proj, peRepo(), peSecret("pe-proj-scm", "tok"), task, iss)
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{Kind: "issue", Action: "labeled", Number: 7, Repo: peRepoURL, ChangedLabel: "tatara", ActorLogin: "alice"}
	s.maybeTriggerLabelMint(context.Background(), "github", proj, ev)
	require.Equal(t, 1, taskCount(t, c), "an owned issue is not re-minted (only the pre-existing task remains)")
}

// --- WS3-M1: MR synchronize -------------------------------------------------

func TestMRSynchronize_MirrorsHeadNoTransition(t *testing.T) {
	task := peTask("t-rev", tatarav1.StageReviewing, "", tatarav1.IssueName("pe-repo", 7))
	task.Status.MRRefs = []string{tatarav1.MergeRequestName("pe-repo", 34)}
	mr := peMR(34, task, tatarav1.MergeRequestStatus{State: "open", HeadSHA: "oldhead"})
	proj := peProject("tatara-bot", "maintainer")
	c := peClient(t, proj, peRepo(), task, mr)
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{Kind: "mr", IsPR: true, Action: "synchronize", Number: 34, Repo: peRepoURL, HeadSHA: "newhead"}
	w := httptest.NewRecorder()
	s.handleMRSynchronize(context.Background(), w, "github", *proj, ev)

	var got tatarav1.MergeRequest
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: peNS, Name: mr.Name}, &got))
	require.Equal(t, "newhead", got.Status.HeadSHA)
	require.Equal(t, tatarav1.StageReviewing, getPETask(t, c, task.Name).Status.Stage, "no review restart on synchronize")
}

// TestMRSynchronize_BotPushStampsLastBotHeadSHA proves a verified bot-push
// webhook advances the bot-head cursor immediately: OP8's ReconcileOwnership
// must not read a push webhook that races ahead of the implement-outcome
// record as an unattributable external push.
func TestMRSynchronize_BotPushStampsLastBotHeadSHA(t *testing.T) {
	task := peTask("t-rev", tatarav1.StageReviewing, "", tatarav1.IssueName("pe-repo", 7))
	task.Status.MRRefs = []string{tatarav1.MergeRequestName("pe-repo", 34)}
	mr := peMR(34, task, tatarav1.MergeRequestStatus{State: "open", HeadSHA: "oldhead"})
	proj := peProject("tatara-bot", "maintainer")
	c := peClient(t, proj, peRepo(), task, mr)
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{Kind: "mr", IsPR: true, Action: "synchronize", Number: 34, Repo: peRepoURL,
		HeadSHA: "bot-head-abc", ActorLogin: "tatara-bot"}
	w := httptest.NewRecorder()
	s.handleMRSynchronize(context.Background(), w, "github", *proj, ev)

	var got tatarav1.MergeRequest
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: peNS, Name: mr.Name}, &got))
	require.Equal(t, "bot-head-abc", got.Status.HeadSHA)
	require.Equal(t, "bot-head-abc", got.Status.LastBotHeadSHA, "a verified bot push must stamp lastBotHeadSHA")
}

// TestMRSynchronize_HumanPushDoesNotStampLastBotHeadSHA proves a non-bot
// pusher advances only the HeadSHA mirror, leaving LastBotHeadSHA stale so
// ReconcileOwnership (OP8) sees the drift and flips.
func TestMRSynchronize_HumanPushDoesNotStampLastBotHeadSHA(t *testing.T) {
	task := peTask("t-rev2", tatarav1.StageReviewing, "", tatarav1.IssueName("pe-repo", 7))
	task.Status.MRRefs = []string{tatarav1.MergeRequestName("pe-repo", 35)}
	mr := peMR(35, task, tatarav1.MergeRequestStatus{State: "open", HeadSHA: "oldhead", LastBotHeadSHA: "old-bot-head"})
	proj := peProject("tatara-bot", "maintainer")
	c := peClient(t, proj, peRepo(), task, mr)
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{Kind: "mr", IsPR: true, Action: "synchronize", Number: 35, Repo: peRepoURL,
		HeadSHA: "human-head-xyz", ActorLogin: "octocat"}
	w := httptest.NewRecorder()
	s.handleMRSynchronize(context.Background(), w, "github", *proj, ev)

	var got tatarav1.MergeRequest
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: peNS, Name: mr.Name}, &got))
	require.Equal(t, "human-head-xyz", got.Status.HeadSHA, "the HeadSHA mirror still advances")
	require.Equal(t, "old-bot-head", got.Status.LastBotHeadSHA, "a human push must NOT move lastBotHeadSHA")
}

// --- PR closed/merged out-of-band ------------------------------------------

func TestMRClosed_Merged_MirrorsMergedState(t *testing.T) {
	mr := peMR(34, nil, tatarav1.MergeRequestStatus{State: "open"})
	proj := peProject("tatara-bot", "maintainer")
	c := peClient(t, proj, peRepo(), mr)
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{Kind: "mr", IsPR: true, Action: "merged", Merged: true, Number: 34, Repo: peRepoURL}
	w := httptest.NewRecorder()
	s.handleMRClosed(context.Background(), w, "github", *proj, ev)

	var got tatarav1.MergeRequest
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: peNS, Name: mr.Name}, &got))
	require.Equal(t, "merged", got.Status.State)
	require.NotNil(t, got.Status.MergedAt)
}

// --- WS3-M4: review on an owned MR whose Task was reaped --------------------

func TestReviewFoldsWhenOwningTaskReaped(t *testing.T) {
	ghost := peTask("ghost-task", tatarav1.StageReviewing, "") // deliberately NOT added to the client
	mr := peMR(34, ghost, tatarav1.MergeRequestStatus{State: "open"})
	proj := peProject("tatara-bot", "maintainer")
	c := peClient(t, proj, peRepo(), peSecret("pe-proj-scm", "tok"), mr) // ghost absent
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{
		Kind: "mr", IsPR: true, IsReview: true, Action: "submitted", ReviewState: "approved",
		ReviewID: "r-1", Number: 34, Repo: peRepoURL, ActorLogin: "maintainer", CommentBody: "lgtm",
	}
	w := httptest.NewRecorder()
	s.handleReview(context.Background(), w, "github", *proj, ev)

	var got tatarav1.MergeRequest
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: peNS, Name: mr.Name}, &got))
	require.NotEmpty(t, got.Status.Comments, "M4: a review whose owning task was reaped must fold to the pending-event path, not vanish")
}
