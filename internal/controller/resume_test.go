package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// resumeWriter is a fake forge for the no-re-entry resume tests: it records
// ClosePR and RemoveLabel (the only two forge writes the resume path makes).
type resumeWriter struct {
	scm.SCMWriter
	closed  []int
	bodies  []string
	removed []string
}

func (w *resumeWriter) ClosePR(_ context.Context, _, _ string, number int, body string) error {
	w.closed = append(w.closed, number)
	w.bodies = append(w.bodies, body)
	return nil
}

func (w *resumeWriter) RemoveLabel(_ context.Context, _, issueRef, label string) error {
	w.removed = append(w.removed, issueRef+"|"+label)
	return nil
}

// botMR builds an OPEN bot PR mirror owned by taskName, on the task/<taskName>
// head branch, so ourMR matches it.
func botMR(name, taskName, repoRef string, number int) *tatarav1alpha1.MergeRequest {
	owner := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: taskName}}
	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.MergeRequestSpec{RepositoryRef: repoRef, Number: number, ProjectRef: "resume"},
	}
	mr.Status.State = "open"
	mr.Status.Author = "tatara-bot"
	mr.Status.HeadBranch = agent.TaskBranch(owner)
	own.AddPlainOwner(mr, owner)
	if err := own.HandOverController(mr, nil, owner); err != nil {
		panic(err)
	}
	return mr
}

// noReentryFixture builds the whole H8 population in one shot: a Task parked
// under an UnparkNever reason, owning an OPEN Issue whose thread ends with a
// maintainer's reply, with that reply also sitting in the Task's pendingEvents.
func noReentryFixture(t *testing.T, reason, replyAuthor string) (
	*tatarav1alpha1.Project, *tatarav1alpha1.Repository, *tatarav1alpha1.Task,
	*tatarav1alpha1.Issue, string, string) {

	t.Helper()
	proj := reapProject("resume")
	repo := reapRepo("resume", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	oldName := tatarav1alpha1.IntakeTaskName("resume", SweepIssueKind, "tatara-operator", 1)
	issName := tatarav1alpha1.IssueName("tatara-operator", 1)

	old := reapTask("resume", oldName, SweepIssueKind, tatarav1alpha1.StateAwaitingReview,
		reason, time.Now().Add(-time.Hour))
	old.Status.IssueRefs = []string{issName}
	old.Status.PendingEvents = []tatarav1alpha1.TaskEvent{
		{At: metav1.Now(), Kind: "issue_comment", Author: replyAuthor, Body: "please continue"},
	}
	iss := ownedIssue(issName, 1, old, tatarav1alpha1.IssueStatus{
		State: "open", Author: "maintainer", Title: "an issue",
		Comments: []tatarav1alpha1.Comment{
			{ExternalID: "c1", Author: replyAuthor, Body: "please continue"},
		},
	})
	return proj, repo, old, iss, oldName, issName
}

// TestNoReentryPark_HumanReplyReMintsTheIssueInOnePass IS THE ONE-REPLY
// GUARANTEE (finding H8): one human reply to a Task parked under an UnparkNever
// reason produces one fresh ACTIVE Task on the Issue, in ONE pass. The
// maintainer must never have to comment a SECOND time.
//
// Before the resume driver existed this was RED for exactly the reason the
// finding names: driveUnparks declines (review-loop-exhausted is UnparkNever),
// the still-LIVE parked Task remains the Issue's controller owner, so
// IsOrphanIssue answers issue_owned and MintForItem answers not_owed. The reply
// bought nothing until ParkRetention (7 days) expired.
func TestNoReentryPark_HumanReplyReMintsTheIssueInOnePass(t *testing.T) {
	ctx := context.Background()
	proj, repo, old, iss, oldName, issName := noReentryFixture(t, stage.ReasonReviewLoopExhausted, "maintainer")
	mrName := tatarav1alpha1.MergeRequestName("tatara-operator", 42)
	old.Status.MRRefs = []string{mrName}
	mr := botMR(mrName, oldName, "tatara-operator", 42)

	c := newMirrorClient(t, proj, repo, reapSecret(), old, iss, mr)
	w := &resumeWriter{}
	r := reapReconciler(c, w)

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	// The old task's own bot PR is closed, before anything cascades its mirror.
	require.Contains(t, w.closed, 42, "the old task's bot PR must be closed")

	// The deterministic intake name now holds a FRESH task, not the parked one.
	fresh, ok := mustGetTask(t, c, oldName)
	require.True(t, ok, "one reply must leave one fresh task on the issue")
	require.Empty(t, fresh.Status.ParkReason,
		"humanHasLastWord: the fresh task must be ACTIVE, not parked behind a second reply")
	require.Empty(t, fresh.Spec.InitialParkReason, "the fresh task must not be minted parked")
	require.NotEqual(t, old.UID, fresh.UID, "it must be a NEW task, not the old one un-parked")

	// It controller-owns the issue again, so the sweep sees no orphan either.
	owner, owned := own.ControllerOwner(mustGetIssue(t, c, issName))
	require.True(t, owned, "the fresh task must controller-own the issue")
	require.Equal(t, oldName, owner)
}

// TestNoReentryPark_BotPendingEventDoesNotResume: the operator's OWN park
// comment must never resume the Task it parked. Only a non-bot pendingEvent is
// a reply.
func TestNoReentryPark_BotPendingEventDoesNotResume(t *testing.T) {
	ctx := context.Background()
	proj, repo, old, iss, oldName, issName := noReentryFixture(t, stage.ReasonReviewLoopExhausted, "tatara-bot")

	c := newMirrorClient(t, proj, repo, reapSecret(), old, iss)
	w := &resumeWriter{}
	r := reapReconciler(c, w)

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	still, ok := mustGetTask(t, c, oldName)
	require.True(t, ok, "a bot-authored event must not collect the parked task")
	require.Equal(t, stage.ReasonReviewLoopExhausted, still.Status.ParkReason, "it must still be parked")
	require.Equal(t, old.UID, still.UID, "no re-mint happened")
	require.Empty(t, w.closed, "no forge write on a bot-only event")
	owner, owned := own.ControllerOwner(mustGetIssue(t, c, issName))
	require.True(t, owned, "the issue must NOT be severed")
	require.Equal(t, oldName, owner)
}

// TestNoReentryPark_ReentryReasonUntouched: awaiting-human is stage.UnparkHuman,
// so driveUnparks owns it. This driver must not touch it - no sever, no PR
// close, no collection.
func TestNoReentryPark_ReentryReasonUntouched(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("resume")
	repo := reapRepo("resume", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	issName := tatarav1alpha1.IssueName("tatara-operator", 1)

	task := reapTask("resume", "aw-task", SweepIssueKind, tatarav1alpha1.StateAwaitingReview,
		stage.ReasonAwaitingHuman, time.Now().Add(-time.Hour))
	task.Status.IssueRefs = []string{issName}
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{
		{At: metav1.Now(), Kind: "issue_comment", Author: "maintainer", Body: "go ahead"},
	}
	iss := ownedIssue(issName, 1, task, tatarav1alpha1.IssueStatus{State: "open", Author: "maintainer"})
	c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
	w := &resumeWriter{} // no ClosePR expected
	r := reapReconciler(c, w)

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	require.Empty(t, w.closed, "an awaiting-human park has a re-entry rule: this driver must not touch it")
	_, ok := mustGetTask(t, c, "aw-task")
	require.True(t, ok, "driveUnparks owns this task; it must not be collected here")
	_, owned := own.ControllerOwner(mustGetIssue(t, c, issName))
	require.True(t, owned, "the issue must NOT be severed from a re-entryable park")
}

// TestNoReentryPark_SecondPassDoesNotDoubleMint: the fresh Task minted by pass
// one is ACTIVE, so it is not a candidate, so pass two is a no-op. Nothing is
// minted twice and nothing is collected twice.
func TestNoReentryPark_SecondPassDoesNotDoubleMint(t *testing.T) {
	ctx := context.Background()
	proj, repo, old, iss, oldName, issName := noReentryFixture(t, stage.ReasonStageDeadline, "maintainer")

	c := newMirrorClient(t, proj, repo, reapSecret(), old, iss)
	r := reapReconciler(c, &resumeWriter{})

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))
	first, ok := mustGetTask(t, c, oldName)
	require.True(t, ok)

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	var tl tatarav1alpha1.TaskList
	require.NoError(t, c.List(ctx, &tl, client.InNamespace(testNS)))
	require.Len(t, tl.Items, 1, "exactly one task on the issue after two passes")
	second, ok := mustGetTask(t, c, oldName)
	require.True(t, ok, "the fresh task must survive the second pass")
	require.Equal(t, first.UID, second.UID, "the second pass must not re-mint")
	owner, owned := own.ControllerOwner(mustGetIssue(t, c, issName))
	require.True(t, owned)
	require.Equal(t, oldName, owner)
}

// TestNoReentryPark_CacheLagStillMintsActive: the CACHED mirror is missing the
// human reply, but the uncached APIReader has it. resumeOne's live read must
// make the reply visible at mint time so the fresh Task still lands ACTIVE -
// the one-reply guarantee holds under Issue-CR cache lag.
func TestNoReentryPark_CacheLagStillMintsActive(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("resume")
	repo := reapRepo("resume", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	issName := tatarav1alpha1.IssueName("tatara-operator", 1)
	oldName := tatarav1alpha1.IntakeTaskName("resume", SweepIssueKind, "tatara-operator", 1)

	mkTask := func() *tatarav1alpha1.Task {
		task := reapTask("resume", oldName, SweepIssueKind, tatarav1alpha1.StateAwaitingReview,
			stage.ReasonStageDeadline, time.Now().Add(-time.Hour))
		task.Status.IssueRefs = []string{issName}
		task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{
			{At: metav1.Now(), Kind: "issue_comment", Author: "maintainer", Body: "please continue"},
		}
		return task
	}

	// Cached store: the reply has NOT landed in the mirror yet.
	cachedIss := ownedIssue(issName, 1, mkTask(), tatarav1alpha1.IssueStatus{State: "open", Author: "maintainer"})
	cached := newMirrorClient(t, proj, repo, reapSecret(), mkTask(), cachedIss)

	// Live store: the SAME issue, WITH the human reply as the last comment. It
	// deliberately holds ONLY the Issue: two fake clients are two independent
	// stores, and stocking the Task in both would make the uncached Task
	// pre-check inside createTaskRaceSafe read a store this pass's delete cannot
	// reach - a fixture artifact, not a behaviour. The lag under test is the
	// Issue mirror's comment thread.
	liveIss := ownedIssue(issName, 1, mkTask(), tatarav1alpha1.IssueStatus{
		State: "open", Author: "maintainer",
		Comments: []tatarav1alpha1.Comment{{ExternalID: "c1", Author: "maintainer", Body: "please continue"}},
	})
	live := newMirrorClient(t, proj, repo, reapSecret(), liveIss)

	r := reapReconciler(cached, &resumeWriter{})
	r.APIReader = live

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	fresh, ok := mustGetTask(t, cached, oldName)
	require.True(t, ok, "the fresh task is minted despite the cached mirror lagging the reply")
	require.Empty(t, fresh.Spec.InitialParkReason,
		"the live read makes the reply visible -> humanHasLastWord -> ACTIVE, not a second-reply park")
}

// TestNoReentryPark_CrashBetweenSeverAndCollectFinishes: the resume is
// interrupted after the sever (the Task carries AnnResumeReleasing and owns no
// open Issue any more). The candidacy test alone would say "nothing to do" and
// strand a live parked Task on the deterministic intake name forever, with the
// orphan Issue re-bound straight back to it by repairIssueBinding. The marker
// is what makes the next pass finish the collection.
func TestNoReentryPark_CrashBetweenSeverAndCollectFinishes(t *testing.T) {
	ctx := context.Background()
	proj, repo, old, _, oldName, _ := noReentryFixture(t, stage.ReasonStageDeadline, "maintainer")
	old.Status.IssueRefs = nil // severed already
	old.Annotations = map[string]string{AnnResumeReleasing: "true"}

	c := newMirrorClient(t, proj, repo, reapSecret(), old)
	r := reapReconciler(c, &resumeWriter{})

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	_, ok := mustGetTask(t, c, oldName)
	require.False(t, ok, "an interrupted resume must be finished, not left holding the intake name")
}

// TestNoReentryPark_ClosedIssueIsSeveredAndCollected is C.4: a CLOSED owned
// Issue is no longer a dead end.
//
// The driver used to bail outright when no owned Issue was open, and that bail
// was a silent vanish. The Task sat parked under a reason nothing un-parks
// until ParkRetention deleted it seven days later, cascade-deleting the mirror
// with it - and IsOrphanIssue needs state == "open", so nothing could ever look
// at that issue again. Now the closed Issue is SEVERED (its mirror survives as
// a zero-owner CR) and the Task is collected early. It is only the MINT the
// closed issue does not get: forgeItemFromMirror describes an OPEN item.
func TestNoReentryPark_ClosedIssueIsSeveredAndCollected(t *testing.T) {
	ctx := context.Background()
	proj, repo, old, iss, oldName, issName := noReentryFixture(t, stage.ReasonStageDeadline, "maintainer")
	iss.Status.State = "closed"

	c := newMirrorClient(t, proj, repo, reapSecret(), old, iss)
	r := reapReconciler(c, &resumeWriter{})

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	_, ok := mustGetTask(t, c, oldName)
	require.False(t, ok, "a closed owned issue collects the stranded task early, it does not strand it for a week")

	var surviving tatarav1alpha1.Issue
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: testNS, Name: issName}, &surviving),
		"the mirror of a closed issue SURVIVES the collection; it must not cascade away")
	_, owned := own.ControllerOwner(&surviving)
	require.False(t, owned, "the severed mirror carries no controller owner")
}

// TestNoReentryPark_MintNotOwedDoesNotLogAsResumed is finding B5 (adversarial
// review): resumeOne used to stringify MintForItem's typed outcome into a
// field on ONE unconditional "resumed" INFO line, logged no matter what the
// mint actually did - the exact #521 shape the enum exists to kill, only this
// time on the resume path itself. Here the reporter allowlist decides nothing
// is owed AFTER the old Task is already deleted, its bot PRs closed, and the
// Issue severed: nothing is minted and the Issue is left orphaned. The log
// must say so, not claim a resume happened.
func TestNoReentryPark_MintNotOwedDoesNotLogAsResumed(t *testing.T) {
	ctx, lines := recordingCtx()
	proj, repo, old, iss, oldName, issName := noReentryFixture(t, stage.ReasonReviewLoopExhausted, "maintainer")
	// "outsider" (the issue's author) is not on this allowlist, so MintForItem's
	// classification answers MintNotOwed even though the issue was just severed
	// and has no owner.
	proj.Spec.Scm.ReporterLogins = []string{"maintainer"}
	iss.Status.Author = "outsider"

	c := newMirrorClient(t, proj, repo, reapSecret(), old, iss)
	w := &resumeWriter{}
	r := reapReconciler(c, w)

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	_, ok := mustGetTask(t, c, oldName)
	require.False(t, ok, "the old task is collected regardless of what the mint decides")

	_, owned := own.ControllerOwner(mustGetIssue(t, c, issName))
	require.False(t, owned, "MintNotOwed must leave the issue orphaned, not silently re-owned")

	require.False(t, containsLine(*lines, "resumed a no-re-entry park from a human reply"),
		"MintNotOwed must never be logged as a successful resume: nothing was minted")
}

// TestNoReentryPark_MintExistingLiveDoesNotClaimResume covers the second
// non-created MintOutcome member (finding B5): the ISSUE's own deterministic
// mint name is already held by a LIVE twin (distinct from the parked owner),
// so MintForItem's natural-key collision repairs that twin's binding instead
// of creating anything. That is not a resume - the twin was already there.
func TestNoReentryPark_MintExistingLiveDoesNotClaimResume(t *testing.T) {
	ctx, lines := recordingCtx()
	proj := reapProject("resume")
	repo := reapRepo("resume", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	const number = 9
	issName := tatarav1alpha1.IssueName("tatara-operator", number)
	twinName := tatarav1alpha1.IntakeTaskName("resume", SweepIssueKind, "tatara-operator", number)

	old := reapTask("resume", "old-existing-live", SweepIssueKind, tatarav1alpha1.StateAwaitingReview,
		stage.ReasonReviewLoopExhausted, time.Now().Add(-time.Hour))
	old.Status.IssueRefs = []string{issName}
	old.Status.PendingEvents = []tatarav1alpha1.TaskEvent{
		{At: metav1.Now(), Kind: "issue_comment", Author: "maintainer", Body: "please continue"},
	}
	iss := ownedIssue(issName, number, old, tatarav1alpha1.IssueStatus{
		State: "open", Author: "maintainer",
		Comments: []tatarav1alpha1.Comment{{ExternalID: "c1", Author: "maintainer", Body: "please continue"}},
	})
	// A LIVE twin already occupies the issue's OWN deterministic mint name -
	// distinct from the parked owner's own name - so the collision inside
	// createTaskRaceSafe answers MintExistingLive, not MintCreated.
	twin := reapTask("resume", twinName, SweepIssueKind, tatarav1alpha1.StateAwaitingReview, "", time.Now())

	c := newMirrorClient(t, proj, repo, reapSecret(), old, iss, twin)
	w := &resumeWriter{}
	r := reapReconciler(c, w)

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	_, ok := mustGetTask(t, c, "old-existing-live")
	require.False(t, ok, "the old parked task is collected regardless of the mint outcome")

	stillTwin, ok := mustGetTask(t, c, twinName)
	require.True(t, ok, "the live twin must survive untouched, not be replaced")
	require.Equal(t, twin.UID, stillTwin.UID)

	owner, owned := own.ControllerOwner(mustGetIssue(t, c, issName))
	require.True(t, owned, "MintExistingLive repairs the binding onto the live twin")
	require.Equal(t, twinName, owner)

	require.False(t, containsLine(*lines, "resumed a no-re-entry park from a human reply"),
		"MintExistingLive is not a fresh resume: no new Task was created")
}

// TestNoReentryPark_MintTombstoneDeletedDoesNotClaimResume covers the third
// non-created MintOutcome member (finding B5): the issue's own deterministic
// mint name is held by a DEAD twin, which MintForItem deletes on collision.
// The mint is still OWED and nothing replaces it in this pass - the exact
// case #521's fix names by name, and the exact one the old unconditional log
// line misreported as a completed resume.
func TestNoReentryPark_MintTombstoneDeletedDoesNotClaimResume(t *testing.T) {
	ctx, lines := recordingCtx()
	proj := reapProject("resume")
	repo := reapRepo("resume", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	const number = 11
	issName := tatarav1alpha1.IssueName("tatara-operator", number)
	deadName := tatarav1alpha1.IntakeTaskName("resume", SweepIssueKind, "tatara-operator", number)

	old := reapTask("resume", "old-tombstone", SweepIssueKind, tatarav1alpha1.StateAwaitingReview,
		stage.ReasonReviewLoopExhausted, time.Now().Add(-time.Hour))
	old.Status.IssueRefs = []string{issName}
	old.Status.PendingEvents = []tatarav1alpha1.TaskEvent{
		{At: metav1.Now(), Kind: "issue_comment", Author: "maintainer", Body: "please continue"},
	}
	iss := ownedIssue(issName, number, old, tatarav1alpha1.IssueStatus{
		State: "open", Author: "maintainer",
		Comments: []tatarav1alpha1.Comment{{ExternalID: "c1", Author: "maintainer", Body: "please continue"}},
	})
	dead := reapTask("resume", deadName, SweepIssueKind, tatarav1alpha1.StateDone, "", time.Now())

	c := newMirrorClient(t, proj, repo, reapSecret(), old, iss, dead)
	w := &resumeWriter{}
	r := reapReconciler(c, w)

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	_, ok := mustGetTask(t, c, deadName)
	require.False(t, ok, "the tombstone must be deleted, freeing the name")

	_, owned := own.ControllerOwner(mustGetIssue(t, c, issName))
	require.False(t, owned, "MintTombstoneDeleted must leave the issue orphaned, not silently re-owned")

	require.False(t, containsLine(*lines, "resumed a no-re-entry park from a human reply"),
		"MintTombstoneDeleted must never be logged as a successful resume: nothing replaced the deleted twin")
}

// TestNoReentryPark_MintCreatedLogsAsResumed is the positive control: MintCreated
// is the ONLY outcome that may be logged as a resume, and it must still be.
func TestNoReentryPark_MintCreatedLogsAsResumed(t *testing.T) {
	ctx, lines := recordingCtx()
	proj, repo, old, iss, oldName, _ := noReentryFixture(t, stage.ReasonReviewLoopExhausted, "maintainer")

	c := newMirrorClient(t, proj, repo, reapSecret(), old, iss)
	r := reapReconciler(c, &resumeWriter{})

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	_, ok := mustGetTask(t, c, oldName)
	require.True(t, ok, "a fresh task must exist on the deterministic name")

	require.True(t, containsLine(*lines, "resumed a no-re-entry park from a human reply"),
		"MintCreated is the ONE outcome allowed to log as a resume")
}

// --- the merge-stage guard --------------------------------------------------

// closeMRFixture is a parked Task owning ONE open bot PR, for the guard tests
// that call closeTaskBotMRs directly: this is the function that issues the
// irreversible forge write, so it is the layer the guard is asserted at.
func closeMRFixture(reason string) (*tatarav1alpha1.Project, *tatarav1alpha1.Repository,
	*tatarav1alpha1.Task, *tatarav1alpha1.MergeRequest) {

	proj := reapProject("resume")
	repo := reapRepo("resume", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	task := reapTask("resume", "close-task", SweepIssueKind, tatarav1alpha1.StateMerged,
		reason, time.Now().Add(-time.Hour))
	mrName := tatarav1alpha1.MergeRequestName("tatara-operator", 16)
	task.Status.MRRefs = []string{mrName}
	return proj, repo, task, botMR(mrName, "close-task", "tatara-operator", 16)
}

// TestCloseTaskBotMRs_RefusesAMergeStageParkedTask is the DEFENCE IN DEPTH
// behind the strand driver's routing: whatever future caller reaches this
// function with a re-mint trigger, a merge-stage park's merge request is
// finished, reviewed work whose blocker is outside the code, and closing it
// destroys the work while leaving the blocker exactly where it was.
//
// It covers the HUMAN-REPLY trigger too, and must: the dead-end notice invites
// the maintainer to comment, and resumeNoReentryParks reaches this function on
// that comment. Refusing only the automatic trigger would leave our own notice
// baiting the human into the same destruction.
func TestCloseTaskBotMRs_RefusesAMergeStageParkedTask(t *testing.T) {
	for _, reason := range []string{stage.ReasonMergeBlocked, stage.ReasonMergeAuthRefused} {
		for _, trigger := range []string{resumeTriggerAutoReentry, resumeTriggerHumanReply} {
			t.Run(reason+"/"+trigger, func(t *testing.T) {
				ctx := context.Background()
				proj, repo, task, mr := closeMRFixture(reason)
				c := newMirrorClient(t, proj, repo, reapSecret(), task, mr)
				w := &resumeWriter{}
				r := reapReconciler(c, w)

				require.NoError(t, r.closeTaskBotMRs(ctx, proj, task, trigger))
				require.Empty(t, w.closed, "a merge-stage park's reviewed merge request is never closed to re-implement it")
			})
		}
	}
}

// TestCloseTaskBotMRs_ClosesAnImplementationParksMR is the control: the guard is
// keyed on the park reason, and an implementation-phase park's half-finished PR
// is still closed before the fresh Task opens its own.
func TestCloseTaskBotMRs_ClosesAnImplementationParksMR(t *testing.T) {
	ctx := context.Background()
	proj, repo, task, mr := closeMRFixture(stage.ReasonStageDeadline)
	c := newMirrorClient(t, proj, repo, reapSecret(), task, mr)
	w := &resumeWriter{}
	r := reapReconciler(c, w)

	require.NoError(t, r.closeTaskBotMRs(ctx, proj, task, resumeTriggerAutoReentry))
	require.Contains(t, w.closed, 16)
	require.Contains(t, w.bodies[0], "restarting this issue", "the re-mint wording is unchanged")
}

// TestCloseTaskBotMRs_AutoCollectStillClosesWithItsOwnWording: the collect path
// is EXEMPT from the guard and stays exempt. It re-mints nothing and every owned
// issue is already closed, so the PR is being cleaned up rather than superseded -
// and leaving it open there would strand an open forge PR whose mirror cascades
// away with the Task the collect is about to delete.
func TestCloseTaskBotMRs_AutoCollectStillClosesWithItsOwnWording(t *testing.T) {
	ctx := context.Background()
	proj, repo, task, mr := closeMRFixture(stage.ReasonMergeBlocked)
	c := newMirrorClient(t, proj, repo, reapSecret(), task, mr)
	w := &resumeWriter{}
	r := reapReconciler(c, w)

	require.NoError(t, r.closeTaskBotMRs(ctx, proj, task, resumeTriggerAutoCollect))
	require.Contains(t, w.closed, 16, "the collect path is unchanged")
	require.Contains(t, w.bodies[0], "the issue this PR was opened for is closed",
		"and keeps its own distinct wording")
}

// --- pacing -----------------------------------------------------------------
//
// wfParkedTask with reason stage.ReasonStageDeadline (UnparkNever) and no
// PendingEvents makes resumeNoReentryParks take the fast "nothing to resume"
// path after its one Task List, exercising the pacing wrapper without the full
// sever+collect+re-mint plumbing (covered above).

// Stream #367 (resumeNoReentryParks half): resumeNoReentryParks does a full
// namespace Task List EVERY Reconcile pass. resumeNoReentryParksPaced puts a
// floor under it, mirroring driveUnparksPaced (unpark.go): calling it twice
// inside the floor runs the underlying block once and returns the residual
// wait; a third call past the floor runs it again.
func TestResumeNoReentryParksPaced_SkipsWithinFloor_FoldsResidualIntoRequeue(t *testing.T) {
	task := wfParkedTask("t-resume", SweepIssueKind, stage.ReasonStageDeadline)
	base := newMirrorClient(t, task)
	cc := &listCountingClient{Client: base}
	r := &ProjectReconciler{Client: cc, Scheme: base.Scheme()}
	proj := wfProject()
	ctx := context.Background()

	t0 := time.Now()
	requeue1, err := r.resumeNoReentryParksPaced(ctx, proj, t0)
	require.NoError(t, err)
	require.Equal(t, defaultResumeNoReentryInterval, requeue1, "first pass returns the full floor")
	require.Equal(t, 1, cc.ListCount(), "first pass must run resumeNoReentryParks (one Task List)")

	// A second call well inside the floor: no second sweep, residual wait returned.
	requeue2, err := r.resumeNoReentryParksPaced(ctx, proj, t0.Add(5*time.Second))
	require.NoError(t, err)
	require.Equal(t, defaultResumeNoReentryInterval-5*time.Second, requeue2, "paced pass returns the residual")
	require.Equal(t, 1, cc.ListCount(), "a paced-out pass must not re-list")

	// A third call once the floor has fully elapsed must run again.
	requeue3, err := r.resumeNoReentryParksPaced(ctx, proj, t0.Add(defaultResumeNoReentryInterval+time.Second))
	require.NoError(t, err)
	require.Equal(t, defaultResumeNoReentryInterval, requeue3, "post-floor pass returns the full floor again")
	require.Equal(t, 2, cc.ListCount(), "post-floor pass must re-list")
}

// Two live Projects must not throttle each other: resumeNoReentryParksPaced is
// keyed per-project (like lastDriveUnparks), not a single cluster-wide clock.
func TestResumeNoReentryParksPaced_PerProjectFloor_DoesNotCrossThrottle(t *testing.T) {
	taskA := wfParkedTask("t-resume-a", SweepIssueKind, stage.ReasonStageDeadline)
	taskB := wfParkedTask("t-resume-b", SweepIssueKind, stage.ReasonStageDeadline)
	taskB.Spec.ProjectRef = "proj-b"
	base := newMirrorClient(t, taskA, taskB)
	cc := &listCountingClient{Client: base}
	r := &ProjectReconciler{Client: cc, Scheme: base.Scheme()}
	projA := wfProject()
	projB := wfProject()
	projB.Name = "proj-b"
	ctx := context.Background()
	t0 := time.Now()

	_, err := r.resumeNoReentryParksPaced(ctx, projA, t0)
	require.NoError(t, err)
	requeueB, err := r.resumeNoReentryParksPaced(ctx, projB, t0.Add(time.Second))
	require.NoError(t, err)
	require.Equal(t, defaultResumeNoReentryInterval, requeueB,
		"project B's first pass must not inherit project A's clock")
	require.Equal(t, 2, cc.ListCount(), "both projects' first passes must each List Task once")
}

// A rapid trigger loop must not turn into an unbounded number of full-namespace
// Lists: 150 calls spanning 149 synthetic seconds cross the 60s floor exactly
// twice, so exactly 3 runs (3 List calls), not 150.
func TestResumeNoReentryParksPaced_ListCallsBoundedUnderRapidTriggerLoop(t *testing.T) {
	task := wfParkedTask("t-resume", SweepIssueKind, stage.ReasonStageDeadline)
	base := newMirrorClient(t, task)
	cc := &listCountingClient{Client: base}
	r := &ProjectReconciler{Client: cc, Scheme: base.Scheme()}
	proj := wfProject()
	ctx := context.Background()
	t0 := time.Now()

	for i := 0; i < 150; i++ {
		_, err := r.resumeNoReentryParksPaced(ctx, proj, t0.Add(time.Duration(i)*time.Second))
		require.NoError(t, err)
	}
	require.Equal(t, 3, cc.ListCount(), "rapid trigger loop must bound List calls to 3 runs")
}

// TestNoReentryPark_ConsumedPendingEventDoesNotResume: ONE COMMENT RELEASES ONE
// PARK. A reply already spent releasing an awaiting-human park
// (stage.consumeUnparkEvents stamps TaskEvent.UnparkConsumedAt) is not a second
// reply, so a Task that later parks under an UnparkNever reason without that
// event being drained must NOT be severed and re-minted on the strength of it.
// hasNonBotPendingEvent is the third copy of stage.hasNonBotEvent's predicate
// and was the one left without the conjunct.
func TestNoReentryPark_ConsumedPendingEventDoesNotResume(t *testing.T) {
	ctx := context.Background()
	proj, repo, old, iss, oldName, issName := noReentryFixture(t, stage.ReasonReviewLoopExhausted, "maintainer")
	spent := metav1.Now()
	old.Status.PendingEvents[0].UnparkConsumedAt = &spent

	c := newMirrorClient(t, proj, repo, reapSecret(), old, iss)
	w := &resumeWriter{}
	r := reapReconciler(c, w)

	require.NoError(t, r.resumeNoReentryParks(ctx, proj, time.Now()))

	still, ok := mustGetTask(t, c, oldName)
	require.True(t, ok, "a spent event must not collect the parked task")
	require.Equal(t, stage.ReasonReviewLoopExhausted, still.Status.ParkReason, "it must still be parked")
	require.Equal(t, old.UID, still.UID, "no re-mint happened")
	require.Empty(t, w.closed, "no forge write on an already-spent event")
	owner, owned := own.ControllerOwner(mustGetIssue(t, c, issName))
	require.True(t, owned, "the issue must NOT be severed")
	require.Equal(t, oldName, owner)
}
