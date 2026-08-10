// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// strandWriter records the two forge writes the C.2/C.3 paths make. Unlike
// reapWriter it never panics on an unexpected call: these tests assert on WHAT
// was written, and a hard panic hides the diff behind a stack trace.
type strandWriter struct {
	reapWriter
}

func newStrandWriter() *strandWriter {
	w := &strandWriter{}
	w.comment = func(string, string) error { return nil }
	w.addLabel = func(string, string) error { return nil }
	w.closePR = func(string, int, string) error { return nil }
	w.deleteBrnch = func(string, string) error { return nil }
	return w
}

func (w *strandWriter) labelled(issueRef, label string) bool {
	for _, got := range w.labels {
		if got == issueRef+"|"+label {
			return true
		}
	}
	return false
}

func (w *strandWriter) commentedWith(needle string) bool {
	for _, got := range w.comments {
		if strings.Contains(got, needle) {
			return true
		}
	}
	return false
}

// scmForOf / r2Metrics are the two seams a TerminalReleaser needs, bound to a
// test's own fake forge and a throwaway registry.
func scmForOf(w scm.SCMWriter) func(string) (scm.SCMWriter, error) {
	return func(string) (scm.SCMWriter, error) { return w, nil }
}

func r2Metrics() *obs.OperatorMetrics { return obs.NewOperatorMetrics(prometheus.NewRegistry()) }

// strandedFixture is a Task parked under an UnparkNever reason with NO human
// reply waiting, owning one OPEN Issue - the exact population C.3 exists for,
// and the exact population resumeNoReentryParks structurally cannot serve.
func strandedFixture(t *testing.T, parkedAgo time.Duration) (
	*tatarav1alpha1.Project, *tatarav1alpha1.Repository, *tatarav1alpha1.Task,
	*tatarav1alpha1.Issue, string, string) {

	t.Helper()
	proj := reapProject("resume")
	repo := reapRepo("resume", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	taskName := tatarav1alpha1.IntakeTaskName("resume", SweepIssueKind, "tatara-operator", 1)
	issName := tatarav1alpha1.IssueName("tatara-operator", 1)

	task := reapTask("resume", taskName, SweepIssueKind, tatarav1alpha1.StateUnderImplementation,
		stage.ReasonCIRed, time.Now().Add(-parkedAgo))
	parked := metav1.NewTime(time.Now().Add(-parkedAgo))
	task.Status.ParkedAt = &parked
	task.Status.IssueRefs = []string{issName}

	iss := ownedIssue(issName, 1, task, tatarav1alpha1.IssueStatus{
		State: "open", Author: "maintainer", Title: "an issue",
	})
	return proj, repo, task, iss, taskName, issName
}

// TestStrandedPark_AutoReentryReMintsWithNoHumanReply IS C.3. Eighteen park
// reasons are UnparkNever, and until this existed each of them was a permanent
// dead end whose only escape required a human to comment. The platform now
// notices by itself: the stranded Task is collected early and its issue re-minted
// through the SAME MintForItem funnel the human path uses, so every intake gate
// re-runs.
func TestStrandedPark_AutoReentryReMintsWithNoHumanReply(t *testing.T) {
	ctx := context.Background()
	proj, repo, task, iss, taskName, issName := strandedFixture(t, 2*time.Hour)

	c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
	r := reapReconciler(c, newStrandWriter())

	require.NoError(t, r.driveStrandedParks(ctx, proj, time.Now()))

	fresh, ok := mustGetTask(t, c, taskName)
	require.True(t, ok, "the stranded issue is picked back up with no human reply at all")
	require.NotEqual(t, task.UID, fresh.UID, "a FRESH task, not the old one un-parked")

	owner, owned := own.ControllerOwner(mustGetIssue(t, c, issName))
	require.True(t, owned, "the fresh task controller-owns the issue again")
	require.Equal(t, taskName, owner)

	require.Equal(t, "1", mustGetIssue(t, c, issName).Annotations[tatarav1alpha1.AnnAutoReentries],
		"the budget is spent on the ISSUE, the only object that outlives the lap")
}

// TestStrandedPark_AllIssuesClosedIsSeveredAndCollected closes the LAST hole in
// the no-re-entry population, and it is a hole BOTH drivers left open.
//
// driveStrandedParks skipped a Task whose every owned Issue was closed, on the
// stated grounds that resumeNoReentryParks would sever and collect it - but that
// driver `continue`s before resumeOne on `!hasNonBotPendingEvent`, so C.4's
// closed-issue handling was unreachable without a human comment. UnparkNever
// park + every owned issue closed + nobody commented = neither driver acted, and
// the Task sat the full ParkRetention. It is not merely untidy: a parked Task is
// not TaskDone, so createTaskRaceSafe answers MintExistingLive for the
// deterministic IntakeTaskName it holds, blocking any re-mint of that
// (project, kind, repo, number) for seven days.
//
// The live shape is a HUMAN closing the issue under a parked Task, which
// ApplyIssueClosedStop structurally cannot convert into a clean terminal
// (issue_apply.go short-circuits on Parked). It is severed and collected on the
// SAME grace clock, with NO mint - there is nothing left to re-mint - and NO
// budget spent, because a collect is not a re-entry.
func TestStrandedPark_AllIssuesClosedIsSeveredAndCollected(t *testing.T) {
	ctx := context.Background()
	proj, repo, task, iss, taskName, issName := strandedFixture(t, 2*time.Hour)
	iss.Status.State = "closed" // a human closed it, e.g. by merging the PR by hand

	c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
	r := reapReconciler(c, newStrandWriter())

	require.NoError(t, r.driveStrandedParks(ctx, proj, time.Now()))

	_, ok := mustGetTask(t, c, taskName)
	require.False(t, ok,
		"a stranded park whose every issue is closed is a corpse: collected early, not held for ParkRetention")

	var surviving tatarav1alpha1.Issue
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: testNS, Name: issName}, &surviving),
		"the mirror survives the collection (C.4); only the ownerRef goes")
	_, owned := own.ControllerOwner(&surviving)
	require.False(t, owned)
	require.Empty(t, surviving.Annotations[tatarav1alpha1.AnnAutoReentries],
		"a collect spends NO automatic budget: nothing was re-minted, so there is no loop to bound")
}

// TestStrandedPark_OwningNoIssueIsLeftToTheReaper: the collect above is
// justified by the intake key an ISSUE-owning Task holds. A Task owning no Issue
// mirror at all has nothing to sever and no key worth freeing early, so it keeps
// the reaper's ordinary ParkRetention clock.
func TestStrandedPark_OwningNoIssueIsLeftToTheReaper(t *testing.T) {
	ctx := context.Background()
	proj, repo, task, _, taskName, _ := strandedFixture(t, 2*time.Hour)
	task.Status.IssueRefs = nil

	c := newMirrorClient(t, proj, repo, reapSecret(), task)
	r := reapReconciler(c, newStrandWriter())

	require.NoError(t, r.driveStrandedParks(ctx, proj, time.Now()))

	still, ok := mustGetTask(t, c, taskName)
	require.True(t, ok, "no owned issue: the reaper's retention clock still owns this task")
	require.Equal(t, task.UID, still.UID)
}

// TestStrandedPark_WaitsOutTheGrace: the automatic pickup is not instant. The
// grace window exists so the strictly better answer - a human replying on the
// issue, which resumes it WITH the reply's context - gets there first.
func TestStrandedPark_WaitsOutTheGrace(t *testing.T) {
	ctx := context.Background()
	proj, repo, task, iss, taskName, issName := strandedFixture(t, time.Minute)

	c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
	r := reapReconciler(c, newStrandWriter())

	require.NoError(t, r.driveStrandedParks(ctx, proj, time.Now()))

	still, ok := mustGetTask(t, c, taskName)
	require.True(t, ok)
	require.Equal(t, task.UID, still.UID, "inside the grace window nothing happens yet")
	require.Empty(t, mustGetIssue(t, c, issName).Annotations[tatarav1alpha1.AnnAutoReentries],
		"and no budget is spent")
}

// TestStrandedPark_DefersToAHumanReply: a Task carrying a human pendingEvent is
// resumeNoReentryParks' this pass. Spending an automatic budget on it as well
// would double-charge the same recovery.
func TestStrandedPark_DefersToAHumanReply(t *testing.T) {
	ctx := context.Background()
	proj, repo, task, iss, taskName, issName := strandedFixture(t, 2*time.Hour)
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{
		{At: metav1.Now(), Kind: "issue_comment", Author: "maintainer", Body: "please continue"},
	}

	c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
	r := reapReconciler(c, newStrandWriter())

	require.NoError(t, r.driveStrandedParks(ctx, proj, time.Now()))

	still, ok := mustGetTask(t, c, taskName)
	require.True(t, ok, "the human path owns this task, not the automatic one")
	require.Equal(t, task.UID, still.UID)
	require.Empty(t, mustGetIssue(t, c, issName).Annotations[tatarav1alpha1.AnnAutoReentries])
}

// TestStrandedPark_ReentryReasonUntouched: ci-blocked is UnparkNever, but
// awaiting-human is UnparkHuman and merge-timeout is UnparkTimer - both already
// have an owner (driveUnparks) and must not be double-driven from here. So must
// backlog-sweep, which never RAN and has no failure to recover from.
func TestStrandedPark_ReentryReasonUntouched(t *testing.T) {
	for _, reason := range []string{stage.ReasonAwaitingHuman, stage.ReasonMergeTimeout, stage.ReasonBacklogSweep} {
		t.Run(reason, func(t *testing.T) {
			ctx := context.Background()
			proj, repo, task, iss, taskName, _ := strandedFixture(t, 2*time.Hour)
			task.Status.ParkReason = reason

			c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
			r := reapReconciler(c, newStrandWriter())

			require.NoError(t, r.driveStrandedParks(ctx, proj, time.Now()))

			still, ok := mustGetTask(t, c, taskName)
			require.True(t, ok, "this driver must not touch a park that already has an owner")
			require.Equal(t, task.UID, still.UID)
		})
	}
}

// TestStrandedPark_IsBoundedAndLandsInARealDeadEnd is the whole safety argument
// for letting UnparkNever stop meaning permanent.
//
// The budget is spent, so the issue STOPS - no further re-mint, ever, without a
// human - and it stops VISIBLY: the once-only notice comment naming the bound,
// plus the tatara-parked label that puts it in the backlog at zero pods. An
// issue that vanished instead would be the exact defect C.4 is about.
func TestStrandedPark_IsBoundedAndLandsInARealDeadEnd(t *testing.T) {
	ctx := context.Background()
	proj, repo, task, iss, taskName, issName := strandedFixture(t, 2*time.Hour)
	iss.Annotations = map[string]string{
		tatarav1alpha1.AnnAutoReentries: strconv.Itoa(tatarav1alpha1.MaxAutoReentries),
	}

	c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
	w := newStrandWriter()
	r := reapReconciler(c, w)

	require.NoError(t, r.driveStrandedParks(ctx, proj, time.Now()))

	still, ok := mustGetTask(t, c, taskName)
	require.True(t, ok, "a spent budget must STOP, not re-mint")
	require.Equal(t, task.UID, still.UID)

	fresh := mustGetIssue(t, c, issName)
	require.Equal(t, strconv.Itoa(tatarav1alpha1.MaxAutoReentries),
		fresh.Annotations[tatarav1alpha1.AnnAutoReentries], "the bound is not exceeded")
	require.NotEmpty(t, fresh.Annotations[tatarav1alpha1.AnnAutoReentryExhausted],
		"the dead-end notice is latched so it is posted exactly once")

	issueRef := "szymonrychu/tatara-operator#1"
	require.True(t, w.labelled(issueRef, TataraParkedLabel),
		"a real dead end is LABELLED, so it is visible in the backlog rather than gone")
	require.True(t, w.commentedWith(strconv.Itoa(tatarav1alpha1.MaxAutoReentries)),
		"the notice names the bound: 'tried once' and 'tried three times' ask different things of a reader")

	// The latch holds on a second pass: no second comment on the forge.
	before := len(w.comments)
	require.NoError(t, r.driveStrandedParks(ctx, proj, time.Now()))
	require.Equal(t, before, len(w.comments), "the dead-end notice is once-only")
}

// TestStrandedPark_SpendsTheBudgetBeforeTheReentry pins the crash ordering. A
// crash between the increment and the re-entry costs one lap of a bounded
// budget; a crash the other way round costs the bound itself, which is the only
// thing standing between C.3 and an unbounded loop.
func TestStrandedPark_SpendsTheBudgetBeforeTheReentry(t *testing.T) {
	ctx := context.Background()
	proj, repo, task, iss, _, issName := strandedFixture(t, 2*time.Hour)
	iss.Annotations = map[string]string{tatarav1alpha1.AnnAutoReentries: "2"}

	c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
	r := reapReconciler(c, newStrandWriter())

	require.NoError(t, r.driveStrandedParks(ctx, proj, time.Now()))
	require.Equal(t, "3", mustGetIssue(t, c, issName).Annotations[tatarav1alpha1.AnnAutoReentries])
}

// TestAutoReentriesFailsOpenOnGarbage: an issue that predates the counter, or
// whose annotation a human hand-edited, is given the FULL budget rather than
// being silently declared a permanent dead end. The bound is still enforced from
// there, because the next write is an absolute value derived from this read.
func TestAutoReentriesFailsOpenOnGarbage(t *testing.T) {
	for _, raw := range []string{"", "not-a-number", "-4"} {
		iss := &tatarav1alpha1.Issue{}
		if raw != "" {
			iss.Annotations = map[string]string{tatarav1alpha1.AnnAutoReentries: raw}
		}
		require.Equal(t, 0, autoReentries(iss), raw)
	}
}

// --- C.2: a terminal transition never drops an open issue -----------------

// TestEnterStage_TerminalParksAStillOpenOwnedIssue is C.2 at the TRANSITION
// CHOKE POINT. A Task going terminal while it still owns an open Issue used to
// leave that issue with no comment and no label, and - since the sweep releases
// the ownerRef anyway - MintStage fell through its label gate and re-minted it
// ACTIVE, with a pod, on every pass, unbounded. The label is what turns every
// such re-mint into parked(backlog-sweep) at zero pods.
func TestEnterStage_TerminalParksAStillOpenOwnedIssue(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("resume")
	repo := reapRepo("resume", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	issName := tatarav1alpha1.IssueName("tatara-operator", 1)

	task := reapTask("resume", "term-task", SweepIssueKind, tatarav1alpha1.StateRefined, "", time.Now())
	task.Status.IssueRefs = []string{issName}
	iss := ownedIssue(issName, 1, task, tatarav1alpha1.IssueStatus{State: "open", Author: "maintainer"})

	c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
	w := newStrandWriter()

	require.NoError(t, EnterStage(ctx, c, objbudget.Spiller(nil), r2Metrics(), task, nil,
		tatarav1alpha1.StateRejected, stage.ReasonDeclined, time.Now(), nil,
		WithTerminalIssueRelease(&TerminalReleaser{Client: c, SCMFor: scmForOf(w), Metrics: r2Metrics()})))

	require.Equal(t, tatarav1alpha1.StateRejected, task.Status.State)
	issueRef := "szymonrychu/tatara-operator#1"
	require.True(t, w.labelled(issueRef, TataraParkedLabel),
		"a terminal transition stamps tatara-parked on every still-open owned issue")
	require.True(t, w.commentedWith("tatara has stopped working this issue"),
		"and says why, before the label lands")
}

// TestEnterStage_TerminalLeavesAClosedIssueAlone: the treatment is for OPEN
// issues. A closed one has nothing left to strand, and commenting on it would be
// noise on a thread the maintainer already finished with.
func TestEnterStage_TerminalLeavesAClosedIssueAlone(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("resume")
	repo := reapRepo("resume", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	issName := tatarav1alpha1.IssueName("tatara-operator", 1)

	task := reapTask("resume", "term-task", SweepIssueKind, tatarav1alpha1.StateRefined, "", time.Now())
	task.Status.IssueRefs = []string{issName}
	iss := ownedIssue(issName, 1, task, tatarav1alpha1.IssueStatus{State: "closed", Author: "maintainer"})

	c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
	w := newStrandWriter()

	require.NoError(t, EnterStage(ctx, c, objbudget.Spiller(nil), r2Metrics(), task, nil,
		tatarav1alpha1.StateRejected, stage.ReasonDeclined, time.Now(), nil,
		WithTerminalIssueRelease(&TerminalReleaser{Client: c, SCMFor: scmForOf(w), Metrics: r2Metrics()})))

	require.Empty(t, w.comments)
	require.Empty(t, w.labels)
}

// TestReapDelivered_RunsTheTerminalSequence is the LIVE BUG the plan names:
// `done` was the one terminal state reapOne never ran releaseTerminal for, so a
// delivered Task's still-open issue got nothing at all.
func TestReapDelivered_RunsTheTerminalSequence(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("resume")
	repo := reapRepo("resume", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	issName := tatarav1alpha1.IssueName("tatara-operator", 1)

	delivered := metav1.NewTime(time.Now().Add(-time.Hour))
	task := reapTask("resume", "done-task", SweepIssueKind, tatarav1alpha1.StateDone, "", delivered.Time)
	task.Status.DeliveredAt = &delivered
	task.Status.IssueRefs = []string{issName}
	iss := ownedIssue(issName, 1, task, tatarav1alpha1.IssueStatus{State: "open", Author: "maintainer"})

	c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
	w := newStrandWriter()
	r := reapReconciler(c, w)

	require.NoError(t, r.ReapTerminal(ctx, proj))

	require.True(t, w.labelled("szymonrychu/tatara-operator#1", TataraParkedLabel),
		"a delivered task's still-open issue is labelled, so the sweep re-mints it parked at zero pods")
	_, owned := own.ControllerOwner(mustGetIssue(t, c, issName))
	require.False(t, owned, "and its ownerRef is released, so the sweep can see it at all")
}

// --- C.4: the mirror of a CLOSED issue survives its owner -----------------

// TestReleaseOwnership_ClosedIssueMirrorSurvives: releaseOwnership used to keep
// the ownerRef of a CLOSED issue so the mirror cascade-deleted with the Task at
// T+7d. After that IsOrphanIssue (which needs state == "open") could never see
// it again - a silent vanish, and the only record that the platform ever worked
// the issue. The ref is released instead, so the CR survives ownerless.
func TestReleaseOwnership_ClosedIssueMirrorSurvives(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("resume")
	repo := reapRepo("resume", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	issName := tatarav1alpha1.IssueName("tatara-operator", 1)

	entered := time.Now().Add(-48 * time.Hour) // past RejectedRetention
	task := reapTask("resume", "rej-task", SweepIssueKind, tatarav1alpha1.StateRejected,
		stage.ReasonDeclined, entered)
	task.Status.IssueRefs = []string{issName}
	iss := ownedIssue(issName, 1, task, tatarav1alpha1.IssueStatus{State: "closed", Author: "maintainer"})

	c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
	r := reapReconciler(c, newStrandWriter())

	require.NoError(t, r.ReapTerminal(ctx, proj))

	_, taskAlive := mustGetTask(t, c, "rej-task")
	require.False(t, taskAlive, "the task itself is collected")

	var surviving tatarav1alpha1.Issue
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: testNS, Name: issName}, &surviving),
		"the closed issue's mirror must NOT cascade away with its owner")
	_, owned := own.ControllerOwner(&surviving)
	require.False(t, owned, "it survives ownerless, which the GC never collects")
}
