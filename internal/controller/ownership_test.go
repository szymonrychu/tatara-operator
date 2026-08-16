package controller

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

func TestReconcileOwnership_BackfillsByAuthor(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	botMR := seedOpenMR(t, ctx, proj, repo, 1, "tatara/feat-1", proj.Spec.Scm.BotLogin, "h1")
	extMR := seedOpenMR(t, ctx, proj, repo, 2, "renovate/x", "octocat", "h2")

	if _, err := d.ReconcileOwnership(ctx, proj, repo, botMR, "h1", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := d.ReconcileOwnership(ctx, proj, repo, extMR, "h2", nil); err != nil {
		t.Fatal(err)
	}
	if getMR(t, ctx, proj, repo, 1).Status.Ownership != tatarav1alpha1.OwnershipTatara {
		t.Fatalf("bot-authored MR must backfill to tatara")
	}
	if getMR(t, ctx, proj, repo, 2).Status.Ownership != tatarav1alpha1.OwnershipExternal {
		t.Fatalf("human MR must backfill to external")
	}
}

func TestReconcileOwnership_FlipsOnUnattributableDrift(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedTataraOwnedMRWithTakeoverTask(t, ctx, proj, repo, 3, "tatara/feat-3", "bot-head") // implement task in implementing
	before := testutil.ToFloat64(obs.OwnershipFlipCounter("to-external", "external-push"))

	flipped, err := d.ReconcileOwnership(ctx, proj, repo, mr, "human-head", nil)
	if err != nil || !flipped {
		t.Fatalf("expected flip, got flipped=%v err=%v", flipped, err)
	}
	got := getMR(t, ctx, proj, repo, 3)
	if got.Status.Ownership != tatarav1alpha1.OwnershipExternal {
		t.Fatalf("ownership = %q, want external", got.Status.Ownership)
	}
	if got.Status.OwnershipReason != "external-push:human-head" {
		t.Fatalf("reason = %q", got.Status.OwnershipReason)
	}
	if after := testutil.ToFloat64(obs.OwnershipFlipCounter("to-external", "external-push")); after-before != 1 {
		t.Fatalf("flip counter not incremented")
	}
	// The bound takeover Task is parked ownership-lost.
	if tk := ownerTaskOf(t, ctx, got); tk.Status.State != tatarav1alpha1.StateUnderImplementation ||
		!tatarav1alpha1.Parked(tk) || tk.Status.ParkReason != stage.ReasonOwnershipLost {
		t.Fatalf("bound task not parked ownership-lost: %q/%q", tk.Status.State, tk.Status.ParkReason)
	}
}

func TestReconcileOwnership_BotAttributableHeadDoesNotFlip(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedTataraOwnedMRWithTakeoverTask(t, ctx, proj, repo, 4, "tatara/feat-4", "bot-head")

	flipped, err := d.ReconcileOwnership(ctx, proj, repo, mr, "bot-head", nil) // live == lastBotHeadSHA
	if err != nil || flipped {
		t.Fatalf("no flip expected when live head matches lastBotHeadSHA")
	}
}

// TestReconcileOwnership_FlipsNormalImplementOwner is the controller
// adjudication's coverage: flipToExternal's park guard is kind != review (any
// pushing-capable owner), not kind == takeover. A NORMAL full-lifecycle Task
// (kind=clarify, the SweepIssueKind an issue-originated Task carries end to
// end - see sweep.go) sitting in implementing must park ownership-lost on an
// unattributable drift exactly like a takeover Task does, and hand-back to
// the review Task must still happen.
func TestReconcileOwnership_FlipsNormalImplementOwner(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedTataraOwnedMRWithNormalTask(t, ctx, proj, repo, 6, "tatara/feat-6", "bot-head")

	flipped, err := d.ReconcileOwnership(ctx, proj, repo, mr, "human-head", nil)
	if err != nil || !flipped {
		t.Fatalf("expected flip, got flipped=%v err=%v", flipped, err)
	}
	got := getMR(t, ctx, proj, repo, 6)
	if got.Status.Ownership != tatarav1alpha1.OwnershipExternal {
		t.Fatalf("ownership = %q, want external", got.Status.Ownership)
	}

	implName := tatarav1alpha1.IntakeTaskName(proj.Name, SweepIssueKind, repo.Name, 6)
	var implTask tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: implName}, &implTask); err != nil {
		t.Fatalf("get normal owner task %s: %v", implName, err)
	}
	if implTask.Status.State != tatarav1alpha1.StateUnderImplementation || !tatarav1alpha1.Parked(&implTask) || implTask.Status.ParkReason != stage.ReasonOwnershipLost {
		t.Fatalf("normal owner task not parked ownership-lost: %q/%q", implTask.Status.State, implTask.Status.ParkReason)
	}

	reviewName := tatarav1alpha1.IntakeTaskName(proj.Name, SweepReviewKind, repo.Name, 6)
	if ctrl, ok := ownerControllerName(got); !ok || ctrl != reviewName {
		t.Fatalf("controller must move to the review task %q; got ctrl=%q ok=%v", reviewName, ctrl, ok)
	}
}

// TestFlipToExternal_OldOwnerSurvivesAsPlainRef covers handBackToReviewTask's
// contract directly: reMintReviewOwner's atomic hand-over (fix #408) must leave
// the PREVIOUS controller (the takeover Task, now parked ownership-lost) as a
// plain (non-controller) owner ref, not remove it - the artifact must stay
// GC-open against it (own package invariant) even though it no longer drives
// the MR.
func TestFlipToExternal_OldOwnerSurvivesAsPlainRef(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedTataraOwnedMRWithTakeoverTask(t, ctx, proj, repo, 7, "tatara/feat-7", "bot-head")
	takeoverName := takeoverTaskName(proj, repo, 7)

	if _, err := d.ReconcileOwnership(ctx, proj, repo, mr, "human-head", nil); err != nil {
		t.Fatal(err)
	}
	got := getMR(t, ctx, proj, repo, 7)

	var oldRef *metav1.OwnerReference
	for i, r := range got.GetOwnerReferences() {
		if r.Name == takeoverName {
			oldRef = &got.GetOwnerReferences()[i]
			break
		}
	}
	if oldRef == nil {
		t.Fatalf("old owner %s must survive hand-back as a plain ref (GC-open), not be removed", takeoverName)
	}
	if oldRef.Controller != nil && *oldRef.Controller {
		t.Fatalf("old owner %s must be demoted to controller=false after hand-back", takeoverName)
	}
	if ctrl, ok := ownerControllerName(got); !ok || ctrl == takeoverName {
		t.Fatalf("controller must have moved off the old owner %s; got ctrl=%q ok=%v", takeoverName, ctrl, ok)
	}
}

// TestReMintReviewOwner_SingleMRUpdate is fix #408's direct unit coverage on
// reMintReviewOwner: capturing prevOwner (own.ControllerOwner) BEFORE minting
// and threading it through MintReviewTask -> bindMRToTask -> ownMergeRequest
// as expectFrom must cost exactly ONE MergeRequest Update - no standalone
// demoteMRController Update racing a separate later promote, which used to
// leave a zero-controller window a RepairZeroController race could jump into.
func TestReMintReviewOwner_SingleMRUpdate(t *testing.T) {
	ctx := context.Background()
	proj, repo := seedProjectRepo(t, ctx)
	mr := seedTataraOwnedMRWithTakeoverTask(t, ctx, proj, repo, 20, "tatara/feat-20", "bot-head")
	takeoverName := takeoverTaskName(proj, repo, 20)

	c, mrUpdates := wrapMRUpdateCounting(k8sClient)
	d := &StageDriver{
		Client:     c,
		APIReader:  k8sClient,
		SpillerFor: func(*tatarav1alpha1.Project) objbudget.Spiller { return &mirrorSpiller{} },
	}

	if err := d.reMintReviewOwner(ctx, proj, repo, mr); err != nil {
		t.Fatalf("reMintReviewOwner: %v", err)
	}
	if got := atomic.LoadInt32(mrUpdates); got != 1 {
		t.Fatalf("MR updates across reMintReviewOwner = %d, want exactly 1 (no demote-then-remint window)", got)
	}

	got := getMR(t, ctx, proj, repo, 20)
	reviewName := tatarav1alpha1.IntakeTaskName(proj.Name, SweepReviewKind, repo.Name, 20)
	if ctrl, ok := ownerControllerName(got); !ok || ctrl != reviewName {
		t.Fatalf("controller must move to the newly minted review task %q; got ctrl=%q ok=%v", reviewName, ctrl, ok)
	}
	found, stillController := false, true
	for _, ref := range got.GetOwnerReferences() {
		if ref.Name == takeoverName {
			found = true
			stillController = ref.Controller != nil && *ref.Controller
		}
	}
	if !found {
		t.Fatalf("old owner %s must survive as a plain ref", takeoverName)
	}
	if stillController {
		t.Fatalf("old owner %s must be demoted to controller=false", takeoverName)
	}
}

// TestReconcileOwnership_FlipsMainlineWithExistingReviewTask exercises the
// MAINLINE flip path a reviewer flagged as missing coverage for: a normal bot
// MR controller-owned by a NON-review owner Task, where a review-kind Task
// ALREADY exists for the same MR (handBackToReviewTask's EXISTS branch, never
// reMintReviewOwner - every other flip test above only seeds an MR with no
// pre-existing review Task, so only the re-mint branch got exercised).
//
// Before the fix, that EXISTS branch mutated owner refs directly on the
// caller's local mr and called d.Update(ctx, mr) with no fresh Get and no
// RetryOnConflict. By the time handBackToReviewTask runs, flipToExternal has
// already written mr's Status via objbudget.FitMergeRequest - which Gets,
// mutates, and Status().Updates a FRESH server copy, bumping resourceVersion
// without the caller's local mr ever finding out. The direct Update then
// carried mr's now-stale resourceVersion and 409ed deterministically: the
// flip returned an error BEFORE obs.OwnershipFlip fired, yet the server's
// Ownership status was already external, so the next reconcile's
// `Ownership == tatara` guard was false and hand-back never got retried.
func TestReconcileOwnership_FlipsMainlineWithExistingReviewTask(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr, reviewName := seedTataraOwnedMRWithOwnerAndReviewTask(t, ctx, proj, repo, 8, "tatara/feat-8", "bot-head")
	implName := tatarav1alpha1.IntakeTaskName(proj.Name, SweepIssueKind, repo.Name, 8)
	before := testutil.ToFloat64(obs.OwnershipFlipCounter("to-external", "external-push"))

	flipped, err := d.ReconcileOwnership(ctx, proj, repo, mr, "human-head", nil)
	if err != nil || !flipped {
		t.Fatalf("expected flip, got flipped=%v err=%v", flipped, err)
	}
	got := getMR(t, ctx, proj, repo, 8)
	if got.Status.Ownership != tatarav1alpha1.OwnershipExternal {
		t.Fatalf("ownership = %q, want external", got.Status.Ownership)
	}
	if after := testutil.ToFloat64(obs.OwnershipFlipCounter("to-external", "external-push")); after-before != 1 {
		t.Fatalf("flip counter not incremented")
	}

	var implTask tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: implName}, &implTask); err != nil {
		t.Fatalf("get owner task %s: %v", implName, err)
	}
	if implTask.Status.State != tatarav1alpha1.StateUnderImplementation || !tatarav1alpha1.Parked(&implTask) || implTask.Status.ParkReason != stage.ReasonOwnershipLost {
		t.Fatalf("owner task not parked ownership-lost: %q/%q", implTask.Status.State, implTask.Status.ParkReason)
	}

	if ctrl, ok := ownerControllerName(got); !ok || ctrl != reviewName {
		t.Fatalf("controller must move to the pre-existing review task %q; got ctrl=%q ok=%v", reviewName, ctrl, ok)
	}

	found, stillController := false, true
	for _, r := range got.GetOwnerReferences() {
		if r.Name == implName {
			found = true
			stillController = r.Controller != nil && *r.Controller
		}
	}
	if !found {
		t.Fatalf("old owner %s must survive hand-back as a plain ref", implName)
	}
	if stillController {
		t.Fatalf("old owner %s must be demoted to controller=false after hand-back", implName)
	}
}

// TestReconcileOwnership_ResumesHalfCompletedFlip is the convergence-hole
// regression test flagged by the in-cluster review agent: flipToExternal
// stamps Status.Ownership=external BEFORE parking the owner Task and handing
// control to the review Task. If that second half fails with a non-conflict
// error, the reconcile requeues but the `Ownership == tatara` drift guard
// (ReconcileOwnership's own flip trigger) is now false forever - park+hand-
// back never re-run, leaving the owner Task unparked and the review Task
// never controller.
//
// This simulates exactly that half-completed state (real flip, then manually
// un-park the owner task and restore its controller ref) and asserts a
// SUBSEQUENT ReconcileOwnership call - the requeue - finishes the job: owner
// re-parked, review Task controller again. It also asserts
// OwnershipChangedAt is untouched by the resume, so the announcement drain
// (OP11, keyed on that timestamp) does not double-post.
func TestReconcileOwnership_ResumesHalfCompletedFlip(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedTataraOwnedMRWithTakeoverTask(t, ctx, proj, repo, 10, "tatara/feat-10", "bot-head")
	takeoverName := takeoverTaskName(proj, repo, 10)
	reviewName := tatarav1alpha1.IntakeTaskName(proj.Name, SweepReviewKind, repo.Name, 10)

	// A real flip: stamps external, parks the takeover Task, hands control to
	// a freshly re-minted review Task.
	flipped, err := d.ReconcileOwnership(ctx, proj, repo, mr, "human-head", nil)
	if err != nil || !flipped {
		t.Fatalf("expected flip, got flipped=%v err=%v", flipped, err)
	}
	flippedAt := getMR(t, ctx, proj, repo, 10).Status.OwnershipChangedAt
	if flippedAt == nil {
		t.Fatalf("precondition: flip must stamp OwnershipChangedAt")
	}

	// Simulate the crash: un-park the owner task...
	var task tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: takeoverName}, &task); err != nil {
		t.Fatalf("get takeover task: %v", err)
	}
	task.Status.State = tatarav1alpha1.StateUnderImplementation
	task.Status.ParkReason = ""
	if err := k8sClient.Status().Update(ctx, &task); err != nil {
		t.Fatalf("un-park owner task: %v", err)
	}
	// ...and restore its controller ref on the MR mirror (it survived the
	// first flip as a plain owner - own.HandOverController just needs it
	// already listed as an owner, which it is).
	got := getMR(t, ctx, proj, repo, 10)
	if err := MutateOwnerRefs(ctx, k8sClient, got, func(fresh *tatarav1alpha1.MergeRequest) error {
		return own.HandOverController(fresh, nil, &task)
	}); err != nil {
		t.Fatalf("restore controller ref: %v", err)
	}
	if ctrl, ok := own.ControllerOwner(getMR(t, ctx, proj, repo, 10)); !ok || ctrl != takeoverName {
		t.Fatalf("precondition: controller must be back on the takeover task, got %q ok=%v", ctrl, ok)
	}

	// The requeue: ReconcileOwnership must notice the half-completed flip and
	// finish it, without treating it as a NEW flip.
	mr2 := getMR(t, ctx, proj, repo, 10)
	resumed, err := d.ReconcileOwnership(ctx, proj, repo, mr2, "human-head", nil)
	if err != nil {
		t.Fatalf("resume reconcile: %v", err)
	}
	if resumed {
		t.Fatalf("resuming a half-completed flip is not itself a new flip")
	}

	var taskAfter tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: takeoverName}, &taskAfter); err != nil {
		t.Fatalf("get takeover task after resume: %v", err)
	}
	if taskAfter.Status.State != tatarav1alpha1.StateUnderImplementation || !tatarav1alpha1.Parked(&taskAfter) || taskAfter.Status.ParkReason != stage.ReasonOwnershipLost {
		t.Fatalf("owner task must be re-parked ownership-lost by the resume: %q/%q", taskAfter.Status.State, taskAfter.Status.ParkReason)
	}

	after := getMR(t, ctx, proj, repo, 10)
	if ctrl, ok := own.ControllerOwner(after); !ok || ctrl != reviewName {
		t.Fatalf("controller must be handed back to the review task %q; got ctrl=%q ok=%v", reviewName, ctrl, ok)
	}
	if after.Status.OwnershipChangedAt == nil || !after.Status.OwnershipChangedAt.Equal(flippedAt) {
		t.Fatalf("resume must not restamp OwnershipChangedAt (would double-post the announcement): got %v, want %v",
			after.Status.OwnershipChangedAt, flippedAt)
	}
}

func TestReconcileOwnership_TerminalMRFrozen(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedOpenMR(t, ctx, proj, repo, 5, "tatara/feat-5", proj.Spec.Scm.BotLogin, "h5")
	// setMRState (ensure_task_test.go) is built for the UNPERSISTED
	// seedOpenExternalMR fixture and Creates it first; seedOpenMR here already
	// persisted mr (ReconcileOwnership only ever converges an existing mirror),
	// so re-Create-ing it 400s on "resourceVersion should not be set on objects
	// to be created". Set the state directly instead.
	mr.Status.State = "merged"
	if err := k8sClient.Status().Update(ctx, mr); err != nil {
		t.Fatalf("update mergerequest %s status: %v", mr.Name, err)
	}
	flipped, err := d.ReconcileOwnership(ctx, proj, repo, mr, "anything", nil)
	if err != nil || flipped {
		t.Fatalf("terminal MR must be frozen")
	}
}

// --- envtest fixtures local to the ownership convergence tests ---

// newOwnershipDriver builds a StageDriver bound to the package's shared
// envtest client, plus a fresh Project+Repository pair (reusing
// seedProjectRepo, the takeover minter tests' fixture).
func newOwnershipDriver(t *testing.T, ctx context.Context) (*StageDriver, *tatarav1alpha1.Project, *tatarav1alpha1.Repository) {
	t.Helper()
	proj, repo := seedProjectRepo(t, ctx)
	d := &StageDriver{
		Client:     k8sClient,
		APIReader:  k8sClient,
		SpillerFor: func(*tatarav1alpha1.Project) objbudget.Spiller { return &mirrorSpiller{} },
	}
	return d, proj, repo
}

// seedOpenMR persists a minimal open MergeRequest mirror with no controller
// owner, for the backfill/terminal tests: unlike seedOpenExternalMR (an
// UNPERSISTED fixture a mint is expected to create), ReconcileOwnership never
// creates the MR itself - it only ever converges one that already exists.
func seedOpenMR(t *testing.T, ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, headBranch, author, headSHA string) *tatarav1alpha1.MergeRequest {
	t.Helper()
	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.MergeRequestName(repo.Name, number),
			Namespace: proj.Namespace,
		},
		Spec: tatarav1alpha1.MergeRequestSpec{
			RepositoryRef: repo.Name,
			ProjectRef:    proj.Name,
			Number:        number,
			URL:           "https://github.com/o/r/pull/" + strconv.Itoa(number),
		},
	}
	if err := k8sClient.Create(ctx, mr); err != nil {
		t.Fatalf("create mergerequest %s: %v", mr.Name, err)
	}
	mr.Status = tatarav1alpha1.MergeRequestStatus{
		Author:     author,
		State:      "open",
		HeadBranch: headBranch,
		HeadSHA:    headSHA,
	}
	if err := k8sClient.Status().Update(ctx, mr); err != nil {
		t.Fatalf("update mergerequest %s status: %v", mr.Name, err)
	}
	return mr
}

// seedTataraOwnedMRWithTakeoverTask persists an already tatara-owned, open
// MergeRequest (status.ownership=tatara, lastBotHeadSHA=lastBotHeadSHA) whose
// controller owner is a kind=takeover Task sitting in stage=implementing - the
// shape a maintainer's earlier "take over" comment (OP5's
// MintOrUnparkTakeoverTask, admitted and pushing) leaves behind, and exactly
// the shape ReconcileOwnership's flip must find and park.
func seedTataraOwnedMRWithTakeoverTask(t *testing.T, ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, headBranch, lastBotHeadSHA string) *tatarav1alpha1.MergeRequest {
	t.Helper()
	return seedTataraOwnedMRWithOwnerTask(t, ctx, proj, repo, number, headBranch, lastBotHeadSHA,
		takeoverKind, takeoverTaskName(proj, repo, number))
}

// seedTataraOwnedMRWithNormalTask is seedTataraOwnedMRWithTakeoverTask's
// counterpart for a NORMAL, non-takeover, non-review, full-lifecycle Task:
// kind=clarify (SweepIssueKind), the kind an issue-originated Task carries
// end to end through implementing/reviewing/merging (sweep.go). The
// controller-adjudicated fix to flipToExternal (park guard is kind != review,
// not kind == takeover) means this shape must park exactly like a takeover
// Task does on an unattributable drift.
func seedTataraOwnedMRWithNormalTask(t *testing.T, ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, headBranch, lastBotHeadSHA string) *tatarav1alpha1.MergeRequest {
	t.Helper()
	name := tatarav1alpha1.IntakeTaskName(proj.Name, SweepIssueKind, repo.Name, number)
	return seedTataraOwnedMRWithOwnerTask(t, ctx, proj, repo, number, headBranch, lastBotHeadSHA, SweepIssueKind, name)
}

// seedTataraOwnedMRWithOwnerTask is the shared body behind
// seedTataraOwnedMRWithTakeoverTask and seedTataraOwnedMRWithNormalTask: a
// tatara-owned, open MergeRequest whose controller owner is a Task of kind,
// named taskName, sitting in stage=implementing.
func seedTataraOwnedMRWithOwnerTask(t *testing.T, ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, headBranch, lastBotHeadSHA, kind, taskName string) *tatarav1alpha1.MergeRequest {
	t.Helper()

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: taskName, Namespace: proj.Namespace},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj.Name,
			RepositoryRef: repo.Name,
			Kind:          kind,
			Goal:          "push to the MR",
			MergeOrder:    []string{repo.Name},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create owner task: %v", err)
	}
	stampTaskStatus(t, ctx, task, tatarav1alpha1.StateUnderImplementation, "")

	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.MergeRequestName(repo.Name, number),
			Namespace: proj.Namespace,
		},
		Spec: tatarav1alpha1.MergeRequestSpec{
			RepositoryRef: repo.Name,
			ProjectRef:    proj.Name,
			Number:        number,
			URL:           "https://github.com/o/r/pull/" + strconv.Itoa(number),
		},
	}
	if err := controllerutil.SetControllerReference(task, mr, k8sClient.Scheme()); err != nil {
		t.Fatalf("set controller ref: %v", err)
	}
	if err := k8sClient.Create(ctx, mr); err != nil {
		t.Fatalf("create mergerequest %s: %v", mr.Name, err)
	}
	mr.Status = tatarav1alpha1.MergeRequestStatus{
		Author:          "octocat",
		State:           "open",
		HeadBranch:      headBranch,
		HeadSHA:         lastBotHeadSHA,
		Ownership:       tatarav1alpha1.OwnershipTatara,
		OwnershipReason: "seed",
		LastBotHeadSHA:  lastBotHeadSHA,
	}
	if err := k8sClient.Status().Update(ctx, mr); err != nil {
		t.Fatalf("update mergerequest %s status: %v", mr.Name, err)
	}
	return mr
}

// seedTataraOwnedMRWithOwnerAndReviewTask is
// seedTataraOwnedMRWithNormalTask's mainline-flip counterpart: the normal
// (kind=clarify) owner Task controller-owns the MR as usual, AND a
// review-kind Task already exists for the same MR (the shape left behind by
// an earlier human comment via EnsureTaskForMRComment, say), so
// handBackToReviewTask's EXISTS branch runs on flip instead of
// reMintReviewOwner. Returns the MR and the review Task's deterministic
// name.
func seedTataraOwnedMRWithOwnerAndReviewTask(t *testing.T, ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, headBranch, lastBotHeadSHA string) (*tatarav1alpha1.MergeRequest, string) {
	t.Helper()

	implName := tatarav1alpha1.IntakeTaskName(proj.Name, SweepIssueKind, repo.Name, number)
	mr := seedTataraOwnedMRWithOwnerTask(t, ctx, proj, repo, number, headBranch, lastBotHeadSHA, SweepIssueKind, implName)

	reviewName := tatarav1alpha1.IntakeTaskName(proj.Name, SweepReviewKind, repo.Name, number)
	review := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: reviewName, Namespace: proj.Namespace},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj.Name,
			RepositoryRef: repo.Name,
			Kind:          SweepReviewKind,
			Goal:          "review the MR",
		},
	}
	if err := k8sClient.Create(ctx, review); err != nil {
		t.Fatalf("create review task: %v", err)
	}
	return mr, reviewName
}

// ownerTaskOf fetches the takeover Task BOUND to mr by its deterministic
// natural key (tatarav1alpha1.IntakeTaskName(..., takeoverKind, ...), the
// same name takeoverTaskName derives from a live proj/repo pair). It
// deliberately does NOT read mr's CURRENT controller owner: a flip's
// handBackToReviewTask reassigns that to the review Task, but the takeover
// Task - now parked ownership-lost and merely a plain owner - is still the
// one this assertion means to inspect.
func ownerTaskOf(t *testing.T, ctx context.Context, mr *tatarav1alpha1.MergeRequest) *tatarav1alpha1.Task {
	t.Helper()
	name := tatarav1alpha1.IntakeTaskName(mr.Spec.ProjectRef, takeoverKind, mr.Spec.RepositoryRef, mr.Spec.Number)
	var task tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: mr.Namespace, Name: name}, &task); err != nil {
		t.Fatalf("get takeover task %s: %v", name, err)
	}
	return &task
}

// CLASSIFYING A BOT MR BEFORE ITS HEAD IS KNOWN MUST NOT ARM A FALSE FLIP.
//
// The backfill seeds LastBotHeadSHA = liveHead precisely so the drift check
// below it does not fire on the classification's own backfill - "which is not a
// drift, it is day one", as the code says. But that seed was conditional on
// liveHead being non-empty, and the mirror is routinely classified on a
// reconcile that has not learned the head yet (a freshly opened PR whose mirror
// syncs the head a beat later). LastBotHeadSHA then stayed empty, and the FIRST
// reconcile that did know the head saw liveHead != "" and liveHead != "" -> flip.
//
// The consequence is not cosmetic: flipToExternal parks the owning Task
// ownership-lost, hands the MR to the review Task, and the issue restarts from
// scratch. Live, on tatara's own work: mr-mtg-decks-19 and mr-mtg-decks-15 both
// ended ownership=external, reason=external-push:<tatara's own commit>, with
// lastBotHeadSHA EMPTY - and mtg-decks#19 (a finished 4199-line deck PR) was
// closed and rebuilt from nothing. mr-mtg-decks-17, seeded normally, was fine.
func TestReconcileOwnership_ClassifyWithoutLiveHeadDoesNotArmAFalseFlip(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedOpenMR(t, ctx, proj, repo, 42, "tatara/feat-42", proj.Spec.Scm.BotLogin, "")

	// Pass 1: the mirror has not synced a head yet. This is the classification
	// that used to leave LastBotHeadSHA empty.
	if _, err := d.ReconcileOwnership(ctx, proj, repo, mr, "", nil); err != nil {
		t.Fatalf("classify pass: %v", err)
	}
	got := getMR(t, ctx, proj, repo, 42)
	if got.Status.Ownership != tatarav1alpha1.OwnershipTatara {
		t.Fatalf("ownership = %q, want tatara: the MR is bot-authored", got.Status.Ownership)
	}

	// Pass 2: the head arrives. It is TATARA'S OWN commit - the agent pushed it -
	// so this must not be read as an external push.
	flipped, err := d.ReconcileOwnership(ctx, proj, repo, got, "agent-head", nil)
	if err != nil {
		t.Fatalf("head pass: %v", err)
	}
	if flipped {
		t.Fatal("flipped to external on the agent's own first head: this closes the PR and restarts the issue from nothing")
	}
	got = getMR(t, ctx, proj, repo, 42)
	if got.Status.Ownership != tatarav1alpha1.OwnershipTatara {
		t.Fatalf("ownership = %q (%s), want tatara", got.Status.Ownership, got.Status.OwnershipReason)
	}

	// A GENUINE external push must still flip, or the fix has just disabled the
	// whole mechanism.
	flipped, err = d.ReconcileOwnership(ctx, proj, repo, got, "human-head", nil)
	if err != nil {
		t.Fatalf("drift pass: %v", err)
	}
	if !flipped {
		t.Fatal("a real unattributable head must still flip to external")
	}
}

// THE DIVERGENCE DECLINE (#604 review round 2).
//
// Routing takeover into `under-implementation` made `declined` a legal action
// for a takeover pod, and the skill that pod is now REQUIRED to invoke
// (tatara-implement-takeover) instructs exactly that on the ORDINARY
// stand-down: an `ls-remote` mismatch or a non-fast-forward push rejection ->
// submit_outcome(action="declined"). Both signals are LOCAL git calls, so the
// pod reaches that verdict long before the operator's own flip does (webhook ->
// mirror headSHA -> ReconcileOwnership).
//
// The agent-first order therefore parks the Task implement-declined, and
// parkOwnerTask used to return early on it - leaving a park that
// MintOrUnparkTakeoverTask refuses to resume, that DrainStandDownMerge cannot
// re-drive, and that restapi.mrTakeover now answers 409. A human push, the very
// thing the job text calls "normal and not a failure", became a seven-day
// terminal.
func TestReconcileOwnership_UpgradesADeclinedTakeoverParkToOwnershipLost(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedTataraOwnedMRWithTakeoverTask(t, ctx, proj, repo, 41, "feat/human-41", "bot-head")
	parkOwnerTaskForTest(t, ctx, ownerTaskOf(t, ctx, mr), stage.ReasonImplementDeclined)

	flipped, err := d.ReconcileOwnership(ctx, proj, repo, mr, "human-head", nil)
	if err != nil || !flipped {
		t.Fatalf("expected flip, got flipped=%v err=%v", flipped, err)
	}
	got := ownerTaskOf(t, ctx, getMR(t, ctx, proj, repo, 41))
	if !tatarav1alpha1.Parked(got) {
		t.Fatal("the upgrade must never leave the Task un-parked, even momentarily")
	}
	if got.Status.ParkReason != stage.ReasonOwnershipLost {
		t.Fatalf("a declined takeover whose branch the human took back must be parked %q, got %q: "+
			"nothing resumes it otherwise and the re-take is refused 409",
			stage.ReasonOwnershipLost, got.Status.ParkReason)
	}
	if got.Status.State != tatarav1alpha1.StateUnderImplementation {
		t.Fatalf("the upgrade must not move State, got %q", got.Status.State)
	}
}

// The upgrade is takeover-only, and DELIBERATELY so: implement-declined on a
// takeover is the one place the platform itself instructs a decline for a cause
// that is not a refusal. On any other kind it means what it says - the agent
// looked at the work and declined it - and a human pushing to a tatara branch
// afterwards must not reopen that verdict.
func TestReconcileOwnership_LeavesANormalDeclinedOwnerParkedDeclined(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedTataraOwnedMRWithNormalTask(t, ctx, proj, repo, 42, "tatara/feat-42", "bot-head")
	name := tatarav1alpha1.IntakeTaskName(proj.Name, SweepIssueKind, repo.Name, 42)
	var task tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: name}, &task); err != nil {
		t.Fatalf("get owner task: %v", err)
	}
	parkOwnerTaskForTest(t, ctx, &task, stage.ReasonImplementDeclined)

	if _, err := d.ReconcileOwnership(ctx, proj, repo, mr, "human-head", nil); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: name}, &task); err != nil {
		t.Fatalf("re-get owner task: %v", err)
	}
	if task.Status.ParkReason != stage.ReasonImplementDeclined {
		t.Fatalf("a NON-takeover decline is a refusal and must stay terminal, got %q", task.Status.ParkReason)
	}
}

// Every OTHER unresumable park on a takeover is a genuine terminal - an
// exhaustion cap, an operator error, a contract mismatch - and a human push
// must not launder it into a resumable one. This is what keeps
// restapi.mrTakeover's 409 gate alive after the upgrade above narrows it.
func TestReconcileOwnership_LeavesOtherTakeoverParksAlone(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedTataraOwnedMRWithTakeoverTask(t, ctx, proj, repo, 43, "feat/human-43", "bot-head")
	parkOwnerTaskForTest(t, ctx, ownerTaskOf(t, ctx, mr), stage.ReasonStageDeadline)

	if _, err := d.ReconcileOwnership(ctx, proj, repo, mr, "human-head", nil); err != nil {
		t.Fatal(err)
	}
	if got := ownerTaskOf(t, ctx, getMR(t, ctx, proj, repo, 43)); got.Status.ParkReason != stage.ReasonStageDeadline {
		t.Fatalf("only implement-declined is upgraded; %q must survive a flip, got %q",
			stage.ReasonStageDeadline, got.Status.ParkReason)
	}
}

// parkOwnerTaskForTest parks an already-seeded owner Task through stage.Park,
// so the fixture carries exactly the shape an outcome-driven park leaves.
func parkOwnerTaskForTest(t *testing.T, ctx context.Context, task *tatarav1alpha1.Task, reason string) {
	t.Helper()
	if err := stage.Park(task, reason, task.Status.StateEnteredAt.Time); err != nil {
		t.Fatalf("park %s: %v", reason, err)
	}
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("persist park %s: %v", reason, err)
	}
}
