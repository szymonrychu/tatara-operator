package controller

import (
	"context"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
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
	if tk := ownerTaskOf(t, ctx, got); tk.Status.Stage != tatarav1alpha1.StageParked || tk.Status.StageReason != stage.ReasonOwnershipLost {
		t.Fatalf("bound task not parked ownership-lost: %q/%q", tk.Status.Stage, tk.Status.StageReason)
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
	if implTask.Status.Stage != tatarav1alpha1.StageParked || implTask.Status.StageReason != stage.ReasonOwnershipLost {
		t.Fatalf("normal owner task not parked ownership-lost: %q/%q", implTask.Status.Stage, implTask.Status.StageReason)
	}

	reviewName := tatarav1alpha1.IntakeTaskName(proj.Name, SweepReviewKind, repo.Name, 6)
	if ctrl, ok := ownerControllerName(got); !ok || ctrl != reviewName {
		t.Fatalf("controller must move to the review task %q; got ctrl=%q ok=%v", reviewName, ctrl, ok)
	}
}

// TestFlipToExternal_OldOwnerSurvivesAsPlainRef covers demoteMRController's
// contract directly: reMintReviewOwner's demote-then-remint must leave the
// PREVIOUS controller (the takeover Task, now parked ownership-lost) as a
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
	stampTaskStatus(t, ctx, task, tatarav1alpha1.StageImplementing, "")

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
