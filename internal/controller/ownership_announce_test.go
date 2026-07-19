package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

func TestDrainOwnershipAnnouncement_PostsOncePerFlip(t *testing.T) {
	ctx := context.Background()
	d, proj, repo, fakeSCM := newAnnounceHarness(t, ctx)
	mr := seedFlippedMR(t, ctx, proj, repo, 20, tatarav1alpha1.OwnershipExternal, "external-push:xyz")

	if err := d.DrainOwnershipAnnouncement(ctx, proj, repo, mr); err != nil {
		t.Fatal(err)
	}
	if n := fakeSCM.commentCount(20); n != 1 {
		t.Fatalf("want exactly one announcement, got %d", n)
	}
	body := fakeSCM.lastComment(20)
	if !strings.Contains(body, "<!-- tatara-ownership dir=to-external") {
		t.Fatalf("announcement missing ownership marker: %q", body)
	}
	if !strings.Contains(strings.ToLower(body), "standing down") {
		t.Fatalf("stand-down copy missing: %q", body)
	}
	// Second drain is a no-op (marker already on the thread).
	if err := d.DrainOwnershipAnnouncement(ctx, proj, repo, getMR(t, ctx, proj, repo, 20)); err != nil {
		t.Fatal(err)
	}
	if n := fakeSCM.commentCount(20); n != 1 {
		t.Fatalf("announcement double-posted, got %d", n)
	}
}

func TestDrainOwnershipAnnouncement_TakeoverCopy(t *testing.T) {
	ctx := context.Background()
	d, proj, repo, fakeSCM := newAnnounceHarness(t, ctx)
	mr := seedFlippedMR(t, ctx, proj, repo, 21, tatarav1alpha1.OwnershipTatara, "takeover-requested-by:alice")
	if err := d.DrainOwnershipAnnouncement(ctx, proj, repo, mr); err != nil {
		t.Fatal(err)
	}
	body := fakeSCM.lastComment(21)
	if !strings.Contains(body, "<!-- tatara-ownership dir=to-tatara") || !strings.Contains(strings.ToLower(body), "taking ownership") {
		t.Fatalf("takeover announcement wrong: %q", body)
	}
}

// TestDrainOwnershipAnnouncement_NoFlipIsANoOp covers the never-flipped and
// initial-classification guards: neither carries an announcement.
func TestDrainOwnershipAnnouncement_NoFlipIsANoOp(t *testing.T) {
	ctx := context.Background()
	d, proj, repo, fakeSCM := newAnnounceHarness(t, ctx)
	mr := seedFlippedMR(t, ctx, proj, repo, 24, tatarav1alpha1.OwnershipTatara, "initial")
	mr.Status.OwnershipChangedAt = nil
	if err := d.DrainOwnershipAnnouncement(ctx, proj, repo, mr); err != nil {
		t.Fatal(err)
	}
	if n := fakeSCM.commentCount(24); n != 0 {
		t.Fatalf("initial classification must not be announced, got %d comments", n)
	}
}

func TestDrainStandDownMerge_ApprovedExternalPushMergesViaParkedOwnerTask(t *testing.T) {
	ctx := context.Background()
	d, proj, repo, _ := newAnnounceHarness(t, ctx)
	// A stood-down MR: external + external-push, an approved review at the
	// current head, and a parked(ownership-lost) takeover Task for this MR
	// (owner-ref currently on the review Task after the flip's hand-back).
	mr := seedStoodDownApprovedMR(t, ctx, proj, repo, 22, "review-task-22", "takeover-repo-a-22", takeoverKind)

	if err := d.DrainStandDownMerge(ctx, proj, repo, mr); err != nil {
		t.Fatal(err)
	}
	tk := getTask(t, "takeover-repo-a-22")
	if tk.Status.Stage != tatarav1alpha1.StageMerging {
		t.Fatalf("parked owner Task must be re-driven to merging, got %q", tk.Status.Stage)
	}
	if tk.Status.StageReason != stage.ReasonOwnershipLost {
		t.Fatalf("re-drive must stamp Reason=ownership-lost explicitly, got %q", tk.Status.StageReason)
	}
	got := getMR(t, ctx, proj, repo, 22)
	if ctrl, ok := ownerControllerName(got); !ok || ctrl != "takeover-repo-a-22" {
		t.Fatalf("control must return to the parked owner Task for the merge, got %q", ctrl)
	}
}

// TestDrainStandDownMerge_NormalKindOwnerAlsoRedriven is obligation 3's own
// coverage: a NORMAL (non-takeover) full-lifecycle Task parked ownership-lost
// by the OP8-widened guard (kind != review, not kind == takeover) must be
// found and re-driven exactly like a takeover-kind Task is - the lookup must
// not assume the takeover Task's deterministic name.
func TestDrainStandDownMerge_NormalKindOwnerAlsoRedriven(t *testing.T) {
	ctx := context.Background()
	d, proj, repo, _ := newAnnounceHarness(t, ctx)
	mr := seedStoodDownApprovedMR(t, ctx, proj, repo, 25, "review-task-25", "clarify-repo-a-25", SweepIssueKind)

	if err := d.DrainStandDownMerge(ctx, proj, repo, mr); err != nil {
		t.Fatal(err)
	}
	tk := getTask(t, "clarify-repo-a-25")
	if tk.Status.Stage != tatarav1alpha1.StageMerging {
		t.Fatalf("normal-kind parked owner Task must be re-driven to merging, got %q", tk.Status.Stage)
	}
	got := getMR(t, ctx, proj, repo, 25)
	if ctrl, ok := ownerControllerName(got); !ok || ctrl != "clarify-repo-a-25" {
		t.Fatalf("control must return to the parked owner Task for the merge, got %q", ctrl)
	}
}

func TestDrainStandDownMerge_IgnoresExternalInitial(t *testing.T) {
	ctx := context.Background()
	d, proj, repo, _ := newAnnounceHarness(t, ctx)
	// external + initial (never taken over) approved -> no parked owner task,
	// no re-drive.
	mr := seedExternalMRWithReviewOwner(t, ctx, proj, repo, 23, "review-task-23")
	setMROwnershipReason(t, ctx, mr, tatarav1alpha1.OwnershipExternal, "initial")
	setMRReviewApproved(t, ctx, mr)
	if err := d.DrainStandDownMerge(ctx, proj, repo, getMR(t, ctx, proj, repo, 23)); err != nil {
		t.Fatal(err)
	}
	if ctrl, _ := ownerControllerName(getMR(t, ctx, proj, repo, 23)); ctrl != "review-task-23" {
		t.Fatalf("an external+initial MR must not be re-driven to a takeover merge; owner=%q", ctrl)
	}
}

// --- envtest fixtures local to the ownership announcement/stand-down tests ---

// newAnnounceHarness builds a StageDriver bound to the package's shared
// envtest client plus a fakeForge/mdReader pair (merge_test.go), so
// DrainOwnershipAnnouncement's forge round-trip (Comment + the thread-marker
// re-read) and DrainStandDownMerge's owner-ref writes both run against real
// primitives.
func newAnnounceHarness(t *testing.T, ctx context.Context) (*StageDriver, *tatarav1alpha1.Project, *tatarav1alpha1.Repository, *fakeForge) {
	t.Helper()
	proj, repo := seedProjectRepo(t, ctx)
	f := newFakeForge(t)
	d := &StageDriver{
		Client:     k8sClient,
		APIReader:  k8sClient,
		SCMFor:     func(string) (scm.SCMWriter, error) { return f, nil },
		ReaderFor:  func(_, _ string) (scm.SCMReader, error) { return mdNewReader(f), nil },
		SpillerFor: func(*tatarav1alpha1.Project) objbudget.Spiller { return &mirrorSpiller{} },
	}
	return d, proj, repo, f
}

// commentCount/lastComment read fakeForge.thread, the same ledger
// postThreadComment and mdReader.ListIssueComments both use - the fake IS the
// forge for these tests.
func (f *fakeForge) commentCount(number int) int { return len(f.thread[number]) }

func (f *fakeForge) lastComment(number int) string {
	th := f.thread[number]
	if len(th) == 0 {
		return ""
	}
	return th[len(th)-1].Body
}

// seedFlippedMR persists an open MergeRequest already carrying a flip's
// aftermath: ownership/reason/changedAt set, as ReconcileOwnership's flip
// writers (flipToExternal, the takeover REST endpoint) leave it.
func seedFlippedMR(t *testing.T, ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, ownership, reason string) *tatarav1alpha1.MergeRequest {
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
			URL:           fmt.Sprintf("https://github.com/o/r/pull/%d", number),
		},
	}
	if err := k8sClient.Create(ctx, mr); err != nil {
		t.Fatalf("create mergerequest %s: %v", mr.Name, err)
	}
	now := metav1.Now()
	mr.Status = tatarav1alpha1.MergeRequestStatus{
		State:              "open",
		Ownership:          ownership,
		OwnershipReason:    reason,
		OwnershipChangedAt: &now,
	}
	if err := k8sClient.Status().Update(ctx, mr); err != nil {
		t.Fatalf("update mergerequest %s status: %v", mr.Name, err)
	}
	return mr
}

// seedStoodDownApprovedMR builds the shape DrainStandDownMerge must act on: a
// stood-down (external + external-push) MR, controller-owned by a kind=review
// Task (reviewTaskName, the flip's hand-back target), with a SECOND, plain
// owner Task (parkedTaskName, of parkedKind - takeover or a normal
// full-lifecycle kind) already parked(ownership-lost) - and an approved
// review pinned to the current head.
func seedStoodDownApprovedMR(t *testing.T, ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, reviewTaskName, parkedTaskName, parkedKind string) *tatarav1alpha1.MergeRequest {
	t.Helper()

	review := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: reviewTaskName, Namespace: proj.Namespace},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj.Name, RepositoryRef: repo.Name, Kind: SweepReviewKind,
			Goal: "review the MR",
		},
	}
	if err := k8sClient.Create(ctx, review); err != nil {
		t.Fatalf("create review task: %v", err)
	}

	parked := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: parkedTaskName, Namespace: proj.Namespace},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj.Name, RepositoryRef: repo.Name, Kind: parkedKind,
			Goal: "push to the MR", MergeOrder: []string{repo.Name},
		},
	}
	if err := k8sClient.Create(ctx, parked); err != nil {
		t.Fatalf("create parked owner task: %v", err)
	}
	stampTaskStatus(t, ctx, parked, tatarav1alpha1.StageParked, stage.ReasonOwnershipLost)

	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.MergeRequestName(repo.Name, number),
			Namespace: proj.Namespace,
		},
		Spec: tatarav1alpha1.MergeRequestSpec{
			RepositoryRef: repo.Name, ProjectRef: proj.Name, Number: number,
			URL: fmt.Sprintf("https://github.com/o/r/pull/%d", number),
		},
	}
	if err := controllerutil.SetControllerReference(review, mr, k8sClient.Scheme()); err != nil {
		t.Fatalf("set controller ref: %v", err)
	}
	own.AddPlainOwner(mr, parked)
	if err := k8sClient.Create(ctx, mr); err != nil {
		t.Fatalf("create mergerequest %s: %v", mr.Name, err)
	}
	sha := "sha-" + strconv.Itoa(number)
	mr.Status = tatarav1alpha1.MergeRequestStatus{
		State:           "open",
		Ownership:       tatarav1alpha1.OwnershipExternal,
		OwnershipReason: "external-push:" + sha,
		HeadSHA:         sha,
		ReviewedSHA:     sha,
		Status:          "approved",
	}
	if err := k8sClient.Status().Update(ctx, mr); err != nil {
		t.Fatalf("update mergerequest %s status: %v", mr.Name, err)
	}
	return mr
}

// setMROwnershipReason stamps mr's ownership/reason directly, bypassing
// ReconcileOwnership's own flip derivation (that derivation is its own
// coverage elsewhere): this fixture only needs to PLACE the MR in an
// external+initial state.
func setMROwnershipReason(t *testing.T, ctx context.Context, mr *tatarav1alpha1.MergeRequest, ownership, reason string) {
	t.Helper()
	var fresh tatarav1alpha1.MergeRequest
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(mr), &fresh); err != nil {
		t.Fatalf("get mergerequest %s: %v", mr.Name, err)
	}
	fresh.Status.Ownership = ownership
	fresh.Status.OwnershipReason = reason
	if err := k8sClient.Status().Update(ctx, &fresh); err != nil {
		t.Fatalf("update mergerequest %s ownership: %v", mr.Name, err)
	}
	*mr = fresh
}

// setMRReviewApproved stamps status.status=approved and reviewedSHA=headSHA
// on mr, the shape clearPendingReview leaves after an accepted approve
// verdict.
func setMRReviewApproved(t *testing.T, ctx context.Context, mr *tatarav1alpha1.MergeRequest) {
	t.Helper()
	var fresh tatarav1alpha1.MergeRequest
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(mr), &fresh); err != nil {
		t.Fatalf("get mergerequest %s: %v", mr.Name, err)
	}
	fresh.Status.Status = "approved"
	fresh.Status.ReviewedSHA = fresh.Status.HeadSHA
	if err := k8sClient.Status().Update(ctx, &fresh); err != nil {
		t.Fatalf("update mergerequest %s review state: %v", mr.Name, err)
	}
	*mr = fresh
}
