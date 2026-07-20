package controller

import (
	"context"
	"fmt"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestEnsureTaskForMRComment_MintsForOrphanOpenMR(t *testing.T) {
	ctx := context.Background()
	proj, repo := seedProjectRepo(t, ctx)
	mr := seedOpenExternalMR(t, ctx, proj, repo, 40, "renovate/foo", "octocat") // no owner, open, human author
	m := newTestMinter(t)

	owner, minted, err := m.EnsureTaskForMRComment(ctx, proj, repo, mr, "octocat")
	if err != nil {
		t.Fatal(err)
	}
	if !minted || owner == "" {
		t.Fatalf("orphan open MR comment must mint a review task, got owner=%q minted=%v", owner, minted)
	}
	got := getMR(t, ctx, proj, repo, 40)
	if ctrl, ok := ownerControllerName(got); !ok || ctrl != owner {
		t.Fatalf("minted review task must controller-own the MR; owner=%q ctrl=%q", owner, ctrl)
	}
}

func TestEnsureTaskForMRComment_ReturnsExistingOwner(t *testing.T) {
	ctx := context.Background()
	proj, repo := seedProjectRepo(t, ctx)
	mr := seedExternalMRWithReviewOwner(t, ctx, proj, repo, 41, "review-task")
	m := newTestMinter(t)

	owner, minted, err := m.EnsureTaskForMRComment(ctx, proj, repo, mr, "octocat")
	if err != nil {
		t.Fatal(err)
	}
	if minted || owner != "review-task" {
		t.Fatalf("existing owner must be returned unchanged, got owner=%q minted=%v", owner, minted)
	}
}

func TestEnsureTaskForMRComment_SkipsBotAuthor(t *testing.T) {
	ctx := context.Background()
	proj, repo := seedProjectRepo(t, ctx)
	mr := seedOpenExternalMR(t, ctx, proj, repo, 42, "renovate/x", "octocat")
	m := newTestMinter(t)

	owner, minted, err := m.EnsureTaskForMRComment(ctx, proj, repo, mr, proj.Spec.Scm.BotLogin)
	if err != nil || minted || owner != "" {
		t.Fatalf("a bot-authored comment must never mint: owner=%q minted=%v err=%v", owner, minted, err)
	}
	_ = tatarav1alpha1.OwnershipTatara
}

func TestEnsureTaskForMRComment_SkipsClosedMR(t *testing.T) {
	ctx := context.Background()
	proj, repo := seedProjectRepo(t, ctx)
	mr := seedOpenExternalMR(t, ctx, proj, repo, 43, "renovate/y", "octocat")
	setMRState(t, ctx, mr, "merged")
	m := newTestMinter(t)

	owner, minted, err := m.EnsureTaskForMRComment(ctx, proj, repo, getMR(t, ctx, proj, repo, 43), "octocat")
	if err != nil || minted || owner != "" {
		t.Fatalf("a closed/merged MR must never mint: owner=%q minted=%v err=%v", owner, minted, err)
	}
}

// --- envtest fixtures local to the ensure-task tests ---

// seedExternalMRWithReviewOwner persists an already-owned MergeRequest mirror:
// a minimal review-kind Task named ownerName controller-owns a fresh MR CR for
// (repo, number). It is the "not orphan" counterpart to seedOpenExternalMR
// (which returns an UNPERSISTED value the caller's own mint is expected to
// bind), used to cover EnsureTaskForMRComment's short-circuit read path.
func seedExternalMRWithReviewOwner(t *testing.T, ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, ownerName string) *tatarav1alpha1.MergeRequest {
	t.Helper()
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: ownerName, Namespace: proj.Namespace},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj.Name,
			Kind:       SweepReviewKind,
			Goal:       fmt.Sprintf("Review external change #%d", number),
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create owner task %s: %v", ownerName, err)
	}
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
	if err := controllerutil.SetControllerReference(task, mr, k8sClient.Scheme()); err != nil {
		t.Fatalf("set controller ref: %v", err)
	}
	if err := k8sClient.Create(ctx, mr); err != nil {
		t.Fatalf("create mergerequest %s: %v", mr.Name, err)
	}
	mr.Status = tatarav1alpha1.MergeRequestStatus{
		Author: "octocat", State: "open", HeadBranch: "renovate/foo", HeadSHA: fmt.Sprintf("sha-%d", number),
	}
	if err := k8sClient.Status().Update(ctx, mr); err != nil {
		t.Fatalf("update mergerequest %s status: %v", mr.Name, err)
	}
	return mr
}

// setMRState persists mr (creating it if it does not exist yet) with
// Status.State overridden to state. seedOpenExternalMR's return value is an
// UNPERSISTED fixture (the mint under test is what is expected to create it),
// so a test exercising the "already closed/merged" gate has to write it into
// the cluster itself before EnsureTaskForMRComment's caller re-reads it via
// getMR.
func setMRState(t *testing.T, ctx context.Context, mr *tatarav1alpha1.MergeRequest, state string) {
	t.Helper()
	status := mr.Status
	status.State = state
	if err := k8sClient.Create(ctx, mr); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create mergerequest %s: %v", mr.Name, err)
	}
	var fresh tatarav1alpha1.MergeRequest
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(mr), &fresh); err != nil {
		t.Fatalf("get mergerequest %s: %v", mr.Name, err)
	}
	fresh.Status = status
	if err := k8sClient.Status().Update(ctx, &fresh); err != nil {
		t.Fatalf("update mergerequest %s status: %v", mr.Name, err)
	}
	*mr = fresh
}
