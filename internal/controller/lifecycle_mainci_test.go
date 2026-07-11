package controller

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// ============================================================
// Task 5 - MainCI poll + close
// ============================================================

// lifecycleFakeSCMWriterMainCI controls commit CI and close calls.
type lifecycleFakeSCMWriterMainCI struct {
	lifecycleFakeSCMWriter
	ciStatus     string
	ciErr        error
	closeIssueFn func() error // optional override; nil = success
}

func (f *lifecycleFakeSCMWriterMainCI) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	return scm.PRState{Author: "bot", CIStatus: "success"}, nil
}

func (f *lifecycleFakeSCMWriterMainCI) Merge(_ context.Context, _, _ string, _ int, _ string) (string, error) {
	return "", nil
}

func (f *lifecycleFakeSCMWriterMainCI) GetCommitCIStatus(_ context.Context, _, _, _ string) (string, error) {
	return f.ciStatus, f.ciErr
}

func (f *lifecycleFakeSCMWriterMainCI) CloseIssue(_ context.Context, _, _ string, _ int, comment string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls = append(f.closeCalls, struct{ repo, comment string }{"", comment})
	if f.closeIssueFn != nil {
		return f.closeIssueFn()
	}
	return nil
}

// SCMReaderMainCI satisfies SCMReader for GetCommitCIStatus in the reconciler.
// (The reconciler calls GetCommitCIStatus via ReaderFor.)
type fakeReaderMainCI struct {
	ciStatus string
	ciErr    error
}

func (f *fakeReaderMainCI) ListOpenPRs(_ context.Context, _, _ string) ([]scm.PRRef, error) {
	return nil, nil
}
func (f *fakeReaderMainCI) ListOpenIssues(_ context.Context, _, _ string) ([]scm.IssueRef, error) {
	return nil, nil
}
func (f *fakeReaderMainCI) ListBoardItems(_ context.Context, _ scm.BoardRef) ([]scm.BoardItem, error) {
	return nil, nil
}
func (f *fakeReaderMainCI) GetCommitCIStatus(_ context.Context, _, _, _ string) (string, error) {
	return f.ciStatus, f.ciErr
}
func (f *fakeReaderMainCI) ListIssueComments(_ context.Context, _, _ string, _ int) ([]scm.IssueComment, error) {
	return nil, nil
}
func (f *fakeReaderMainCI) GetIssue(_ context.Context, _, _ string, _ int) (scm.IssueContent, error) {
	return scm.IssueContent{}, nil
}
func (f *fakeReaderMainCI) GetDefaultBranchHeadSHA(_ context.Context, _, _ string) (string, error) {
	return "", nil
}
func (f *fakeReaderMainCI) ListClosedIssues(_ context.Context, _, _ string, _ time.Time) ([]scm.IssueRef, error) {
	return nil, nil
}
func (f *fakeReaderMainCI) ListCommits(_ context.Context, _, _ string, _ time.Time) ([]scm.CommitRef, error) {
	return nil, nil
}

func seedMainCITask(t *testing.T, suffix string, fw *lifecycleFakeSCMWriterMainCI, deadlineOffset time.Duration) (*TaskReconciler, string) {
	t.Helper()
	ctx := context.Background()
	name := "lc-mainci-" + suffix
	proj := "lc-mcp-" + suffix
	repo := "lc-mcr-" + suffix
	sec := "lc-mcs-" + suffix
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#11", URL: "https://github.com/o/r/issues/11",
		Number: 11,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	task.Status.DeployState = "MainCI"
	task.Status.MergeCommitSHA = "deadbeef"
	task.Status.PRNumber = 55
	task.Status.PrURL = "https://github.com/o/r/pull/55"
	if deadlineOffset != 0 {
		dl := metav1.NewTime(time.Now().Add(deadlineOffset))
		task.Status.DeadlineAt = &dl
	}
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed mainci task: %v", err)
	}

	r := newLifecycleReconciler(t, &fw.lifecycleFakeSCMWriter)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &fakeReaderMainCI{ciStatus: fw.ciStatus, ciErr: fw.ciErr}, nil
	}
	return r, name
}

func TestLifecycleMainCI_PendingRequeues(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMainCI{ciStatus: "pending"}
	r, name := seedMainCITask(t, "pending", fw, time.Hour)

	res, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("pending MainCI must requeue")
	}
	got := fetchTask(t, name)
	if got.Status.DeployState != "MainCI" {
		t.Errorf("DeployState = %q, want MainCI on pending", got.Status.DeployState)
	}
}

func TestLifecycleMainCI_SuccessClosesDoneIdempotent(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	// closeIssueFn returns nil (idempotent - issue may already be closed)
	fw := &lifecycleFakeSCMWriterMainCI{ciStatus: "success"}
	r, name := seedMainCITask(t, "success", fw, time.Hour)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.DeployState != "Done" {
		t.Errorf("DeployState = %q, want Done on MainCI success", got.Status.DeployState)
	}
}

// TestLifecycleMainCI_LedgerCloseProjection drives the REAL handleMainCI success
// path and asserts the source issue ledger entry flips to state:closed while a
// cross-repo sibling role:closes entry stays open for the backstop. Exercises the
// best-effort closeSourceIssueLedger projection at its persistence site, not the
// pure helper.
func TestLifecycleMainCI_LedgerCloseProjection(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMainCI{ciStatus: "success"}
	r, name := seedMainCITask(t, "ledgerclose", fw, time.Hour)

	task := fetchTask(t, name)
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 11, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
		{Provider: "github", Repo: "o/sibling", Number: 8, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleCloses, State: tatarav1alpha1.WIOpen},
	}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed workitems: %v", err)
	}

	if _, err := r.reconcileLifecycle(ctx, fetchTask(t, name)); err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.DeployState != "Done" {
		t.Fatalf("DeployState = %q, want Done", got.Status.DeployState)
	}
	var src, sib *tatarav1alpha1.WorkItemRef
	for i := range got.Status.WorkItems {
		wi := &got.Status.WorkItems[i]
		if wi.Repo == "o/r" && wi.Number == 11 {
			src = wi
		}
		if wi.Repo == "o/sibling" && wi.Number == 8 {
			sib = wi
		}
	}
	if src == nil || src.State != tatarav1alpha1.WIClosed {
		t.Errorf("source issue entry must be closed; got %+v", src)
	}
	if sib == nil || sib.State != tatarav1alpha1.WIOpen {
		t.Errorf("cross-repo sibling must stay open for the backstop; got %+v", sib)
	}
}

func TestLifecycleMainCI_SuccessCloseIssueIdempotentOnNotFound(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	// Simulate already-closed: CloseIssue returns a 404 HTTPError (Closes #N
	// in the MR body may have already closed it).
	fw := &lifecycleFakeSCMWriterMainCI{
		ciStatus: "success",
		closeIssueFn: func() error {
			return &scm.HTTPError{Status: 404, Body: "not found", Path: "/issues/close"}
		},
	}
	r, name := seedMainCITask(t, "idem", fw, time.Hour)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle on idempotent close: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.DeployState != "Done" {
		t.Errorf("DeployState = %q, want Done (idempotent CloseIssue)", got.Status.DeployState)
	}
}

func TestLifecycleMainCI_FailureReentersImplement(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMainCI{ciStatus: "failure"}
	r, name := seedMainCITask(t, "failure", fw, time.Hour)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.DeployState != "Implement" {
		t.Errorf("DeployState = %q, want Implement on MainCI failure", got.Status.DeployState)
	}
	if got.Status.ImplementContext == "" {
		t.Error("ImplementContext must be set on MainCI failure")
	}
}

func TestLifecycleMainCI_DeadlineParks(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMainCI{ciStatus: "pending"}
	r, name := seedMainCITask(t, "deadline", fw, -time.Minute) // already past

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.DeployState != "Parked" {
		t.Errorf("DeployState = %q, want Parked on MainCI deadline", got.Status.DeployState)
	}
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.commentCalls) == 0 {
		t.Error("deadline park must post comment")
	}
}

func (f *lifecycleFakeSCMWriterMainCI) EnsureLabel(_ context.Context, _, _, _, _ string) error {
	return nil
}
