package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// ============================================================
// Task 3 - MRCI poll state
// ============================================================

// lifecycleFakeSCMWriterMRCI extends lifecycleFakeSCMWriter with GetPRState control.
type lifecycleFakeSCMWriterMRCI struct {
	lifecycleFakeSCMWriter
	prState scm.PRState
	prErr   error
}

func (f *lifecycleFakeSCMWriterMRCI) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.prState, f.prErr
}

func (f *lifecycleFakeSCMWriterMRCI) Merge(_ context.Context, _, _ string, _ int, _ string) (string, error) {
	return "", nil
}

func seedMRCITask(t *testing.T, suffix string, prState scm.PRState, deadlineOffset time.Duration) (*TaskReconciler, *lifecycleFakeSCMWriterMRCI, string) {
	t.Helper()
	ctx := context.Background()
	name := "lc-mrci-" + suffix
	proj := "lc-mrcip-" + suffix
	repo := "lc-mrcir-" + suffix
	sec := "lc-mrcis-" + suffix
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#8", URL: "https://github.com/o/r/issues/8",
		Number: 8,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	task.Status.DeployState = "MRCI"
	task.Status.PRNumber = 42
	task.Status.PrURL = "https://github.com/o/r/pull/42"
	task.Status.HeadBranch = "tatara/task-" + name
	if deadlineOffset != 0 {
		dl := metav1.NewTime(time.Now().Add(deadlineOffset))
		task.Status.DeadlineAt = &dl
	}
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed mrci task: %v", err)
	}

	fw := &lifecycleFakeSCMWriterMRCI{prState: prState}
	r := newLifecycleReconciler(t, &fw.lifecycleFakeSCMWriter)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	return r, fw, name
}

func TestLifecycleMRCI_PendingRequeues(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, name := seedMRCITask(t, "pending", scm.PRState{Author: "bot", CIStatus: "pending"}, time.Hour)

	res, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("pending CI must requeue")
	}
	got := fetchTask(t, name)
	if got.Status.DeployState != "MRCI" {
		t.Errorf("DeployState = %q, want MRCI on pending CI", got.Status.DeployState)
	}
}

func TestLifecycleMRCI_SuccessTransitionsToMerge(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, name := seedMRCITask(t, "success", scm.PRState{Author: "bot", CIStatus: "success"}, time.Hour)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.DeployState != "Merge" {
		t.Errorf("DeployState = %q, want Merge on CI success", got.Status.DeployState)
	}
	if got.Status.DeadlineAt != nil {
		t.Error("DeadlineAt must be cleared on transition out of MRCI")
	}
}

func TestLifecycleMRCI_FailureSetsContextAndReentersImplement(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, name := seedMRCITask(t, "failure", scm.PRState{Author: "bot", CIStatus: "failure"}, time.Hour)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.DeployState != "Implement" {
		t.Errorf("DeployState = %q, want Implement on CI failure", got.Status.DeployState)
	}
	if got.Status.ImplementContext == "" {
		t.Error("ImplementContext must be set on MRCI failure")
	}
	if !strings.Contains(got.Status.ImplementContext, "pipeline") && !strings.Contains(got.Status.ImplementContext, "MR") && !strings.Contains(got.Status.ImplementContext, "CI") {
		t.Errorf("ImplementContext = %q, should mention pipeline/CI failure", got.Status.ImplementContext)
	}
}

func TestLifecycleMRCI_NoCITransitionsToMerge(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	// CIStatus="" means no CI configured
	r, _, name := seedMRCITask(t, "noci", scm.PRState{Author: "bot", CIStatus: ""}, time.Hour)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.DeployState != "Merge" {
		t.Errorf("DeployState = %q, want Merge when no CI", got.Status.DeployState)
	}
}

func TestLifecycleMRCI_DeadlineParks(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	// deadline already passed (negative offset)
	r, fw, name := seedMRCITask(t, "deadline", scm.PRState{Author: "bot", CIStatus: "pending"}, -time.Minute)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.DeployState != "Parked" {
		t.Errorf("DeployState = %q, want Parked on deadline", got.Status.DeployState)
	}
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.commentCalls) == 0 {
		t.Error("deadline park must post a comment")
	}
}

func TestLifecycleMRCI_NonBotAuthorParks(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, fw, name := seedMRCITask(t, "notbot", scm.PRState{Author: "someuser", CIStatus: "pending"}, time.Hour)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.DeployState != "Parked" {
		t.Errorf("DeployState = %q, want Parked (non-bot author)", got.Status.DeployState)
	}
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.commentCalls) == 0 {
		t.Error("non-bot author park must post a comment")
	}
}
