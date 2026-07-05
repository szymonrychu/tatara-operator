package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// ============================================================
// Task 4 - Merge state + 405 regression guard
// ============================================================

// lifecycleFakeSCMWriterMerge extends the base fake with controlled Merge behaviour.
type lifecycleFakeSCMWriterMerge struct {
	lifecycleFakeSCMWriter
	prState  scm.PRState
	mergeSHA string
	mergeErr error
}

func (f *lifecycleFakeSCMWriterMerge) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.prState, nil
}

func (f *lifecycleFakeSCMWriterMerge) Merge(_ context.Context, _, _ string, _ int, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mergeSHA, f.mergeErr
}

func seedMergeTask(t *testing.T, suffix string, fw *lifecycleFakeSCMWriterMerge, deadlineOffset time.Duration) (*TaskReconciler, string) {
	t.Helper()
	ctx := context.Background()
	name := "lc-merge-" + suffix
	proj := "lc-mergep-" + suffix
	repo := "lc-merger-" + suffix
	sec := "lc-merges-" + suffix
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#9", URL: "https://github.com/o/r/issues/9",
		Number: 9,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	task.Status.LifecycleState = "Merge"
	task.Status.PRNumber = 42
	task.Status.PrURL = "https://github.com/o/r/pull/42"
	task.Status.HeadBranch = "tatara/task-" + name
	if deadlineOffset != 0 {
		dl := metav1.NewTime(time.Now().Add(deadlineOffset))
		task.Status.DeadlineAt = &dl
	}
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed merge task: %v", err)
	}

	r := newLifecycleReconciler(t, &fw.lifecycleFakeSCMWriter)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	return r, name
}

func TestLifecycleMerge_AllowedOK_TransitionsToMainCIWithSHA(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMerge{
		prState:  scm.PRState{Author: "bot", CIStatus: "success"},
		mergeSHA: "abc123sha",
	}
	r, name := seedMergeTask(t, "ok", fw, time.Hour)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "MainCI" {
		t.Errorf("LifecycleState = %q, want MainCI on successful merge", got.Status.LifecycleState)
	}
	if got.Status.MergeCommitSHA != "abc123sha" {
		t.Errorf("MergeCommitSHA = %q, want abc123sha", got.Status.MergeCommitSHA)
	}
	if got.Status.DeadlineAt != nil {
		t.Error("DeadlineAt must be cleared after merge")
	}
}

// TestLifecycleMerge_LedgerOpenedPRFlipsMerged drives the REAL handleMerge path
// (not the pure UpsertWorkItem helper) and asserts the role:openedPR ledger entry
// is flipped to state:merged with the right repo slug/number at the RetryOnConflict
// persistence site - exercising repoSlugFromURL, the mergeRepoSlug/number guards,
// and the placement before setLifecycleState.
func TestLifecycleMerge_LedgerOpenedPRFlipsMerged(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMerge{
		prState:  scm.PRState{Author: "bot", CIStatus: "success"},
		mergeSHA: "mergedsha1",
	}
	r, name := seedMergeTask(t, "ledger", fw, time.Hour)

	// Seed an openedPR ledger entry (number 42 matches seedMergeTask PRNumber) plus
	// a source issue entry to assert the source stays untouched by the merge.
	task := fetchTask(t, name)
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 42, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen},
		{Provider: "github", Repo: "o/r", Number: 9, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
	}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed workitems: %v", err)
	}

	if _, err := r.reconcileLifecycle(ctx, fetchTask(t, name)); err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, name)
	var pr, src *tatarav1alpha1.WorkItemRef
	for i := range got.Status.WorkItems {
		switch got.Status.WorkItems[i].Role {
		case tatarav1alpha1.RoleOpenedPR:
			pr = &got.Status.WorkItems[i]
		case tatarav1alpha1.RoleSource:
			src = &got.Status.WorkItems[i]
		}
	}
	if pr == nil {
		t.Fatal("openedPR ledger entry missing after merge")
	}
	if pr.State != tatarav1alpha1.WIMerged {
		t.Errorf("openedPR state = %q, want merged", pr.State)
	}
	if pr.Repo != "o/r" || pr.Number != 42 {
		t.Errorf("openedPR ledger entry = {%s, %d}, want {o/r, 42}", pr.Repo, pr.Number)
	}
	if src == nil || src.State != tatarav1alpha1.WIOpen {
		t.Error("source issue entry must remain open after merge")
	}
}

// TestLifecycleMerge_405ConflictSpawnsResolveAttempt_ErrNil is the explicit
// live-loop guard: a 405 from Merge must NOT return an error to controller-runtime
// (which would trigger exponential backoff), and must transition to Implement with
// ImplementContext set.
func TestLifecycleMerge_405ConflictSpawnsResolveAttempt_ErrNil(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMerge{
		prState:  scm.PRState{Author: "bot", CIStatus: "success"},
		mergeErr: scm.ErrMergeConflict,
	}
	r, name := seedMergeTask(t, "405", fw, time.Hour)

	result, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	// THE CRITICAL ASSERTION: err must be nil (no controller-runtime backoff loop).
	if err != nil {
		t.Errorf("405 conflict must NOT return error (live-loop guard): got err = %v", err)
	}
	_ = result

	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Implement" {
		t.Errorf("LifecycleState = %q, want Implement (spawn resolve attempt)", got.Status.LifecycleState)
	}
	if got.Status.ImplementContext == "" {
		t.Error("ImplementContext must be set for conflict resolve instruction")
	}
	if !strings.Contains(got.Status.ImplementContext, "conflict") && !strings.Contains(got.Status.ImplementContext, "rebase") {
		t.Errorf("ImplementContext = %q; should mention conflict/rebase", got.Status.ImplementContext)
	}
}

// lifecycleFakeSCMWriterMergeLabel extends the merge fake to record the semver
// EnsureLabel/AddLabel calls the push-CD gate (issue #229) makes before merging.
type lifecycleFakeSCMWriterMergeLabel struct {
	lifecycleFakeSCMWriterMerge
	ensured []string
	added   []struct{ ref, label string }
}

func (f *lifecycleFakeSCMWriterMergeLabel) EnsureLabel(_ context.Context, _, _, name, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensured = append(f.ensured, name)
	return nil
}

func (f *lifecycleFakeSCMWriterMergeLabel) AddLabel(_ context.Context, _, ref, label string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.added = append(f.added, struct{ ref, label string }{ref, label})
	return nil
}

// TestLifecycleMerge_StampsSemverLabelBeforeMerge asserts the lifecycle Merge
// phase stamps the declared-significance semver label on the PR before merging
// (issue #229): a directly-merged bot PR must carry a semver:<level> label so
// push-CD's release tag step does not fail closed.
func TestLifecycleMerge_StampsSemverLabelBeforeMerge(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMergeLabel{
		lifecycleFakeSCMWriterMerge: lifecycleFakeSCMWriterMerge{
			prState:  scm.PRState{Author: "bot", CIStatus: "success"},
			mergeSHA: "sha-labeled",
		},
	}
	r, name := seedMergeTask(t, "semverlabel", &fw.lifecycleFakeSCMWriterMerge, time.Hour)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	// Declare a minor significance so the stamp reads it (not the patch default).
	task := fetchTask(t, name)
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{Significance: "minor"}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed change summary: %v", err)
	}

	if _, err := r.reconcileLifecycle(ctx, fetchTask(t, name)); err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.LifecycleState != "MainCI" {
		t.Errorf("LifecycleState = %q, want MainCI on merge", got.Status.LifecycleState)
	}
	if len(fw.added) != 1 || fw.added[0].label != "semver:minor" || fw.added[0].ref != "o/r#42" {
		t.Errorf("added labels = %+v, want one semver:minor on o/r#42", fw.added)
	}
	found := false
	for _, e := range fw.ensured {
		if e == "semver:minor" {
			found = true
		}
	}
	if !found {
		t.Errorf("ensured labels = %v, want semver:minor ensured on the repo", fw.ensured)
	}
}

// TestLifecycleMerge_DefaultsSemverPatchWhenNoSignificance asserts that when the
// task declared no change significance, the Merge phase defaults the label to
// semver:patch (issue #229) rather than leaving the PR unlabeled.
func TestLifecycleMerge_DefaultsSemverPatchWhenNoSignificance(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMergeLabel{
		lifecycleFakeSCMWriterMerge: lifecycleFakeSCMWriterMerge{
			prState:  scm.PRState{Author: "bot", CIStatus: "success"},
			mergeSHA: "sha-patch",
		},
	}
	r, name := seedMergeTask(t, "semverpatch", &fw.lifecycleFakeSCMWriterMerge, time.Hour)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	// No ChangeSummary on the task (default seeded state) -> expect patch.
	if _, err := r.reconcileLifecycle(ctx, fetchTask(t, name)); err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	if len(fw.added) != 1 || fw.added[0].label != "semver:patch" || fw.added[0].ref != "o/r#42" {
		t.Errorf("added labels = %+v, want one semver:patch on o/r#42", fw.added)
	}
}

func TestLifecycleMerge_NotAllowedRequeues(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	// autoMergeOnGreenCI with pending CI -> mergeAllowed=false
	fw := &lifecycleFakeSCMWriterMerge{
		prState: scm.PRState{Author: "bot", CIStatus: "pending"},
	}
	r, name := seedMergeTask(t, "notallowed", fw, time.Hour)
	// Set autoMergeOnGreenCI policy on project.
	proj := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "lc-mergep-notallowed"}, proj); err != nil {
		t.Fatalf("get project: %v", err)
	}
	proj.Spec.Scm.MergePolicy = "autoMergeOnGreenCI"
	if err := k8sClient.Update(context.Background(), proj); err != nil {
		t.Fatalf("update project policy: %v", err)
	}

	res, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("not-allowed merge must requeue")
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Merge" {
		t.Errorf("LifecycleState = %q, want Merge (not allowed, requeue)", got.Status.LifecycleState)
	}
}

func TestLifecycleMerge_TransientErrorRequeues(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMerge{
		prState:  scm.PRState{Author: "bot", CIStatus: "success"},
		mergeErr: &scm.HTTPError{Status: 503, Body: "service unavailable", Path: "/merge"},
	}
	r, name := seedMergeTask(t, "transient", fw, time.Hour)

	res, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("transient merge error must requeue")
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Merge" {
		t.Errorf("LifecycleState = %q, want Merge (transient)", got.Status.LifecycleState)
	}
}

func TestLifecycleMerge_TransientDeadlineParks(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMerge{
		prState:  scm.PRState{Author: "bot", CIStatus: "success"},
		mergeErr: &scm.HTTPError{Status: 503, Body: "unavailable", Path: "/merge"},
	}
	r, name := seedMergeTask(t, "trans-dl", fw, -time.Minute) // deadline already passed

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked (transient error + deadline)", got.Status.LifecycleState)
	}
}
