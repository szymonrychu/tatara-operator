package controller

// Task 9: conflict self-heal via merge-not-rebase resolve-or-close escalation.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// mergeConflictFakeWriter returns ErrMergeConflict from Merge and a bot-authored
// open PR with success CI from GetPRState. Extends lifecycleFakeSCMWriter so
// newLifecycleReconciler can wire it.
type mergeConflictFakeWriter struct {
	lifecycleFakeSCMWriter
}

func (f *mergeConflictFakeWriter) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	return scm.PRState{Author: "bot", CIStatus: "success", Closed: false}, nil
}

func (f *mergeConflictFakeWriter) Merge(_ context.Context, _, _ string, _ int, _ string) (string, error) {
	return "", scm.ErrMergeConflict
}

// seedMergeConflictTask creates a task in LifecycleState=Merge with PR #267 and
// HeadBranch set. Returns the reconciler, the project, and the task name.
func seedMergeConflictTask(t *testing.T) (*TaskReconciler, *tatarav1alpha1.Project, string) {
	t.Helper()
	ctx := context.Background()
	fw := &mergeConflictFakeWriter{}

	name := "lc-mc-conflict"
	projName := "lc-mc-proj"
	repoName := "lc-mc-repo"
	secName := "lc-mc-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#1", URL: "https://github.com/o/r/issues/1",
		Number: 1,
	}
	task := seedLifecycleTask(t, name, projName, repoName, secName, src)

	// Set up as a Merge-state task with PR #267.
	dl := metav1.NewTime(time.Now().Add(time.Hour))
	task.Status.LifecycleState = "Merge"
	task.Status.PRNumber = 267
	task.Status.PrURL = "https://github.com/o/r/pull/267"
	task.Status.HeadBranch = "tatara/task-x"
	task.Status.DeadlineAt = &dl
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed merge conflict task status: %v", err)
	}

	r := newLifecycleReconciler(t, &fw.lifecycleFakeSCMWriter)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	proj := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: testNS, Name: projName}, proj); err != nil {
		t.Fatalf("get project %s: %v", projName, err)
	}
	return r, proj, name
}

// TestHandleMerge_ConflictSeedsMergeNotRebaseContext asserts that ErrMergeConflict
// from Merge produces an ImplementContext instructing merge-not-rebase, carries
// the PR ref, and offers the superseded close path. No error must be returned
// (controller-runtime backoff live-loop guard).
func TestHandleMerge_ConflictSeedsMergeNotRebaseContext(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, proj, name := seedMergeConflictTask(t)

	task := fetchTask(t, name)
	_, err := r.handleMerge(ctx, proj, task)
	require.NoError(t, err, "conflict path MUST return nil err (no controller-runtime backoff)")

	var got tatarav1alpha1.Task
	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(task), &got))
	assert.Equal(t, "Implement", got.Status.LifecycleState)

	ctxMsg := got.Status.ImplementContext
	assert.Contains(t, ctxMsg, "git merge origin/", "must instruct merge, not rebase")
	assert.NotContains(t, ctxMsg, "rebase", "must never instruct a rebase (force-push is denied)")
	assert.Contains(t, ctxMsg, "pr_outcome", "must offer the close signal")
	assert.Contains(t, ctxMsg, "superseded", "must offer the superseded fast path")
	assert.Contains(t, ctxMsg, "#267", "must carry the PR ref")
}
