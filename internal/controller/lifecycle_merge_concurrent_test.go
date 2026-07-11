package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// concurrentMergeFakeWriter models the double-merge race: Merge returns
// ErrMergeConflict (GitHub 405) because the PR was ALREADY merged concurrently by
// superviseApprovedPRs or the forge's native auto-merge, and GetPRState reports it
// merged.
type concurrentMergeFakeWriter struct {
	lifecycleFakeSCMWriter
}

func (f *concurrentMergeFakeWriter) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	return scm.PRState{Author: "bot", CIStatus: "success", Merged: true, HeadSHA: "mergedsha"}, nil
}

func (f *concurrentMergeFakeWriter) Merge(_ context.Context, _, _ string, _ int, _ string) (string, error) {
	return "", scm.ErrMergeConflict
}

// TestHandleMerge_ConcurrentMergeAdvancesToMainCI is the finding-6 regression: when
// handleMerge's Merge returns ErrMergeConflict but the PR is in fact already merged
// (405 from a double-merge), it must re-fetch GetPRState, recognise the merge, and
// advance to MainCI idempotently - NOT reroll an already-merged change back to
// Implement (which would re-run implement on commits that already landed on main).
func TestHandleMerge_ConcurrentMergeAdvancesToMainCI(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &concurrentMergeFakeWriter{}

	name := "lc-mc-concurrent"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#1", URL: "https://github.com/o/r/issues/1", Number: 1,
	}
	task := seedLifecycleTask(t, name, "lc-mcc-proj", "lc-mcc-repo", "lc-mcc-sec", src)
	dl := metav1.NewTime(time.Now().Add(time.Hour))
	task.Status.DeployState = "Merge"
	task.Status.PRNumber = 512
	task.Status.PrURL = "https://github.com/o/r/pull/512"
	task.Status.HeadBranch = "tatara/task-y"
	task.Status.DeadlineAt = &dl
	require.NoError(t, k8sClient.Status().Update(ctx, task))

	r := newLifecycleReconciler(t, &fw.lifecycleFakeSCMWriter)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	proj := &tatarav1alpha1.Project{}
	require.NoError(t, r.Get(ctx, client.ObjectKey{Namespace: testNS, Name: "lc-mcc-proj"}, proj))

	_, err := r.handleMerge(ctx, proj, fetchTask(t, name))
	require.NoError(t, err, "already-merged path MUST return nil err")

	var got tatarav1alpha1.Task
	require.NoError(t, r.Get(ctx, client.ObjectKey{Namespace: testNS, Name: name}, &got))
	require.Equal(t, "MainCI", got.Status.DeployState,
		"an already-merged PR must advance to MainCI, never reroll to Implement")
	require.NotEmpty(t, got.Status.MergeCommitSHA,
		"the merge SHA must be recorded so a later re-entry short-circuits idempotently")

	// The openedPR ledger entry must reflect the merge.
	var merged bool
	for _, wi := range got.Status.WorkItems {
		if wi.Kind == tatarav1alpha1.WorkItemPR && wi.Number == 512 && wi.State == tatarav1alpha1.WIMerged {
			merged = true
		}
	}
	require.True(t, merged, "the ledger openedPR entry must be flipped to state:merged")
}
