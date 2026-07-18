package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/stretchr/testify/require"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
)

// reviewingTask builds a Task already in reviewing, owned by "p", for the
// review-applier tests.
func reviewingTask(name, kind string) *tatarav1alpha1.Task {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "p", Kind: kind, Goal: "g"},
	}
	task.Status.Stage = tatarav1alpha1.StageReviewing
	ent := metav1.NewTime(time.Now().Add(-time.Hour))
	task.Status.StageEnteredAt = &ent
	return task
}

// ownedMR builds a MergeRequest CR, open and controller-owned by taskName, for
// the review-applier tests.
func ownedMR(name, taskName, repoRef string, number int) *tatarav1alpha1.MergeRequest {
	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.MergeRequestSpec{RepositoryRef: repoRef, Number: number, ProjectRef: "p"},
	}
	mr.Status.State = "open"
	owner := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: taskName}}
	own.AddPlainOwner(mr, owner)
	if err := own.HandOverController(mr, nil, owner); err != nil {
		panic(err)
	}
	return mr
}

// changes_requested on a non-terminal implementing-produced Task re-enters
// implementing, when the owned MR is NOT merged.
func TestApplyReviewChangesRequested_ReentersImplementing(t *testing.T) {
	proj := sweepProject("p")
	task := reviewingTask("t1", "clarify")
	mr := ownedMR("mr-tatara-operator-42", "t1", "tatara-operator", 42)
	c := newMirrorClient(t, proj, task, mr)
	reentered, err := ApplyReviewChangesRequested(context.Background(), c, task, time.Now())
	require.NoError(t, err)
	require.True(t, reentered)

	var got tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "t1"}, &got))
	require.Equal(t, tatarav1alpha1.StageImplementing, got.Status.Stage)
}

// changes_requested on a Task whose owned MR is MERGED does NOT rewind.
func TestApplyReviewChangesRequested_MergedMR_NoRewind(t *testing.T) {
	proj := sweepProject("p")
	task := reviewingTask("t2", "clarify")
	mr := ownedMR("mr-tatara-operator-42", "t2", "tatara-operator", 42)
	mr.Status.State = "merged"
	c := newMirrorClient(t, proj, task, mr)
	reentered, err := ApplyReviewChangesRequested(context.Background(), c, task, time.Now())
	require.NoError(t, err)
	require.False(t, reentered, "an already-merged MR is finished; no rewind")

	var got tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "t2"}, &got))
	require.Equal(t, tatarav1alpha1.StageReviewing, got.Status.Stage)
}

// approved on a reviewing non-review Task clears PendingReview and enters merging.
func TestApplyReviewApproval_EntersMerging(t *testing.T) {
	proj := sweepProject("p")
	task := reviewingTask("t3", "clarify")
	mr := ownedMR("mr-tatara-operator-42", "t3", "tatara-operator", 42)
	mr.Status.PendingReview = &tatarav1alpha1.PendingReview{Round: 1} // bot review still owed
	c := newMirrorClient(t, proj, task, mr)
	advanced, err := ApplyReviewApproval(context.Background(), c, nil, proj, task, "deadbeef", time.Now())
	require.NoError(t, err)
	require.True(t, advanced)

	var gotMR tatarav1alpha1.MergeRequest
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "mr-tatara-operator-42"}, &gotMR))
	require.Nil(t, gotMR.Status.PendingReview, "maintainer approval short-circuits the pending bot review")
	require.Equal(t, "approved", gotMR.Status.Status)
	require.Equal(t, "deadbeef", gotMR.Status.ReviewedSHA)

	var got tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "t3"}, &got))
	require.Equal(t, tatarav1alpha1.StageMerging, got.Status.Stage)
}

// approved on a kind=review Task never merges.
func TestApplyReviewApproval_ReviewKind_NoMerge(t *testing.T) {
	proj := sweepProject("p")
	task := reviewingTask("t4", "review")
	mr := ownedMR("mr-tatara-operator-42", "t4", "tatara-operator", 42)
	c := newMirrorClient(t, proj, task, mr)
	advanced, err := ApplyReviewApproval(context.Background(), c, nil, proj, task, "sha", time.Now())
	require.NoError(t, err)
	require.False(t, advanced)
}
