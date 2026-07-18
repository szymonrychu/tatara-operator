package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/stretchr/testify/require"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// opRecorder wraps a client.Client and records the ORDER of the operations the
// review appliers care about: the wrapper-pod Delete and the MergeRequest
// PendingReview-clear (a MergeRequest status Update whose PendingReview is nil).
// It backs the F5 pod-delete-before-clear ordering assertion.
type opRecorder struct {
	client.Client
	ops *[]string
}

func (r opRecorder) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*tatarav1alpha1.Task); ok {
		*r.ops = append(*r.ops, "get-task")
	}
	return r.Client.Get(ctx, key, obj, opts...)
}

func (r opRecorder) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if _, ok := obj.(*corev1.Pod); ok {
		*r.ops = append(*r.ops, "delete-pod")
	}
	return r.Client.Delete(ctx, obj, opts...)
}

func (r opRecorder) Status() client.SubResourceWriter {
	return opRecorderStatus{r.Client.Status(), r.ops}
}

type opRecorderStatus struct {
	client.SubResourceWriter
	ops *[]string
}

func (r opRecorderStatus) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if mr, ok := obj.(*tatarav1alpha1.MergeRequest); ok && mr.Status.PendingReview == nil {
		*r.ops = append(*r.ops, "clear-pendingreview")
	}
	return r.SubResourceWriter.Update(ctx, obj, opts...)
}

func opIndex(ops []string, want string) int {
	for i, o := range ops {
		if o == want {
			return i
		}
	}
	return -1
}

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
	reentered, err := ApplyReviewChangesRequested(context.Background(), c, c, proj, task, time.Now())
	require.NoError(t, err)
	require.True(t, reentered)

	var got tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "t1"}, &got))
	require.Equal(t, tatarav1alpha1.StageImplementing, got.Status.Stage)
}

// parkedReviewTask builds a parked Task with the given reason, owned by "p".
func parkedReviewTask(name, kind, reason string) *tatarav1alpha1.Task {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "p", Kind: kind, Goal: "g"},
	}
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = reason
	ent := metav1.NewTime(time.Now().Add(-time.Hour))
	task.Status.StageEnteredAt = &ent
	return task
}

// changes_requested on a parked(merge-timeout) Task resumes MERGING (never
// implementing), accounting one MergeReentries - exactly like Unpark (F1).
func TestApplyReviewChangesRequested_ParkedMergeTimeout_ResumesMerging(t *testing.T) {
	proj := sweepProject("p")
	task := parkedReviewTask("t-mt", "clarify", stage.ReasonMergeTimeout)
	mr := ownedMR("mr-tatara-operator-42", "t-mt", "tatara-operator", 42)
	c := newMirrorClient(t, proj, task, mr)
	reentered, err := ApplyReviewChangesRequested(context.Background(), c, c, proj, task, time.Now())
	require.NoError(t, err)
	require.True(t, reentered)

	var got tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "t-mt"}, &got))
	require.Equal(t, tatarav1alpha1.StageMerging, got.Status.Stage)
	require.Equal(t, 1, got.Status.MergeReentries)
}

// changes_requested on a parked(review-loop-exhausted) Task does NOT re-enter:
// it folds to the pending-event path so a human review cannot escape
// maxReviewRounds one review at a time (F1).
func TestApplyReviewChangesRequested_ParkedReviewLoopExhausted_Folds(t *testing.T) {
	proj := sweepProject("p")
	task := parkedReviewTask("t-rle", "clarify", stage.ReasonReviewLoopExhausted)
	mr := ownedMR("mr-tatara-operator-42", "t-rle", "tatara-operator", 42)
	c := newMirrorClient(t, proj, task, mr)
	reentered, err := ApplyReviewChangesRequested(context.Background(), c, c, proj, task, time.Now())
	require.NoError(t, err)
	require.False(t, reentered, "review-loop-exhausted must not re-enter on a human review")

	var got tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "t-rle"}, &got))
	require.Equal(t, tatarav1alpha1.StageParked, got.Status.Stage)
}

// changes_requested on a Task whose owned MR is MERGED does NOT rewind.
func TestApplyReviewChangesRequested_MergedMR_NoRewind(t *testing.T) {
	proj := sweepProject("p")
	task := reviewingTask("t2", "clarify")
	mr := ownedMR("mr-tatara-operator-42", "t2", "tatara-operator", 42)
	mr.Status.State = "merged"
	c := newMirrorClient(t, proj, task, mr)
	reentered, err := ApplyReviewChangesRequested(context.Background(), c, c, proj, task, time.Now())
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
	advanced, err := ApplyReviewApproval(context.Background(), c, c, nil, proj, task, "deadbeef", time.Now())
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
	advanced, err := ApplyReviewApproval(context.Background(), c, c, nil, proj, task, "sha", time.Now())
	require.NoError(t, err)
	require.False(t, advanced)
}

// F2 regression guard: with DISTINCT cached and live stores - the cached client
// still showing the MR unmerged, the live reader showing it merged - the applier
// must fold on the live view and NOT rewind. A same-store test would pass even if
// the in-loop reload/merged-recheck were deleted; distinct stores catch that.
func TestApplyReviewChangesRequested_MergedInLiveReaderOnly_Folds(t *testing.T) {
	proj := sweepProject("p")

	// Cached store: MR still open, so a cached read would wrongly re-enter.
	cachedTask := reviewingTask("t-f2", "clarify")
	cachedMR := ownedMR("mr-tatara-operator-42", "t-f2", "tatara-operator", 42)
	cached := newMirrorClient(t, proj, cachedTask, cachedMR)

	// Live store: same Task+MR name, but the MR has MERGED.
	liveTask := reviewingTask("t-f2", "clarify")
	liveMR := ownedMR("mr-tatara-operator-42", "t-f2", "tatara-operator", 42)
	liveMR.Status.State = "merged"
	live := newMirrorClient(t, proj, liveTask, liveMR)

	reentered, err := ApplyReviewChangesRequested(context.Background(), cached, live, proj, cachedTask, time.Now())
	require.NoError(t, err)
	require.False(t, reentered, "the live reader shows the MR merged; must fold, not rewind")

	var got tatarav1alpha1.Task
	require.NoError(t, cached.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "t-f2"}, &got))
	require.Equal(t, tatarav1alpha1.StageReviewing, got.Status.Stage, "no rewind on the cached store either")
}

// F5 ordering: the review pod is deleted AFTER the live reviewing-confirm read and
// BEFORE PendingReview is cleared, closing the /outcome re-arm window.
func TestApplyReviewApproval_DeletesPodBeforeClearingPendingReview(t *testing.T) {
	proj := sweepProject("p")
	task := reviewingTask("t-f5a", "clarify")
	mr := ownedMR("mr-tatara-operator-42", "t-f5a", "tatara-operator", 42)
	mr.Status.PendingReview = &tatarav1alpha1.PendingReview{Round: 1}
	base := newMirrorClient(t, proj, task, mr)
	var ops []string
	rec := opRecorder{Client: base, ops: &ops}

	advanced, err := ApplyReviewApproval(context.Background(), rec, rec, nil, proj, task, "deadbeef", time.Now())
	require.NoError(t, err)
	require.True(t, advanced)

	del := opIndex(ops, "delete-pod")
	clear := opIndex(ops, "clear-pendingreview")
	getTask := opIndex(ops, "get-task")
	require.NotEqual(t, -1, del, "the review pod is torn down")
	require.NotEqual(t, -1, clear, "PendingReview is cleared")
	require.NotEqual(t, -1, getTask, "a live reviewing-confirm read happened")
	require.Less(t, getTask, del, "the reviewing-confirm read precedes the pod delete")
	require.Less(t, del, clear, "the pod delete precedes the PendingReview clear (F5)")
}

// F5: an approval that arrives while the owning Task is PARKED (not reviewing)
// folds - no pod delete, no MR mutation - because the live reviewing-confirm read
// short-circuits before either.
func TestApplyReviewApproval_ParkedTask_FoldsWithoutPodDeleteOrMutation(t *testing.T) {
	proj := sweepProject("p")
	task := parkedReviewTask("t-f5b", "clarify", stage.ReasonMergeTimeout)
	mr := ownedMR("mr-tatara-operator-42", "t-f5b", "tatara-operator", 42)
	mr.Status.PendingReview = &tatarav1alpha1.PendingReview{Round: 1}
	base := newMirrorClient(t, proj, task, mr)
	var ops []string
	rec := opRecorder{Client: base, ops: &ops}

	advanced, err := ApplyReviewApproval(context.Background(), rec, rec, nil, proj, task, "deadbeef", time.Now())
	require.NoError(t, err)
	require.False(t, advanced, "approval off reviewing folds")
	require.Equal(t, -1, opIndex(ops, "delete-pod"), "no pod is deleted on the fold")
	require.Equal(t, -1, opIndex(ops, "clear-pendingreview"), "no MR mutation on the fold")

	var gotMR tatarav1alpha1.MergeRequest
	require.NoError(t, base.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "mr-tatara-operator-42"}, &gotMR))
	require.NotNil(t, gotMR.Status.PendingReview, "PendingReview untouched")
}
