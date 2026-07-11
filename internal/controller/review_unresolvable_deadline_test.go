package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestWriteBackReview_UnresolvableMember_ParksPastDeadline is liveness-hardening
// finding #4: an umbrella review whose member repo URL stays unresolvable used to
// error-loop FOREVER (no deadline, no comment, no park). It must now bound the
// retries with a wall-clock deadline and, on exhaustion, park recoverable with an
// issue comment naming the unresolvable member instead of requeueing indefinitely.
func TestWriteBackReview_UnresolvableMember_ParksPastDeadline(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "rev-unres-deadline", "rud-proj", "rud-repo", "rud-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review the stream",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#5", IsPR: false, Number: 5,
			},
		}, nil)
	// o/r2 is deliberately NOT enrolled: its member URL is unresolvable.
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 9, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
		{Provider: "github", Repo: "o/r2", Number: 21, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
	}
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
	// The resolve-retry window has already elapsed (stamped on an earlier reconcile).
	past := metav1.NewTime(time.Now().Add(-time.Minute))
	task.Status.ReviewResolveDeadline = &past
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err, "past the resolve deadline the review must stop requeueing (no error)")

	got := &tatarav1alpha1.Task{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, got))
	require.Equal(t, "Parked", got.Status.DeployState, "an unresolvable member past deadline must park, not loop forever")
	require.True(t, fw.commentCalled, "the exhausted resolve must post an issue comment naming the stuck member")
	require.Contains(t, fw.commentBody, "o/r2", "the park comment must name the unresolvable member repo")
	require.False(t, fw.approveCalled, "nothing may be approved when a member URL is unresolvable")
}

// TestWriteBackReview_UnresolvableMember_StampsDeadlineFirst: on the FIRST
// unresolvable encounter (no deadline yet) the review stamps the resolve deadline
// and still requeues (error) - it does not park prematurely.
func TestWriteBackReview_UnresolvableMember_StampsDeadlineFirst(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "rev-unres-first", "ruf2-proj", "ruf2-repo", "ruf2-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review the stream",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#5", IsPR: false, Number: 5,
			},
		}, nil)
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 9, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
		{Provider: "github", Repo: "o/r2", Number: 21, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
	}
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.Error(t, err, "first unresolvable encounter still requeues")

	got := &tatarav1alpha1.Task{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, got))
	require.NotNil(t, got.Status.ReviewResolveDeadline, "the resolve deadline must be stamped on first sight")
	require.NotEqual(t, "Parked", got.Status.DeployState, "must not park on the first encounter")
}
