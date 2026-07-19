package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
)

// severTask builds a Task carrying issName in Status.IssueRefs at the given
// stage/reason.
func severTask(stg, reason string, issRefs ...string) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-1", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj"},
		Status:     tatarav1alpha1.TaskStatus{Stage: stg, StageReason: reason, IssueRefs: issRefs},
	}
}

func TestSeverIssueFromTask_DeleteCR_LeavesNoCRAndNoRef(t *testing.T) {
	ctx := context.Background()
	issName := tatarav1alpha1.IssueName("tatara-operator", 1)
	task := severTask(tatarav1alpha1.StageRejected, "issue-closed", issName)
	iss := ownedIssue(issName, 1, task, tatarav1alpha1.IssueStatus{State: "closed"})
	c := newMirrorClient(t, task, iss)

	require.NoError(t, SeverIssueFromTask(ctx, c, task.DeepCopy(), issName, SeverDeleteCR))

	// IssueRefs entry gone.
	fresh := getTaskCR(t, c, task.Name)
	require.NotContains(t, fresh.Status.IssueRefs, issName)
	// CR deleted (no leak).
	err := c.Get(ctx, types.NamespacedName{Namespace: testNS, Name: issName}, &tatarav1alpha1.Issue{})
	require.True(t, apierrors.IsNotFound(err), "DeleteCR must delete the mirror CR")
}

func TestSeverIssueFromTask_Orphan_LeavesOwnerlessCRNoParkedLabel(t *testing.T) {
	ctx := context.Background()
	issName := tatarav1alpha1.IssueName("tatara-operator", 2)
	task := severTask(tatarav1alpha1.StageParked, "review-loop-exhausted", issName)
	iss := ownedIssue(issName, 2, task, tatarav1alpha1.IssueStatus{
		State: "open", Labels: []string{TataraParkedLabel, "bug"},
	})
	c := newMirrorClient(t, task, iss)

	require.NoError(t, SeverIssueFromTask(ctx, c, task.DeepCopy(), issName, SeverOrphan))

	fresh := getTaskCR(t, c, task.Name)
	require.NotContains(t, fresh.Status.IssueRefs, issName)
	got := getIssueCR(t, c, issName)
	_, owned := own.ControllerOwner(got)
	require.False(t, owned, "Orphan must leave the CR ownerless")
	require.NotContains(t, got.Status.Labels, TataraParkedLabel, "Orphan must strip the mirror tatara-parked label")
	require.Contains(t, got.Status.Labels, "bug", "other labels are preserved")
}

// TestSeverIssueFromTask_CrashBetweenStepsBenign asserts the Task-side-first
// ordering: with IssueRefs already cleared (step 1 done, step 2 not), the CR
// still carries a valid controller owner (no B.2-rule-5 violation) and a re-run
// completes the delete idempotently.
func TestSeverIssueFromTask_CrashBetweenStepsBenign(t *testing.T) {
	ctx := context.Background()
	issName := tatarav1alpha1.IssueName("tatara-operator", 3)
	// Crash state: the Task no longer lists the issue, but the CR is still owned.
	task := severTask(tatarav1alpha1.StageRejected, "issue-closed")
	iss := ownedIssue(issName, 3, task, tatarav1alpha1.IssueStatus{State: "closed"})
	c := newMirrorClient(t, task, iss)

	// The CR keeps a valid controller owner: ownedIssues would skip it (not listed
	// by the Task), so no spurious terminal comment / label re-stamp.
	_, owned := own.ControllerOwner(getIssueCR(t, c, issName))
	require.True(t, owned)

	// Re-run completes step 2 idempotently.
	require.NoError(t, SeverIssueFromTask(ctx, c, task.DeepCopy(), issName, SeverDeleteCR))
	err := c.Get(ctx, types.NamespacedName{Namespace: testNS, Name: issName}, &tatarav1alpha1.Issue{})
	require.True(t, apierrors.IsNotFound(err))
}

// TestSeverIssueFromTask_Orphan_HandsOverToSurvivingOwner asserts the B.2
// rule-5 fix: when a second, still-live Task holds a plain ownerRef on the
// Issue, SeverOrphan must NOT leave the CR with zero controller owners. It
// hands the controller flag to that surviving Task instead of bare-dropping
// the severed Task's ref.
func TestSeverIssueFromTask_Orphan_HandsOverToSurvivingOwner(t *testing.T) {
	ctx := context.Background()
	issName := tatarav1alpha1.IssueName("tatara-operator", 5)
	taskA := severTask(tatarav1alpha1.StageParked, "review-loop-exhausted", issName)
	taskB := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-2", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj"},
	}
	iss := ownedIssue(issName, 5, taskA, tatarav1alpha1.IssueStatus{
		State: "open", Labels: []string{TataraParkedLabel, "bug"},
	})
	require.True(t, own.AddPlainOwner(iss, taskB))
	c := newMirrorClient(t, taskA, taskB, iss)

	require.NoError(t, SeverIssueFromTask(ctx, c, taskA.DeepCopy(), issName, SeverOrphan))

	fresh := getTaskCR(t, c, taskA.Name)
	require.NotContains(t, fresh.Status.IssueRefs, issName)

	got := getIssueCR(t, c, issName)
	owner, owned := own.ControllerOwner(got)
	require.True(t, owned, "Orphan must hand the controller flag to the surviving owner, not leave the CR ownerless")
	require.Equal(t, taskB.Name, owner)
	require.NotContains(t, got.Status.Labels, TataraParkedLabel, "Orphan must strip the mirror tatara-parked label")
	require.Contains(t, got.Status.Labels, "bug", "other labels are preserved")
}

func TestSeverIssueFromTask_Idempotent(t *testing.T) {
	ctx := context.Background()
	issName := tatarav1alpha1.IssueName("tatara-operator", 4)
	task := severTask(tatarav1alpha1.StageRejected, "issue-closed", issName)
	iss := ownedIssue(issName, 4, task, tatarav1alpha1.IssueStatus{State: "closed"})
	c := newMirrorClient(t, task, iss)

	require.NoError(t, SeverIssueFromTask(ctx, c, task.DeepCopy(), issName, SeverDeleteCR))
	// Second call: CR already gone, ref already cleared - a clean no-op.
	require.NoError(t, SeverIssueFromTask(ctx, c, task.DeepCopy(), issName, SeverDeleteCR))
}
