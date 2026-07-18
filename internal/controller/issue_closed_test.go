package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// closedIssueSetup wires a Project/Repository/Task/closed-Issue and returns a
// reconciler over them. The Task owns the closed issue (controller ref +
// IssueRefs).
func closedIssueSetup(t *testing.T, taskStage, taskReason string, issRefs bool) (*IssueReconciler, client.Client, *tatarav1alpha1.Task, string) {
	t.Helper()
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	task := taskAtStage(taskStage, taskReason)
	issName := tatarav1alpha1.IssueName(repo.Name, 1)
	if issRefs {
		task.Status.IssueRefs = []string{issName}
	}
	iss := ownedIssue(issName, 1, task, tatarav1alpha1.IssueStatus{State: "closed"})
	c := newMirrorClient(t, proj, repo, task, iss, scmSecret())
	r := newIssueReconciler(c, &mirrorWriter{}, nil)
	return r, c, task, issName
}

func issueGone(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &tatarav1alpha1.Issue{})
	return apierrors.IsNotFound(err)
}

// TestIssueClosed_LiveStageStops asserts WS3-I3: a human-closed issue whose owner
// Task is in a live source stage stops the Task at rejected(issue-closed),
// deletes the mirror CR, and clears IssueRefs.
func TestIssueClosed_LiveStageStops(t *testing.T) {
	for _, stg := range []string{
		tatarav1alpha1.StageTriaging, tatarav1alpha1.StageClarifying,
		tatarav1alpha1.StageImplementing, tatarav1alpha1.StageReviewing,
		tatarav1alpha1.StageMerging,
	} {
		t.Run(stg, func(t *testing.T) {
			r, c, task, issName := closedIssueSetup(t, stg, "", true)
			reconcileIssue(t, r, issName)

			got := getTaskCR(t, c, task.Name)
			require.Equal(t, tatarav1alpha1.StageRejected, got.Status.Stage)
			require.Equal(t, stage.ReasonIssueClosed, got.Status.StageReason)
			require.NotContains(t, got.Status.IssueRefs, issName)
			require.True(t, issueGone(t, c, issName), "the closed mirror CR must be deleted, not leaked")
		})
	}
}

// TestIssueClosed_DeployingNoStop asserts deploying is excluded: a merged,
// deploying change is not rewound by a late issue close (this is also the
// operator's own C.4 close, which must never be mistaken for a human stop).
func TestIssueClosed_DeployingNoStop(t *testing.T) {
	r, c, task, issName := closedIssueSetup(t, tatarav1alpha1.StageDeploying, "", true)
	reconcileIssue(t, r, issName)

	got := getTaskCR(t, c, task.Name)
	require.Equal(t, tatarav1alpha1.StageDeploying, got.Status.Stage, "deploying is not stopped")
	require.False(t, issueGone(t, c, issName), "the mirror CR survives")
}

// TestIssueClosed_ParkedNoStop asserts a Task already parked when the close lands
// is not stopped (the parked reaper handles its closed issues).
func TestIssueClosed_ParkedNoStop(t *testing.T) {
	r, c, task, issName := closedIssueSetup(t, tatarav1alpha1.StageParked, stage.ReasonBacklogSweep, true)
	reconcileIssue(t, r, issName)

	got := getTaskCR(t, c, task.Name)
	require.Equal(t, tatarav1alpha1.StageParked, got.Status.Stage)
	require.False(t, issueGone(t, c, issName))
}

// TestIssueClosed_ReSeverCompletesAfterCrash asserts review addition (a): a
// rejected(issue-closed) owner Task with a still-present closed CR (the crash
// window: IssueRefs cleared but the CR delete never ran) has the DeleteCR
// finished promptly by the IssueReconciler.
func TestIssueClosed_ReSeverCompletesAfterCrash(t *testing.T) {
	// issRefs=false models the crash state: step 1 (IssueRefs clear) landed,
	// step 2 (CR delete) did not.
	r, c, _, issName := closedIssueSetup(t, tatarav1alpha1.StageRejected, stage.ReasonIssueClosed, false)
	reconcileIssue(t, r, issName)
	require.True(t, issueGone(t, c, issName), "re-sever must finish the interrupted DeleteCR")
}
