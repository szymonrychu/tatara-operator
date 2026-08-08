package controller

// These two tests exercise task_stage.go's enqueueDeployTimeoutComment. They
// lived in resume_deploy_test.go, which #521 deleted with resume.go; the
// function they test is unrelated to resume and losing them would be a silent
// coverage regression.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

var nowStamp = metav1.Now()

func deployTimeoutTask(name string, issName string) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", RepositoryRef: "tatara-operator"},
		Status: tatarav1alpha1.TaskStatus{
			State: tatarav1alpha1.StateDeployed, ParkReason: stage.ReasonDeployTimeout,
			ParkedAt: &nowStamp, ParkedFromState: tatarav1alpha1.StateDeployed,
			IssueRefs: []string{issName}, DeployReentries: 1,
		},
	}
}

func TestEnqueueDeployTimeoutComment_FirstOnly(t *testing.T) {
	ctx := context.Background()
	proj := mirrorProject("tatara-bot")
	issName := tatarav1alpha1.IssueName("tatara-operator", 1)
	task := deployTimeoutTask("t-deploy", issName)
	iss := ownedIssue(issName, 1, task, tatarav1alpha1.IssueStatus{State: "open"})
	mr := ownedMR(tatarav1alpha1.MergeRequestName("tatara-operator", 42), "t-deploy", "tatara-operator", 42)
	mr.Status.State = "merged" // merged but not deployed
	c := newMirrorClient(t, proj, task, iss, mr)
	r := &TaskReconciler{Client: c}
	mrs := []tatarav1alpha1.MergeRequest{*mr}
	now := time.Now()

	require.NoError(t, enqueueDeployTimeoutComment(ctx, r.Client, r.spiller(proj), task, mrs, now))
	got := getIssueCR(t, c, issName)
	require.Len(t, got.Status.PendingComments, 1)
	require.NotNil(t, got.Status.LastDeployTimeoutCommentAt)
	require.Contains(t, got.Status.PendingComments[0].Body, "tatara-operator")

	// A second timeout retry must NOT enqueue a duplicate (its own cooldown marker).
	require.NoError(t, enqueueDeployTimeoutComment(ctx, r.Client, r.spiller(proj), task, mrs, now.Add(time.Hour)))
	got2 := getIssueCR(t, c, issName)
	require.Len(t, got2.Status.PendingComments, 1, "own cooldown: no duplicate on the deploy-timeout retry")
}

// TestEnqueueDeployTimeoutComment_DoesNotClobberRefireCooldown proves the DISTINCT
// marker: an issue that is also an incident tracker keeps its LastRefireCommentAt.
func TestEnqueueDeployTimeoutComment_DoesNotClobberRefireCooldown(t *testing.T) {
	ctx := context.Background()
	proj := mirrorProject("tatara-bot")
	issName := tatarav1alpha1.IssueName("tatara-operator", 1)
	task := deployTimeoutTask("t-deploy2", issName)
	refireAt := metav1.NewTime(time.Now().Add(-time.Minute))
	iss := ownedIssue(issName, 1, task, tatarav1alpha1.IssueStatus{
		State: "open", RefireCount: 3, LastRefireCommentAt: &refireAt,
	})
	mr := ownedMR(tatarav1alpha1.MergeRequestName("tatara-operator", 42), "t-deploy2", "tatara-operator", 42)
	mr.Status.State = "merged"
	c := newMirrorClient(t, proj, task, iss, mr)
	r := &TaskReconciler{Client: c}

	require.NoError(t, enqueueDeployTimeoutComment(ctx, r.Client, r.spiller(proj), task, []tatarav1alpha1.MergeRequest{*mr}, time.Now()))
	got := getIssueCR(t, c, issName)
	require.NotNil(t, got.Status.LastDeployTimeoutCommentAt, "the deploy-timeout marker is set")
	require.Equal(t, refireAt.Unix(), got.Status.LastRefireCommentAt.Unix(), "the incident-refire cooldown must be untouched")
}
