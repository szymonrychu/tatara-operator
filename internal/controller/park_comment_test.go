package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Liveness-hardening finding #2: silent parks. clarify-timeout, implement-failed
// and triage-failed all used setDeployState(Parked,...) with NO issue comment, so a
// reporter could not tell "waiting/thinking" from "dead". Every park must now emit
// an issue comment.

// TestImplementFailed_ParksWithComment: a failed implement agent run parks with a
// diagnostic note on the issue, not silently.
func TestImplementFailed_ParksWithComment(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#411",
		URL: "https://github.com/o/r/issues/411", Number: 411, AuthorLogin: "human",
	}
	task := seedLifecycleTask(t, "pk-impl-failed", "pk-if-proj", "pk-if-repo", "pk-if-sec", src)
	task.Status.DeployState = "Implement"
	task.Status.Phase = "Failed"
	require.NoError(t, k8sClient.Status().Update(ctx, task))

	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)

	_, err := r.finishImplement(ctx, getTask(t, "pk-impl-failed"))
	require.NoError(t, err)

	got := getTask(t, "pk-impl-failed")
	require.Equal(t, "Parked", got.Status.DeployState)
	require.Equal(t, "implement-failed", got.Status.ParkReason)
	require.NotEmpty(t, fw.commentBodies("o/r#411"), "implement-failed park must post a diagnostic issue comment")
}

// TestTriageFailed_ParksWithComment: a failed triage agent run parks with a
// diagnostic note on the issue.
func TestTriageFailed_ParksWithComment(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#412",
		URL: "https://github.com/o/r/issues/412", Number: 412, AuthorLogin: "human",
	}
	task := seedLifecycleTask(t, "pk-triage-failed", "pk-tf-proj", "pk-tf-repo", "pk-tf-sec", src)
	task.Status.DeployState = "Triage"
	task.Status.Phase = "Failed"
	require.NoError(t, k8sClient.Status().Update(ctx, task))

	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "pk-tf-proj"}, &proj))
	noop := func(context.Context, *tatarav1alpha1.Project, *tatarav1alpha1.Task) (ctrl.Result, bool, error) {
		return ctrl.Result{}, true, nil
	}
	_, err := r.finishFrontHalf(ctx, &proj, getTask(t, "pk-triage-failed"), noop)
	require.NoError(t, err)

	got := getTask(t, "pk-triage-failed")
	require.Equal(t, "Parked", got.Status.DeployState)
	require.Equal(t, "triage-failed", got.Status.ParkReason)
	require.NotEmpty(t, fw.commentBodies("o/r#412"), "triage-failed park must post a diagnostic issue comment")
}

// TestClarifyTimeout_ParksWithComment: the 1h discuss-window timeout parks with an
// issue comment telling the reporter it timed out and how to resume, not silently.
func TestClarifyTimeout_ParksWithComment(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, fw, name := seedClarifyTask(t, "to-comment", "human", nil)
	setClarifyConversation(t, ctx, name, -time.Minute, nil)
	proj := getClarifyProject(t, ctx, "to-comment")

	_, err := r.handleClarifyConversation(ctx, proj, getClarifyTask(t, ctx, name))
	require.NoError(t, err)

	got := getClarifyTask(t, ctx, name)
	require.Equal(t, "Parked", got.Status.DeployState)
	require.Equal(t, "clarify-timeout", got.Status.ParkReason)
	require.NotEmpty(t, fw.comments, "clarify-timeout park must post a resume-hint issue comment")
}
