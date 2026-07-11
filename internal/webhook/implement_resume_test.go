package webhook_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// parkedImplementTask builds a discrete implement-kind Task that Parked at the
// give-up cap (a labelled-but-dead issue) for issue o/r#9.
func parkedImplementTask(name, projectRef, repoRef, deployState, phase string) *tatarav1.Task {
	return &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels: map[string]string{
				tatarav1.LabelSourceKind: "implement",
			},
		},
		Spec: tatarav1.TaskSpec{
			ProjectRef:    projectRef,
			RepositoryRef: repoRef,
			Kind:          "implement",
			Goal:          "Implement issue o/r#9",
			Source: &tatarav1.TaskSource{
				Provider: "github", IssueRef: "o/r#9", Number: 9,
			},
		},
		Status: tatarav1.TaskStatus{
			Phase:            phase,
			DeployState:      deployState,
			ParkReason:       "implement-failed",
			ImplementGiveUps: 3,
		},
	}
}

// TestIssueComment_ReactivatesParkedDiscreteImplement is liveness-hardening finding
// #3: a discrete implement Task Parked at the give-up cap on an open, labelled issue
// is a permanent wedge - the producer stays blocked and the kind is not
// comment-resumable. A human comment must now re-drive that Parked implement task
// back to a live Running state (fresh give-up budget), not silently do nothing.
func TestIssueComment_ReactivatesParkedDiscreteImplement(t *testing.T) {
	const secretVal = "whsec-impl9"
	proj := projectWithBot("projimpl9", "projimpl9-scm", "tatara", "tatara-bot")
	repo := repository("repoimpl9", "projimpl9", "https://github.com/o/r.git", "main")
	task := parkedImplementTask("taskimpl9", "projimpl9", "repoimpl9", "Parked", "Failed")

	c := seedClient(t, proj, secret("projimpl9-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), task))
	require.NoError(t, c.Status().Update(context.Background(), task))

	h, _ := newServer(t, c)
	body := []byte(issueCommentBodyIssue9)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projimpl9", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	// No fresh clarify/lifecycle task or QueuedEvent: the Parked implement is revived.
	var tasks tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tasks, client.InNamespace(ns)))
	require.Len(t, tasks.Items, 1, "the Parked implement must be reactivated, not duplicated by a fresh task")
	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Empty(t, qel.Items, "no fresh clarify QueuedEvent when the Parked implement is reactivated")

	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "taskimpl9"}, &got))
	require.False(t, tatarav1.TaskTerminal(&got), "the Parked implement must be revived to a live state")
	require.Equal(t, "", got.Status.Phase, "Phase cleared so the implement run re-spawns")
	require.Equal(t, 0, got.Status.ImplementGiveUps, "the give-up budget is reset so it is not instantly re-parked")
}

// TestIssueComment_DoesNotResurrectDeployingImplement: a Kind=implement Task in the
// pod-less Deploying phase is ALIVE (mid-deploy). A comment must NOT reactivate /
// mutate it - only Parked back-half tasks are comment-resumable.
func TestIssueComment_DoesNotResurrectDeployingImplement(t *testing.T) {
	const secretVal = "whsec-impdep"
	proj := projectWithBot("projimpldep", "projimpldep-scm", "tatara", "tatara-bot")
	repo := repository("repoimpldep", "projimpldep", "https://github.com/o/r.git", "main")
	task := parkedImplementTask("taskimpldep", "projimpldep", "repoimpldep", tatarav1.DeployStateDeploying, tatarav1.PhaseDeploying)
	task.Status.ParkReason = ""
	task.Status.ImplementGiveUps = 0

	c := seedClient(t, proj, secret("projimpldep-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), task))
	require.NoError(t, c.Status().Update(context.Background(), task))

	h, _ := newServer(t, c)
	body := []byte(issueCommentBodyIssue9)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projimpldep", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "taskimpldep"}, &got))
	require.True(t, tatarav1.TaskDeploying(&got), "a live Deploying implement must NOT be reactivated by a comment")
	require.Equal(t, tatarav1.PhaseDeploying, got.Status.Phase)
}
