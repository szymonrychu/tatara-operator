package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// fakeTokenRecordingWriter records the token passed to CloseIssue so we can
// assert writeBackIssue actually threads the secret token through.
type fakeTokenRecordingWriter struct {
	scm.SCMWriter
	capturedToken string
}

func (f *fakeTokenRecordingWriter) GetIssueState(_ context.Context, _, _ string, _ int) (scm.IssueState, error) {
	return scm.IssueState{}, nil
}
func (f *fakeTokenRecordingWriter) CloseIssue(_ context.Context, token, _ string, _ int, _ string) error {
	f.capturedToken = token
	return nil
}

// TestWriteBackIssueClosePassesToken verifies that the token read from the SCM
// secret is forwarded to CloseIssue (fix for the 401 bug where token was
// discarded from scmContext and struct c.token was empty on the writer).
func TestWriteBackIssueClosePassesToken(t *testing.T) {
	ctx := context.Background()
	fw := &fakeTokenRecordingWriter{}
	r := newWriteBackReconciler(t, &fakeWriter{})
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cit-scm", Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("secret-pat-value"), "webhookSecret": []byte("w")},
	}
	require.NoError(t, k8sClient.Create(ctx, sec))

	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "cit-proj", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: "cit-scm", Scm: &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "bot"}},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))

	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "cit-repo", Namespace: testNS},
		Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: "cit-proj", URL: "https://github.com/o/r.git", DefaultBranch: "main", ReingestSchedule: "0 6 * * *"},
	}
	require.NoError(t, k8sClient.Create(ctx, repo))

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "cit-task", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    "cit-proj",
			RepositoryRef: "cit-repo",
			Goal:          "triage",
			Kind:          "triageIssue",
			Source:        &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#8", Number: 8, IsPR: false},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, task))
	task.Status.Phase = "Succeeded"
	task.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: "close", Comment: "done"}
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type: "WritebackPending", Status: metav1.ConditionTrue, Reason: "x", Message: "x",
	})
	require.NoError(t, k8sClient.Status().Update(ctx, task))

	_, err := reconcileWriteback(t, r, "cit-task")
	require.NoError(t, err)
	require.Equal(t, "secret-pat-value", fw.capturedToken,
		"CloseIssue must receive the token from the SCM secret, not the empty struct field")
}
