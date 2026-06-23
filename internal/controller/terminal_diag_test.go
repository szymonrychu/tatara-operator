package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// scmWiredReconciler builds a TaskReconciler with SCM wired to fw and creates
// the project/repo/secret a terminal-diagnostics comment needs to resolve an
// SCM context. Returns the reconciler so a test can drive a terminal path.
func scmWiredReconciler(t *testing.T, fw *fakeWriter, projName, repoName, secretName string) *TaskReconciler {
	t.Helper()
	ctx := context.Background()
	r := newTaskReconciler(newFakeSession())
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("pat"), "webhookSecret": []byte("w")},
	}
	require.NoError(t, k8sClient.Create(ctx, sec))
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: projName, Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: secretName, Scm: &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "bot"}},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))
	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: repoName, Namespace: testNS},
		Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: projName, URL: "https://github.com/o/r.git", DefaultBranch: "main", ReingestSchedule: "0 6 * * *"},
	}
	require.NoError(t, k8sClient.Create(ctx, repo))
	return r
}

// TestHandleAgentUnreachableCommentsTerminalDiagnostics asserts that once a
// Ready-but-unreachable wrapper is failed past the boot deadline, the cause is
// posted exactly once to the linked issue (generalized from boot-crash, #116),
// so it survives terminal-CRD GC (#81).
func TestHandleAgentUnreachableCommentsTerminalDiagnostics(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &fakeWriter{}
	r := scmWiredReconciler(t, fw, "td-unr-proj", "td-unr-repo", "td-unr-scm")

	task := &tatarav1alpha1.Task{}
	task.Name = "td-unreachable"
	task.Namespace = testNS
	task.Spec.ProjectRef = "td-unr-proj"
	task.Spec.RepositoryRef = "td-unr-repo"
	task.Spec.Goal = "g"
	task.Spec.Kind = "issueLifecycle"
	task.Spec.Source = &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#99", Number: 99}
	// Unreachable since longer ago than agentBootDeadline -> next detection fails.
	task.Annotations = map[string]string{annAgentUnreachableSince: time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)}
	require.NoError(t, k8sClient.Create(ctx, task))
	task.Status.Phase = "Running"
	require.NoError(t, k8sClient.Status().Update(ctx, task))

	_, err, handled := r.handleAgentUnreachable(ctx, getTask(t, "td-unreachable"),
		&agent.UnreachableError{Err: errors.New("connection refused")})
	require.NoError(t, err)
	require.True(t, handled)

	got := getTask(t, "td-unreachable")
	require.Equal(t, "Failed", got.Status.Phase)
	cond := apimeta.FindStatusCondition(got.Status.Conditions, "Ready")
	require.NotNil(t, cond)
	require.Equal(t, "AgentUnreachable", cond.Reason)

	require.Len(t, fw.commentArgs, 1)
	require.Contains(t, fw.commentArgs[0], "o/r#99|")
	require.Contains(t, fw.commentArgs[0], "AgentUnreachable")
	require.Contains(t, fw.commentArgs[0], "unreachable for over")
}

// TestCommentTerminalDiagnosticsBody checks the generic comment body and that a
// project-scoped task (no SCM source) is a safe no-op.
func TestCommentTerminalDiagnosticsBody(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &fakeWriter{}
	r := scmWiredReconciler(t, fw, "td-body-proj", "td-body-repo", "td-body-scm")

	task := &tatarav1alpha1.Task{}
	task.Name = "td-body"
	task.Namespace = testNS
	task.Spec.ProjectRef = "td-body-proj"
	task.Spec.RepositoryRef = "td-body-repo"
	task.Spec.Source = &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#7", Number: 7}

	r.commentTerminalDiagnostics(ctx, task, "TurnTimeout", "turn t-1 exceeded timeout")
	require.Len(t, fw.commentArgs, 1)
	require.Equal(t, "o/r#7|Task run terminated (`Failed` / `TurnTimeout`).\n\nturn t-1 exceeded timeout", fw.commentArgs[0])

	// No issue source -> no-op (and no panic).
	task.Spec.Source = nil
	r.commentTerminalDiagnostics(ctx, task, "PodLost", "wrapper pod lost")
	require.Len(t, fw.commentArgs, 1)
}
