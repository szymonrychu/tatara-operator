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
// the project/repo/secret a terminal reconcile needs to resolve an SCM
// context, so a test can assert the writer stays untouched on termination.
// Returns the metrics registry too, so a test can assert the termination
// metric still fires.
func scmWiredReconciler(t *testing.T, fw *fakeWriter, projName, repoName, secretName string) (*TaskReconciler, *prometheus.Registry) {
	t.Helper()
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	r := newTaskReconciler(newFakeSession())
	r.Metrics = obs.NewOperatorMetrics(reg)
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
	return r, reg
}

// TestHandleAgentUnreachableDoesNotComment asserts that a Ready-but-unreachable
// wrapper failed past the boot deadline still terminates (Phase=Failed,
// reason=AgentUnreachable) and still fires the termination metric, but no
// longer posts a comment to the linked issue - the operator-internal failure
// is observable via the structured log and operator_task_terminal_total /
// operator_agent_unreachable_termination_total instead (noise reduction; the
// PodLost alert covers the human-facing signal).
func TestHandleAgentUnreachableDoesNotComment(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &fakeWriter{}
	r, reg := scmWiredReconciler(t, fw, "td-unr-proj", "td-unr-repo", "td-unr-scm")

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

	_, err, handled := r.handleTransientWrapper(ctx, getTask(t, "td-unreachable"),
		&agent.UnreachableError{Err: errors.New("connection refused")})
	require.NoError(t, err)
	require.True(t, handled)

	got := getTask(t, "td-unreachable")
	require.Equal(t, "Failed", got.Status.Phase)
	cond := apimeta.FindStatusCondition(got.Status.Conditions, "Ready")
	require.NotNil(t, cond)
	require.Equal(t, "AgentUnreachable", cond.Reason)

	require.Empty(t, fw.commentArgs, "termination must not post an issue comment")

	if v := counterValue(t, reg, "operator_agent_unreachable_termination_total", nil); v != 1 {
		t.Fatalf("agent-unreachable termination metric = %v, want 1", v)
	}
}
