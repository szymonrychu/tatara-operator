package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"k8s.io/apimachinery/pkg/types"
)

// labelWriter (defined in labels_test.go) overrides only AddLabel/RemoveLabel.
// finishTriage also calls Comment (discuss) and CloseIssue (close); add no-op
// overrides so the embedded nil SCMWriter is never dereferenced.
func (w *labelWriter) Comment(_ context.Context, _, _, _ string) error { return nil }
func (w *labelWriter) CloseIssue(_ context.Context, _, _ string, _ int, _ string) error {
	return nil
}

func reconcilerFor(w scm.SCMWriter, rdr scm.SCMReader) *TaskReconciler {
	return &TaskReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(),
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:    func(string) (scm.SCMWriter, error) { return w, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return rdr, nil }}
}

func markSucceededWithOutcome(t *testing.T, name, action string) {
	t.Helper()
	ctx := context.Background()
	var fresh tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, &fresh))
	fresh.Status.Phase = "Succeeded"
	fresh.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: action, Comment: "c"}
	require.NoError(t, k8sClient.Status().Update(ctx, &fresh))
}

func getTaskByName(t *testing.T, name string) *tatarav1alpha1.Task {
	t.Helper()
	var fresh tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &fresh))
	return &fresh
}

func projOf(t *testing.T, task *tatarav1alpha1.Task) *tatarav1alpha1.Project {
	t.Helper()
	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))
	return &proj
}

func TestFinishTriage_HumanFiledImplement_Approved(t *testing.T) {
	_, task, w := seedLabelTask(t, "hf-impl", []string{"tatara-idea"})
	r := reconcilerFor(w, &commentReader{})
	proj := projOf(t, task)
	markSucceededWithOutcome(t, task.Name, "implement")
	_, err := r.finishTriage(context.Background(), proj, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, []string{"tatara-approved"}, w.added)
	require.Equal(t, "Implement", getTaskByName(t, task.Name).Status.LifecycleState)
}

func TestFinishTriage_Close_Rejected(t *testing.T) {
	_, task, w := seedLabelTask(t, "close-rej", []string{"tatara-idea"})
	r := reconcilerFor(w, &commentReader{})
	proj := projOf(t, task)
	markSucceededWithOutcome(t, task.Name, "close")
	_, err := r.finishTriage(context.Background(), proj, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, []string{"tatara-rejected"}, w.added)
	require.Equal(t, "Done", getTaskByName(t, task.Name).Status.LifecycleState)
}

func TestFinishTriage_Discuss_Idea(t *testing.T) {
	_, task, w := seedLabelTask(t, "disc-idea", nil)
	r := reconcilerFor(w, &commentReader{})
	proj := projOf(t, task)
	markSucceededWithOutcome(t, task.Name, "discuss")
	_, err := r.finishTriage(context.Background(), proj, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, []string{"tatara-idea"}, w.added)
	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.LifecycleState)
}

func TestFinishTriage_BotAuthoredImplement_NoHumanComment_ParksIdea(t *testing.T) {
	_, task, w := seedLabelTask(t, "bot-noh", nil)
	got := getTaskByName(t, task.Name)
	got.Spec.Source.AuthorLogin = "tatara-bot"
	require.NoError(t, k8sClient.Update(context.Background(), got))
	r := reconcilerFor(w, &commentReader{comments: []scm.IssueComment{{Author: "tatara-bot", Body: "my idea"}}})
	proj := projOf(t, task)
	markSucceededWithOutcome(t, task.Name, "implement")
	_, err := r.finishTriage(context.Background(), proj, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, []string{"tatara-idea"}, w.added)
	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.LifecycleState)
}

func TestFinishTriage_BotAuthoredImplement_WithHumanComment_Approved(t *testing.T) {
	_, task, w := seedLabelTask(t, "bot-h", nil)
	got := getTaskByName(t, task.Name)
	got.Spec.Source.AuthorLogin = "tatara-bot"
	require.NoError(t, k8sClient.Update(context.Background(), got))
	r := reconcilerFor(w, &commentReader{comments: []scm.IssueComment{{Author: "szymon", Body: "approved, go"}}})
	proj := projOf(t, task)
	markSucceededWithOutcome(t, task.Name, "implement")
	_, err := r.finishTriage(context.Background(), proj, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, []string{"tatara-approved"}, w.added)
	require.Equal(t, "Implement", getTaskByName(t, task.Name).Status.LifecycleState)
}
