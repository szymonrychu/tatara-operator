package restapi

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestToProjectDTO(t *testing.T) {
	p := tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "demo"},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "demo-scm", TriggerLabel: "tatara", MaxConcurrentTasks: 3,
			Agent: tatarav1alpha1.AgentSpec{Model: "claude", Image: "img:1",
				PermissionMode: "bypassPermissions", MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800},
		},
		Status: tatarav1alpha1.ProjectStatus{WebhookURL: "https://x/operator/webhooks/demo"},
	}
	d := toProjectDTO(p)
	require.Equal(t, "demo", d.Name)
	require.Equal(t, "tatara", d.TriggerLabel)
	require.Equal(t, 3, d.MaxConcurrentTasks)
	require.Equal(t, "claude", d.Agent.Model)
	require.Equal(t, "https://x/operator/webhooks/demo", d.Status.WebhookURL)
}

func TestToTaskDTO_Source(t *testing.T) {
	task := tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1"},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "demo", RepositoryRef: "repo", Goal: "do the thing",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#1", URL: "https://gh/1"},
		},
		Status: tatarav1alpha1.TaskStatus{Phase: "Running", TurnsCompleted: 2},
	}
	d := toTaskDTO(task)
	require.Equal(t, "do the thing", d.Goal)
	require.NotNil(t, d.Source)
	require.Equal(t, "github", d.Source.Provider)
	require.Equal(t, "Running", d.Status.Phase)
	require.Equal(t, 2, d.Status.TurnsCompleted)
}

func TestToSubtaskDTO(t *testing.T) {
	st := tatarav1alpha1.Subtask{
		ObjectMeta: metav1.ObjectMeta{Name: "s1"},
		Spec:       tatarav1alpha1.SubtaskSpec{TaskRef: "t1", Title: "step", Detail: "d", Order: 1},
		Status:     tatarav1alpha1.SubtaskStatus{Phase: "Done", TurnID: "turn-9", Result: "ok"},
	}
	d := toSubtaskDTO(st)
	require.Equal(t, "s1", d.Name)
	require.Equal(t, 1, d.Order)
	require.Equal(t, "Done", d.Status.Phase)
	require.Equal(t, "turn-9", d.Status.TurnID)
	_ = time.Now()
}
