package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// mkInterjectFixture creates a Project and an issueLifecycle Task (state
// Implement) with the given turn annotations and pending interjections, then
// returns the freshly-read Task ready to hand to reconcileLifecycle.
func mkInterjectFixture(t *testing.T, name string, ann map[string]string, pending []string) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()

	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-proj", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: name + "-scm"},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-task", Namespace: testNS, Annotations: ann},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    name + "-proj",
			RepositoryRef: name + "-repo",
			Goal:          "issue",
			Kind:          "issueLifecycle",
		},
	}
	require.NoError(t, k8sClient.Create(ctx, task))
	task.Status.LifecycleState = "Implement"
	task.Status.PendingInterjections = pending
	require.NoError(t, k8sClient.Status().Update(ctx, task))

	got := &tatarav1alpha1.Task{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name + "-task"}, got))
	return got
}

func getInterjectTask(t *testing.T, name string) *tatarav1alpha1.Task {
	t.Helper()
	got := &tatarav1alpha1.Task{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name + "-task"}, got))
	return got
}

// TestLifecycleInterject_InflightTurn_DeliversAndClears verifies that queued
// interjections are delivered to the live session (in order) when a turn is in
// flight, then cleared from the Task status.
func TestLifecycleInterject_InflightTurn_DeliversAndClears(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fs := newFakeSession()
	r := newTaskReconciler(fs)

	ann := map[string]string{tatarav1alpha1.AnnCurrentTurn: "turn-1"} // in flight (no turn-complete)
	task := mkInterjectFixture(t, "ijdeliver", ann, []string{"hello", "world"})

	_, err := r.reconcileLifecycle(ctx, task)
	require.NoError(t, err)

	got := fs.allInterjects()
	require.Len(t, got, 2)
	want := agent.BaseURL(task, testNS)
	require.Equal(t, interjection{BaseURL: want, Text: "hello"}, got[0])
	require.Equal(t, interjection{BaseURL: want, Text: "world"}, got[1])

	require.Empty(t, getInterjectTask(t, "ijdeliver").Status.PendingInterjections, "queue must be cleared after delivery")
}

// TestLifecycleInterject_NoInflightTurn_DropsQueue verifies that interjections
// queued when no turn is in flight are dropped (the next turn re-reads the
// thread) and never delivered.
func TestLifecycleInterject_NoInflightTurn_DropsQueue(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fs := newFakeSession()
	r := newTaskReconciler(fs)

	// Turn already completed: current-turn set but turn-complete stamped.
	ann := map[string]string{
		tatarav1alpha1.AnnCurrentTurn:  "turn-1",
		tatarav1alpha1.AnnTurnComplete: "2026-06-14T07:00:00Z",
	}
	task := mkInterjectFixture(t, "ijdrop", ann, []string{"stale"})

	_, err := r.reconcileLifecycle(ctx, task)
	require.NoError(t, err)

	require.Empty(t, fs.allInterjects(), "no interjection may be delivered without an in-flight turn")
	require.Empty(t, getInterjectTask(t, "ijdrop").Status.PendingInterjections, "stale queue must be cleared")
}

// TestLifecycleInterject_Unreachable_RetainsQueue verifies that an
// UnreachableError (wrapper still booting) keeps the queue intact for a retry.
func TestLifecycleInterject_Unreachable_RetainsQueue(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fs := newFakeSession()
	fs.interjectErr = &agent.UnreachableError{}
	r := newTaskReconciler(fs)

	ann := map[string]string{tatarav1alpha1.AnnCurrentTurn: "turn-1"} // in flight
	task := mkInterjectFixture(t, "ijunreach", ann, []string{"keep"})

	res, err := r.reconcileLifecycle(ctx, task)
	require.NoError(t, err)
	require.Greater(t, res.RequeueAfter.Nanoseconds(), int64(0), "unreachable should requeue after a delay")

	require.Empty(t, fs.allInterjects(), "an errored interject records no delivery")
	require.Equal(t, []string{"keep"}, getInterjectTask(t, "ijunreach").Status.PendingInterjections,
		"queue must be retained when the wrapper is unreachable")
}
