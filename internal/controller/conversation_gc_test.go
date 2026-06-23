package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

type fakeConvGC struct {
	objects map[string]bool
	deleted []string
}

func (f *fakeConvGC) Exists(_ context.Context, k string) (bool, error) { return f.objects[k], nil }
func (f *fakeConvGC) Delete(_ context.Context, k string) error {
	delete(f.objects, k)
	f.deleted = append(f.deleted, k)
	return nil
}

func convGCServer(store conversationGC) *CallbackServer {
	return &CallbackServer{
		Client:                k8sClient,
		Metrics:               obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Namespace:             testNS,
		ConvStore:             store,
		ConversationRetention: time.Hour,
	}
}

// mkConvTask creates a conversation-bearing Task with a recorded conversation key
// and (optionally) a fork-from key + terminal state aged past the GC grace.
func mkConvTask(t *testing.T, name, kind, convKey, forkKey string, terminal bool) *tatarav1alpha1.Task {
	t.Helper()
	ann := map[string]string{}
	if forkKey != "" {
		ann[annForkFromConversationKey] = forkKey
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Annotations: ann},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "demo", Kind: kind, Goal: "g"},
	}
	require.NoError(t, k8sClient.Create(context.Background(), task))
	task.Status.ConversationObjectKey = convKey
	old := metav1.NewTime(time.Now().Add(-2 * time.Hour)) // past the 1h grace
	task.Status.LastActivityAt = &old
	if terminal {
		if kind == "issueLifecycle" {
			task.Status.LifecycleState = "Done"
		} else {
			task.Status.Phase = "Succeeded"
		}
	} else {
		task.Status.LifecycleState = "Triage"
		task.Status.Phase = "Running"
	}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))
	return task
}

func listConvTasks(t *testing.T) []tatarav1alpha1.Task {
	t.Helper()
	var l tatarav1alpha1.TaskList
	require.NoError(t, k8sClient.List(context.Background(), &l))
	return l.Items
}

func TestGCConversations_DeletesFullyClosedBatch(t *testing.T) {
	const bkey = "demo/task-brainstorm-gc1.jsonl"
	store := &fakeConvGC{objects: map[string]bool{
		bkey:                       true,
		"demo/r/issue-1.jsonl":     true,
		"demo/r/issue-2.jsonl":     true,
		"demo/other/issue-9.jsonl": true, // unrelated, still open -> must survive
	}}
	mkConvTask(t, "gc1-brain", "brainstorm", bkey, "", true)
	mkConvTask(t, "gc1-child-1", "issueLifecycle", "demo/r/issue-1.jsonl", bkey, true)
	mkConvTask(t, "gc1-child-2", "issueLifecycle", "demo/r/issue-2.jsonl", bkey, true)
	mkConvTask(t, "gc1-unrelated", "issueLifecycle", "demo/other/issue-9.jsonl", "", false)

	convGCServer(store).gcConversations(context.Background(), listConvTasks(t))

	require.False(t, store.objects[bkey], "brainstorm parent key must be deleted")
	require.False(t, store.objects["demo/r/issue-1.jsonl"], "sibling 1 must be deleted")
	require.False(t, store.objects["demo/r/issue-2.jsonl"], "sibling 2 must be deleted")
	require.True(t, store.objects["demo/other/issue-9.jsonl"], "unrelated open issue must survive")
}

func TestGCConversations_KeepsBatchWithAnOpenSibling(t *testing.T) {
	const bkey = "demo/task-brainstorm-gc2.jsonl"
	store := &fakeConvGC{objects: map[string]bool{
		bkey:                   true,
		"demo/r/issue-3.jsonl": true,
		"demo/r/issue-4.jsonl": true,
	}}
	mkConvTask(t, "gc2-brain", "brainstorm", bkey, "", true)
	mkConvTask(t, "gc2-child-3", "issueLifecycle", "demo/r/issue-3.jsonl", bkey, true)
	mkConvTask(t, "gc2-child-4", "issueLifecycle", "demo/r/issue-4.jsonl", bkey, false) // still open

	convGCServer(store).gcConversations(context.Background(), listConvTasks(t))

	require.Empty(t, store.deleted, "no key may be deleted while any sibling is open")
	require.True(t, store.objects[bkey])
	require.True(t, store.objects["demo/r/issue-3.jsonl"])
}

func TestGCConversations_DisabledWhenNoStore(t *testing.T) {
	// Nil store must be a safe no-op (S3 not configured).
	s := &CallbackServer{Client: k8sClient, Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()), Namespace: testNS}
	s.gcConversations(context.Background(), listConvTasks(t))
}

func TestGCConversations_SoloIssueDeletedWhenTerminal(t *testing.T) {
	store := &fakeConvGC{objects: map[string]bool{"demo/r/issue-5.jsonl": true}}
	mkConvTask(t, "gc-solo-5", "issueLifecycle", "demo/r/issue-5.jsonl", "", true)

	convGCServer(store).gcConversations(context.Background(), listConvTasks(t))
	require.False(t, store.objects["demo/r/issue-5.jsonl"], "a closed solo issue's conversation is deleted")
}
