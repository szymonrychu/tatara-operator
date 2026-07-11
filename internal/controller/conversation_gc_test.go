package controller

import (
	"context"
	"fmt"
	"syscall"
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

// unreachableConvGC fails every probe with a store-wide error and counts the
// probes, so a test can assert the GC pass short-circuits instead of looping.
type unreachableConvGC struct {
	err         error
	existsCalls int
	deleted     []string
}

func (u *unreachableConvGC) Exists(_ context.Context, _ string) (bool, error) {
	u.existsCalls++
	return false, u.err
}
func (u *unreachableConvGC) Delete(_ context.Context, k string) error {
	u.deleted = append(u.deleted, k)
	return nil
}

// convGCMetric reads operator_conversation_gc_total{result=<result>} from reg.
func convGCMetric(t *testing.T, reg *prometheus.Registry, result string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "operator_conversation_gc_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "result" && lp.GetValue() == result {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
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
			task.Status.DeployState = "Done"
		} else {
			task.Status.Phase = "Succeeded"
		}
	} else {
		task.Status.DeployState = "Triage"
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

// TestGCConversations_DocumentationSoloDeletedWhenTerminal asserts a terminal
// documentation Task's S3 transcript is GC-eligible: it is SHA-keyed (no
// Source.Number, so ConversationKey falls to the task-name form), and must be
// included in the batched-Kind case or its transcript would leak forever.
func TestGCConversations_DocumentationSoloDeletedWhenTerminal(t *testing.T) {
	store := &fakeConvGC{objects: map[string]bool{"demo/task-doc-gc1.jsonl": true}}
	mkConvTask(t, "doc-gc1", "documentation", "demo/task-doc-gc1.jsonl", "", true)

	convGCServer(store).gcConversations(context.Background(), listConvTasks(t))
	require.False(t, store.objects["demo/task-doc-gc1.jsonl"], "a closed documentation Task's conversation is deleted")
}

// Issue #149: a store-wide / connection-level failure must short-circuit the
// whole pass after the first probe - one "unavailable" metric, no per-object
// "error" churn, and the remaining backlog keys are never probed - so one
// object-store outage cannot become a burst of duplicate ERROR lines.
func TestGCConversations_SkipsPassWhenStoreUnreachable(t *testing.T) {
	store := &unreachableConvGC{
		err: fmt.Errorf("objstore exists k: connect: %w", syscall.ECONNREFUSED),
	}
	mkConvTask(t, "gc-unreach-a", "issueLifecycle", "demo/r/issue-150-a.jsonl", "", true)
	mkConvTask(t, "gc-unreach-b", "issueLifecycle", "demo/r/issue-150-b.jsonl", "", true)
	mkConvTask(t, "gc-unreach-c", "issueLifecycle", "demo/r/issue-150-c.jsonl", "", true)

	reg := prometheus.NewRegistry()
	s := &CallbackServer{
		Client:                k8sClient,
		Metrics:               obs.NewOperatorMetrics(reg),
		Namespace:             testNS,
		ConvStore:             store,
		ConversationRetention: time.Hour,
	}
	s.gcConversations(context.Background(), listConvTasks(t))

	require.Equal(t, 1, store.existsCalls, "pass must short-circuit after the first unreachable probe")
	require.Empty(t, store.deleted, "nothing may be deleted while the store is unreachable")
	require.Equal(t, 1.0, convGCMetric(t, reg, "unavailable"), "exactly one unavailable result recorded")
	require.Equal(t, 0.0, convGCMetric(t, reg, "error"), "no per-object error may be recorded")
}
