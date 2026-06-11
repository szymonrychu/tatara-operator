package controller

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// conflictOnceWriter wraps a SubResourceWriter and returns a Conflict error on
// the first Update call, then delegates normally. Used to exercise RetryOnConflict.
type conflictOnceWriter struct {
	client.SubResourceWriter
	calls *atomic.Int32
	gr    schema.GroupResource
	name  string
}

func (c *conflictOnceWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if c.calls.Add(1) == 1 {
		return apierrors.NewConflict(c.gr, c.name, nil)
	}
	return c.SubResourceWriter.Update(ctx, obj, opts...)
}

// conflictOnceProjectClient wraps k8sClient, injecting one conflict on the
// first Status().Update for any object.
type conflictOnceProjectClient struct {
	client.Client
	calls *atomic.Int32
}

func (c *conflictOnceProjectClient) Status() client.SubResourceWriter {
	return &conflictOnceWriter{
		SubResourceWriter: c.Client.Status(),
		calls:             c.calls,
		gr:                schema.GroupResource{Group: "tatara.dev", Resource: "projects"},
		name:              "stamp-retry-proj",
	}
}

// TestStampScanRetriesOnConflict verifies that stampScan successfully records
// the Last*Scan timestamp even when the first Status().Update returns a Conflict.
func TestStampScanRetriesOnConflict(t *testing.T) {
	ctx := context.Background()

	mkSecret(t, "stamp-retry-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "stamp-retry-proj", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "stamp-retry-scm",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "bot"},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))

	var calls atomic.Int32
	cc := &conflictOnceProjectClient{Client: k8sClient, calls: &calls}
	r := &ProjectReconciler{
		Client:  cc,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}

	r.stampScan(ctx, proj, "issueScan")

	// Verify the stamp landed despite the initial conflict.
	var got tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "stamp-retry-proj"}, &got))
	require.NotNil(t, got.Status.LastIssueScan, "LastIssueScan must be stamped even after one conflict")
	require.GreaterOrEqual(t, calls.Load(), int32(2), "must have retried at least once")
}

// conflictOnceTaskClient wraps k8sClient, injecting one conflict on the first
// Status().Update for Task objects.
type conflictOnceTaskClient struct {
	client.Client
	calls *atomic.Int32
}

func (c *conflictOnceTaskClient) Status() client.SubResourceWriter {
	return &conflictOnceWriter{
		SubResourceWriter: c.Client.Status(),
		calls:             c.calls,
		gr:                schema.GroupResource{Group: "tatara.dev", Resource: "tasks"},
		name:              "cwp-retry-task",
	}
}

// TestClearWritebackPendingRetriesOnConflict verifies that clearWritebackPending
// successfully sets WritebackPending=False even when the first Status().Update
// returns a Conflict.
func TestClearWritebackPendingRetriesOnConflict(t *testing.T) {
	ctx := context.Background()

	mkSecret(t, "cwp-retry-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "cwp-retry-proj", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: "cwp-retry-scm"},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))

	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "cwp-retry-repo", Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:       "cwp-retry-proj",
			URL:              "https://github.com/o/r.git",
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		},
	}
	require.NoError(t, k8sClient.Create(ctx, repo))

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "cwp-retry-task", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    "cwp-retry-proj",
			RepositoryRef: "cwp-retry-repo",
			Goal:          "test conflict retry",
		},
	}
	require.NoError(t, k8sClient.Create(ctx, task))
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type: "WritebackPending", Status: metav1.ConditionTrue, Reason: "AwaitingM5",
	})
	require.NoError(t, k8sClient.Status().Update(ctx, task))

	var calls atomic.Int32
	cc := &conflictOnceTaskClient{Client: k8sClient, calls: &calls}
	r := &TaskReconciler{
		Client:  cc,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
	}

	r.clearWritebackPending(ctx, task, "TestReason", "test cleared despite conflict")

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "cwp-retry-task"}, &got))
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status, "WritebackPending must be False after retry")
	require.Equal(t, "TestReason", cond.Reason)
	require.GreaterOrEqual(t, calls.Load(), int32(2), "must have retried at least once")
}
