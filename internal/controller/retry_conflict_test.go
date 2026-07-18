package controller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

	require.NoError(t, r.stampScan(ctx, proj, "issueScan"))

	// Verify the stamp landed despite the initial conflict.
	var got tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "stamp-retry-proj"}, &got))
	require.NotNil(t, got.Status.LastIssueScan, "LastIssueScan must be stamped even after one conflict")
	require.GreaterOrEqual(t, calls.Load(), int32(2), "must have retried at least once")
}

// stampScan is the successor heartbeat for the brainstorm/documentation/
// issueScan crons now that tatara_scan_items_total is pruned (metric-wiring
// audit, issue #370): TataraLoopStalled's deadman alert and the tatara-loop
// dashboard panel both queried tatara_scan_items_total/
// tatara_scan_tasks_created_total to detect a stalled scan cron, and both
// are repointed onto obs.SweepLastSuccessTimestamp{activity=...} instead -
// the same heartbeat gauge sweep.go's B.4 pass already sets for
// sweep/nightlySweep, extended to the other scan activities.
func TestStampScanSetsHeartbeatGauge(t *testing.T) {
	ctx := context.Background()
	mkSecret(t, "stamp-heartbeat-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "stamp-heartbeat-proj", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "stamp-heartbeat-scm",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "bot"},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))

	r := &ProjectReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}

	before := time.Now().Unix()
	require.NoError(t, r.stampScan(ctx, proj, "brainstorm"))
	got := testutil.ToFloat64(obs.SweepLastSuccessTimestamp.WithLabelValues("brainstorm"))
	require.GreaterOrEqual(t, got, float64(before),
		"obs.SweepLastSuccessTimestamp{activity=brainstorm} must be set on a successful stampScan")
}
