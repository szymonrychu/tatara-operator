package controller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// reconcileRepoWith runs a single Reconcile with the given reconciler.
func reconcileRepoWith(t *testing.T, r *RepositoryReconciler, name string) (ctrl.Result, error) {
	t.Helper()
	return r.Reconcile(logf.IntoContext(context.Background(), logf.Log), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: name},
	})
}

// mkFailedIngestPod creates a Pod labelled as belonging to jobName whose ingest
// container terminated non-zero with the given FallbackToLogsOnError message,
// mirroring what kube leaves behind for a failed ingest Job.
func mkFailedIngestPod(t *testing.T, jobName, message string) {
	t.Helper()
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName + "-pod",
			Namespace: testNS,
			Labels:    map[string]string{"batch.kubernetes.io/job-name": jobName},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{{Name: "ingest", Image: "x"}},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, pod))
	pod.Status.Phase = corev1.PodFailed
	pod.Status.StartTime = &metav1.Time{Time: time.Now()}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "ingest",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 1,
			Reason:   "Error",
			Message:  message,
		}},
	}}
	require.NoError(t, k8sClient.Status().Update(ctx, pod))
}

// TestRepoReconcile_JobFailureCapturesPodReason verifies that a failed ingest Job
// surfaces the pod's terminated-container message into the Ingested condition and
// a Kubernetes Event, instead of only "ingest job X failed".
func TestRepoReconcile_JobFailureCapturesPodReason(t *testing.T) {
	mkProject(t, "rp-reason", "rp-reason-scm")
	mkSecret(t, "rp-reason-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "reasonrepo", "rp-reason")
	setProjectMemoryReady(t, "rp-reason", "http://mem-rp-reason.tatara.svc:8080")

	r := newRepoReconciler()
	rec := events.NewFakeRecorder(8)
	r.Recorder = rec

	if _, err := reconcileRepoWith(t, r, "reasonrepo"); err != nil {
		t.Fatalf("launch reconcile: %v", err)
	}
	jobName := waitRepoJob(t, "reasonrepo")

	const cause = "fatal: missing --since commit deadbeef in repo reasonrepo"
	mkFailedIngestPod(t, jobName, cause)
	markJob(t, jobName, batchv1.JobFailed)

	if _, err := reconcileRepoWith(t, r, "reasonrepo"); err != nil {
		t.Fatalf("post-failure reconcile: %v", err)
	}

	got := getRepo(t, "reasonrepo")
	c := apimeta.FindStatusCondition(got.Status.Conditions, "Ingested")
	require.NotNil(t, c)
	require.Equal(t, metav1.ConditionFalse, c.Status)
	require.Equal(t, "IngestFailed", c.Reason)
	require.Contains(t, c.Message, cause,
		"condition message must carry the captured pod termination reason")

	select {
	case ev := <-rec.Events:
		require.Contains(t, ev, "IngestFailed")
		require.Contains(t, ev, cause)
	default:
		t.Fatal("expected an IngestFailed Event carrying the pod reason")
	}
}

// TestRepoReconcile_JobFailureWithoutPodFallsBack verifies that when the failed
// Job's pod is already GC'd (no pod found) the reconcile still records the failure
// with the generic message and does not error.
func TestRepoReconcile_JobFailureWithoutPodFallsBack(t *testing.T) {
	mkProject(t, "rp-nopod", "rp-nopod-scm")
	mkSecret(t, "rp-nopod-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkRepo(t, "nopodrepo", "rp-nopod")
	setProjectMemoryReady(t, "rp-nopod", "http://mem-rp-nopod.tatara.svc:8080")

	if _, err := reconcileRepo(t, "nopodrepo"); err != nil {
		t.Fatalf("launch reconcile: %v", err)
	}
	jobName := waitRepoJob(t, "nopodrepo")
	markJob(t, jobName, batchv1.JobFailed)

	if _, err := reconcileRepo(t, "nopodrepo"); err != nil {
		t.Fatalf("post-failure reconcile: %v", err)
	}

	got := getRepo(t, "nopodrepo")
	c := apimeta.FindStatusCondition(got.Status.Conditions, "Ingested")
	require.NotNil(t, c)
	require.Equal(t, "IngestFailed", c.Reason)
	require.Equal(t, "ingest job "+jobName+" failed", c.Message,
		"with no retained pod the message stays generic")
}

// conflictOnceRepoClient wraps k8sClient, injecting one conflict on the first
// Status().Update for Repository objects, to exercise patchStatus's retry.
type conflictOnceRepoClient struct {
	client.Client
	calls *atomic.Int32
	name  string
}

func (c *conflictOnceRepoClient) Status() client.SubResourceWriter {
	return &conflictOnceWriter{
		SubResourceWriter: c.Client.Status(),
		calls:             c.calls,
		gr:                schema.GroupResource{Group: "tatara.dev", Resource: "repositories"},
		name:              c.name,
	}
}

// TestPatchStatusRetriesOnConflict verifies patchStatus lands the status write even
// when the first Status().Update returns a Conflict (the 409 storm the issue
// describes), and re-Gets a fresh object each attempt.
func TestPatchStatusRetriesOnConflict(t *testing.T) {
	ctx := context.Background()
	mkProject(t, "rp-pc", "rp-pc-scm")
	mkSecret(t, "rp-pc-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	repo := mkRepo(t, "pcrepo", "rp-pc")

	var calls atomic.Int32
	cc := &conflictOnceRepoClient{Client: k8sClient, calls: &calls, name: "pcrepo"}
	r := &RepositoryReconciler{
		Client:  cc,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}

	require.NoError(t, r.patchStatus(ctx, repo, func(fresh *tataradevv1alpha1.Repository) bool {
		return apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:    "IngestBackoff",
			Status:  metav1.ConditionTrue,
			Reason:  "IngestFailing",
			Message: "test",
		})
	}))

	got := getRepo(t, "pcrepo")
	c := apimeta.FindStatusCondition(got.Status.Conditions, "IngestBackoff")
	require.NotNil(t, c)
	require.Equal(t, metav1.ConditionTrue, c.Status)
	require.GreaterOrEqual(t, calls.Load(), int32(2), "must have retried at least once")
}

// TestPatchStatusSkipsWriteWhenUnchanged verifies that when mutate reports no
// change, patchStatus performs no Status().Update (avoiding needless resourceVersion
// churn) yet still succeeds.
func TestPatchStatusSkipsWriteWhenUnchanged(t *testing.T) {
	ctx := context.Background()
	mkProject(t, "rp-noop", "rp-noop-scm")
	mkSecret(t, "rp-noop-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	repo := mkRepo(t, "nooprepo", "rp-noop")

	var calls atomic.Int32
	cc := &conflictOnceRepoClient{Client: k8sClient, calls: &calls, name: "nooprepo"}
	r := &RepositoryReconciler{
		Client:  cc,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}

	require.NoError(t, r.patchStatus(ctx, repo, func(*tataradevv1alpha1.Repository) bool {
		return false
	}))
	require.Equal(t, int32(0), calls.Load(), "no Status().Update when mutate reports no change")
}
