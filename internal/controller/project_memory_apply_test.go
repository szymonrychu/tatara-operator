package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/prometheus/client_golang/prometheus/testutil"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// webhookUnreachableErr reproduces, verbatim in shape, what the apiserver
// returned during the 2026-07-24 reboot (issue #439): a 500 InternalError whose
// message names the webhook it could not call.
func webhookUnreachableErr() error {
	return apierrors.NewInternalError(fmt.Errorf(
		`failed calling webhook "mcluster.cnpg.io": failed to call webhook: ` +
			`Post "https://cnpg-webhook-service.postgres-operator.svc:443/mutate-postgresql-cnpg-io-v1-cluster?timeout=10s": ` +
			`dial tcp 10.107.94.180:443: connect: connection refused`))
}

// webhookDeniedErr is the OTHER webhook outcome: the webhook was reached and
// rejected the object (the cnpg storage-shrink rejection of issue #248).
func webhookDeniedErr() error {
	return apierrors.NewForbidden(
		schema.GroupResource{Group: "postgresql.cnpg.io", Resource: "clusters"},
		"mem-tatara-pg",
		fmt.Errorf(`admission webhook "vcluster.cnpg.io" denied the request: spec.storage.size: can't be decreased`),
	)
}

func TestIsTransientApplyError(t *testing.T) {
	gr := schema.GroupResource{Group: "postgresql.cnpg.io", Resource: "clusters"}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"webhook unreachable (the #439 error)", webhookUnreachableErr(), true},
		{"webhook unreachable, wrapped by applyMemoryStack",
			fmt.Errorf("apply *v1.Cluster mem-tatara-pg: %w", webhookUnreachableErr()), true},
		{"webhook denied the request", webhookDeniedErr(), false},
		{"too many requests", apierrors.NewTooManyRequests("slow down", 1), true},
		{"service unavailable", apierrors.NewServiceUnavailable("apiserver draining"), true},
		{"server timeout", apierrors.NewServerTimeout(gr, "patch", 1), true},
		{"request timeout", apierrors.NewTimeoutError("timed out", 1), true},
		{"internal error NOT naming a webhook",
			apierrors.NewInternalError(fmt.Errorf("etcdserver: mvcc: database space exceeded")), false},
		{"invalid spec", apierrors.NewBadRequest("bad spec"), false},
		{"not found", apierrors.NewNotFound(gr, "mem-tatara-pg"), false},
		{"conflict", apierrors.NewConflict(gr, "mem-tatara-pg", fmt.Errorf("modified")), false},
		{"plain error mentioning a webhook", fmt.Errorf("failed calling webhook: nope"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTransientApplyError(tc.err); got != tc.want {
				t.Fatalf("isTransientApplyError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestMemoryApplyRetryBackoff(t *testing.T) {
	cases := []struct {
		elapsed time.Duration
		want    time.Duration
	}{
		{0, memoryRequeue},
		{59 * time.Second, memoryRequeue},
		{time.Minute, 30 * time.Second},
		{4 * time.Minute, 30 * time.Second},
		{5 * time.Minute, time.Minute},
		{time.Hour, time.Minute},
	}
	for _, tc := range cases {
		if got := memoryApplyRetryBackoff(tc.elapsed); got != tc.want {
			t.Fatalf("memoryApplyRetryBackoff(%s) = %s, want %s", tc.elapsed, got, tc.want)
		}
	}
}

// TestNoteTransientApply_GraceIsWallClockNotPassCount is the core of the #439
// fix: a reboot-driven storm of reconcile passes inside a few seconds must NOT
// exhaust the grace window, and a run that genuinely outlasts the window must.
func TestNoteTransientApply_GraceIsWallClockNotPassCount(t *testing.T) {
	r := &ProjectReconciler{}
	t0 := time.Now()

	// 200 passes crammed into 5 seconds - the Owns() watch storm.
	for i := range 200 {
		backoff, escalate := r.noteTransientApply("proj", t0.Add(time.Duration(i)*25*time.Millisecond))
		if escalate {
			t.Fatalf("escalated on pass %d after %s, want absorbed", i, time.Duration(i)*25*time.Millisecond)
		}
		if backoff <= 0 {
			t.Fatalf("pass %d: backoff = %s, want a positive requeue", i, backoff)
		}
	}

	// Still inside the grace window an instant before it expires.
	if _, escalate := r.noteTransientApply("proj", t0.Add(memoryApplyTransientGrace-time.Second)); escalate {
		t.Fatal("escalated before the grace window expired")
	}
	// And escalates once it does.
	if _, escalate := r.noteTransientApply("proj", t0.Add(memoryApplyTransientGrace)); !escalate {
		t.Fatal("did not escalate after the grace window expired")
	}
	// Escalating resets the mark, so the next outage gets a full window again.
	if _, escalate := r.noteTransientApply("proj", t0.Add(memoryApplyTransientGrace+time.Second)); escalate {
		t.Fatal("escalated immediately after a prior escalation; grace window was not reset")
	}
}

func TestNoteTransientApply_PerProjectAndClearedOnSuccess(t *testing.T) {
	r := &ProjectReconciler{}
	t0 := time.Now()

	r.noteTransientApply("a", t0)
	// A second project starts its OWN window; project a's head start must not
	// escalate it.
	if _, escalate := r.noteTransientApply("b", t0.Add(memoryApplyTransientGrace-time.Second)); escalate {
		t.Fatal("project b escalated on project a's clock")
	}

	// A successful apply ends the run, so the next failure starts over.
	r.clearTransientApply("a")
	if _, escalate := r.noteTransientApply("a", t0.Add(memoryApplyTransientGrace+time.Minute)); escalate {
		t.Fatal("project a escalated on a cleared run's clock")
	}
}

// applyErrClient fails Patch (the SSA apply path) for one object kind with a
// caller-supplied error, and passes everything else through to envtest.
type applyErrClient struct {
	client.Client
	err   error
	calls *int
}

func (c *applyErrClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if _, isCluster := obj.(*cnpgv1.Cluster); isCluster && c.err != nil {
		if c.calls != nil {
			*c.calls++
		}
		return fmt.Errorf("apply %T %s: %w", obj, obj.GetName(), c.err)
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

// TestReconcile_TransientApplyErrorDoesNotFailTheStack is the end-to-end #439
// assertion: an unreachable admission webhook must not produce a reconcile
// error (which controller-runtime logs as an ERROR "Reconciler error" line, the
// thing that amplified 1 outage into 58) and must not record phase=Failed
// (which is what the critical memory-stack alert reads).
func TestReconcile_TransientApplyErrorDoesNotFailTheStack(t *testing.T) {
	r, reg := newMemoryReconcilerWithReg()
	p := mkMemoryProject(t, "apply-transient")

	calls := 0
	r.Client = &applyErrClient{Client: k8sClient, err: webhookUnreachableErr(), calls: &calls}

	// Every pass in the outage window: no error, no Failed, positive requeue.
	for i := range 5 {
		res, err := reconcileMemory(t, r, p.Name)
		if err != nil {
			t.Fatalf("pass %d: reconcile returned an error on an unreachable webhook: %v", i, err)
		}
		if res.RequeueAfter <= 0 {
			t.Fatalf("pass %d: RequeueAfter = %s, want a positive retry poll", i, res.RequeueAfter)
		}
		got := getProject(t, p.Name)
		if got.Status.Memory == nil {
			t.Fatalf("pass %d: status.memory is nil", i)
		}
		if got.Status.Memory.Phase == "Failed" {
			t.Fatalf("pass %d: phase = Failed on a transient webhook outage", i)
		}
		if c := meta.FindStatusCondition(got.Status.Conditions, "MemoryReady"); c != nil && c.Reason == "ApplyError" {
			t.Fatalf("pass %d: MemoryReady reason = ApplyError on a transient webhook outage", i)
		}
	}
	if calls == 0 {
		t.Fatal("the injected apply failure never fired; the test proves nothing")
	}
	if got := testutil.ToFloat64(r.Metrics.MemoryApplyTransientErrorCounter(p.Name)); got != 5 {
		t.Fatalf("operator_memory_apply_transient_errors_total{project=%q} = %v, want 5", p.Name, got)
	}
	// The gauge the critical alert reads must not report this project Failed.
	r.updateMemoryStackCounts(context.Background())
	if v := gatherMemoryStackProjectPhase(t, reg, p.Name, "Failed"); v != 0 {
		t.Fatalf("operator_memory_stacks{project=%q,phase=Failed} = %v, want 0", p.Name, v)
	}

	// The webhook comes back: the stack applies and the run is cleared.
	r.Client = k8sClient
	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("reconcile after recovery: %v", err)
	}
	if _, still := r.memoryApplyTransientSince[p.Name]; still {
		t.Fatal("transient-failure run not cleared after a successful apply")
	}
}

// TestReconcile_TransientApplyErrorEscalatesAfterGrace proves the fix does not
// trade alert spam for alert blindness: a webhook that never comes back still
// lands as phase=Failed and a returned reconcile error.
func TestReconcile_TransientApplyErrorEscalatesAfterGrace(t *testing.T) {
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "apply-transient-escalate")
	r.Client = &applyErrClient{Client: k8sClient, err: webhookUnreachableErr()}

	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	// Backdate the run's start past the grace window.
	r.memoryApplyTransientSince[p.Name] = time.Now().Add(-memoryApplyTransientGrace - time.Minute)

	_, err := reconcileMemory(t, r, p.Name)
	if err == nil {
		t.Fatal("a webhook unreachable for longer than the grace window must surface as a reconcile error")
	}
	if !strings.Contains(err.Error(), "reconcile memory") {
		t.Fatalf("error = %q, want the failMemory wrapping", err)
	}
	got := getProject(t, p.Name)
	if got.Status.Memory.Phase != "Failed" {
		t.Fatalf("phase = %q, want Failed after the grace window", got.Status.Memory.Phase)
	}
	c := meta.FindStatusCondition(got.Status.Conditions, "MemoryReady")
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != "ApplyError" {
		t.Fatalf("MemoryReady = %+v, want False/ApplyError", c)
	}
}

// TestReconcile_DeniedWebhookStillFailsImmediately pins the narrowness of the
// classifier: a webhook that REJECTS the object is a real spec problem and must
// keep failing on the very first pass, with no grace at all.
func TestReconcile_DeniedWebhookStillFailsImmediately(t *testing.T) {
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "apply-denied")
	r.Client = &applyErrClient{Client: k8sClient, err: webhookDeniedErr()}

	_, err := reconcileMemory(t, r, p.Name)
	if err == nil {
		t.Fatal("a denied admission webhook must fail the reconcile on the first pass")
	}
	got := getProject(t, p.Name)
	if got.Status.Memory.Phase != "Failed" {
		t.Fatalf("phase = %q, want Failed on a denied admission webhook", got.Status.Memory.Phase)
	}
	if _, marked := r.memoryApplyTransientSince[p.Name]; marked {
		t.Fatal("a denied admission webhook was recorded as a transient run")
	}
}
