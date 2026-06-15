// audit-fixes tests for task_controller.go findings 1-8.
package controller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// --- Finding 7: bootDeadlineExceeded anchors to StartTime, not CreationTimestamp ---

// TestBootDeadlineExceeded_UsesStartTime verifies that a pod whose
// CreationTimestamp is older than agentBootDeadline but whose StartTime is
// recent is NOT considered past the deadline. Before the fix,
// bootDeadlineExceeded used CreationTimestamp, so a slow image-pull that
// consumed the 5-minute window would trigger a needless respawn.
func TestBootDeadlineExceeded_UsesStartTime(t *testing.T) {
	// Pod was created long ago (CreationTimestamp >> agentBootDeadline),
	// but the container runtime only started it recently (StartTime = now).
	recentStart := metav1.NewTime(time.Now().Add(-30 * time.Second))
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * agentBootDeadline)),
		},
		Status: corev1.PodStatus{
			StartTime: &recentStart,
		},
	}
	if bootDeadlineExceeded(&pod) {
		t.Error("pod with recent StartTime must NOT be past the boot deadline even if CreationTimestamp is old; " +
			"image-pull time should not consume the readiness window")
	}
}

// TestBootDeadlineExceeded_StartTimeOld verifies that a pod whose StartTime is
// older than agentBootDeadline IS considered past the deadline.
func TestBootDeadlineExceeded_StartTimeOld(t *testing.T) {
	oldStart := metav1.NewTime(time.Now().Add(-2 * agentBootDeadline))
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.NewTime(time.Now().Add(-3 * agentBootDeadline)),
		},
		Status: corev1.PodStatus{
			StartTime: &oldStart,
		},
	}
	if !bootDeadlineExceeded(&pod) {
		t.Error("pod with old StartTime must be past the boot deadline")
	}
}

// TestBootDeadlineExceeded_NoStartTimeFallsBackToCreation verifies that when
// StartTime is unset (pod still being scheduled), CreationTimestamp is used.
func TestBootDeadlineExceeded_NoStartTimeFallsBackToCreation(t *testing.T) {
	oldPod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * agentBootDeadline)),
		},
		// No StartTime: pod has not been accepted by kubelet yet.
	}
	if !bootDeadlineExceeded(&oldPod) {
		t.Error("with no StartTime, should fall back to CreationTimestamp; old pod must be past deadline")
	}

	newPod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.Now(),
		},
	}
	if bootDeadlineExceeded(&newPod) {
		t.Error("with no StartTime and recent CreationTimestamp, pod must NOT be past deadline")
	}
}

// --- Finding 5: bumpRecreations uses RetryOnConflict ---

// conflictOnceMetadataWriter returns a Conflict error on the first Update
// call to the main resource (not Status), then delegates normally.
type conflictOnceMetadataWriter struct {
	client.Client
	calls *atomic.Int32
}

func (c *conflictOnceMetadataWriter) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if c.calls.Add(1) == 1 {
		return apierrors.NewConflict(schema.GroupResource{Group: "tatara.dev", Resource: "tasks"}, obj.GetName(), nil)
	}
	return c.Client.Update(ctx, obj, opts...)
}

// TestBumpRecreations_RetriesOnConflict verifies that bumpRecreations
// successfully increments the recreation counter even when the first Update
// returns a Conflict. Before the fix, bumpRecreations did a plain Update
// (no RetryOnConflict) and would return an error on any write conflict.
func TestBumpRecreations_RetriesOnConflict(t *testing.T) {
	ctx := context.Background()

	mkTaskProject(t, "p-bumpretry", 3)
	mkTaskRepository(t, "r-bumpretry", "p-bumpretry")
	mkTask(t, "t-bumpretry", "p-bumpretry", "r-bumpretry")
	task := getTask(t, "t-bumpretry")

	var calls atomic.Int32
	cc := &conflictOnceMetadataWriter{Client: k8sClient, calls: &calls}
	r := &TaskReconciler{
		Client:  cc,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
	}

	if err := r.bumpRecreations(ctx, task); err != nil {
		t.Fatalf("bumpRecreations must succeed despite initial conflict: %v", err)
	}

	got := getTask(t, "t-bumpretry")
	if got.Annotations[annPodRecreations] != "1" {
		t.Errorf("pod recreations = %q, want 1", got.Annotations[annPodRecreations])
	}
	if calls.Load() < 2 {
		t.Errorf("expected at least 2 Update calls (retry after conflict), got %d", calls.Load())
	}
}

// --- Finding 1: recordTurn wraps both Updates in RetryOnConflict ---

// conflictOnceStatusAndMetadataWriter returns a Conflict on the FIRST
// Status().Update call, then delegates normally.
type conflictOnceStatusAndMetaWriter struct {
	client.Client
	statusCalls *atomic.Int32
}

func (c *conflictOnceStatusAndMetaWriter) Status() client.SubResourceWriter {
	return &conflictOnceWriter{
		SubResourceWriter: c.Client.Status(),
		calls:             c.statusCalls,
		gr:                schema.GroupResource{Group: "tatara.dev", Resource: "tasks"},
		name:              "",
	}
}

// TestRecordTurn_TurnsCompletedLandsAfterStatusConflict verifies that when the
// Status().Update inside recordTurn conflicts (e.g. callback server wrote
// CumulativeTokens concurrently), the TurnsCompleted increment is retried and
// eventually lands. Before the fix, a conflict here returned an error and the
// increment was lost, weakening the maxTurns safety bound.
func TestRecordTurn_TurnsCompletedLandsAfterStatusConflict(t *testing.T) {
	ctx := context.Background()

	mkTaskProject(t, "p-rcretry", 3)
	mkTaskRepository(t, "r-rcretry", "p-rcretry")
	mkTask(t, "t-rcretry", "p-rcretry", "r-rcretry")
	// Simulate a prior callback: annTurnComplete is set so recordTurn bumps TurnsCompleted.
	annotate(t, "t-rcretry", map[string]string{
		annTurnComplete: time.Now().UTC().Format(time.RFC3339),
		annCurrentTurn:  "old-turn",
	})
	task := getTask(t, "t-rcretry")

	var statusCalls atomic.Int32
	cc := &conflictOnceStatusAndMetaWriter{Client: k8sClient, statusCalls: &statusCalls}
	r := &TaskReconciler{
		Client:  cc,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
	}

	if _, err := r.recordTurn(ctx, task, "new-turn", ""); err != nil {
		t.Fatalf("recordTurn must succeed despite status conflict: %v", err)
	}

	got := getTask(t, "t-rcretry")
	if got.Status.TurnsCompleted != 1 {
		t.Errorf("TurnsCompleted = %d, want 1 (must land despite initial conflict)", got.Status.TurnsCompleted)
	}
}

// --- Finding 2: markSubtaskDone wraps Status().Update in RetryOnConflict ---

// TestMarkSubtaskDone_RetriesOnStatusConflict verifies that markSubtaskDone
// successfully marks the subtask Done even when the first Status().Update
// returns a Conflict (the callback's recordResult raced it). Before the fix,
// markSubtaskDone used a plain Status().Update and would fail on conflict.
func TestMarkSubtaskDone_RetriesOnStatusConflict(t *testing.T) {
	ctx := context.Background()

	mkTaskProject(t, "p-msdretry", 3)
	mkTaskRepository(t, "r-msdretry", "p-msdretry")
	mkTask(t, "t-msdretry", "p-msdretry", "r-msdretry")
	mkSubtask(t, "t-msdretry-s1", "t-msdretry", 1)

	var statusCalls atomic.Int32
	cc := &conflictOnceStatusAndMetaWriter{Client: k8sClient, statusCalls: &statusCalls}
	r := &TaskReconciler{
		Client:  cc,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
	}

	if err := r.markSubtaskDone(ctx, testNS, "t-msdretry-s1", "turn-xyz"); err != nil {
		t.Fatalf("markSubtaskDone must succeed despite status conflict: %v", err)
	}

	st := getSubtask(t, "t-msdretry-s1")
	if st.Status.Phase != "Done" {
		t.Errorf("subtask phase = %q, want Done", st.Status.Phase)
	}
	if st.Status.TurnID != "turn-xyz" {
		t.Errorf("subtask TurnID = %q, want turn-xyz", st.Status.TurnID)
	}
	if statusCalls.Load() < 2 {
		t.Errorf("expected >= 2 Status().Update calls (retry), got %d", statusCalls.Load())
	}
}

// --- Finding 3: terminate wraps Status().Update in RetryOnConflict ---

// TestTerminate_StatusLandsAfterConflict verifies that terminate successfully
// marks the Task terminal even when the first Status().Update conflicts (e.g.
// the callback server wrote CumulativeTokens between the Get and the Update).
// Before the fix, terminate operated on the stale task object with no retry,
// leaving the Task NOT terminal on conflict.
func TestTerminate_StatusLandsAfterConflict(t *testing.T) {
	ctx := context.Background()

	mkTaskProject(t, "p-termretry", 3)
	mkTaskRepository(t, "r-termretry", "p-termretry")
	mkTask(t, "t-termretry", "p-termretry", "r-termretry")
	setProjectMemoryReady(t, "p-termretry", "http://mem.svc:8080")
	setTaskPhase(t, "t-termretry", "Running")

	// Create the wrapper pod so DeleteSession / deleteWrapper don't fail.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "wrapper-t-termretry", Namespace: testNS},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "w", Image: "w:1"}}},
	}
	if err := k8sClient.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create pod: %v", err)
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "wrapper-t-termretry", Namespace: testNS},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8080}}},
	}
	if err := k8sClient.Create(ctx, svc); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create svc: %v", err)
	}

	var statusCalls atomic.Int32
	cc := &conflictOnceStatusAndMetaWriter{Client: k8sClient, statusCalls: &statusCalls}
	r := &TaskReconciler{
		Client:  cc,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:   testNS,
			CallbackURL: "http://op-internal.tatara.svc:8082",
		},
	}

	task := getTask(t, "t-termretry")
	if _, err := r.terminate(ctx, task, "Failed", "TestReason", "test conflict retry"); err != nil {
		t.Fatalf("terminate must succeed despite status conflict: %v", err)
	}

	got := getTask(t, "t-termretry")
	if got.Status.Phase != "Failed" {
		t.Errorf("phase = %q, want Failed (must land despite initial conflict)", got.Status.Phase)
	}
	cond := findCond(got.Status.Conditions, "Ready")
	if cond == nil || cond.Reason != "TestReason" {
		t.Errorf("Ready condition = %+v, want reason TestReason", cond)
	}
}

// --- Finding 4: TurnSubmit metric + INFO log on SubmitTurn ---

// TestTurnSubmitted_MetricEmitted verifies that a successful plan-turn
// submission emits operator_turn_submit_total{kind, result="ok"} and
// operator_turn_submit_duration_seconds. Before the fix, no metric was emitted
// for the happy-path turn submission.
func TestTurnSubmitted_MetricEmitted(t *testing.T) {
	mkTaskProject(t, "p-tsmetic", 3)
	mkTaskRepository(t, "r-tsmetic", "p-tsmetic")

	tk := &tatarav1alpha1.Task{}
	tk.Name = "t-tsmetic"
	tk.Namespace = testNS
	tk.Spec.ProjectRef = "p-tsmetic"
	tk.Spec.RepositoryRef = "r-tsmetic"
	tk.Spec.Goal = "test metric"
	tk.Spec.Kind = "implement"
	if err := k8sClient.Create(context.Background(), tk); err != nil {
		t.Fatalf("create task: %v", err)
	}
	setProjectMemoryReady(t, "p-tsmetic", "http://mem-p-tsmetic.tatara.svc:8080")

	fs := newFakeSession()
	r, reg := newTaskReconcilerReg(fs)

	// Spawn pod.
	if _, err := reconcileTask(t, r, "t-tsmetic"); err != nil {
		t.Fatalf("reconcile spawn: %v", err)
	}
	markPodReady(t, "wrapper-t-tsmetic")

	// Submit plan turn - this is where the metric should be emitted.
	if _, err := reconcileTask(t, r, "t-tsmetic"); err != nil {
		t.Fatalf("reconcile plan turn: %v", err)
	}

	// Verify counter.
	if v := counterValue(t, reg, "operator_turn_submit_total",
		map[string]string{"kind": "implement", "result": "ok"}); v < 1 {
		t.Errorf("operator_turn_submit_total{kind=implement,result=ok} = %v, want >= 1 (metric must be emitted on successful turn submit)", v)
	}

	// Verify histogram (any observation means the duration was recorded).
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	found := false
	for _, mf := range mfs {
		if mf.GetName() == "operator_turn_submit_duration_seconds" {
			for _, m := range mf.GetMetric() {
				if m.GetHistogram().GetSampleCount() > 0 {
					found = true
				}
			}
		}
	}
	if !found {
		t.Error("operator_turn_submit_duration_seconds must have at least one observation after a successful turn submit")
	}
}

// TestTurnSubmitted_ErrorMetricEmitted verifies that a failed SubmitTurn call
// emits operator_turn_submit_total{result="error"}.
func TestTurnSubmitted_ErrorMetricEmitted(t *testing.T) {
	mkTaskProject(t, "p-tserr", 3)
	mkTaskRepository(t, "r-tserr", "p-tserr")

	// Use a task with an explicit kind so the label is non-empty and unambiguous.
	tk := &tatarav1alpha1.Task{}
	tk.Name = "t-tserr"
	tk.Namespace = testNS
	tk.Spec.ProjectRef = "p-tserr"
	tk.Spec.RepositoryRef = "r-tserr"
	tk.Spec.Goal = "test error metric"
	tk.Spec.Kind = "review"
	if err := k8sClient.Create(context.Background(), tk); err != nil {
		t.Fatalf("create task: %v", err)
	}
	setProjectMemoryReady(t, "p-tserr", "http://mem-p-tserr.tatara.svc:8080")

	fs := newFakeSession()
	r, reg := newTaskReconcilerReg(fs)

	if _, err := reconcileTask(t, r, "t-tserr"); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	markPodReady(t, "wrapper-t-tserr")
	fs.submitErr = &agent.HTTPError{Status: 500, Body: "internal error"}

	// Submit fails - error metric must fire.
	if _, err := reconcileTask(t, r, "t-tserr"); err == nil {
		t.Fatal("expected error from HTTP 500 submit")
	}

	if v := counterValue(t, reg, "operator_turn_submit_total",
		map[string]string{"kind": "review", "result": "error"}); v < 1 {
		t.Errorf("operator_turn_submit_total{kind=review,result=error} = %v, want >= 1 (metric must fire on submit failure)", v)
	}
}
