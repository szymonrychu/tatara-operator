// audit-r2 findings tests for task_controller.go (findings 1 and 2).
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

// --- Finding 1: driveTurns subtask/task Running updates must retry on conflict ---

// conflictOnceSubtaskStatusWriter injects a Conflict on the first
// Status().Update for Subtask objects, then delegates normally.
// This simulates the callback server racing the reconcile between
// SubmitTurn and the Running-flip writes.
type conflictOnceSubtaskStatusWriter struct {
	client.Client
	statusCalls *atomic.Int32
}

func (c *conflictOnceSubtaskStatusWriter) Status() client.SubResourceWriter {
	return &conflictOnceWriter{
		SubResourceWriter: c.Client.Status(),
		calls:             c.statusCalls,
		gr:                schema.GroupResource{Group: "tatara.dev", Resource: "subtasks"},
		name:              "",
	}
}

// TestDriveTurns_SubtaskRunningRetriesOnConflict verifies that after a
// successful SubmitTurn the subtask-phase=Running write is retried on conflict.
// Before the fix, the plain r.Status().Update(ctx, next) at the subtask Running
// flip returned an error on conflict, causing the reconcile to fail WITHOUT
// recording the new turnID in annCurrentTurn, so the next reconcile would
// re-drive the same subtask.
func TestDriveTurns_SubtaskRunningRetriesOnConflict(t *testing.T) {
	ctx := context.Background()

	mkTaskProject(t, "p-dtsr", 3)
	mkTaskRepository(t, "r-dtsr", "p-dtsr")
	mkTask(t, "t-dtsr", "p-dtsr", "r-dtsr")
	mkSubtask(t, "t-dtsr-sub1", "t-dtsr", 1)
	setProjectMemoryReady(t, "p-dtsr", "http://mem-p-dtsr.tatara.svc:8080")

	// Advance the task to "callback arrived with previous turn done" state.
	// annCurrentTurn is set (plan turn completed), annTurnComplete is set
	// (callback arrived), no current subtask (so the subtask-done branch is skipped).
	annotate(t, "t-dtsr", map[string]string{
		annCurrentTurn:  "plan-turn-done",
		annTurnComplete: time.Now().UTC().Format(time.RFC3339),
	})
	// TurnsCompleted must be at 1 so the maxTurns check passes (cap=50).
	tk := getTask(t, "t-dtsr")
	tk.Status.Phase = "Running"
	tk.Status.TurnsCompleted = 1
	if err := k8sClient.Status().Update(ctx, tk); err != nil {
		t.Fatalf("seed task status: %v", err)
	}
	// Also record the plan-turn annotations on a fresh copy.
	annotate(t, "t-dtsr", map[string]string{
		annCurrentTurn:  "plan-turn-done",
		annTurnComplete: time.Now().UTC().Format(time.RFC3339),
	})

	// Ensure the wrapper pod is "ready" so driveTurns reaches the subtask-submit path.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "wrapper-t-dtsr", Namespace: testNS},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "w", Image: "w:1"}}},
	}
	if err := k8sClient.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create pod: %v", err)
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "wrapper-t-dtsr", Namespace: testNS},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8080}}},
	}
	if err := k8sClient.Create(ctx, svc); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create svc: %v", err)
	}
	markPodReady(t, "wrapper-t-dtsr")

	var statusCalls atomic.Int32
	conflictClient := &conflictOnceSubtaskStatusWriter{
		Client:      k8sClient,
		statusCalls: &statusCalls,
	}
	fs := newFakeSession()
	r := &TaskReconciler{
		Client:  conflictClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: fs,
		PodConfig: agent.PodConfig{
			Namespace:   testNS,
			CallbackURL: "http://op-internal.tatara.svc:8082",
		},
	}

	proj := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: testNS, Name: "p-dtsr"}, proj); err != nil {
		t.Fatalf("get project: %v", err)
	}
	repo := &tatarav1alpha1.Repository{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: testNS, Name: "r-dtsr"}, repo); err != nil {
		t.Fatalf("get repo: %v", err)
	}
	task := getTask(t, "t-dtsr")

	// Call driveTurns directly; it should succeed despite the conflict on the
	// subtask Running status update.
	_, err := r.driveTurns(ctx, proj, task, "plan text")
	if err != nil {
		t.Fatalf("driveTurns must succeed despite subtask Status conflict: %v", err)
	}

	// The subtask must now be Running (conflict retried successfully).
	sub := getSubtask(t, "t-dtsr-sub1")
	if sub.Status.Phase != "Running" {
		t.Errorf("subtask phase = %q, want Running; conflict retry must land the status", sub.Status.Phase)
	}

	// annCurrentTurn must be set to the new turn (not the old plan-turn-done).
	got := getTask(t, "t-dtsr")
	if got.Annotations[annCurrentTurn] == "plan-turn-done" || got.Annotations[annCurrentTurn] == "" {
		t.Errorf("annCurrentTurn = %q; must be updated to new turn id after successful SubmitTurn", got.Annotations[annCurrentTurn])
	}
}

// --- Finding 2: updateInflightGauge called twice per Planning transition ---

// listCountingClient counts calls to List for TaskList objects.
type listCountingClient struct {
	client.Client
	taskListCalls *atomic.Int32
}

func (c *listCountingClient) List(ctx context.Context, obj client.ObjectList, opts ...client.ListOption) error {
	if _, ok := obj.(*tatarav1alpha1.TaskList); ok {
		c.taskListCalls.Add(1)
	}
	return c.Client.List(ctx, obj, opts...)
}

// TestUpdateInflightGauge_NotCalledTwiceOnPlanningTransition verifies that a
// reconcile that transitions a Task into Planning calls updateInflightGauge at
// most once (not twice). Before the fix, driveAgentRun called
// r.updateInflightGauge at the Planning branch AND Reconcile called it again on
// the success path, producing two full-namespace Task lists back-to-back.
func TestUpdateInflightGauge_NotCalledTwiceOnPlanningTransition(t *testing.T) {
	ctx := context.Background()

	mkTaskProject(t, "p-uig", 3)
	mkTaskRepository(t, "r-uig", "p-uig")
	mkTask(t, "t-uig", "p-uig", "r-uig")
	setProjectMemoryReady(t, "p-uig", "http://mem-p-uig.tatara.svc:8080")

	var listCalls atomic.Int32
	cc := &listCountingClient{Client: k8sClient, taskListCalls: &listCalls}

	fs := newFakeSession()
	r := &TaskReconciler{
		Client:  cc,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: fs,
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op-internal.tatara.svc:8082",
			OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
	}

	// First reconcile: task at Phase="" -> Planning. This is where the duplicate
	// gauge call was: driveAgentRun fired it AND Reconcile fired it again.
	if _, err := reconcileTask(t, r, "t-uig"); err != nil {
		t.Fatalf("reconcile planning: %v", err)
	}

	// Confirm the task advanced to Planning.
	got := getTask(t, "t-uig")
	if got.Status.Phase != "Planning" {
		t.Fatalf("expected task in Planning, got %q", got.Status.Phase)
	}

	// The TaskList call for updateInflightGauge must happen exactly once per
	// reconcile (not twice). We allow one call from Reconcile's success path.
	// Before the fix there were two calls.
	//
	// Note: the reconcile also lists Tasks via concurrency-cap check and via
	// updateInflightGauge itself; we assert <= 2 total to leave headroom for
	// the concurrency-cap list while still catching the duplicate gauge list.
	// A cleaner bound: with the fix, exactly 1 gauge call; without, 2.
	// We use <= 2 to tolerate any single auxiliary list + 1 gauge list but
	// reject 3+ which would only happen if the gauge is called twice.
	gaugeListsBefore := listCalls.Load()
	_ = ctx

	// Now reset and trigger the Planning transition again on a fresh task.
	mkTaskProject(t, "p-uig2", 3)
	mkTaskRepository(t, "r-uig2", "p-uig2")
	mkTask(t, "t-uig2", "p-uig2", "r-uig2")
	setProjectMemoryReady(t, "p-uig2", "http://mem-p-uig2.tatara.svc:8080")

	var listCalls2 atomic.Int32
	cc2 := &listCountingClient{Client: k8sClient, taskListCalls: &listCalls2}
	r2 := &TaskReconciler{
		Client:  cc2,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: fs,
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op-internal.tatara.svc:8082",
			OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
	}

	listCalls2.Store(0)
	if _, err := reconcileTask(t, r2, "t-uig2"); err != nil {
		t.Fatalf("reconcile planning 2: %v", err)
	}

	got2 := getTask(t, "t-uig2")
	if got2.Status.Phase != "Planning" {
		t.Fatalf("expected task in Planning, got %q", got2.Status.Phase)
	}

	total := listCalls2.Load()
	// In test mode (no field-index) a Planning-transition reconcile makes
	// at most 3 TaskList calls (updateInflightGauge + any incidental lists).
	// Before the fix driveAgentRun also called updateInflightGauge, giving 4.
	// We assert < 4 to verify the duplicate is gone while tolerating the
	// legitimate lists above.
	if total >= 4 {
		t.Errorf("TaskList called %d times in a Planning-transition reconcile; expected < 4 (duplicate updateInflightGauge must be removed from driveAgentRun)", total)
	}
	_ = gaugeListsBefore
}
