// audit-r3 findings tests for task_controller.go (findings 1 and 2 of round 3).
// Finding 1 (medium/observability): terminate must emit an INFO log on every
//
//	terminal Task transition.
//
// Finding 2 (low/observability): terminate must emit l.Error on DeleteSession
//
//	failure so persistent teardown failures are observable.
//
// Log content is code-review-verified; the tests below are structural guards
// confirming the paths work correctly with both success and failure sessions.
package controller

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// errSession is a session whose DeleteSession always returns an error.
type errSession struct {
	*fakeSession
	deleteErr error
}

func (e *errSession) DeleteSession(ctx context.Context, baseURL string) error {
	return e.deleteErr
}

// captureLogger returns a context with a zap logger that writes to buf.
func captureLogger(buf *bytes.Buffer) context.Context {
	logger := zap.New(zap.WriteTo(buf), zap.UseDevMode(true))
	return log.IntoContext(context.Background(), logger)
}

// --- Finding 1: terminate must emit INFO log on every terminal transition ---

// TestTerminate_InfoLogOnSuccess verifies that a successful terminate call
// (Succeeded) logs at INFO. Before the fix, terminate emitted no INFO log
// whatsoever, making terminal Task transitions invisible in the log stream.
func TestTerminate_InfoLogOnSuccess(t *testing.T) {
	var buf bytes.Buffer
	ctx := captureLogger(&buf)

	mkTaskProject(t, "p-termlog", 3)
	mkTaskRepository(t, "r-termlog", "p-termlog")
	mkTask(t, "t-termlog", "p-termlog", "r-termlog")
	setProjectMemoryReady(t, "p-termlog", "http://mem.svc:8080")
	setTaskPhase(t, "t-termlog", "Running")

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "wrapper-t-termlog", Namespace: testNS},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "w", Image: "w:1"}}},
	}
	if err := k8sClient.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create pod: %v", err)
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "wrapper-t-termlog", Namespace: testNS},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8080}}},
	}
	if err := k8sClient.Create(ctx, svc); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create svc: %v", err)
	}

	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:   testNS,
			CallbackURL: "http://op-internal.tatara.svc:8082",
		},
	}

	task := getTask(t, "t-termlog")
	if _, err := r.terminate(ctx, task, "Succeeded", "NoPendingSubtasks", "all done"); err != nil {
		t.Fatalf("terminate: %v", err)
	}

	// The task must have reached the terminal phase.
	got := getTask(t, "t-termlog")
	if got.Status.Phase != "Succeeded" {
		t.Errorf("phase = %q, want Succeeded", got.Status.Phase)
	}

	// The log buffer must contain "task terminated" (INFO log added by finding 1 fix).
	output := buf.String()
	if !bytes.Contains([]byte(output), []byte("task terminated")) {
		t.Errorf("terminate must emit INFO log containing 'task terminated'; log output:\n%s", output)
	}
}

// TestTerminate_InfoLogOnFailed verifies that terminate also logs INFO for a
// Failed terminal state (not only Succeeded).
func TestTerminate_InfoLogOnFailed(t *testing.T) {
	var buf bytes.Buffer
	ctx := captureLogger(&buf)

	mkTaskProject(t, "p-termfail", 3)
	mkTaskRepository(t, "r-termfail", "p-termfail")
	mkTask(t, "t-termfail", "p-termfail", "r-termfail")
	setProjectMemoryReady(t, "p-termfail", "http://mem.svc:8080")
	setTaskPhase(t, "t-termfail", "Running")

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "wrapper-t-termfail", Namespace: testNS},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "w", Image: "w:1"}}},
	}
	if err := k8sClient.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create pod: %v", err)
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "wrapper-t-termfail", Namespace: testNS},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8080}}},
	}
	if err := k8sClient.Create(ctx, svc); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create svc: %v", err)
	}

	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:   testNS,
			CallbackURL: "http://op-internal.tatara.svc:8082",
		},
	}

	task := getTask(t, "t-termfail")
	if _, err := r.terminate(ctx, task, "Failed", "TurnTimeout", "turn timed out"); err != nil {
		t.Fatalf("terminate: %v", err)
	}

	output := buf.String()
	if !bytes.Contains([]byte(output), []byte("task terminated")) {
		t.Errorf("terminate must emit INFO log containing 'task terminated' for Failed phase; log output:\n%s", output)
	}
}

// --- Finding 2: terminate must log l.Error on DeleteSession failure ---

// TestTerminate_LogsErrorOnDeleteSessionFailure verifies that when DeleteSession
// returns an error (e.g. wrapper pod already gone or network issue), terminate
// emits an Error-level log so persistent teardown failures are observable beyond
// the buried SessionDeleteFailed status condition. Before the fix, the error
// was silently swallowed into the condition with no log.
func TestTerminate_LogsErrorOnDeleteSessionFailure(t *testing.T) {
	var buf bytes.Buffer
	ctx := captureLogger(&buf)

	mkTaskProject(t, "p-termdsf", 3)
	mkTaskRepository(t, "r-termdsf", "p-termdsf")
	mkTask(t, "t-termdsf", "p-termdsf", "r-termdsf")
	setProjectMemoryReady(t, "p-termdsf", "http://mem.svc:8080")
	setTaskPhase(t, "t-termdsf", "Running")

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "wrapper-t-termdsf", Namespace: testNS},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "w", Image: "w:1"}}},
	}
	if err := k8sClient.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create pod: %v", err)
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "wrapper-t-termdsf", Namespace: testNS},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8080}}},
	}
	if err := k8sClient.Create(ctx, svc); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create svc: %v", err)
	}

	sess := &errSession{
		fakeSession: newFakeSession(),
		deleteErr:   errors.New("wrapper gone"),
	}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: sess,
		PodConfig: agent.PodConfig{
			Namespace:   testNS,
			CallbackURL: "http://op-internal.tatara.svc:8082",
		},
	}

	task := getTask(t, "t-termdsf")
	if _, err := r.terminate(ctx, task, "Failed", "PodLost", "pod lost"); err != nil {
		t.Fatalf("terminate must still succeed on DeleteSession error (best-effort): %v", err)
	}

	// Task must still reach terminal phase (DeleteSession failure is non-fatal).
	got := getTask(t, "t-termdsf")
	if got.Status.Phase != "Failed" {
		t.Errorf("phase = %q, want Failed (terminate must succeed despite DeleteSession error)", got.Status.Phase)
	}

	// The log must contain an error-level entry for the DeleteSession failure.
	output := buf.String()
	if !bytes.Contains([]byte(output), []byte("delete session")) {
		t.Errorf("terminate must emit error log containing 'delete session' on DeleteSession failure; log output:\n%s", output)
	}
}
