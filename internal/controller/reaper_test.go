package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// reaperServer returns a CallbackServer with ReaperGrace=1ns so freshly
// created test pods are not protected by the grace window.
func reaperServer() *CallbackServer {
	return &CallbackServer{
		Client:      k8sClient,
		Metrics:     obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Namespace:   testNS,
		ReaperGrace: time.Nanosecond,
	}
}

// mkWrapperPodSvc creates a labelled wrapper Pod + Service named after the pod,
// correlated to taskName/taskUID via the reaper's labels.
func mkWrapperPodSvc(t *testing.T, name, taskName, taskUID string) {
	t.Helper()
	labels := map[string]string{
		agent.LabelManagedBy: agent.ManagedByValue,
		agent.LabelComponent: agent.ComponentAgent,
		agent.LabelTask:      taskName,
	}
	if taskUID != "" {
		labels[agent.LabelTaskUID] = taskUID
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: labels},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{{Name: "wrapper", Image: "wrapper:1"}},
		},
	}
	if err := k8sClient.Create(context.Background(), pod); err != nil {
		t.Fatalf("create pod %s: %v", name, err)
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: labels},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8080}}},
	}
	if err := k8sClient.Create(context.Background(), svc); err != nil {
		t.Fatalf("create service %s: %v", name, err)
	}
}

func podExists(t *testing.T, name string) bool {
	t.Helper()
	err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &corev1.Pod{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("get pod %s: %v", name, err)
	}
	return err == nil
}

func svcExists(t *testing.T, name string) bool {
	t.Helper()
	err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &corev1.Service{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("get service %s: %v", name, err)
	}
	return err == nil
}

func TestReapOrphans_TaskAbsent(t *testing.T) {
	mkWrapperPodSvc(t, "reap-absent", "no-such-task", "uid-x")
	reaperServer().ReapOrphans(context.Background())
	if podExists(t, "reap-absent") {
		t.Error("expected pod for absent task to be reaped")
	}
}

func TestReapOrphans_TerminalPhase(t *testing.T) {
	mkTaskProject(t, "p-reap-ph", 3)
	mkTaskRepository(t, "r-reap-ph", "p-reap-ph")
	mkTask(t, "t-reap-ph", "p-reap-ph", "r-reap-ph")
	setTaskPhase(t, "t-reap-ph", "Succeeded")
	mkWrapperPodSvc(t, "reap-phase", "t-reap-ph", string(getTask(t, "t-reap-ph").UID))

	reaperServer().ReapOrphans(context.Background())
	if podExists(t, "reap-phase") {
		t.Error("expected pod for Succeeded task to be reaped")
	}
}

func TestReapOrphans_TerminalLifecycle(t *testing.T) {
	mkTaskProject(t, "p-reap-lc", 3)
	mkTaskRepository(t, "r-reap-lc", "p-reap-lc")
	mkTask(t, "t-reap-lc", "p-reap-lc", "r-reap-lc")
	tk := getTask(t, "t-reap-lc")
	tk.Status.LifecycleState = "Done"
	if err := k8sClient.Status().Update(context.Background(), tk); err != nil {
		t.Fatalf("set lifecycle: %v", err)
	}
	mkWrapperPodSvc(t, "reap-lc", "t-reap-lc", string(tk.UID))

	reaperServer().ReapOrphans(context.Background())
	if podExists(t, "reap-lc") {
		t.Error("expected pod for Done lifecycle task to be reaped")
	}
}

func TestReapOrphans_StaleUID(t *testing.T) {
	mkTaskProject(t, "p-reap-uid", 3)
	mkTaskRepository(t, "r-reap-uid", "p-reap-uid")
	mkTask(t, "t-reap-uid", "p-reap-uid", "r-reap-uid")
	// Pod carries a UID from a prior incarnation that reused the task name.
	mkWrapperPodSvc(t, "reap-uid", "t-reap-uid", "stale-uid-from-old-task")

	reaperServer().ReapOrphans(context.Background())
	if podExists(t, "reap-uid") {
		t.Error("expected pod with stale task-uid to be reaped")
	}
}

func TestReapOrphans_LiveTaskKept(t *testing.T) {
	mkTaskProject(t, "p-reap-live", 3)
	mkTaskRepository(t, "r-reap-live", "p-reap-live")
	mkTask(t, "t-reap-live", "p-reap-live", "r-reap-live")
	setTaskPhase(t, "t-reap-live", "Running")
	mkWrapperPodSvc(t, "reap-live", "t-reap-live", string(getTask(t, "t-reap-live").UID))

	reaperServer().ReapOrphans(context.Background())
	if !podExists(t, "reap-live") {
		t.Error("expected pod for live non-terminal task to be kept")
	}
}

// TestReapOrphans_CreationGrace verifies that a freshly spawned pod is never
// reaped even when its task is absent in the cache snapshot (finding 1/2/7).
func TestReapOrphans_CreationGrace(t *testing.T) {
	// Use default grace (pollRequeue = 30s); pod is just created so it is fresh.
	srv := &CallbackServer{
		Client:    k8sClient,
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Namespace: testNS,
		// ReaperGrace zero => uses pollRequeue default (30s)
	}
	mkWrapperPodSvc(t, "reap-grace", "no-such-task-grace", "uid-grace")
	srv.ReapOrphans(context.Background())
	if !podExists(t, "reap-grace") {
		t.Error("expected freshly created pod to be protected by grace window")
	}
	// Clean up: delete with reaperServer (no grace) so subsequent tests are clean.
	reaperServer().ReapOrphans(context.Background())
}

// TestReapOrphans_CtxCancelled verifies that a cancelled context stops the
// reaper loop before issuing deletes (finding 6).
func TestReapOrphans_CtxCancelled(t *testing.T) {
	mkWrapperPodSvc(t, "reap-ctx", "no-such-task-ctx", "uid-ctx")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	srv := &CallbackServer{
		Client:      k8sClient,
		Metrics:     obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Namespace:   testNS,
		ReaperGrace: time.Nanosecond,
	}
	srv.ReapOrphans(ctx)
	// Pod should still exist: cancelled ctx means no deletes were issued.
	if !podExists(t, "reap-ctx") {
		t.Error("expected pod to be kept when context is already cancelled")
	}
	// Clean up
	reaperServer().ReapOrphans(context.Background())
}

// TestReapOrphans_OrphanedServiceReaped verifies that a Service whose backing
// Pod is already gone is reaped on the next reaper pass (finding: service leak
// when Pod delete succeeds but Service delete fails transiently, pod already
// gone on next pass so pod-list-only reaper never sees it again).
func TestReapOrphans_OrphanedServiceReaped(t *testing.T) {
	ctx := context.Background()
	srv := reaperServer()

	// Create a labelled Service without a matching Pod to simulate the state
	// left behind after a successful Pod delete but a failed Service delete.
	labels := map[string]string{
		agent.LabelManagedBy: agent.ManagedByValue,
		agent.LabelComponent: agent.ComponentAgent,
		agent.LabelTask:      "no-such-task-svc-orphan",
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "reap-orphan-svc",
			Namespace: testNS,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8080}}},
	}
	if err := k8sClient.Create(ctx, svc); err != nil {
		t.Fatalf("create orphan service: %v", err)
	}

	srv.ReapOrphans(ctx)

	if svcExists(t, "reap-orphan-svc") {
		t.Error("expected orphaned Service (no backing Pod) to be reaped")
	}
}

// TestReapOrphans_OrphanServiceSuccessCounter verifies that a successful second-pass
// orphan Service delete increments operator_orphan_reaped_total (finding: success
// metric missing from else branch, violating rule 13).
func TestReapOrphans_OrphanServiceSuccessCounter(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	srv := &CallbackServer{
		Client:      k8sClient,
		Metrics:     obs.NewOperatorMetrics(reg),
		Namespace:   testNS,
		ReaperGrace: time.Nanosecond,
	}

	labels := map[string]string{
		agent.LabelManagedBy: agent.ManagedByValue,
		agent.LabelComponent: agent.ComponentAgent,
		agent.LabelTask:      "no-such-task-svc-counter",
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "reap-svc-counter",
			Namespace: testNS,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8080}}},
	}
	if err := k8sClient.Create(ctx, svc); err != nil {
		t.Fatalf("create orphan service: %v", err)
	}

	srv.ReapOrphans(ctx)

	if svcExists(t, "reap-svc-counter") {
		t.Fatal("expected orphaned Service to be reaped")
	}

	// Verify operator_orphan_reaped_total{reason="orphan service"} == 1.
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	var got float64
	for _, mf := range mfs {
		if mf.GetName() == "operator_orphan_reaped_total" {
			for _, m := range mf.GetMetric() {
				for _, lp := range m.GetLabel() {
					if lp.GetName() == "reason" && lp.GetValue() == "orphan service" {
						got = m.GetCounter().GetValue()
					}
				}
			}
		}
	}
	if got != 1 {
		t.Errorf("operator_orphan_reaped_total{reason=orphan service} = %v, want 1", got)
	}
}

// TestReapOrphans_YoungServiceNotReaped guards the spawn-vs-reap race: a Service
// is created right after its Pod, and the Pod LIST and Service LIST in one reaper
// pass hit the cache at different instants. A freshly created Service whose Pod
// has not yet propagated to the Pod LIST must NOT be deleted, or the reaper would
// sever the operator -> wrapper connection for a still-starting agent.
func TestReapOrphans_YoungServiceNotReaped(t *testing.T) {
	ctx := context.Background()
	// Real grace window so a just-created Service is protected.
	srv := &CallbackServer{
		Client:      k8sClient,
		Metrics:     obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Namespace:   testNS,
		ReaperGrace: time.Hour,
	}

	labels := map[string]string{
		agent.LabelManagedBy: agent.ManagedByValue,
		agent.LabelComponent: agent.ComponentAgent,
		agent.LabelTask:      "no-such-task-young-svc",
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "reap-young-svc",
			Namespace: testNS,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8080}}},
	}
	if err := k8sClient.Create(ctx, svc); err != nil {
		t.Fatalf("create young service: %v", err)
	}

	srv.ReapOrphans(ctx)

	if !svcExists(t, "reap-young-svc") {
		t.Error("expected young Service (within grace) to be kept; reaper raced a still-propagating Pod")
	}

	// Clean up so it does not leak into later tests.
	_ = k8sClient.Delete(ctx, svc)
}
