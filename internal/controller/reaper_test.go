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

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
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

func taskExists(t *testing.T, name string) bool {
	t.Helper()
	err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &tatarav1alpha1.Task{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("get task %s: %v", name, err)
	}
	return err == nil
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

// TestReapOrphans_TerminalPhaseWithLiveLifecycleKept covers the pod-blip fix:
// a task shows phase Succeeded transiently between a turn-batch drain
// (NoPendingSubtasks) and resetAgentRun reviving it, while its lifecycle
// DeployState is still live (Conversation). The reaper must NOT phase-reap it -
// that would kill the warm pod mid-continuation.
func TestReapOrphans_TerminalPhaseWithLiveLifecycleKept(t *testing.T) {
	mkTaskProject(t, "p-reap-phlc", 3)
	mkTaskRepository(t, "r-reap-phlc", "p-reap-phlc")
	mkTask(t, "t-reap-phlc", "p-reap-phlc", "r-reap-phlc")
	setTaskPhase(t, "t-reap-phlc", "Succeeded")
	tk := getTask(t, "t-reap-phlc")
	tk.Status.DeployState = "Conversation"
	if err := k8sClient.Status().Update(context.Background(), tk); err != nil {
		t.Fatalf("set lifecycle: %v", err)
	}
	mkWrapperPodSvc(t, "reap-phlc", "t-reap-phlc", string(tk.UID))

	reaperServer().ReapOrphans(context.Background())
	if !podExists(t, "reap-phlc") {
		t.Error("expected pod for phase-Succeeded task with live Conversation lifecycle to be kept")
	}
}

func TestReapOrphans_TerminalLifecycle(t *testing.T) {
	mkTaskProject(t, "p-reap-lc", 3)
	mkTaskRepository(t, "r-reap-lc", "p-reap-lc")
	mkTask(t, "t-reap-lc", "p-reap-lc", "r-reap-lc")
	tk := getTask(t, "t-reap-lc")
	tk.Status.DeployState = "Done"
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

// setTaskAnns sets metadata annotations on the named Task (a metadata Update,
// separate from the status subresource).
func setTaskAnns(t *testing.T, name string, anns map[string]string) {
	t.Helper()
	tk := getTask(t, name)
	if tk.Annotations == nil {
		tk.Annotations = map[string]string{}
	}
	for k, v := range anns {
		tk.Annotations[k] = v
	}
	if err := k8sClient.Update(context.Background(), tk); err != nil {
		t.Fatalf("set annotations %s: %v", name, err)
	}
}

// idleReaperServer arms the issue #237 idle backstop: ReaperGrace tiny so a fresh
// pod is eligible, and IdlePodReapAfter tiny so any pod with no live turn is past
// the idle window immediately.
func idleReaperServer() *CallbackServer {
	s := reaperServer()
	s.IdlePodReapAfter = time.Nanosecond
	return s
}

// TestReapOrphans_IdleNoLiveTurn covers issue #237: a non-terminal Task whose
// wrapper delivered its turn-complete callback (annCurrentTurn set,
// annTurnComplete set => no in-flight turn) but was never torn down is reaped
// once it has sat idle past IdlePodReapAfter.
func TestReapOrphans_IdleNoLiveTurn(t *testing.T) {
	mkTaskProject(t, "p-reap-idle", 3)
	mkTaskRepository(t, "r-reap-idle", "p-reap-idle")
	mkTask(t, "t-reap-idle", "p-reap-idle", "r-reap-idle")
	setTaskPhase(t, "t-reap-idle", "Running")
	old := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	setTaskAnns(t, "t-reap-idle", map[string]string{
		annCurrentTurn:  "turn-1",
		annTurnComplete: old,
	})
	mkWrapperPodSvc(t, "reap-idle", "t-reap-idle", string(getTask(t, "t-reap-idle").UID))

	idleReaperServer().ReapOrphans(context.Background())
	if podExists(t, "reap-idle") {
		t.Error("expected idle pod with no live turn to be reaped")
	}
}

// TestReapOrphans_InflightTurnKept is the safety counterpart: a Task with a turn
// in flight (annCurrentTurn set, annTurnComplete empty) is owned by the
// turn-timeout path, so the idle backstop must never reap it mid-turn even with
// the idle window set to zero.
func TestReapOrphans_InflightTurnKept(t *testing.T) {
	mkTaskProject(t, "p-reap-inflight", 3)
	mkTaskRepository(t, "r-reap-inflight", "p-reap-inflight")
	mkTask(t, "t-reap-inflight", "p-reap-inflight", "r-reap-inflight")
	setTaskPhase(t, "t-reap-inflight", "Running")
	setTaskAnns(t, "t-reap-inflight", map[string]string{annCurrentTurn: "turn-1"})
	mkWrapperPodSvc(t, "reap-inflight", "t-reap-inflight", string(getTask(t, "t-reap-inflight").UID))

	idleReaperServer().ReapOrphans(context.Background())
	if !podExists(t, "reap-inflight") {
		t.Error("expected pod with in-flight turn to be kept")
	}
}

// TestReapOrphans_RecentActivityKept verifies a pod whose last turn ended within
// the idle window is kept (the healthy between-turns gap must not be reaped).
func TestReapOrphans_RecentActivityKept(t *testing.T) {
	mkTaskProject(t, "p-reap-recent", 3)
	mkTaskRepository(t, "r-reap-recent", "p-reap-recent")
	mkTask(t, "t-reap-recent", "p-reap-recent", "r-reap-recent")
	setTaskPhase(t, "t-reap-recent", "Running")
	now := time.Now().UTC().Format(time.RFC3339)
	setTaskAnns(t, "t-reap-recent", map[string]string{
		annCurrentTurn:  "turn-1",
		annTurnComplete: now,
	})
	mkWrapperPodSvc(t, "reap-recent", "t-reap-recent", string(getTask(t, "t-reap-recent").UID))

	srv := reaperServer()
	srv.IdlePodReapAfter = time.Hour // fresh completion is well inside the window
	srv.ReapOrphans(context.Background())
	if !podExists(t, "reap-recent") {
		t.Error("expected pod with recent turn activity to be kept")
	}
}

// TestReapOrphans_IdleDisabled verifies IdlePodReapAfter=0 disables the idle
// backstop: a long-idle pod on a non-terminal Task is left running.
func TestReapOrphans_IdleDisabled(t *testing.T) {
	mkTaskProject(t, "p-reap-idledis", 3)
	mkTaskRepository(t, "r-reap-idledis", "p-reap-idledis")
	mkTask(t, "t-reap-idledis", "p-reap-idledis", "r-reap-idledis")
	setTaskPhase(t, "t-reap-idledis", "Running")
	old := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	setTaskAnns(t, "t-reap-idledis", map[string]string{
		annCurrentTurn:  "turn-1",
		annTurnComplete: old,
	})
	mkWrapperPodSvc(t, "reap-idledis", "t-reap-idledis", string(getTask(t, "t-reap-idledis").UID))

	reaperServer().ReapOrphans(context.Background()) // IdlePodReapAfter defaults to 0
	if !podExists(t, "reap-idledis") {
		t.Error("expected idle pod to be kept when idle backstop disabled")
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

// gcServer returns a CallbackServer with the given terminal-Task retention and a
// tiny ReaperGrace so pod/service passes never interfere with the GC assertions.
func gcServer(reg *prometheus.Registry, retention time.Duration) *CallbackServer {
	return &CallbackServer{
		Client:        k8sClient,
		Metrics:       obs.NewOperatorMetrics(reg),
		Namespace:     testNS,
		ReaperGrace:   time.Nanosecond,
		TaskRetention: retention,
	}
}

// TestReapOrphans_GCOldTerminalTask verifies a terminal Task older than the
// retention window is garbage-collected and the operator_tasks_gc_total counter
// increments for its kind.
func TestReapOrphans_GCOldTerminalTask(t *testing.T) {
	mkTaskProject(t, "p-gc-old", 3)
	mkTaskRepository(t, "r-gc-old", "p-gc-old")
	mkTask(t, "t-gc-old", "p-gc-old", "r-gc-old")
	setTaskPhase(t, "t-gc-old", "Succeeded")

	reg := prometheus.NewRegistry()
	// Retention of 1ns: the just-created Task is already past the window.
	gcServer(reg, time.Nanosecond).ReapOrphans(context.Background())

	if taskExists(t, "t-gc-old") {
		t.Error("expected terminal Task past retention to be garbage-collected")
	}

	kind := "implement" // mkTask Tasks default to kind=implement
	var got float64
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "operator_tasks_gc_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "kind" && lp.GetValue() == kind {
					got = m.GetCounter().GetValue()
				}
			}
		}
	}
	// >= 1 (not == 1): GC lists Tasks namespace-wide and other tests in this
	// shared-envtest package may leave terminal Tasks the same sweep collects.
	if got < 1 {
		t.Errorf("operator_tasks_gc_total{kind=%s} = %v, want >= 1", kind, got)
	}
}

// TestReapOrphans_GCKeepsYoungTerminalTask verifies a terminal Task younger than
// the retention window is kept.
func TestReapOrphans_GCKeepsYoungTerminalTask(t *testing.T) {
	mkTaskProject(t, "p-gc-young", 3)
	mkTaskRepository(t, "r-gc-young", "p-gc-young")
	mkTask(t, "t-gc-young", "p-gc-young", "r-gc-young")
	setTaskPhase(t, "t-gc-young", "Failed")

	gcServer(prometheus.NewRegistry(), time.Hour).ReapOrphans(context.Background())

	if !taskExists(t, "t-gc-young") {
		t.Error("expected terminal Task within retention window to be kept")
	}
}

// TestReapOrphans_GCKeepsNonTerminalTask verifies a non-terminal Task is never
// garbage-collected regardless of age.
func TestReapOrphans_GCKeepsNonTerminalTask(t *testing.T) {
	mkTaskProject(t, "p-gc-live", 3)
	mkTaskRepository(t, "r-gc-live", "p-gc-live")
	mkTask(t, "t-gc-live", "p-gc-live", "r-gc-live")
	setTaskPhase(t, "t-gc-live", "Running")

	gcServer(prometheus.NewRegistry(), time.Nanosecond).ReapOrphans(context.Background())

	if !taskExists(t, "t-gc-live") {
		t.Error("expected non-terminal Task to be kept by GC")
	}
}

// TestReapOrphans_GCDisabledWhenRetentionZero verifies a zero retention disables
// the GC pass entirely (the unset-field safety guard).
func TestReapOrphans_GCDisabledWhenRetentionZero(t *testing.T) {
	mkTaskProject(t, "p-gc-off", 3)
	mkTaskRepository(t, "r-gc-off", "p-gc-off")
	mkTask(t, "t-gc-off", "p-gc-off", "r-gc-off")
	setTaskPhase(t, "t-gc-off", "Succeeded")

	// reaperServer() leaves TaskRetention at zero.
	reaperServer().ReapOrphans(context.Background())

	if !taskExists(t, "t-gc-off") {
		t.Error("expected GC disabled (retention=0) to keep the terminal Task")
	}
}
