package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

func newTaskReconciler(fs agent.Session) *TaskReconciler {
	return &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: fs,
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op-internal.tatara.svc:8082",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
	}
}

func reconcileTask(t *testing.T, r *TaskReconciler, name string) (ctrl.Result, error) {
	t.Helper()
	return r.Reconcile(logf.IntoContext(context.Background(), logf.Log), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: name},
	})
}

func mkTaskProject(t *testing.T, name string, maxConcurrent int) {
	t.Helper()
	p := &tatarav1alpha1.Project{}
	p.Name = name
	p.Namespace = testNS
	p.Spec.ScmSecretRef = name + "-scm"
	p.Spec.MaxConcurrentTasks = maxConcurrent
	p.Spec.Agent = tatarav1alpha1.AgentSpec{
		Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
		MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
	}
	if err := k8sClient.Create(context.Background(), p); err != nil {
		t.Fatalf("create project: %v", err)
	}
}

func mkTaskRepository(t *testing.T, name, projectRef string) {
	t.Helper()
	r := &tatarav1alpha1.Repository{}
	r.Name = name
	r.Namespace = testNS
	r.Spec.ProjectRef = projectRef
	r.Spec.URL = "https://git/acme/" + name
	r.Spec.DefaultBranch = "main"
	if err := k8sClient.Create(context.Background(), r); err != nil {
		t.Fatalf("create repository: %v", err)
	}
}

func mkTask(t *testing.T, name, projectRef, repoRef string) {
	t.Helper()
	tk := &tatarav1alpha1.Task{}
	tk.Name = name
	tk.Namespace = testNS
	tk.Spec.ProjectRef = projectRef
	tk.Spec.RepositoryRef = repoRef
	tk.Spec.Goal = "ship the feature"
	if err := k8sClient.Create(context.Background(), tk); err != nil {
		t.Fatalf("create task: %v", err)
	}
}

func getTask(t *testing.T, name string) *tatarav1alpha1.Task {
	t.Helper()
	tk := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, tk); err != nil {
		t.Fatalf("get task %s: %v", name, err)
	}
	return tk
}

func setTaskPhase(t *testing.T, name, phase string) {
	t.Helper()
	tk := getTask(t, name)
	tk.Status.Phase = phase
	if err := k8sClient.Status().Update(context.Background(), tk); err != nil {
		t.Fatalf("set phase %s: %v", name, err)
	}
}

func markPodReady(t *testing.T, podName string) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: podName}, pod); err != nil {
		t.Fatalf("get pod %s: %v", podName, err)
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if err := k8sClient.Status().Update(context.Background(), pod); err != nil {
		t.Fatalf("mark pod ready %s: %v", podName, err)
	}
}

func mkSubtask(t *testing.T, name, taskRef string, order int) {
	t.Helper()
	st := &tatarav1alpha1.Subtask{}
	st.Name = name
	st.Namespace = testNS
	st.Spec.TaskRef = taskRef
	st.Spec.Title = name + "-title"
	st.Spec.Detail = name + "-detail"
	st.Spec.Order = order
	if err := k8sClient.Create(context.Background(), st); err != nil {
		t.Fatalf("create subtask %s: %v", name, err)
	}
}

func getSubtask(t *testing.T, name string) *tatarav1alpha1.Subtask {
	t.Helper()
	st := &tatarav1alpha1.Subtask{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, st); err != nil {
		t.Fatalf("get subtask %s: %v", name, err)
	}
	return st
}

func annotate(t *testing.T, name string, kv map[string]string) {
	t.Helper()
	tk := getTask(t, name)
	if tk.Annotations == nil {
		tk.Annotations = map[string]string{}
	}
	for k, v := range kv {
		tk.Annotations[k] = v
	}
	if err := k8sClient.Update(context.Background(), tk); err != nil {
		t.Fatalf("annotate %s: %v", name, err)
	}
}

func findCond(conds []metav1.Condition, typ string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == typ {
			return &conds[i]
		}
	}
	return nil
}

// ----- Task 6: concurrency gate + spawn -----

func TestTaskReconcile_SpawnsPodAndService(t *testing.T) {
	mkTaskProject(t, "p-spawn", 3)
	mkTaskRepository(t, "r-spawn", "p-spawn")
	mkTask(t, "t-spawn", "p-spawn", "r-spawn")

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	if _, err := reconcileTask(t, r, "t-spawn"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	tk := getTask(t, "t-spawn")
	if tk.Status.Phase != "Planning" {
		t.Errorf("phase = %q, want Planning", tk.Status.Phase)
	}

	pod := &corev1.Pod{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: agent.PodName(tk)}, pod); err != nil {
		t.Fatalf("expected pod %s: %v", agent.PodName(tk), err)
	}
	svc := &corev1.Service{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: agent.PodName(tk)}, svc); err != nil {
		t.Fatalf("expected service %s: %v", agent.PodName(tk), err)
	}
	if tk.Status.PodName != agent.PodName(tk) {
		t.Errorf("status.podName = %q, want %q", tk.Status.PodName, agent.PodName(tk))
	}
}

func TestTaskReconcile_GatesAtCap(t *testing.T) {
	mkTaskProject(t, "p-cap", 1)
	mkTaskRepository(t, "r-cap", "p-cap")
	mkTask(t, "t-running", "p-cap", "r-cap")
	mkTask(t, "t-queued", "p-cap", "r-cap")
	setTaskPhase(t, "t-running", "Running")

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	res, err := reconcileTask(t, r, "t-queued")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("expected requeue at cap")
	}
	// no pod created for the queued task
	pod := &corev1.Pod{}
	err = k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: "wrapper-t-queued"}, pod)
	if !apierrors.IsNotFound(err) {
		t.Errorf("queued task must not spawn a pod, got err=%v", err)
	}
	_ = metav1.Now
}

func TestTaskReconcile_TerminalNoop(t *testing.T) {
	mkTaskProject(t, "p-term", 3)
	mkTaskRepository(t, "r-term", "p-term")
	mkTask(t, "t-done", "p-term", "r-term")
	setTaskPhase(t, "t-done", "Succeeded")

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	if _, err := reconcileTask(t, r, "t-done"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, ok := fs.lastSubmit(); ok {
		t.Error("terminal task must not submit a turn")
	}
}

// ----- Task 7: plan turn + subtask iteration -----

func TestTaskReconcile_PlanTurnSubmitted(t *testing.T) {
	mkTaskProject(t, "p-plan", 3)
	mkTaskRepository(t, "r-plan", "p-plan")
	mkTask(t, "t-plan", "p-plan", "r-plan")

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	// First reconcile: spawn + planning. Mark the pod Ready, then reconcile again.
	if _, err := reconcileTask(t, r, "t-plan"); err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	markPodReady(t, "wrapper-t-plan")
	if _, err := reconcileTask(t, r, "t-plan"); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}

	sub, ok := fs.lastSubmit()
	if !ok {
		t.Fatal("expected a plan turn submission")
	}
	if !contains(sub.Text, "ship the feature") {
		t.Errorf("plan turn text = %q", sub.Text)
	}
	tk := getTask(t, "t-plan")
	if tk.Annotations[annCurrentTurn] != sub.TurnID {
		t.Errorf("current-turn = %q, want %q", tk.Annotations[annCurrentTurn], sub.TurnID)
	}
}

func TestTaskReconcile_AdvancesToNextSubtask(t *testing.T) {
	mkTaskProject(t, "p-adv", 3)
	mkTaskRepository(t, "r-adv", "p-adv")
	mkTask(t, "t-adv", "p-adv", "r-adv")
	mkSubtask(t, "t-adv-s1", "t-adv", 1)
	mkSubtask(t, "t-adv-s2", "t-adv", 2)

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	if _, err := reconcileTask(t, r, "t-adv"); err != nil {
		t.Fatalf("reconcile spawn: %v", err)
	}
	markPodReady(t, "wrapper-t-adv")
	if _, err := reconcileTask(t, r, "t-adv"); err != nil { // plan turn
		t.Fatalf("reconcile plan: %v", err)
	}
	planTurn, _ := fs.lastSubmit()

	// Simulate the plan-turn callback: turn complete, no executing subtask.
	annotate(t, "t-adv", map[string]string{annTurnComplete: "2026-06-06T10:00:00Z"})
	if _, err := reconcileTask(t, r, "t-adv"); err != nil { // submit s1
		t.Fatalf("reconcile s1: %v", err)
	}
	s1Turn, _ := fs.lastSubmit()
	if s1Turn.TurnID == planTurn.TurnID {
		t.Fatal("expected a new turn for subtask 1")
	}
	if !contains(s1Turn.Text, "t-adv-s1-title") {
		t.Errorf("s1 turn text = %q", s1Turn.Text)
	}
	tk := getTask(t, "t-adv")
	if tk.Status.Phase != "Running" {
		t.Errorf("phase = %q, want Running", tk.Status.Phase)
	}
	if tk.Annotations[annCurrentSubtask] != "t-adv-s1" {
		t.Errorf("current-subtask = %q, want t-adv-s1", tk.Annotations[annCurrentSubtask])
	}

	// Simulate s1 callback delivering a result; reconcile should mark s1 Done
	// and submit s2.
	st1 := getSubtask(t, "t-adv-s1")
	st1.Status.Result = "s1 result"
	if err := k8sClient.Status().Update(context.Background(), st1); err != nil {
		t.Fatalf("set s1 result: %v", err)
	}
	annotate(t, "t-adv", map[string]string{annTurnComplete: "2026-06-06T10:05:00Z"})
	if _, err := reconcileTask(t, r, "t-adv"); err != nil {
		t.Fatalf("reconcile s2: %v", err)
	}
	if getSubtask(t, "t-adv-s1").Status.Phase != "Done" {
		t.Errorf("s1 phase = %q, want Done", getSubtask(t, "t-adv-s1").Status.Phase)
	}
	s2Turn, _ := fs.lastSubmit()
	if !contains(s2Turn.Text, "t-adv-s2-title") {
		t.Errorf("s2 turn text = %q", s2Turn.Text)
	}
}

// ----- Task 8: termination, cleanup, maxTurns, pod-loss -----

func TestTaskReconcile_TerminatesWhenNoPending(t *testing.T) {
	mkTaskProject(t, "p-end", 3)
	mkTaskRepository(t, "r-end", "p-end")
	mkTask(t, "t-end", "p-end", "r-end")

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	if _, err := reconcileTask(t, r, "t-end"); err != nil { // spawn
		t.Fatalf("spawn: %v", err)
	}
	markPodReady(t, "wrapper-t-end")
	if _, err := reconcileTask(t, r, "t-end"); err != nil { // plan turn
		t.Fatalf("plan: %v", err)
	}
	// Plan turn callback, but the agent created no subtasks -> terminate Succeeded.
	annotate(t, "t-end", map[string]string{annTurnComplete: "2026-06-06T11:00:00Z"})
	if _, err := reconcileTask(t, r, "t-end"); err != nil {
		t.Fatalf("terminate: %v", err)
	}

	tk := getTask(t, "t-end")
	if tk.Status.Phase != "Succeeded" {
		t.Errorf("phase = %q, want Succeeded", tk.Status.Phase)
	}
	if len(fs.deleted) == 0 {
		t.Error("expected DELETE /v1/session")
	}
	// pod + service deleted
	pod := &corev1.Pod{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: "wrapper-t-end"}, pod); err == nil && pod.DeletionTimestamp == nil {
		t.Error("expected wrapper pod deleted")
	}
	// M5 hook marker
	if findCond(tk.Status.Conditions, "WritebackPending") == nil {
		t.Error("expected WritebackPending condition for the M5 write-back hook")
	}
}

func TestTaskReconcile_MaxTurnsCap(t *testing.T) {
	mkTaskProject(t, "p-max", 3)
	mkTaskRepository(t, "r-max", "p-max")
	tk := &tatarav1alpha1.Task{}
	tk.Name = "t-max"
	tk.Namespace = testNS
	tk.Spec.ProjectRef = "p-max"
	tk.Spec.RepositoryRef = "r-max"
	tk.Spec.Goal = "g"
	tk.Spec.MaxTurns = 1
	if err := k8sClient.Create(context.Background(), tk); err != nil {
		t.Fatalf("create task: %v", err)
	}
	mkSubtask(t, "t-max-s1", "t-max", 1)
	mkSubtask(t, "t-max-s2", "t-max", 2)

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	if _, err := reconcileTask(t, r, "t-max"); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	markPodReady(t, "wrapper-t-max")
	if _, err := reconcileTask(t, r, "t-max"); err != nil { // plan turn (turnsCompleted stays 0)
		t.Fatalf("plan: %v", err)
	}
	annotate(t, "t-max", map[string]string{annTurnComplete: "2026-06-06T11:10:00Z"})
	if _, err := reconcileTask(t, r, "t-max"); err != nil { // s1 turn -> turnsCompleted=1
		t.Fatalf("s1: %v", err)
	}
	annotate(t, "t-max", map[string]string{annTurnComplete: "2026-06-06T11:15:00Z"})
	if _, err := reconcileTask(t, r, "t-max"); err != nil { // hits cap -> terminate
		t.Fatalf("cap: %v", err)
	}
	tk2 := getTask(t, "t-max")
	if tk2.Status.Phase != "Succeeded" && tk2.Status.Phase != "Failed" {
		t.Errorf("phase = %q, want terminal after maxTurns", tk2.Status.Phase)
	}
}

// ----- Fix 2: per-turn timeout via reconciler -----

func TestTaskReconcile_TurnTimeout(t *testing.T) {
	mkTaskProject(t, "p-tt", 3)
	mkTaskRepository(t, "r-tt", "p-tt")
	mkTask(t, "t-tt", "p-tt", "r-tt")

	fs := newFakeSession()
	r := newTaskReconciler(fs)

	// First reconcile: spawn pod.
	if _, err := reconcileTask(t, r, "t-tt"); err != nil {
		t.Fatalf("reconcile spawn: %v", err)
	}
	markPodReady(t, "wrapper-t-tt")

	// Second reconcile: submit plan turn.
	if _, err := reconcileTask(t, r, "t-tt"); err != nil {
		t.Fatalf("reconcile plan turn: %v", err)
	}

	// Backdate turn-started-at to simulate the deadline already having passed.
	annotate(t, "t-tt", map[string]string{
		annTurnStartedAt: "2000-01-01T00:00:00Z",
	})

	// Third reconcile: turn is in-flight (no annTurnComplete), deadline exceeded.
	if _, err := reconcileTask(t, r, "t-tt"); err != nil {
		t.Fatalf("reconcile timeout: %v", err)
	}

	tk := getTask(t, "t-tt")
	if tk.Status.Phase != "Failed" {
		t.Errorf("phase = %q, want Failed after turn timeout", tk.Status.Phase)
	}
	cond := findCond(tk.Status.Conditions, "Ready")
	if cond == nil || cond.Reason != "TurnTimeout" {
		t.Errorf("expected Ready/TurnTimeout condition, got %+v", cond)
	}
	// Session.DeleteSession must have been called.
	if len(fs.deleted) == 0 {
		t.Error("expected DeleteSession call on turn timeout")
	}
}

func TestTaskReconcile_PodLostRecreatesThenFails(t *testing.T) {
	mkTaskProject(t, "p-lost", 3)
	mkTaskRepository(t, "r-lost", "p-lost")
	mkTask(t, "t-lost", "p-lost", "r-lost")
	setTaskPhase(t, "t-lost", "Running")
	annotate(t, "t-lost", map[string]string{
		annPodRecreations: "3",
		annCurrentTurn:    "turn-1",
	})

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	// Pod absent (never created) + recreations exhausted -> Failed.
	if _, err := reconcileTask(t, r, "t-lost"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	tk := getTask(t, "t-lost")
	if tk.Status.Phase != "Failed" {
		t.Errorf("phase = %q, want Failed (pod lost, retries exhausted)", tk.Status.Phase)
	}
}
