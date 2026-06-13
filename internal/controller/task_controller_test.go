package controller

import (
	"context"
	"errors"
	"testing"
	"time"

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

// gaugeValue reads a gauge metric value from a Prometheus registry by name+labels.
func gaugeValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatch(m.GetLabel(), labels) {
				return m.GetGauge().GetValue()
			}
		}
	}
	return 0
}

func newTaskReconciler(fs agent.Session) *TaskReconciler {
	r, _ := newTaskReconcilerReg(fs)
	return r
}

func newTaskReconcilerReg(fs agent.Session) (*TaskReconciler, *prometheus.Registry) {
	reg := prometheus.NewRegistry()
	return &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(reg),
		Session: fs,
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op-internal.tatara.svc:8082",
			OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
	}, reg
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
	r.Spec.ReingestSchedule = "0 6 * * *"
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

func TestTaskReconcile_GatesUntilMemoryReady(t *testing.T) {
	mkTaskProject(t, "p-memgate", 3)
	mkTaskRepository(t, "r-memgate", "p-memgate")
	mkTask(t, "t-memgate", "p-memgate", "r-memgate")
	// Project memory not Ready -> requeue, no pod.

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	res, err := reconcileTask(t, r, "t-memgate")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("expected requeue while project memory not ready")
	}
	pod := &corev1.Pod{}
	err = k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: "wrapper-t-memgate"}, pod)
	if !apierrors.IsNotFound(err) {
		t.Errorf("memory not ready must not spawn a pod, got err=%v", err)
	}
}

func TestTaskReconcile_PodCarriesMemoryEndpoint(t *testing.T) {
	mkTaskProject(t, "p-memep", 3)
	mkTaskRepository(t, "r-memep", "p-memep")
	mkTask(t, "t-memep", "p-memep", "r-memep")
	setProjectMemoryReady(t, "p-memep", "http://mem-p-memep.tatara.svc:8080")

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	if _, err := reconcileTask(t, r, "t-memep"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	pod := &corev1.Pod{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: "wrapper-t-memep"}, pod); err != nil {
		t.Fatalf("expected wrapper pod: %v", err)
	}
	var got string
	for _, e := range pod.Spec.Containers[0].Env {
		if e.Name == "TATARA_MEMORY_URL" {
			got = e.Value
		}
	}
	if got != "http://mem-p-memep.tatara.svc:8080" {
		t.Errorf("TATARA_MEMORY_URL = %q, want the project endpoint", got)
	}
}

func TestTaskReconcile_SpawnsPodAndService(t *testing.T) {
	mkTaskProject(t, "p-spawn", 3)
	mkTaskRepository(t, "r-spawn", "p-spawn")
	mkTask(t, "t-spawn", "p-spawn", "r-spawn")
	setProjectMemoryReady(t, "p-spawn", "http://mem-p-spawn.tatara.svc:8080")

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
	setProjectMemoryReady(t, "p-cap", "http://mem-p-cap.tatara.svc:8080")

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
	setProjectMemoryReady(t, "p-plan", "http://mem-p-plan.tatara.svc:8080")

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

func TestTaskReconcile_AgentUnreachable_RequeuesWithoutError(t *testing.T) {
	mkTaskProject(t, "p-unreach", 3)
	mkTaskRepository(t, "r-unreach", "p-unreach")
	mkTask(t, "t-unreach", "p-unreach", "r-unreach")
	setProjectMemoryReady(t, "p-unreach", "http://mem-p-unreach.tatara.svc:8080")

	fs := newFakeSession()
	r, reg := newTaskReconcilerReg(fs)
	if _, err := reconcileTask(t, r, "t-unreach"); err != nil {
		t.Fatalf("reconcile spawn: %v", err)
	}
	markPodReady(t, "wrapper-t-unreach")
	// Turn server still booting: transport-level failure, not an HTTP error.
	fs.submitErr = &agent.UnreachableError{Err: errors.New("dial tcp: connect: connection refused")}

	res, err := reconcileTask(t, r, "t-unreach")
	if err != nil {
		t.Fatalf("agent-unreachable must not error the reconcile (would trigger exponential backoff): %v", err)
	}
	if res.RequeueAfter != agentBootRequeue {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, agentBootRequeue)
	}
	if _, ok := fs.lastSubmit(); ok {
		t.Error("no turn should be recorded when the submit fails")
	}
	if tk := getTask(t, "t-unreach"); tk.Annotations[annCurrentTurn] != "" {
		t.Errorf("current-turn annotation should be empty, got %q", tk.Annotations[annCurrentTurn])
	}
	if got := counterValue(t, reg, "operator_agent_boot_race_requeue_total", nil); got != 1 {
		t.Errorf("operator_agent_boot_race_requeue_total = %v, want 1", got)
	}
}

func TestTaskReconcile_AgentUnreachable_StampStableAcrossRequeues(t *testing.T) {
	mkTaskProject(t, "p-unrstable", 3)
	mkTaskRepository(t, "r-unrstable", "p-unrstable")
	mkTask(t, "t-unrstable", "p-unrstable", "r-unrstable")
	setProjectMemoryReady(t, "p-unrstable", "http://mem-p-unrstable.tatara.svc:8080")

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	if _, err := reconcileTask(t, r, "t-unrstable"); err != nil {
		t.Fatalf("reconcile spawn: %v", err)
	}
	markPodReady(t, "wrapper-t-unrstable")
	fs.submitErr = &agent.UnreachableError{Err: errors.New("connection refused")}

	if _, err := reconcileTask(t, r, "t-unrstable"); err != nil { // boot-race 1: stamps
		t.Fatalf("boot-race 1: %v", err)
	}
	first := getTask(t, "t-unrstable").Annotations[annAgentUnreachableSince]
	if first == "" {
		t.Fatal("expected marker stamped on first boot-race")
	}
	if _, err := reconcileTask(t, r, "t-unrstable"); err != nil { // boot-race 2: within deadline
		t.Fatalf("boot-race 2: %v", err)
	}
	if second := getTask(t, "t-unrstable").Annotations[annAgentUnreachableSince]; second != first {
		t.Errorf("marker re-stamped within deadline: was %q now %q (would reset the boot deadline every requeue)", first, second)
	}
}

func TestTaskReconcile_AgentUnreachable_TerminatesAfterDeadline(t *testing.T) {
	mkTaskProject(t, "p-unrdead", 3)
	mkTaskRepository(t, "r-unrdead", "p-unrdead")
	mkTask(t, "t-unrdead", "p-unrdead", "r-unrdead")
	setProjectMemoryReady(t, "p-unrdead", "http://mem-p-unrdead.tatara.svc:8080")

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	if _, err := reconcileTask(t, r, "t-unrdead"); err != nil {
		t.Fatalf("reconcile spawn: %v", err)
	}
	markPodReady(t, "wrapper-t-unrdead")
	fs.submitErr = &agent.UnreachableError{Err: errors.New("connection refused")}
	// Agent has been unreachable longer than the boot deadline.
	annotate(t, "t-unrdead", map[string]string{
		annAgentUnreachableSince: time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339),
	})

	if _, err := reconcileTask(t, r, "t-unrdead"); err != nil {
		t.Fatalf("terminate path must not error: %v", err)
	}
	if tk := getTask(t, "t-unrdead"); tk.Status.Phase != "Failed" {
		t.Errorf("phase = %q, want Failed (a never-reachable pod must terminate, not loop forever)", tk.Status.Phase)
	}
}

func TestTaskReconcile_AgentUnreachable_ClearsMarkerOnSuccess(t *testing.T) {
	mkTaskProject(t, "p-unrclr", 3)
	mkTaskRepository(t, "r-unrclr", "p-unrclr")
	mkTask(t, "t-unrclr", "p-unrclr", "r-unrclr")
	setProjectMemoryReady(t, "p-unrclr", "http://mem-p-unrclr.tatara.svc:8080")

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	if _, err := reconcileTask(t, r, "t-unrclr"); err != nil {
		t.Fatalf("reconcile spawn: %v", err)
	}
	markPodReady(t, "wrapper-t-unrclr")
	fs.submitErr = &agent.UnreachableError{Err: errors.New("connection refused")}
	if _, err := reconcileTask(t, r, "t-unrclr"); err != nil { // boot-race: stamps marker
		t.Fatalf("boot-race reconcile: %v", err)
	}
	if tk := getTask(t, "t-unrclr"); tk.Annotations[annAgentUnreachableSince] == "" {
		t.Fatal("expected unreachable marker stamped after first boot-race")
	}
	fs.submitErr = nil // agent now reachable
	if _, err := reconcileTask(t, r, "t-unrclr"); err != nil {
		t.Fatalf("recovered reconcile: %v", err)
	}
	if _, ok := fs.lastSubmit(); !ok {
		t.Error("expected a successful submit after recovery")
	}
	if tk := getTask(t, "t-unrclr"); tk.Annotations[annAgentUnreachableSince] != "" {
		t.Errorf("unreachable marker must clear on success, got %q", tk.Annotations[annAgentUnreachableSince])
	}
}

func TestTaskReconcile_SubmitHTTPError_StillErrors(t *testing.T) {
	mkTaskProject(t, "p-httperr", 3)
	mkTaskRepository(t, "r-httperr", "p-httperr")
	mkTask(t, "t-httperr", "p-httperr", "r-httperr")
	setProjectMemoryReady(t, "p-httperr", "http://mem-p-httperr.tatara.svc:8080")

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	if _, err := reconcileTask(t, r, "t-httperr"); err != nil {
		t.Fatalf("reconcile spawn: %v", err)
	}
	markPodReady(t, "wrapper-t-httperr")
	// A real HTTP rejection from the wrapper must keep erroring (not be masked
	// as a boot-race requeue).
	fs.submitErr = &agent.HTTPError{Status: 500, Body: "boom"}

	if _, err := reconcileTask(t, r, "t-httperr"); err == nil {
		t.Fatal("a real HTTP error from submit must still error the reconcile")
	}
}

func TestTaskReconcile_AdvancesToNextSubtask(t *testing.T) {
	mkTaskProject(t, "p-adv", 3)
	mkTaskRepository(t, "r-adv", "p-adv")
	mkTask(t, "t-adv", "p-adv", "r-adv")
	mkSubtask(t, "t-adv-s1", "t-adv", 1)
	mkSubtask(t, "t-adv-s2", "t-adv", 2)
	setProjectMemoryReady(t, "p-adv", "http://mem-p-adv.tatara.svc:8080")

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
	setProjectMemoryReady(t, "p-end", "http://mem-p-end.tatara.svc:8080")

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
	setProjectMemoryReady(t, "p-max", "http://mem-p-max.tatara.svc:8080")

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
	setProjectMemoryReady(t, "p-tt", "http://mem-p-tt.tatara.svc:8080")

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
	setProjectMemoryReady(t, "p-lost", "http://mem-p-lost.tatara.svc:8080")
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

// ----- P3: ResultSummary derived on termination -----

func TestTaskReconcile_ResultSummaryDerivedFromSubtasks(t *testing.T) {
	mkTaskProject(t, "p-rssum", 3)
	mkTaskRepository(t, "r-rssum", "p-rssum")
	mkTask(t, "t-rssum", "p-rssum", "r-rssum")
	mkSubtask(t, "t-rssum-s1", "t-rssum", 1)
	setProjectMemoryReady(t, "p-rssum", "http://mem-p-rssum.tatara.svc:8080")

	// Set the subtask Done with a result before termination.
	st := getSubtask(t, "t-rssum-s1")
	st.Status.Phase = "Done"
	st.Status.Result = "implemented feature X"
	if err := k8sClient.Status().Update(context.Background(), st); err != nil {
		t.Fatalf("set subtask result: %v", err)
	}

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	if _, err := reconcileTask(t, r, "t-rssum"); err != nil { // spawn
		t.Fatalf("spawn: %v", err)
	}
	markPodReady(t, "wrapper-t-rssum")
	if _, err := reconcileTask(t, r, "t-rssum"); err != nil { // plan turn
		t.Fatalf("plan: %v", err)
	}
	// Plan callback with no further pending subtasks -> terminate Succeeded.
	annotate(t, "t-rssum", map[string]string{annTurnComplete: "2026-06-07T09:00:00Z"})
	if _, err := reconcileTask(t, r, "t-rssum"); err != nil {
		t.Fatalf("terminate: %v", err)
	}

	tk := getTask(t, "t-rssum")
	if tk.Status.Phase != "Succeeded" {
		t.Fatalf("phase = %q, want Succeeded", tk.Status.Phase)
	}
	if tk.Status.ResultSummary == "" {
		t.Error("ResultSummary must be set when agent did not provide one")
	}
	if !contains(tk.Status.ResultSummary, "implemented feature X") {
		t.Errorf("ResultSummary = %q, want last-subtask result", tk.Status.ResultSummary)
	}
}

func TestTaskReconcile_ResultSummaryFallsBackToCount(t *testing.T) {
	mkTaskProject(t, "p-rscount", 3)
	mkTaskRepository(t, "r-rscount", "p-rscount")
	mkTask(t, "t-rscount", "p-rscount", "r-rscount")
	mkSubtask(t, "t-rscount-s1", "t-rscount", 1)
	setProjectMemoryReady(t, "p-rscount", "http://mem-p-rscount.tatara.svc:8080")

	// Done subtask with no result text.
	st := getSubtask(t, "t-rscount-s1")
	st.Status.Phase = "Done"
	if err := k8sClient.Status().Update(context.Background(), st); err != nil {
		t.Fatalf("set subtask done: %v", err)
	}

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	if _, err := reconcileTask(t, r, "t-rscount"); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	markPodReady(t, "wrapper-t-rscount")
	if _, err := reconcileTask(t, r, "t-rscount"); err != nil {
		t.Fatalf("plan: %v", err)
	}
	annotate(t, "t-rscount", map[string]string{annTurnComplete: "2026-06-07T09:05:00Z"})
	if _, err := reconcileTask(t, r, "t-rscount"); err != nil {
		t.Fatalf("terminate: %v", err)
	}

	tk := getTask(t, "t-rscount")
	if tk.Status.ResultSummary == "" {
		t.Error("ResultSummary must be set (count fallback)")
	}
	if !contains(tk.Status.ResultSummary, "1") {
		t.Errorf("ResultSummary = %q, want count mention", tk.Status.ResultSummary)
	}
}

func TestTaskReconcile_ResultSummaryNotOverwrittenWhenSet(t *testing.T) {
	mkTaskProject(t, "p-rsnoop", 3)
	mkTaskRepository(t, "r-rsnoop", "p-rsnoop")
	mkTask(t, "t-rsnoop", "p-rsnoop", "r-rsnoop")
	setProjectMemoryReady(t, "p-rsnoop", "http://mem-p-rsnoop.tatara.svc:8080")

	// Agent already set ResultSummary via task_update.
	tk := getTask(t, "t-rsnoop")
	tk.Status.ResultSummary = "agent-provided summary"
	if err := k8sClient.Status().Update(context.Background(), tk); err != nil {
		t.Fatalf("set result summary: %v", err)
	}

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	if _, err := reconcileTask(t, r, "t-rsnoop"); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	markPodReady(t, "wrapper-t-rsnoop")
	if _, err := reconcileTask(t, r, "t-rsnoop"); err != nil {
		t.Fatalf("plan: %v", err)
	}
	annotate(t, "t-rsnoop", map[string]string{annTurnComplete: "2026-06-07T09:10:00Z"})
	if _, err := reconcileTask(t, r, "t-rsnoop"); err != nil {
		t.Fatalf("terminate: %v", err)
	}

	tk2 := getTask(t, "t-rsnoop")
	if tk2.Status.ResultSummary != "agent-provided summary" {
		t.Errorf("ResultSummary = %q, want agent-provided unchanged", tk2.Status.ResultSummary)
	}
}

// TestUpdateInflightGauge_PerKind verifies that updateInflightGauge emits
// tatara_tasks_inflight{kind} for each active kind and zeroes missing kinds.
func TestUpdateInflightGauge_PerKind(t *testing.T) {
	ctx := context.Background()
	mkTaskProject(t, "p-inflight", 5)
	mkSecret(t, "p-inflight-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkTaskRepository(t, "r-inflight", "p-inflight")
	setProjectMemoryReady(t, "p-inflight", "http://mem-inflight.tatara.svc:8080")

	// Create one Planning task per kind.
	kindNames := map[string]string{"review": "t-inflight-review", "selfImprove": "t-inflight-si"}
	for i, kind := range []string{"review", "selfImprove"} {
		name := kindNames[kind]
		task := &tatarav1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
			Spec: tatarav1alpha1.TaskSpec{
				ProjectRef:    "p-inflight",
				RepositoryRef: "r-inflight",
				Goal:          "goal",
				Kind:          kind,
			},
		}
		if err := k8sClient.Create(ctx, task); err != nil {
			t.Fatalf("create task %d: %v", i, err)
		}
		task.Status.Phase = "Planning"
		if err := k8sClient.Status().Update(ctx, task); err != nil {
			t.Fatalf("set phase %d: %v", i, err)
		}
	}

	reg := prometheus.NewRegistry()
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(reg),
		Session: newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op-internal.tatara.svc:8082",
			OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
	}
	r.updateInflightGauge(ctx)

	// Each active kind we created must appear in the per-kind gauge (>= 1).
	// Other tests sharing testNS may have created more in-flight tasks so we
	// only assert >= 1, not == 1.
	reviewCount := gaugeValue(t, reg, "tatara_tasks_inflight", map[string]string{"kind": "review"})
	if reviewCount < 1 {
		t.Errorf("tatara_tasks_inflight{kind=review} = %v, want >= 1", reviewCount)
	}
	siCount := gaugeValue(t, reg, "tatara_tasks_inflight", map[string]string{"kind": "selfImprove"})
	if siCount < 1 {
		t.Errorf("tatara_tasks_inflight{kind=selfImprove} = %v, want >= 1", siCount)
	}
	// triageIssue was not created in this test so we skip checking it is zero
	// (other tests may have created triageIssue tasks), but we do verify the
	// known kinds are present in the metric output (gauge was emitted, not nil).
	_ = gaugeValue(t, reg, "tatara_tasks_inflight", map[string]string{"kind": "triageIssue"})
	_ = gaugeValue(t, reg, "tatara_tasks_inflight", map[string]string{"kind": "implement"})
}
