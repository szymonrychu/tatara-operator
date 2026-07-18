package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
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

// mkWrapperPodSvcKind is mkWrapperPodSvc plus an explicit agent.LabelAgentKind
// label, for the superseded-stage reap tests: the reaper compares the pod's
// stamped kind against the Task's CURRENT stage kind.
func mkWrapperPodSvcKind(t *testing.T, name, taskName, taskUID, agentKind string) {
	t.Helper()
	labels := map[string]string{
		agent.LabelManagedBy: agent.ManagedByValue,
		agent.LabelComponent: agent.ComponentAgent,
		agent.LabelTask:      taskName,
	}
	if taskUID != "" {
		labels[agent.LabelTaskUID] = taskUID
	}
	if agentKind != "" {
		labels[agent.LabelAgentKind] = agentKind
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

func TestReapOrphans_FinishedTask(t *testing.T) {
	mkTaskProject(t, "p-reap-ph", 3)
	mkTaskRepository(t, "r-reap-ph", "p-reap-ph")
	mkTask(t, "t-reap-ph", "p-reap-ph", "r-reap-ph")
	setTaskStage(t, "t-reap-ph", tatarav1alpha1.StageDelivered)
	mkWrapperPodSvc(t, "reap-phase", "t-reap-ph", string(getTask(t, "t-reap-ph").UID))

	reaperServer().ReapOrphans(context.Background())
	if podExists(t, "reap-phase") {
		t.Error("expected pod for a delivered task to be reaped")
	}
}

// TestReapOrphans_PodlessLiveStageKept covers the pod-blip class: a Task in a
// POD-LESS but LIVE stage (merging) is not finished, so the reaper must not
// touch a pod that belongs to it.
func TestReapOrphans_PodlessLiveStageKept(t *testing.T) {
	mkTaskProject(t, "p-reap-phlc", 3)
	mkTaskRepository(t, "r-reap-phlc", "p-reap-phlc")
	mkTask(t, "t-reap-phlc", "p-reap-phlc", "r-reap-phlc")
	setTaskStage(t, "t-reap-phlc", tatarav1alpha1.StageMerging)
	mkWrapperPodSvc(t, "reap-phlc", "t-reap-phlc", string(getTask(t, "t-reap-phlc").UID))

	reaperServer().ReapOrphans(context.Background())
	if !podExists(t, "reap-phlc") {
		t.Error("expected the pod of a live pod-less stage to be kept")
	}
}

func TestReapOrphans_ParkedTask(t *testing.T) {
	mkTaskProject(t, "p-reap-lc", 3)
	mkTaskRepository(t, "r-reap-lc", "p-reap-lc")
	mkTask(t, "t-reap-lc", "p-reap-lc", "r-reap-lc")
	setTaskStage(t, "t-reap-lc", tatarav1alpha1.StageParked)
	mkWrapperPodSvc(t, "reap-lc", "t-reap-lc", string(getTask(t, "t-reap-lc").UID))

	reaperServer().ReapOrphans(context.Background())
	if podExists(t, "reap-lc") {
		t.Error("expected pod for a parked task to be reaped")
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
	setTaskStage(t, "t-reap-live", tatarav1alpha1.StageImplementing)
	mkWrapperPodSvc(t, "reap-live", "t-reap-live", string(getTask(t, "t-reap-live").UID))

	reaperServer().ReapOrphans(context.Background())
	if !podExists(t, "reap-live") {
		t.Error("expected pod for live non-terminal task to be kept")
	}
}

// ---------------------------------------------------------------------------
// SUPERSEDED-STAGE POD REAP (production bug): a finished-stage pod (e.g. the
// investigating pod of an incident Task that has already advanced to
// clarifying) stayed Running forever - no existing rule covered "pod's
// stamped agent kind != the Task's CURRENT stage agent kind".
// ---------------------------------------------------------------------------

// TestReapOrphans_SupersededStagePodReaped covers the production symptom
// directly: an incident Task's investigating pod (LabelAgentKind=incident) is
// still around after the Task advanced to clarifying (AgentKindFor=clarify).
// Both kinds are non-empty and disagree, so it is reaped as superseded.
func TestReapOrphans_SupersededStagePodReaped(t *testing.T) {
	mkTaskProject(t, "p-reap-superseded", 3)
	mkTaskRepository(t, "r-reap-superseded", "p-reap-superseded")
	mkTask(t, "t-reap-superseded", "p-reap-superseded", "r-reap-superseded")
	setTaskStage(t, "t-reap-superseded", tatarav1alpha1.StageClarifying)
	mkWrapperPodSvcKind(t, "reap-superseded", "t-reap-superseded",
		string(getTask(t, "t-reap-superseded").UID), stage.AgentIncident)

	reaperServer().ReapOrphans(context.Background())
	if podExists(t, "reap-superseded") {
		t.Error("expected a pod stamped for a superseded stage (incident) to be reaped once its task moved to clarifying")
	}
}

// TestReapOrphans_MatchingStagePodKept is the safety counterpart: a pod whose
// stamped agent kind MATCHES the Task's current stage must never be reaped by
// the new rule.
func TestReapOrphans_MatchingStagePodKept(t *testing.T) {
	mkTaskProject(t, "p-reap-matching", 3)
	mkTaskRepository(t, "r-reap-matching", "p-reap-matching")
	mkTask(t, "t-reap-matching", "p-reap-matching", "r-reap-matching")
	setTaskStage(t, "t-reap-matching", tatarav1alpha1.StageInvestigating)
	mkWrapperPodSvcKind(t, "reap-matching", "t-reap-matching",
		string(getTask(t, "t-reap-matching").UID), stage.AgentIncident)

	reaperServer().ReapOrphans(context.Background())
	if !podExists(t, "reap-matching") {
		t.Error("expected a pod whose kind matches the current stage to be kept")
	}
}

// TestReapOrphans_SupersededStagePodFreshKept verifies the creation-grace
// guard still protects a freshly spawned pod even when its kind mismatches
// the Task's current stage (the spawn-vs-advance race).
func TestReapOrphans_SupersededStagePodFreshKept(t *testing.T) {
	mkTaskProject(t, "p-reap-freshsup", 3)
	mkTaskRepository(t, "r-reap-freshsup", "p-reap-freshsup")
	mkTask(t, "t-reap-freshsup", "p-reap-freshsup", "r-reap-freshsup")
	setTaskStage(t, "t-reap-freshsup", tatarav1alpha1.StageClarifying)
	mkWrapperPodSvcKind(t, "reap-freshsup", "t-reap-freshsup",
		string(getTask(t, "t-reap-freshsup").UID), stage.AgentIncident)

	srv := &CallbackServer{
		Client:    k8sClient,
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Namespace: testNS,
		// ReaperGrace zero => uses pollRequeue default (30s); pod is fresh.
	}
	srv.ReapOrphans(context.Background())
	if !podExists(t, "reap-freshsup") {
		t.Error("expected a freshly created superseded-kind pod to be protected by the grace window")
	}
	// Clean up so subsequent tests see a clean fixture set.
	reaperServer().ReapOrphans(context.Background())
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
	setTaskStage(t, "t-reap-idle", tatarav1alpha1.StageImplementing)
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
	setTaskStage(t, "t-reap-inflight", tatarav1alpha1.StageImplementing)
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
	setTaskStage(t, "t-reap-recent", tatarav1alpha1.StageImplementing)
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
	setTaskStage(t, "t-reap-idledis", tatarav1alpha1.StageImplementing)
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

// ============================================================================
// B.5 / B.6: the terminal-stage reaper - the ONLY Task GC.
// ============================================================================

// reapWriter is the fake forge the reaper tests run against. EVERY write method
// PANICS by default: the NEVER-CLOSE-WHAT-WE-DID-NOT-CREATE test asserts the
// forge receives ZERO write calls for a human's fork PR, and a panicking fake is
// the only assertion that cannot be satisfied by a call that "happened to be a
// no-op". Tests that expect a write install a func.
type reapWriter struct {
	scm.SCMWriter

	comment     func(issueRef, body string) error
	addLabel    func(issueRef, label string) error
	closePR     func(repoURL string, number int, body string) error
	deleteBrnch func(repoURL, branch string) error

	comments   []string // issueRef|body
	labels     []string // issueRef|label
	closed     []int
	deleted    []string
	closeOrder []string // what the client saw at ClosePR time (ordering probe)
}

func (w *reapWriter) Comment(_ context.Context, _, issueRef, body string) error {
	if w.comment == nil {
		panic("reaper called Comment on an artifact it did not create: " + issueRef)
	}
	w.comments = append(w.comments, issueRef+"|"+body)
	return w.comment(issueRef, body)
}

func (w *reapWriter) AddLabel(_ context.Context, _, issueRef, label string) error {
	if w.addLabel == nil {
		panic("reaper called AddLabel on an artifact it did not create: " + issueRef)
	}
	w.labels = append(w.labels, issueRef+"|"+label)
	return w.addLabel(issueRef, label)
}

func (w *reapWriter) ClosePR(_ context.Context, repoURL, _ string, number int, body string) error {
	if w.closePR == nil {
		panic("reaper called ClosePR on a PR it did not create")
	}
	w.closed = append(w.closed, number)
	return w.closePR(repoURL, number, body)
}

func (w *reapWriter) DeleteBranch(_ context.Context, repoURL, _, branch string) error {
	if w.deleteBrnch == nil {
		panic("reaper called DeleteBranch on a branch it did not push: " + branch)
	}
	w.deleted = append(w.deleted, branch)
	return w.deleteBrnch(repoURL, branch)
}

func reapProject(name string) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "scm-secret",
			MaxOpenTasks: 6,
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", BotLogin: "tatara-bot"},
			Documentation: &tatarav1alpha1.DocumentationSpec{
				Enabled: true,
				Repo:    "https://github.com/szymonrychu/tatara-documentation.git",
			},
		},
	}
}

func reapSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "scm-secret", Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("pat")},
	}
}

func reapRepo(proj, name, url string) *tatarav1alpha1.Repository {
	return &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: proj, URL: url},
	}
}

// reapTask builds a Task already IN a stage. It never hand-writes a stage the
// F.3 table would refuse: stage.Enter is the one way in, and the tests that need
// an aged stage rewind stageEnteredAt afterwards.
func reapTask(proj, name, kind, stg, reason string, entered time.Time) *tatarav1alpha1.Task {
	t := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, UID: types.UID("uid-" + name)},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj, Kind: kind},
	}
	stamp := metav1.NewTime(entered)
	t.Status.Stage = stg
	t.Status.StageReason = reason
	t.Status.StageEnteredAt = &stamp
	return t
}

func reapOwnerRef(taskName string, controller bool) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: tatarav1alpha1.GroupVersion.String(), Kind: "Task",
		Name: taskName, UID: types.UID("uid-" + taskName),
		Controller: boolp(controller), BlockOwnerDeletion: boolp(true),
	}
}

func reapReconciler(c client.Client, w scm.SCMWriter) *ProjectReconciler {
	return &ProjectReconciler{
		Client:  c,
		Scheme:  c.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:  func(string) (scm.SCMWriter, error) { return w, nil },
	}
}

func mustGetTask(t *testing.T, c client.Client, name string) (*tatarav1alpha1.Task, bool) {
	t.Helper()
	var tk tatarav1alpha1.Task
	err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &tk)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("get task %s: %v", name, err)
	}
	return &tk, true
}

func mustGetMR(t *testing.T, c client.Client, name string) *tatarav1alpha1.MergeRequest {
	t.Helper()
	var mr tatarav1alpha1.MergeRequest
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &mr); err != nil {
		t.Fatalf("get mr %s: %v", name, err)
	}
	return &mr
}

func mustGetIssue(t *testing.T, c client.Client, name string) *tatarav1alpha1.Issue {
	t.Helper()
	var iss tatarav1alpha1.Issue
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &iss); err != nil {
		t.Fatalf("get issue %s: %v", name, err)
	}
	return &iss
}

// TestReapNeverClosesWhatWeDidNotCreate IS THE REGRESSION TEST (fix V6-2). It is
// the first test in this task for a reason.
//
// Fix C3-2 makes every review-kind Task non-bot-authored BY CONSTRUCTION, so a
// review Task controller-owns a CONTRIBUTOR'S MergeRequest. Its only terminal is
// parked(awaiting-human) and B.6 reaps a non-backlog park at 7d. v5's rule ("on
// terminal entry, close every owned open MR and delete its head branch") then
// closed the human's PR and deleted their branch - and on a fork the branch
// delete 403s, the step is BLOCKING, and the reap requeues forever hammering the
// forge.
//
// The fake forge's ClosePR and DeleteBranch PANIC. Nothing about this assertion
// can be satisfied by a call that happened to be a no-op.
func TestReapNeverClosesWhatWeDidNotCreate(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("neverclose")
	repo := reapRepo("neverclose", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	// A review Task, parked(awaiting-human) 8 days ago: PAST parkRetention.
	task := reapTask("neverclose", "rev-task", "review",
		tatarav1alpha1.StageParked, stage.ReasonAwaitingHuman, time.Now().Add(-8*24*time.Hour))
	task.Status.MRRefs = []string{tatarav1alpha1.MergeRequestName(repo.Name, 7)}

	// The HUMAN's fork PR. We controller-own the mirror CR; we did not create the
	// PR, we do not own the branch, and the head lives on their fork.
	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.MergeRequestName(repo.Name, 7), Namespace: testNS,
			OwnerReferences: []metav1.OwnerReference{reapOwnerRef("rev-task", true)},
		},
		Spec: tatarav1alpha1.MergeRequestSpec{RepositoryRef: repo.Name, Number: 7, ProjectRef: "neverclose"},
	}
	mr.Status.Author = "outside-contributor"
	mr.Status.HeadBranch = "fix/their-branch"
	mr.Status.State = "open"

	c := newMirrorClient(t, proj, repo, reapSecret(), task, mr)
	// Every write method panics. ZERO forge writes is the assertion.
	w := &reapWriter{}
	r := reapReconciler(c, w)

	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal: %v", err)
	}

	if len(w.closed) != 0 || len(w.deleted) != 0 || len(w.comments) != 0 || len(w.labels) != 0 {
		t.Fatalf("forge was written to: closed=%v deleted=%v comments=%v labels=%v",
			w.closed, w.deleted, w.comments, w.labels)
	}
	// The PR is STILL OPEN and the branch STILL EXISTS: the mirror is untouched.
	got := mustGetMR(t, c, mr.Name)
	if got.Status.State != "open" || got.Status.HeadBranch != "fix/their-branch" {
		t.Fatalf("the human's MR mirror was mutated: %+v", got.Status)
	}
	// The Task itself IS reaped (a 7d park ages out) - it just takes nothing with it.
	if _, ok := mustGetTask(t, c, "rev-task"); ok {
		t.Fatal("parked(awaiting-human) task past parkRetention was not reaped")
	}
}

// TestReapOrphansTheHumansOpenMRRatherThanCascadingIt is THE MERGEREQUEST
// ORPHAN / RE-MINT GAP, and it is the OTHER half of "never close what we did not
// create": v6 got the FORGE half right and the MIRROR half exactly wrong.
//
// B.6 step 4 (closeOwnMRs) correctly leaves a human's PR completely alone -
// clause (a) fails, so no close, no branch delete, ZERO forge writes. But step 3
// (releaseOwnership) kept the ownerRef on EVERY MR, on the reasoning that "a bot
// MR we are about to close must never be re-adopted". That reasoning is right for
// a BOT MR and WRONG for a HUMAN's: keeping the ref means the MergeRequest CR
// CASCADES with the Task when the Task is deleted, so a contributor whose PR is
// still OPEN on the forge silently loses its mirror. Their next comment lands on
// nothing.
//
// The rule is the one the Issue path has had since fix H13: an artifact that is
// still OPEN and is NOT ours to close must be re-mintable RIGHT NOW. Drop the
// ownerRef, let the CR survive OWNERLESS (B.1: a zero-owner object is never
// garbage collected), and let the sweep re-adopt it.
func TestReapOrphansTheHumansOpenMRRatherThanCascadingIt(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("orphanmr")
	repo := reapRepo("orphanmr", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	task := reapTask("orphanmr", "rev-task", "review",
		tatarav1alpha1.StageParked, stage.ReasonAwaitingHuman, time.Now().Add(-8*24*time.Hour))
	task.Status.MRRefs = []string{tatarav1alpha1.MergeRequestName(repo.Name, 7)}
	// Two of the five V7-9 rounds are already spent on this thread.
	task.Status.HumanReviewRounds = 2

	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.MergeRequestName(repo.Name, 7), Namespace: testNS,
			OwnerReferences: []metav1.OwnerReference{reapOwnerRef("rev-task", true)},
		},
		Spec: tatarav1alpha1.MergeRequestSpec{RepositoryRef: repo.Name, Number: 7, ProjectRef: "orphanmr"},
	}
	mr.Status.Author = "outside-contributor"
	mr.Status.HeadBranch = "fix/their-branch"
	mr.Status.State = "open"
	mr.Status.Status = "needs-changes" // a review WAS posted on it

	c := newMirrorClient(t, proj, repo, reapSecret(), task, mr)
	w := &reapWriter{} // every write method PANICS
	r := reapReconciler(c, w)

	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal: %v", err)
	}

	// V6-2 still holds: ZERO forge writes.
	if len(w.closed) != 0 || len(w.deleted) != 0 || len(w.comments) != 0 || len(w.labels) != 0 {
		t.Fatalf("forge was written to: closed=%v deleted=%v comments=%v labels=%v",
			w.closed, w.deleted, w.comments, w.labels)
	}
	if _, ok := mustGetTask(t, c, "rev-task"); ok {
		t.Fatal("parked(awaiting-human) task past parkRetention was not reaped")
	}

	// THE FIX: the mirror is ALIVE and OWNERLESS. A surviving ownerRef here is the
	// bug - it makes the CR cascade with the Task and the human's open PR loses
	// its mirror.
	got := mustGetMR(t, c, mr.Name)
	if len(got.OwnerReferences) != 0 {
		t.Fatalf("the human's still-OPEN MR kept owner refs %v: the mirror CASCADES with the reaped Task and is LOST",
			got.OwnerReferences)
	}
	if got.Status.State != "open" || got.Status.HeadBranch != "fix/their-branch" {
		t.Fatalf("the human's MR mirror was mutated: %+v", got.Status)
	}
	// The V7-9 counter is CARRIED on the surviving mirror. Without it the re-mint
	// resets it to zero and the human's PR gets five MORE review pods every seven
	// days - the same cost amplifier, one week slower.
	if got.Annotations[AnnHumanReviewRounds] != "2" {
		t.Fatalf("humanReviewRounds annotation = %q, want \"2\" carried onto the surviving mirror",
			got.Annotations[AnnHumanReviewRounds])
	}
}

// TestReapCascadesAlreadyClosedHumanMR: the orphan rule is "NOT OURS **and still
// OPEN**". A human's MR that has already been CLOSED has nothing left to mirror,
// so it CASCADES with the Task like any other spent artifact. Orphaning it would
// leak a zero-owner CR that nothing ever collects and nothing ever re-adopts (the
// sweep only lists OPEN PRs).
func TestReapCascadesAlreadyClosedHumanMR(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("closedhuman")
	repo := reapRepo("closedhuman", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	task := reapTask("closedhuman", "rev-task", "review",
		tatarav1alpha1.StageParked, stage.ReasonAwaitingHuman, time.Now().Add(-8*24*time.Hour))
	task.Status.MRRefs = []string{tatarav1alpha1.MergeRequestName(repo.Name, 8)}

	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.MergeRequestName(repo.Name, 8), Namespace: testNS,
			OwnerReferences: []metav1.OwnerReference{reapOwnerRef("rev-task", true)},
		},
		Spec: tatarav1alpha1.MergeRequestSpec{RepositoryRef: repo.Name, Number: 8, ProjectRef: "closedhuman"},
	}
	mr.Status.Author = "outside-contributor"
	mr.Status.HeadBranch = "fix/their-branch"
	mr.Status.State = "closed"

	c := newMirrorClient(t, proj, repo, reapSecret(), task, mr)
	w := &reapWriter{} // still ZERO forge writes: we did not create it, closed or not
	r := reapReconciler(c, w)

	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal: %v", err)
	}
	if len(w.closed) != 0 || len(w.deleted) != 0 {
		t.Fatalf("the reaper touched a human's CLOSED PR: closed=%v deleted=%v", w.closed, w.deleted)
	}
	got := mustGetMR(t, c, mr.Name)
	owner, ok := own.ControllerOwner(got)
	if !ok || owner != "rev-task" {
		t.Fatalf("a CLOSED human MR was ORPHANED (owner=%q/%v): it has nothing left to mirror and must CASCADE",
			owner, ok)
	}
}

// TestReapClosesOwnBotMRAfterHandover is the other half of fix V6-2: a bot MR on
// task/<this-task> with NO surviving owner IS closed and its branch deleted - and
// the close runs AFTER the B.5 ownership handover, never before. v5 closed the MR
// first and then handed the corpse to the live survivor.
func TestReapClosesOwnBotMRAfterHandover(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("closeown")
	repo := reapRepo("closeown", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	dying := reapTask("closeown", "impl-task", "clarify",
		tatarav1alpha1.StageFailed, stage.ReasonTurnBudgetExhausted, time.Now().Add(-8*24*time.Hour))
	dying.Status.MRRefs = []string{
		tatarav1alpha1.MergeRequestName(repo.Name, 1),
		tatarav1alpha1.MergeRequestName(repo.Name, 2),
	}
	// A LIVE sibling that holds a plain ref on MR #1.
	sibling := reapTask("closeown", "sib-task", "clarify",
		tatarav1alpha1.StageImplementing, "", time.Now())

	botMR := func(number int, owners []metav1.OwnerReference) *tatarav1alpha1.MergeRequest {
		mr := &tatarav1alpha1.MergeRequest{
			ObjectMeta: metav1.ObjectMeta{
				Name: tatarav1alpha1.MergeRequestName(repo.Name, number), Namespace: testNS,
				OwnerReferences: owners,
			},
			Spec: tatarav1alpha1.MergeRequestSpec{RepositoryRef: repo.Name, Number: number, ProjectRef: "closeown"},
		}
		mr.Status.Author = "tatara-bot"
		mr.Status.HeadBranch = TaskBranchPrefix + "impl-task"
		mr.Status.State = "open"
		return mr
	}
	// #1: SURVIVING plain owner -> clause (d) refuses the close; the flag hands over.
	mr1 := botMR(1, []metav1.OwnerReference{reapOwnerRef("impl-task", true), reapOwnerRef("sib-task", false)})
	// #2: no survivor -> all four clauses pass -> closed, branch deleted.
	mr2 := botMR(2, []metav1.OwnerReference{reapOwnerRef("impl-task", true)})

	c := newMirrorClient(t, proj, repo, reapSecret(), dying, sibling, mr1, mr2)
	w := &reapWriter{}
	w.closePR = func(_ string, number int, _ string) error {
		// ORDERING PROBE: at the instant we close #2, #1 must ALREADY carry the
		// survivor as its controller owner. A close-before-handover hands the
		// corpse to the survivor.
		cur := mustGetMR(t, c, tatarav1alpha1.MergeRequestName(repo.Name, 1))
		owner, _ := own.ControllerOwner(cur)
		w.closeOrder = append(w.closeOrder, owner)
		_ = number
		return nil
	}
	w.deleteBrnch = func(string, string) error { return nil }
	r := reapReconciler(c, w)

	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal: %v", err)
	}

	if len(w.closed) != 1 || w.closed[0] != 2 {
		t.Fatalf("ClosePR calls = %v, want exactly [2] (clause (d) protects #1)", w.closed)
	}
	if len(w.deleted) != 1 || w.deleted[0] != TaskBranchPrefix+"impl-task" {
		t.Fatalf("DeleteBranch calls = %v, want exactly [task/impl-task]", w.deleted)
	}
	if len(w.closeOrder) != 1 || w.closeOrder[0] != "sib-task" {
		t.Fatalf("at ClosePR time MR#1's controller owner was %v, want sib-task: the close ran BEFORE the B.5 handover", w.closeOrder)
	}
	if got, _ := own.ControllerOwner(mustGetMR(t, c, mr1.Name)); got != "sib-task" {
		t.Fatalf("MR#1 controller owner = %q, want sib-task (B.5 handover)", got)
	}
	// #2 is OURS and we just closed it, so it KEEPS the ref and CASCADES with the
	// Task. The orphan rule that saves a human's open PR must never re-open this
	// hole: an orphaned bot MR would be re-adoptable, and the whole point of
	// closing it was that nothing ever picks it up again.
	if got, ok := own.ControllerOwner(mustGetMR(t, c, mr2.Name)); !ok || got != "impl-task" {
		t.Fatalf("the bot MR we CLOSED was orphaned (owner=%q/%v); it must keep its ref and CASCADE", got, ok)
	}
}

// TestReapParkCommentBlocks: the reaper BLOCKS on the park comment. A 403
// REQUEUES the reap; it does not proceed without it (fix M25). The label stamp is
// BLOCKING TOO (fix M3-11) - a label that silently fails to land makes the next
// sweep mint the issue ACTIVE and the loop M25 kills comes right back.
func TestReapParkCommentBlocks(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("blocked")
	repo := reapRepo("blocked", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	task := reapTask("blocked", "clar-task", "clarify",
		tatarav1alpha1.StageParked, stage.ReasonAwaitingHuman, time.Now().Add(-8*24*time.Hour))
	task.Status.IssueRefs = []string{tatarav1alpha1.IssueName(repo.Name, 5)}

	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.IssueName(repo.Name, 5), Namespace: testNS,
			OwnerReferences: []metav1.OwnerReference{reapOwnerRef("clar-task", true)},
		},
		Spec: tatarav1alpha1.IssueSpec{RepositoryRef: repo.Name, Number: 5, ProjectRef: "blocked"},
	}
	iss.Status.State = "open"

	c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
	calls := 0
	w := &reapWriter{}
	w.comment = func(string, string) error {
		calls++
		if calls == 1 {
			return &scm.HTTPError{Status: 403, Path: "/issues/5/comments"}
		}
		return nil
	}
	w.addLabel = func(string, string) error { return nil }
	r := reapReconciler(c, w)

	// Pass 1: the comment 403s. The reap REQUEUES and the Task is STILL THERE.
	if err := r.ReapTerminal(ctx, proj); err == nil {
		t.Fatal("ReapTerminal returned nil after a 403 on the park comment; it must requeue")
	}
	if _, ok := mustGetTask(t, c, "clar-task"); !ok {
		t.Fatal("the task was reaped even though the park comment never landed")
	}
	if len(w.labels) != 0 {
		t.Fatalf("the label was stamped before the comment landed: %v", w.labels)
	}

	// Pass 2: the comment lands. Comment, then label, then release, then reap.
	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal (second pass): %v", err)
	}
	if len(w.labels) != 1 || w.labels[0] != "szymonrychu/tatara-operator#5|"+TataraParkedLabel {
		t.Fatalf("labels = %v, want the tatara-parked stamp on the issue", w.labels)
	}
	if _, ok := mustGetTask(t, c, "clar-task"); ok {
		t.Fatal("the task was not reaped after the park comment landed")
	}
	// The Issue survives, OWNERLESS: the next sweep re-mints it as
	// parked(backlog-sweep) and ADOPTS the CR (B.4, fix M3-10).
	got := mustGetIssue(t, c, iss.Name)
	if _, owned := own.ControllerOwner(got); owned {
		t.Fatal("the reaped task still controller-owns its issue")
	}
	if len(got.OwnerReferences) != 0 {
		t.Fatalf("owner refs = %v, want the ref DROPPED entirely", got.OwnerReferences)
	}
	// And the comment is posted EXACTLY ONCE across the two passes.
	if len(w.comments) != 2 || calls != 2 {
		t.Fatalf("comment attempts = %d (bodies %v); the 403 must be retried exactly once", calls, w.comments)
	}
}

// TestReapFailedReleasesIssuesImmediately (fix H13). v3 let a failed Task hold
// its Issues hostage for SEVEN DAYS, silently. The cutover amplifier makes that
// fatal: an image-pin skew fails every Task INSTANTLY and would freeze every
// Issue for a week with no comment. The Task CR still survives 7d as a DEBUGGING
// ARTIFACT - owning nothing, blocking nothing.
func TestReapFailedReleasesIssuesImmediately(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("failfast")
	repo := reapRepo("failfast", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	// Entered `failed` ONE MINUTE ago: nowhere near failedRetention.
	task := reapTask("failfast", "fail-task", "clarify",
		tatarav1alpha1.StageFailed, stage.ReasonAgentContractMismatch, time.Now().Add(-time.Minute))
	task.Status.IssueRefs = []string{tatarav1alpha1.IssueName(repo.Name, 9)}

	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.IssueName(repo.Name, 9), Namespace: testNS,
			OwnerReferences: []metav1.OwnerReference{reapOwnerRef("fail-task", true)},
		},
		Spec: tatarav1alpha1.IssueSpec{RepositoryRef: repo.Name, Number: 9, ProjectRef: "failfast"},
	}
	iss.Status.State = "open"

	c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
	w := &reapWriter{
		comment:  func(string, string) error { return nil },
		addLabel: func(string, string) error { return nil },
	}
	r := reapReconciler(c, w)

	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal: %v", err)
	}

	// The Task is NOT deleted (it is 1 minute old, not 7 days).
	tk, ok := mustGetTask(t, c, "fail-task")
	if !ok {
		t.Fatal("a fresh failed task was deleted; it must survive as a debugging artifact")
	}
	if tk.Annotations[AnnTerminalReleased] != "true" {
		t.Fatal("the failed task did not record its terminal release")
	}
	// But the Issue is RELEASED RIGHT NOW: commented, labelled, ownerRef dropped.
	if len(w.comments) != 1 {
		t.Fatalf("comments = %v, want exactly one naming the stageReason", w.comments)
	}
	if len(w.labels) != 1 {
		t.Fatalf("labels = %v, want the tatara-parked stamp", w.labels)
	}
	got := mustGetIssue(t, c, iss.Name)
	if len(got.OwnerReferences) != 0 {
		t.Fatalf("the failed task still owns its issue: %v", got.OwnerReferences)
	}
	if got.Annotations[AnnTerminalCommented] != "fail-task" {
		t.Fatal("the issue does not record which task commented; a requeue would double-post")
	}

	// Idempotent: a second pass re-posts NOTHING.
	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal (second pass): %v", err)
	}
	if len(w.comments) != 1 {
		t.Fatalf("the terminal comment was re-posted on requeue: %v", w.comments)
	}
}

// A Task deleted by the B.6 reaper leaves its per-issue
// operator_task_tokens_total/operator_task_turns_total series behind forever
// unless the reaper clears them at the same point it deletes the Task CR -
// DeleteTaskSeries existed but had zero production callers (metric-wiring
// audit, issue #370).
func TestReapRejectedDeletesTaskAndClearsTokenSeries(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("tokgc")
	repo := reapRepo("tokgc", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	// Entered `rejected` well past RejectedRetention (24h): eligible for delete.
	task := reapTask("tokgc", "rej-task", "clarify",
		tatarav1alpha1.StageRejected, stage.ReasonDeclined, time.Now().Add(-25*time.Hour))
	task.Spec.RepositoryRef = repo.Name
	task.Spec.Source = &tatarav1alpha1.TaskSource{IssueRef: "tatara-operator#9"}
	task.Status.ResolvedModel = "claude-opus-4-8"

	c := newMirrorClient(t, proj, repo, reapSecret(), task)
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	r := &ProjectReconciler{
		Client:  c,
		Scheme:  c.Scheme(),
		Metrics: metrics,
		SCMFor:  func(string) (scm.SCMWriter, error) { return &reapWriter{}, nil },
	}

	metrics.AddTaskTokens("tokgc", repo.Name, "clarify", "tatara-operator#9", "claude-opus-4-8", 100, 50, 30, 10)
	metrics.AddTaskTurn("tokgc", repo.Name, "clarify", "tatara-operator#9")

	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal: %v", err)
	}
	if _, ok := mustGetTask(t, c, "rej-task"); ok {
		t.Fatal("a rejected task 25h old must be deleted (RejectedRetention is 24h)")
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "operator_task_tokens_total" && mf.GetName() != "operator_task_turns_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "issue" && lp.GetValue() == "tatara-operator#9" {
					t.Errorf("%s{issue=tatara-operator#9} must be cleared when the Task is reaped", mf.GetName())
				}
			}
		}
	}
}

// TestReapBacklogSweepNeverAgesOut: parked(backlog-sweep) is the durable mirror
// anchor, not a stalled work item. It is NEVER reaped on age - only when EVERY
// owned Issue is closed. Ageing it out would churn mint/reap forever. And it is
// not a failure, so it gets NO comment and NO label (the writer panics on both).
func TestReapBacklogSweepNeverAgesOut(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("backlog")
	repo := reapRepo("backlog", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	seed := func(state string) (client.Client, *reapWriter, *ProjectReconciler) {
		task := reapTask("backlog", "bl-task", "clarify",
			tatarav1alpha1.StageParked, stage.ReasonBacklogSweep, time.Now().Add(-90*24*time.Hour))
		task.Status.IssueRefs = []string{tatarav1alpha1.IssueName(repo.Name, 3)}
		iss := &tatarav1alpha1.Issue{
			ObjectMeta: metav1.ObjectMeta{
				Name: tatarav1alpha1.IssueName(repo.Name, 3), Namespace: testNS,
				OwnerReferences: []metav1.OwnerReference{reapOwnerRef("bl-task", true)},
			},
			Spec: tatarav1alpha1.IssueSpec{RepositoryRef: repo.Name, Number: 3, ProjectRef: "backlog"},
		}
		iss.Status.State = state
		c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
		w := &reapWriter{} // panics on every write
		return c, w, reapReconciler(c, w)
	}

	// 90 days old with an OPEN issue: still there.
	c, _, r := seed("open")
	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal: %v", err)
	}
	if _, ok := mustGetTask(t, c, "bl-task"); !ok {
		t.Fatal("parked(backlog-sweep) was aged out; it must NEVER be reaped on age")
	}

	// Same Task, issue CLOSED: reaped.
	c, _, r = seed("closed")
	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal (closed issue): %v", err)
	}
	if _, ok := mustGetTask(t, c, "bl-task"); ok {
		t.Fatal("parked(backlog-sweep) with every owned issue closed was not reaped")
	}
}

// TestReapDeliveredWaitsForDocumentation: a delivered Task is NOT reaped before
// it is covered, and IS reaped once documentedBy is stamped and the 48h TTL has
// passed. A Task with ZERO merged MRs (a brainstorm skip, a declined implement)
// is never documented and reaps on the TTL alone.
func TestReapDeliveredWaitsForDocumentation(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("delivered")
	repo := reapRepo("delivered", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	deliveredAt := metav1.NewTime(time.Now().Add(-72 * time.Hour)) // past the 48h TTL

	mergedMR := func(number int, owner string) *tatarav1alpha1.MergeRequest {
		mr := &tatarav1alpha1.MergeRequest{
			ObjectMeta: metav1.ObjectMeta{
				Name: tatarav1alpha1.MergeRequestName(repo.Name, number), Namespace: testNS,
				OwnerReferences: []metav1.OwnerReference{reapOwnerRef(owner, true)},
			},
			Spec: tatarav1alpha1.MergeRequestSpec{RepositoryRef: repo.Name, Number: number, ProjectRef: "delivered"},
		}
		mr.Status.Author = "tatara-bot"
		mr.Status.HeadBranch = TaskBranchPrefix + owner
		mr.Status.State = "merged"
		return mr
	}

	// (a) undocumented, has a merged MR: BLOCKED.
	undoc := reapTask("delivered", "undoc-task", "clarify", tatarav1alpha1.StageDelivered, "", deliveredAt.Time)
	undoc.Status.DeliveredAt = &deliveredAt
	undoc.Status.MRRefs = []string{tatarav1alpha1.MergeRequestName(repo.Name, 11)}
	// (b) zero merged MRs (a brainstorm skip): reaped on the TTL alone.
	noMR := reapTask("delivered", "nomr-task", "brainstorm", tatarav1alpha1.StageDelivered, "", deliveredAt.Time)
	noMR.Status.DeliveredAt = &deliveredAt

	c := newMirrorClient(t, proj, repo, reapSecret(), undoc, noMR, mergedMR(11, "undoc-task"))
	w := &reapWriter{}
	r := reapReconciler(c, w)

	before := testutil.ToFloat64(obs.GCBlockedTotal.WithLabelValues(obs.GCBlockedDocReference))
	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal: %v", err)
	}
	if _, ok := mustGetTask(t, c, "undoc-task"); !ok {
		t.Fatal("a delivered task with a merged MR and no documentedBy was reaped BEFORE it was covered")
	}
	if got := testutil.ToFloat64(obs.GCBlockedTotal.WithLabelValues(obs.GCBlockedDocReference)); got <= before {
		t.Fatalf("operator_gc_blocked_total{reason=doc_reference} = %v, want > %v", got, before)
	}
	if _, ok := mustGetTask(t, c, "nomr-task"); ok {
		t.Fatal("a delivered task with ZERO merged MRs was not reaped at the 48h TTL")
	}

	// Stamp documentedBy: now it reaps.
	tk, _ := mustGetTask(t, c, "undoc-task")
	tk.Status.DocumentedBy = "doc-batch-1"
	if err := c.Status().Update(ctx, tk); err != nil {
		t.Fatalf("stamp documentedBy: %v", err)
	}
	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal (documented): %v", err)
	}
	if _, ok := mustGetTask(t, c, "undoc-task"); ok {
		t.Fatal("a documented delivered task past its 48h TTL was not reaped")
	}
}

// TestReapSkipsFoldInFlight: the reaper SKIP list. Any Task named in a LIVE
// Task's status.foldInFlight is skipped, and operator_gc_blocked_total records
// why. Reaping a fold member mid-adoption destroys the artifacts the umbrella is
// halfway through adopting.
func TestReapSkipsFoldInFlight(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("foldskip")

	member := reapTask("foldskip", "member-task", "clarify",
		tatarav1alpha1.StageFailed, stage.ReasonOperatorError, time.Now().Add(-30*24*time.Hour))
	umbrella := reapTask("foldskip", "umbrella-task", "refine",
		tatarav1alpha1.StageRefining, "", time.Now())
	umbrella.Status.FoldInFlight = []string{"member-task"}

	c := newMirrorClient(t, proj, reapSecret(), member, umbrella)
	w := &reapWriter{} // any forge write here is a bug
	r := reapReconciler(c, w)

	before := testutil.ToFloat64(obs.GCBlockedTotal.WithLabelValues(obs.GCBlockedFoldInFlight))
	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal: %v", err)
	}
	if _, ok := mustGetTask(t, c, "member-task"); !ok {
		t.Fatal("a Task named in a live Task's foldInFlight was reaped")
	}
	if got := testutil.ToFloat64(obs.GCBlockedTotal.WithLabelValues(obs.GCBlockedFoldInFlight)); got <= before {
		t.Fatalf("operator_gc_blocked_total{reason=fold_in_flight} = %v, want > %v", got, before)
	}
}

// TestReapIgnoresUnstampedTasks: the reaper is gated on status.stage != "", so a
// Task the stage machine has not touched yet is never collected.
func TestReapIgnoresUnstampedTasks(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("additive")

	old := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "old-task", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "additive", Kind: "clarify"},
	}

	c := newMirrorClient(t, proj, reapSecret(), old)
	r := reapReconciler(c, &reapWriter{})

	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal: %v", err)
	}
	if _, ok := mustGetTask(t, c, "old-task"); !ok {
		t.Fatal("ReapTerminal collected a Task with no status.stage")
	}
}
