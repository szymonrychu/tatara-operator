// Tests for issue #527: the idle backstop (30m, issue #237) and the pod TTL
// (3600s, G.7) were unaware of each other. A pod past its own t0 belongs to the
// TTL stop, which needs the wrapper ALIVE to offer the one handoff turn the
// agent still gets. The reaper deleting it first guarantees that turn is never
// offered and leaves the stop sequence talking to a corpse.
package controller

import (
	"context"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// ttlStopTestTurnTimeoutSeconds is the turn timeout every pod in this file
// carries, so the G.7 stop's own hard cap - t0 + 2*turnTimeout + TTLGrace - is
// t0 + 31m. Deliberately the stock value: it is LONGER than the 30m idle
// backstop, which is why the stand-down window is bounded by the cap rather
// than by IdlePodReapAfter.
const ttlStopTestTurnTimeoutSeconds = 900

// mkWrapperPodSvcTTL is mkWrapperPodSvc with AGENT_POD_TTL_SECONDS and
// TURN_TIMEOUT_SECONDS stamped on the wrapper container, exactly as
// agent.PodSpec stamps them. They are the pod's own copy of the Project's TTL
// and turn timeout, and the only handle the reaper has on either: the reaper
// resolves no Projects. Together they bound the window in which the G.7 stop
// owns this pod. ttlSeconds <= 0 stamps neither.
func mkWrapperPodSvcTTL(t *testing.T, name, taskName, taskUID string, ttlSeconds int) {
	t.Helper()
	labels := map[string]string{
		agent.LabelManagedBy: agent.ManagedByValue,
		agent.LabelComponent: agent.ComponentAgent,
		agent.LabelTask:      taskName,
		agent.LabelTaskUID:   taskUID,
	}
	var env []corev1.EnvVar
	if ttlSeconds > 0 {
		env = []corev1.EnvVar{
			{Name: agent.EnvAgentPodTTLSeconds, Value: strconv.Itoa(ttlSeconds)},
			{Name: agent.EnvTurnTimeoutSeconds, Value: strconv.Itoa(ttlStopTestTurnTimeoutSeconds)},
		}
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: labels},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{{Name: "wrapper", Image: "wrapper:1", Env: env}},
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

func setPodStartedAt(t *testing.T, taskName string, at time.Time) {
	t.Helper()
	tk := getTask(t, taskName)
	stamp := metav1.NewTime(at)
	tk.Status.PodStartedAt = &stamp
	if err := k8sClient.Status().Update(context.Background(), tk); err != nil {
		t.Fatalf("set podStartedAt on %s: %v", taskName, err)
	}
}

// TestReapOrphans_PastTTLPodLeftToTTLStop: the pod is idle AND inside the window
// the TTL stop owns, so the idle backstop must stand down. This is the race the
// incident caught live: reap at 12:10:19Z, TTL stop at 12:37:58Z against a pod
// that no longer existed, reported as force_deleted.
func TestReapOrphans_PastTTLPodLeftToTTLStop(t *testing.T) {
	mkTaskProject(t, "p-reap-ttl", 3)
	mkTaskRepository(t, "r-reap-ttl", "p-reap-ttl")
	mkTask(t, "t-reap-ttl", "p-reap-ttl", "r-reap-ttl")
	setTaskStage(t, "t-reap-ttl", tatarav1alpha1.StageImplementing)
	setTaskAnns(t, "t-reap-ttl", map[string]string{
		annCurrentTurn:  "turn-1",
		annTurnComplete: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
	})
	mkWrapperPodSvcTTL(t, "reap-ttl", "t-reap-ttl", string(getTask(t, "t-reap-ttl").UID), 3600)
	setPodStartedAt(t, "t-reap-ttl", time.Now().Add(-70*time.Minute)) // t0 was 10m ago; the cap is 31m out

	idleReaperServer().ReapOrphans(context.Background())
	if !podExists(t, "reap-ttl") {
		t.Error("a pod inside its TTL-stop window was idle-reaped: the G.7 stop now has no wrapper to offer the handoff turn to")
	}
}

// TestReapOrphans_PastTTLStopHardCapReapedAgain is the far end of that window.
// The stand-down is exclusive OWNERSHIP, not an exemption: it is only sound
// while the stop could still legitimately be running. reconcilePodStage
// early-returns before the TTL gate on a committed handoff or an unadmitted
// ticket, and any persistent error upstream of the gate never reaches ttlStop
// either - so "past t0" alone would disarm issue #237's backstop forever on
// exactly the wedged reconciles it exists for, leaving a live claude session
// holding a node slot with nothing recording that it is stuck.
func TestReapOrphans_PastTTLStopHardCapReapedAgain(t *testing.T) {
	mkTaskProject(t, "p-reap-cap", 3)
	mkTaskRepository(t, "r-reap-cap", "p-reap-cap")
	mkTask(t, "t-reap-cap", "p-reap-cap", "r-reap-cap")
	setTaskStage(t, "t-reap-cap", tatarav1alpha1.StageImplementing)
	setTaskAnns(t, "t-reap-cap", map[string]string{
		annCurrentTurn:  "turn-1",
		annTurnComplete: time.Now().Add(-3 * time.Hour).UTC().Format(time.RFC3339),
	})
	mkWrapperPodSvcTTL(t, "reap-cap", "t-reap-cap", string(getTask(t, "t-reap-cap").UID), 3600)
	// t0 was 2h ago and the stop's hard cap 89m ago: no stop is coming.
	setPodStartedAt(t, "t-reap-cap", time.Now().Add(-3*time.Hour))

	idleReaperServer().ReapOrphans(context.Background())
	if podExists(t, "reap-cap") {
		t.Error("an idle pod well past the TTL stop's own hard cap was kept: the issue #237 backstop never re-arms")
	}
}

// TestReapOrphans_BeforeTTLIdlePodStillReaped is the counterpart. The idle
// backstop exists for issue #237 (leaked wrappers) and must keep working for
// every pod the TTL stop is not yet responsible for.
func TestReapOrphans_BeforeTTLIdlePodStillReaped(t *testing.T) {
	mkTaskProject(t, "p-reap-prettl", 3)
	mkTaskRepository(t, "r-reap-prettl", "p-reap-prettl")
	mkTask(t, "t-reap-prettl", "p-reap-prettl", "r-reap-prettl")
	setTaskStage(t, "t-reap-prettl", tatarav1alpha1.StageImplementing)
	setTaskAnns(t, "t-reap-prettl", map[string]string{
		annCurrentTurn:  "turn-1",
		annTurnComplete: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
	})
	mkWrapperPodSvcTTL(t, "reap-prettl", "t-reap-prettl", string(getTask(t, "t-reap-prettl").UID), 3600)
	setPodStartedAt(t, "t-reap-prettl", time.Now().Add(-5*time.Minute)) // t0 is 55m away

	idleReaperServer().ReapOrphans(context.Background())
	if podExists(t, "reap-prettl") {
		t.Error("an idle pod well inside its TTL was kept: the issue #237 backstop is disarmed")
	}
}

// TestReapOrphans_NoTTLEnvIdlePodStillReaped: a pod carrying no
// AGENT_POD_TTL_SECONDS (no TTL configured, or an older wrapper spec) has no t0
// and therefore no TTL stop coming. The backstop must not be silently disabled
// by an unreadable TTL.
func TestReapOrphans_NoTTLEnvIdlePodStillReaped(t *testing.T) {
	mkTaskProject(t, "p-reap-nottl", 3)
	mkTaskRepository(t, "r-reap-nottl", "p-reap-nottl")
	mkTask(t, "t-reap-nottl", "p-reap-nottl", "r-reap-nottl")
	setTaskStage(t, "t-reap-nottl", tatarav1alpha1.StageImplementing)
	setTaskAnns(t, "t-reap-nottl", map[string]string{
		annCurrentTurn:  "turn-1",
		annTurnComplete: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
	})
	mkWrapperPodSvcTTL(t, "reap-nottl", "t-reap-nottl", string(getTask(t, "t-reap-nottl").UID), 0)
	setPodStartedAt(t, "t-reap-nottl", time.Now().Add(-2*time.Hour))

	idleReaperServer().ReapOrphans(context.Background())
	if podExists(t, "reap-nottl") {
		t.Error("an idle pod with no pod TTL was kept: nothing else is going to stop it")
	}
}
