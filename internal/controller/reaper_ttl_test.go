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

// mkWrapperPodSvcTTL is mkWrapperPodSvc with AGENT_POD_TTL_SECONDS stamped on
// the wrapper container, exactly as agent.PodSpec stamps it. That env is the
// pod's own copy of the Project's TTL and the only handle the reaper has on it:
// the reaper resolves no Projects. ttlSeconds <= 0 stamps nothing.
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
		env = []corev1.EnvVar{{Name: "AGENT_POD_TTL_SECONDS", Value: strconv.Itoa(ttlSeconds)}}
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

// TestReapOrphans_PastTTLPodLeftToTTLStop: the pod is idle AND past t0. The TTL
// stop owns it from t0 onward, so the idle backstop must stand down. This is the
// race the incident caught live: reap at 12:10:19Z, TTL stop at 12:37:58Z
// against a pod that no longer existed, reported as force_deleted.
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
	setPodStartedAt(t, "t-reap-ttl", time.Now().Add(-2*time.Hour)) // t0 was an hour ago

	idleReaperServer().ReapOrphans(context.Background())
	if !podExists(t, "reap-ttl") {
		t.Error("a pod past its TTL was idle-reaped: the G.7 stop now has no wrapper to offer the handoff turn to")
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
