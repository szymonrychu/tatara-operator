package controller

import (
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// ttlRaceFixture builds the exact shape of #527's 12:37:58Z event: a live Task
// whose wrapper pod holds no turn and has shown no activity for longer than the
// idle backstop, with the pod carrying the TTL env the operator stamps on it.
//
// podAge places podStartedAt, so the pod is past t0 when podAge > ttl.
func ttlRaceFixture(podAge, ttl, turnTimeout time.Duration, stampTTL bool) (*corev1.Pod, map[string]*tatarav1alpha1.Task) {
	started := metav1.NewTime(time.Now().Add(-podAge))
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-race", Namespace: testNS, UID: "uid-race"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Kind: "implement"},
		Status: tatarav1alpha1.TaskStatus{
			State:        tatarav1alpha1.StateUnderImplementation,
			AgentKind:    "implement",
			PodStartedAt: &started,
		},
	}
	env := []corev1.EnvVar{{Name: agent.EnvTurnTimeoutSeconds, Value: strconv.Itoa(int(turnTimeout.Seconds()))}}
	if stampTTL {
		env = append(env, corev1.EnvVar{Name: agent.EnvAgentPodTTLSeconds, Value: strconv.Itoa(int(ttl.Seconds()))})
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              agent.PodName(task),
			Namespace:         testNS,
			CreationTimestamp: started,
			Labels: map[string]string{
				agent.LabelTask:      task.Name,
				agent.LabelTaskUID:   string(task.UID),
				agent.LabelAgentKind: "implement",
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "wrapper", Env: env}}},
	}
	return pod, map[string]*tatarav1alpha1.Task{task.Name: task}
}

// INSIDE ITS TTL-STOP WINDOW, THE G.7 STOP OWNS THE POD.
//
// This is the actual race behind #527, observed end to end on one Task: the idle
// backstop reaped the wrapper at 12:10:19Z and the TTL stop ran at 12:37:58Z
// against nothing. The two clocks were unaware of each other and the reaper
// always wins - idlePodReapMinutes (30) is far below agentPodTTLSeconds (3600),
// so any pod idle since before t0-30m is deleted before its stop can run.
//
// The stop needs the wrapper ALIVE: it is the only thing that can offer the
// agent its one handoff turn, and status.notes is the only continuation state
// the next pod gets. Reaping inside the window guarantees the handoff turn is
// never offered.
func TestReaper_StandsDownInsideTheTTLStopWindow(t *testing.T) {
	s := reaperServer()
	s.IdlePodReapAfter = 30 * time.Minute

	pod, tasks := ttlRaceFixture(70*time.Minute, time.Hour, 15*time.Minute, true)

	reason, reap := s.orphanReason(pod, tasks)
	if reap {
		t.Fatalf("reaped a pod inside its TTL-stop window (reason %q): the G.7 stop owns this pod, "+
			"and deleting it is what made every synthetic handoff note content-free", reason)
	}
}

// BEFORE t0 THE BACKSTOP'S REACH IS UNCHANGED. No stop is pending, and a pod
// reaped here simply respawns through the ordinary pod-gone path, which has
// nothing to hand off anyway. Standing down early would disarm #237 for the
// whole first hour of every pod's life.
func TestReaper_StillReapsAnIdlePodBeforeT0(t *testing.T) {
	s := reaperServer()
	s.IdlePodReapAfter = 30 * time.Minute

	pod, tasks := ttlRaceFixture(45*time.Minute, time.Hour, 15*time.Minute, true)

	if _, reap := s.orphanReason(pod, tasks); !reap {
		t.Fatal("a pod idle past the backstop and BEFORE t0 must still be reaped: no stop is pending")
	}
}

// PAST THE STOP'S OWN HARD CAP, #237 RE-ARMS WITH ITS FULL REACH.
//
// The window has a far end deliberately. A stand-down that ran from t0 forever
// would replace a backstop with the ASSUMPTION that a reconcile reaches the TTL
// gate - and reconcilePodStage can early-return before it, while any persistent
// error upstream never gets there at all. That is precisely the wedged-reconcile
// class #237 exists for, and an open-ended stand-down would leave a live claude
// session holding a node slot indefinitely.
//
// The far end is the STOPPER's cap (t0 + 2*turnTimeout + 60s), not the reaper's
// own IdlePodReapAfter: at the stock turnTimeoutSeconds=900 that cap is 31m,
// LONGER than the 30m backstop, so borrowing the backstop constant would re-arm
// the reaper mid-sequence and re-open the race.
func TestReaper_ReArmsPastTheStopsHardCap(t *testing.T) {
	s := reaperServer()
	s.IdlePodReapAfter = 30 * time.Minute

	// t0 at 2h ago; cap = t0 + 2*15m + 60s = t0 + 31m, an hour and a half behind.
	pod, tasks := ttlRaceFixture(3*time.Hour, time.Hour, 15*time.Minute, true)

	if _, reap := s.orphanReason(pod, tasks); !reap {
		t.Fatal("past the stop's hard cap the stop has finished or is not coming; #237 must re-arm")
	}
}

// A POD WITH NO READABLE TTL IS NOT SPOKEN FOR. No TTL means no t0 and no stop,
// so the stand-down must not fire - failing open here would turn an unstamped or
// legacy pod into one the idle backstop can never reach.
func TestReaper_ReapsAPodCarryingNoTTL(t *testing.T) {
	s := reaperServer()
	s.IdlePodReapAfter = 30 * time.Minute

	pod, tasks := ttlRaceFixture(70*time.Minute, time.Hour, 15*time.Minute, false)

	if _, reap := s.orphanReason(pod, tasks); !reap {
		t.Fatal("no TTL on the pod means no TTL stop is coming; the backstop must still apply")
	}
}
