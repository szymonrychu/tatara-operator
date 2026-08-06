// Tests for the review finding on issue #527's first fix: the reaper's
// stand-down for a pod the G.7 stop owns was a HALF-LINE (everything from t0
// onward) rather than a WINDOW. A stand-down with no far end substitutes an
// assumption - "a reconcile will reach the TTL gate" - for the issue #237
// backstop, and #237 exists precisely for reconciles that never get to do their
// job. PodTTLStopWindowFromSpec is the far end: the stopper's own step-4 hard
// cap, computed from the pod's own env because the reaper resolves no Projects.
package agent_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// windowPod builds a wrapper pod carrying the two env vars agent.PodSpec stamps
// and PodTTLStopWindowFromSpec reads back. A negative seconds value omits the
// var entirely, which is how an alien or hand-made pod looks.
func windowPod(ttlSeconds, turnTimeoutSeconds int) *corev1.Pod {
	var env []corev1.EnvVar
	if ttlSeconds >= 0 {
		env = append(env, corev1.EnvVar{Name: agent.EnvAgentPodTTLSeconds, Value: strconv.Itoa(ttlSeconds)})
	}
	if turnTimeoutSeconds >= 0 {
		env = append(env, corev1.EnvVar{Name: agent.EnvTurnTimeoutSeconds, Value: strconv.Itoa(turnTimeoutSeconds)})
	}
	return &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "wrapper", Env: env}}}}
}

func windowTask(startedAt time.Time) *tatarav1alpha1.Task {
	stamp := metav1.NewTime(startedAt)
	return &tatarav1alpha1.Task{
		Status: tatarav1alpha1.TaskStatus{
			Stage:        tatarav1alpha1.StageImplementing,
			PodStartedAt: &stamp,
		},
	}
}

// The window's far end is the stopper's OWN hard cap, not a borrowed constant.
// StopWithHandoff bounds itself at t0 + 2*turnTimeout + TTLGrace; past that the
// stop has either finished or is never coming, and the backstop must re-arm.
func TestPodTTLStopWindowFromSpec_EndsAtTheStoppersHardCap(t *testing.T) {
	started := time.Now().Add(-2 * time.Hour)
	start, end, ok := agent.PodTTLStopWindowFromSpec(windowPod(3600, 900), windowTask(started))
	require.True(t, ok)
	require.Equal(t, started.Add(time.Hour), start)
	require.Equal(t, started.Add(time.Hour).Add(2*900*time.Second+agent.TTLGrace), end)
}

// The 30-minute idle backstop is NOT a safe substitute for the hard cap: at the
// stock turnTimeoutSeconds the cap is LONGER than it, so a 30m window would
// re-arm the reaper while the stop is still legitimately mid-sequence and
// re-open the race the guard exists to close.
func TestPodTTLStopWindowFromSpec_HardCapCanExceedTheIdleBackstop(t *testing.T) {
	started := time.Now().Add(-2 * time.Hour)
	start, end, ok := agent.PodTTLStopWindowFromSpec(windowPod(3600, 900), windowTask(started))
	require.True(t, ok)
	require.Greater(t, end.Sub(start), 30*time.Minute)
}

// No readable pod TTL means no t0 and no stop coming: the caller must NOT treat
// the pod as spoken for. Same stance as PodTTLDeadlineFromSpec.
func TestPodTTLStopWindowFromSpec_NoTTLIsNotSpokenFor(t *testing.T) {
	_, _, ok := agent.PodTTLStopWindowFromSpec(windowPod(-1, 900), windowTask(time.Now().Add(-2*time.Hour)))
	require.False(t, ok)
}

// A pod with no podStartedAt has no clock at all.
func TestPodTTLStopWindowFromSpec_NoAnchorIsNotSpokenFor(t *testing.T) {
	_, _, ok := agent.PodTTLStopWindowFromSpec(windowPod(3600, 900), &tatarav1alpha1.Task{})
	require.False(t, ok)
}

// An unreadable TURN_TIMEOUT_SECONDS degrades to a zero turn timeout, which is
// exactly what the stopper itself would use: TTLStopInput.TurnTimeout comes from
// the same Project field, so its waits collapse and the whole sequence completes
// inside TTLGrace. The window must not silently widen to compensate.
func TestPodTTLStopWindowFromSpec_MissingTurnTimeoutFallsBackToGraceOnly(t *testing.T) {
	started := time.Now().Add(-2 * time.Hour)
	start, end, ok := agent.PodTTLStopWindowFromSpec(windowPod(3600, -1), windowTask(started))
	require.True(t, ok)
	require.Equal(t, start.Add(agent.TTLGrace), end)
}
