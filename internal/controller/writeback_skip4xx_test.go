package controller

// Tests for issue #166: the un-triageable 4xx-skip writeback loop.
// A Succeeded task whose writeback gets a permanent 4xx on every project repo
// must not re-sweep the SCM forever, and the skip metric must name the failure.

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// rearmWritebackPending forces WritebackPending back to True, simulating the
// pathological re-entry (a lost clear, or a stray Succeeded transition) that the
// issue-166 cap must survive.
func rearmWritebackPending(t *testing.T, name string) {
	t.Helper()
	tk := getTask(t, name)
	apimeta.SetStatusCondition(&tk.Status.Conditions, metav1.Condition{
		Type:   "WritebackPending",
		Status: metav1.ConditionTrue,
		Reason: "AwaitingM5",
	})
	if err := k8sClient.Status().Update(context.Background(), tk); err != nil {
		t.Fatalf("rearm WritebackPending on %s: %v", name, err)
	}
}

func openCallsOf(fw *trackingFakeWriter) int {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	return fw.openCalls
}

// TestWriteback_Skip4xxLoopIsCapped is the core issue-166 guard: when every
// project repo returns a permanent 4xx, the writeback re-sweep is bounded to
// writebackSkip4xxCap SCM sweeps even if WritebackPending keeps getting
// re-armed, after which a terminal WritebackFailed condition is recorded and no
// further SCM call is made.
func TestWriteback_Skip4xxLoopIsCapped(t *testing.T) {
	reg := prometheus.NewRegistry()
	fw := &trackingFakeWriter{openErr: &scm.HTTPError{Status: 404, Body: "Not Found", Path: "/repos/o/r/pulls"}}
	r := newTrackingReconciler(t, fw, reg)
	task := seedWritebackPending(t, "skip4xx-cap", "skip4xx-scm", "skip4xx-proj", "skip4xx-repo")

	sweeps := 0
	rounds := writebackSkip4xxCap + 3
	for i := 0; i < rounds; i++ {
		rearmWritebackPending(t, task.Name)
		before := openCallsOf(fw)
		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err, "round %d must not error", i)
		if openCallsOf(fw) > before {
			sweeps++
		}
	}

	// Only the budgeted number of sweeps actually reach the SCM; every later
	// re-arm is refused by the gate.
	require.Equal(t, writebackSkip4xxCap, sweeps,
		"SCM must be swept at most writebackSkip4xxCap times despite %d re-arms", rounds)

	got := getTask(t, task.Name)
	require.Equal(t, writebackSkip4xxCap, got.Status.WritebackSkip4xxAttempts,
		"attempt counter must saturate at the cap")

	failed := findCond(got.Status.Conditions, "WritebackFailed")
	require.NotNil(t, failed, "terminal WritebackFailed condition must be set")
	require.Equal(t, metav1.ConditionTrue, failed.Status)
	require.Equal(t, "Skip4xxCapReached", failed.Reason)

	// Both the capped-give-up outcome and the per-skip detail metric are emitted.
	require.GreaterOrEqual(t,
		testutil.ToFloat64(r.Metrics.WritebackOutcomeCounter("skip_4xx_capped")), float64(1),
		"skip_4xx_capped outcome must be emitted once the cap fires")
	require.Equal(t, float64(writebackSkip4xxCap),
		testutil.ToFloat64(r.Metrics.WritebackSkip4xxCounter("404", "other")),
		"each genuine 4xx skip must emit operator_writeback_skip_4xx_total{status,reason}")
}

// TestWriteback_Skip4xxOneSweepNoRearm confirms the healthy case is unchanged:
// a single all-4xx writeback sweeps once, clears WritebackPending, and does NOT
// reach the cap or set WritebackFailed when nothing re-arms the gate.
func TestWriteback_Skip4xxOneSweepNoRearm(t *testing.T) {
	reg := prometheus.NewRegistry()
	fw := &trackingFakeWriter{openErr: &scm.HTTPError{Status: 403, Body: "Forbidden", Path: "/repos/o/r/pulls"}}
	r := newTrackingReconciler(t, fw, reg)
	task := seedWritebackPending(t, "skip4xx-1sweep", "skip4xx1-scm", "skip4xx1-proj", "skip4xx1-repo")

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)
	// A second reconcile without re-arming must be a no-op (gate already cleared).
	_, err = reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.Equal(t, 1, openCallsOf(fw), "no re-arm => exactly one SCM sweep")

	got := getTask(t, task.Name)
	require.Equal(t, 1, got.Status.WritebackSkip4xxAttempts)
	require.Nil(t, findCond(got.Status.Conditions, "WritebackFailed"),
		"one sweep must not trip the terminal give-up")
	pending := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, pending)
	require.Equal(t, metav1.ConditionFalse, pending.Status)
	require.Equal(t, "WritebackSkipped4xx", pending.Reason)
	require.Equal(t, float64(1), testutil.ToFloat64(r.Metrics.WritebackSkip4xxCounter("403", "other")))
}

// TestWriteback_EmptyImplementDoesNotArmCap verifies that a 422 "No commits"
// (empty implement) is NOT a permanent 4xx skip and must not advance the
// skip-4xx attempt counter: it keeps the plain terminal clear.
func TestWriteback_EmptyImplementDoesNotArmCap(t *testing.T) {
	reg := prometheus.NewRegistry()
	fw := &trackingFakeWriter{openErr: &scm.HTTPError{Status: 422, Body: "No commits between main and tatara/task-x", Path: "/repos/o/r/pulls"}}
	r := newTrackingReconciler(t, fw, reg)
	task := seedWritebackPending(t, "skip4xx-empty", "skip4xxe-scm", "skip4xxe-proj", "skip4xxe-repo")

	for i := 0; i < writebackSkip4xxCap+2; i++ {
		rearmWritebackPending(t, task.Name)
		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)
	}

	got := getTask(t, task.Name)
	require.Equal(t, 0, got.Status.WritebackSkip4xxAttempts,
		"empty-implement (422 no-commits) must never arm the 4xx-skip cap")
	require.Nil(t, findCond(got.Status.Conditions, "WritebackFailed"))
	require.Equal(t, float64(0), testutil.ToFloat64(r.Metrics.WritebackOutcomeCounter("skip_4xx_capped")))
}
