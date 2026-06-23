// Issue #107: terminate must treat an already-gone wrapper pod as a clean
// session teardown, not a SessionDeleteFailed condition. By the time terminate
// runs, the wrapper pod is frequently already gone (reaped, evicted, or the
// Service has no endpoints), so the HTTP DELETE fails at the transport layer
// with an *agent.UnreachableError. That is the desired terminal state for a
// teardown, so it must NOT be recorded as a failure. Only a reachable-but-
// refused wrapper (*agent.HTTPError) or a timeout, where the session may
// genuinely still be alive, gets a SessionDeleteFailed condition.
package controller

import (
	"bytes"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
)

func mkTerminateReconciler(sess agent.Session) *TaskReconciler {
	return &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: sess,
		PodConfig: agent.PodConfig{
			Namespace:   testNS,
			CallbackURL: "http://op-internal.tatara.svc:8082",
		},
	}
}

// TestTerminate_UnreachableSessionIsCleanTeardown verifies that an
// *agent.UnreachableError from DeleteSession (the wrapper pod is already gone)
// is treated as a clean teardown: no SessionDeleteFailed condition, and an INFO
// "session already gone" log instead of an error.
func TestTerminate_UnreachableSessionIsCleanTeardown(t *testing.T) {
	var buf bytes.Buffer
	ctx := captureLogger(&buf)

	mkTaskProject(t, "p-term107u", 3)
	mkTaskRepository(t, "r-term107u", "p-term107u")
	mkTask(t, "t-term107u", "p-term107u", "r-term107u")
	setProjectMemoryReady(t, "p-term107u", "http://mem.svc:8080")
	setTaskPhase(t, "t-term107u", "Running")

	sess := &errSession{
		fakeSession: newFakeSession(),
		deleteErr:   &agent.UnreachableError{Err: errors.New("dial tcp: connect: connection refused")},
	}
	r := mkTerminateReconciler(sess)

	task := getTask(t, "t-term107u")
	if _, err := r.terminate(ctx, task, "Succeeded", "NoPendingSubtasks", "all done"); err != nil {
		t.Fatalf("terminate must succeed when the wrapper pod is already gone: %v", err)
	}

	got := getTask(t, "t-term107u")
	if got.Status.Phase != "Succeeded" {
		t.Errorf("phase = %q, want Succeeded", got.Status.Phase)
	}
	if cond := apimeta.FindStatusCondition(got.Status.Conditions, "SessionDeleteFailed"); cond != nil {
		t.Errorf("an already-gone wrapper pod must NOT stamp SessionDeleteFailed; got condition: %+v", cond)
	}

	output := buf.String()
	if !bytes.Contains([]byte(output), []byte("session already gone")) {
		t.Errorf("terminate must emit INFO log 'session already gone' for an unreachable wrapper; log output:\n%s", output)
	}
}

// TestTerminate_HTTPErrorStampsSessionDeleteFailed verifies that a reachable
// wrapper that refuses the delete (*agent.HTTPError) - where the session may
// genuinely still be alive - still records the SessionDeleteFailed condition.
func TestTerminate_HTTPErrorStampsSessionDeleteFailed(t *testing.T) {
	var buf bytes.Buffer
	ctx := captureLogger(&buf)

	mkTaskProject(t, "p-term107h", 3)
	mkTaskRepository(t, "r-term107h", "p-term107h")
	mkTask(t, "t-term107h", "p-term107h", "r-term107h")
	setProjectMemoryReady(t, "p-term107h", "http://mem.svc:8080")
	setTaskPhase(t, "t-term107h", "Running")

	sess := &errSession{
		fakeSession: newFakeSession(),
		deleteErr:   &agent.HTTPError{Status: 500, Body: "boom"},
	}
	r := mkTerminateReconciler(sess)

	task := getTask(t, "t-term107h")
	if _, err := r.terminate(ctx, task, "Failed", "PodLost", "pod lost"); err != nil {
		t.Fatalf("terminate must still succeed on a non-fatal DeleteSession error: %v", err)
	}

	got := getTask(t, "t-term107h")
	if got.Status.Phase != "Failed" {
		t.Errorf("phase = %q, want Failed", got.Status.Phase)
	}
	cond := apimeta.FindStatusCondition(got.Status.Conditions, "SessionDeleteFailed")
	if cond == nil {
		t.Fatalf("a reachable wrapper that refuses the delete must stamp SessionDeleteFailed")
	}
	if cond.Reason != "DeleteError" {
		t.Errorf("condition reason = %q, want DeleteError", cond.Reason)
	}

	output := buf.String()
	if !bytes.Contains([]byte(output), []byte("delete session")) {
		t.Errorf("terminate must emit error log 'delete session' on a genuine delete failure; log output:\n%s", output)
	}
}
