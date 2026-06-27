// Issue #169: operator_turn_submit_total{result="error"} must count only genuine
// dispatch loss, not the two self-healing transients the operator already retries
// successfully - a wrapper boot-race (*agent.UnreachableError) and 409 session-busy
// back-pressure (*agent.HTTPError{Status:409}). Those carry distinct result labels
// ("unreachable", "busy") so the turn-submit-failure-ratio alert (keyed on
// result="error") no longer trips on retried-and-succeeded submits. A 409 also
// requeues after pollRequeue instead of returning a hard reconcile error.
package controller

import (
	"errors"
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/agent"
)

func TestTaskReconcile_SubmitSessionBusy_RequeuesAsBusy(t *testing.T) {
	mkTaskProject(t, "p-busy", 3)
	mkTaskRepository(t, "r-busy", "p-busy")
	mkTask(t, "t-busy", "p-busy", "r-busy")
	setProjectMemoryReady(t, "p-busy", "http://mem-p-busy.tatara.svc:8080")

	fs := newFakeSession()
	r, reg := newTaskReconcilerReg(fs)
	if _, err := reconcileTask(t, r, "t-busy"); err != nil {
		t.Fatalf("reconcile spawn: %v", err)
	}
	markPodReady(t, "wrapper-t-busy")
	// A turn is already in flight on the wrapper session: deliberate back-pressure.
	fs.submitErr = &agent.HTTPError{Status: 409, Body: "session busy"}

	res, err := reconcileTask(t, r, "t-busy")
	if err != nil {
		t.Fatalf("409 session-busy must not error the reconcile (back-pressure, not loss): %v", err)
	}
	if res.RequeueAfter != pollRequeue {
		t.Errorf("RequeueAfter = %v, want %v (requeue and retry, like a turn in flight)", res.RequeueAfter, pollRequeue)
	}
	if _, ok := fs.lastSubmit(); ok {
		t.Error("no turn should be recorded when the submit is rejected as busy")
	}
	if tk := getTask(t, "t-busy"); tk.Annotations[annCurrentTurn] != "" {
		t.Errorf("current-turn annotation should be empty, got %q", tk.Annotations[annCurrentTurn])
	}
	if got := counterValue(t, reg, "operator_turn_submit_total", map[string]string{"result": "busy"}); got != 1 {
		t.Errorf("operator_turn_submit_total{result=busy} = %v, want 1", got)
	}
	if got := counterValue(t, reg, "operator_turn_submit_total", map[string]string{"result": "error"}); got != 0 {
		t.Errorf("operator_turn_submit_total{result=error} = %v, want 0 (a 409 is not a dispatch error)", got)
	}
}

func TestTaskReconcile_SubmitBootRace_LabeledUnreachable(t *testing.T) {
	mkTaskProject(t, "p-unrlbl", 3)
	mkTaskRepository(t, "r-unrlbl", "p-unrlbl")
	mkTask(t, "t-unrlbl", "p-unrlbl", "r-unrlbl")
	setProjectMemoryReady(t, "p-unrlbl", "http://mem-p-unrlbl.tatara.svc:8080")

	fs := newFakeSession()
	r, reg := newTaskReconcilerReg(fs)
	if _, err := reconcileTask(t, r, "t-unrlbl"); err != nil {
		t.Fatalf("reconcile spawn: %v", err)
	}
	markPodReady(t, "wrapper-t-unrlbl")
	fs.submitErr = &agent.UnreachableError{Err: errors.New("dial tcp: connect: connection refused")}

	if _, err := reconcileTask(t, r, "t-unrlbl"); err != nil {
		t.Fatalf("boot-race must not error the reconcile: %v", err)
	}
	if got := counterValue(t, reg, "operator_turn_submit_total", map[string]string{"result": "unreachable"}); got != 1 {
		t.Errorf("operator_turn_submit_total{result=unreachable} = %v, want 1", got)
	}
	if got := counterValue(t, reg, "operator_turn_submit_total", map[string]string{"result": "error"}); got != 0 {
		t.Errorf("operator_turn_submit_total{result=error} = %v, want 0 (a boot-race is not a dispatch error)", got)
	}
}

func TestTaskReconcile_SubmitGenuineError_LabeledError(t *testing.T) {
	mkTaskProject(t, "p-err", 3)
	mkTaskRepository(t, "r-err", "p-err")
	mkTask(t, "t-err", "p-err", "r-err")
	setProjectMemoryReady(t, "p-err", "http://mem-p-err.tatara.svc:8080")

	fs := newFakeSession()
	r, reg := newTaskReconcilerReg(fs)
	if _, err := reconcileTask(t, r, "t-err"); err != nil {
		t.Fatalf("reconcile spawn: %v", err)
	}
	markPodReady(t, "wrapper-t-err")
	// A real 5xx rejection: genuine dispatch failure, must keep erroring.
	fs.submitErr = &agent.HTTPError{Status: 500, Body: "boom"}

	if _, err := reconcileTask(t, r, "t-err"); err == nil {
		t.Fatal("a real HTTP error from submit must still error the reconcile")
	}
	if got := counterValue(t, reg, "operator_turn_submit_total", map[string]string{"result": "error"}); got != 1 {
		t.Errorf("operator_turn_submit_total{result=error} = %v, want 1", got)
	}
	if got := counterValue(t, reg, "operator_turn_submit_total", map[string]string{"result": "busy"}); got != 0 {
		t.Errorf("operator_turn_submit_total{result=busy} = %v, want 0 (a 500 is not back-pressure)", got)
	}
}
