package controller

// Envtest coverage for the O6 event-driven refill wiring: the Watches(&Issue{})
// edge fires through ProjectReconciler.Reconcile itself (not just brainstorm(),
// which O4 already covers directly), so these drive the real Reconcile entry
// point end to end against the shared package envtest harness
// (newScanReconciler / seedBrainstormProject / seedProposalIssue /
// listBrainstormQEs, all from projectscan_run_harness_test.go and
// projectscan_brainstorm_target_test.go - reused rather than duplicated per the
// task brief).

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

// reconcileEventProject drives one real Reconcile pass for project name and
// fails the test on error. It asserts nothing about the result beyond "no
// error" - callers inspect QueuedEvents/RequeueAfter themselves.
func reconcileEventProject(t *testing.T, r *ProjectReconciler, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: name},
	})
	if err != nil {
		t.Fatalf("reconcile %s: %v", name, err)
	}
	return res
}

// TestEventRefillOnApproval is the headline case the whole task exists for:
// a maintainer approving a proposal must launch the next brainstorm session on
// the VERY NEXT reconcile, not wait for the brainstorm cron tick.
func TestEventRefillOnApproval(t *testing.T) {
	proj, _ := seedBrainstormProject(t, "ev-approve", []string{"o/r1"}, ptrInt(1))
	r := newScanReconciler(emptyReader("o/r1"))
	iss := seedProposalIssue(t, r, proj, "ev-approve-o-r1", 1, "brainstorm", "open", "new")

	reconcileEventProject(t, r, proj.Name)
	if got := len(listBrainstormQEs(t, proj.Name)); got != 0 {
		t.Fatalf("a full backlog (target=1, pending=1) must not refill, got %d events", got)
	}

	iss.Status.Status = "approved"
	if err := r.Status().Update(context.Background(), iss); err != nil {
		t.Fatalf("approve issue: %v", err)
	}
	reconcileEventProject(t, r, proj.Name)
	if got := len(listBrainstormQEs(t, proj.Name)); got != 1 {
		t.Fatalf("approval must free the slot and launch exactly one session, got %d events", got)
	}
}

// TestEventRefillOnDiscard mirrors the approval case for the other verdict a
// maintainer can render: closing the issue without approving it.
func TestEventRefillOnDiscard(t *testing.T) {
	proj, _ := seedBrainstormProject(t, "ev-discard", []string{"o/r1"}, ptrInt(1))
	r := newScanReconciler(emptyReader("o/r1"))
	iss := seedProposalIssue(t, r, proj, "ev-discard-o-r1", 1, "brainstorm", "open", "new")

	reconcileEventProject(t, r, proj.Name)
	if got := len(listBrainstormQEs(t, proj.Name)); got != 0 {
		t.Fatalf("a full backlog (target=1, pending=1) must not refill, got %d events", got)
	}

	iss.Status.State = "closed"
	if err := r.Status().Update(context.Background(), iss); err != nil {
		t.Fatalf("close issue: %v", err)
	}
	reconcileEventProject(t, r, proj.Name)
	if got := len(listBrainstormQEs(t, proj.Name)); got != 1 {
		t.Fatalf("a discard must free the slot and launch exactly one session, got %d events", got)
	}
}

// TestEventRefillNeverDoubleSpawns is the read-your-writes guarantee: nothing
// about wiring the event trigger onto every reconcile may turn a reconcile
// storm into a proposal-Task storm. Two back-to-back reconciles against the
// identical backlog state must still produce exactly one brainstorm
// QueuedEvent - guarded independently by the in-flight Task check inside
// brainstorm() AND by the dedup key EnqueueEvent applies per project.
func TestEventRefillNeverDoubleSpawns(t *testing.T) {
	proj, _ := seedBrainstormProject(t, "ev-once", []string{"o/r1"}, ptrInt(3))
	r := newScanReconciler(emptyReader("o/r1"))

	reconcileEventProject(t, r, proj.Name)
	reconcileEventProject(t, r, proj.Name)

	if got := len(listBrainstormQEs(t, proj.Name)); got != 1 {
		t.Fatalf("two rapid reconciles must produce exactly one brainstorm session, got %d", got)
	}
}

// TestReconcileRequeuesAtTheResyncInterval asserts the periodic resync is a
// bounded RequeueAfter poll interval, not the workqueue's error-driven
// backoff: a brainstorm-enabled project must always come back within
// brainstormResyncInterval even if no Issue write ever lands to wake it via
// the watch.
func TestReconcileRequeuesAtTheResyncInterval(t *testing.T) {
	proj, _ := seedBrainstormProject(t, "ev-resync", []string{"o/r1"}, ptrInt(3))
	r := newScanReconciler(emptyReader("o/r1"))

	res := reconcileEventProject(t, r, proj.Name)
	if res.RequeueAfter <= 0 {
		t.Fatalf("a brainstorm-enabled project must resync, got RequeueAfter=%v", res.RequeueAfter)
	}
	if res.RequeueAfter > brainstormResyncInterval {
		t.Fatalf("RequeueAfter=%v exceeds brainstormResyncInterval=%v", res.RequeueAfter, brainstormResyncInterval)
	}
}
