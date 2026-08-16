package controller

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// TestBrainstormChainWakesProjectReconcile runs a REAL manager against the
// envtest control plane with the REAL brainstormChainEdge (the same function
// SetupWithManager registers) and a recording reconciler, and drives the exact
// production sequence of a SKIPPED cycle:
//
//	create the cycle Task      -> must NOT wake (the parent creates these)
//	brainstorming (status)     -> must NOT wake (mid-cycle)
//	delivered (status)         -> MUST wake, immediately
//
// The last edge is a status-subresource write, which is why
// GenerationChangedPredicate is forbidden on this edge - it would drop it - and
// `delivered` is not a terminalStages member, which is why the predicate reads
// TaskDone. Before this change the refill waited up to defaultUnparkDriveInterval
// (30s, the Project reconcile's self-requeue floor) for that third edge, NOT
// brainstormResyncInterval (15m); this edge makes the wake immediate instead.
func TestBrainstormChainWakesProjectReconcile(t *testing.T) {
	mgr := newTestManager(t)
	key := types.NamespacedName{Namespace: testNS, Name: "chain-proj"}
	rec, wakes := recordWakesFor(key)

	// .Named() is REQUIRED: controller-runtime's controller-name registry is
	// process-wide, and project_controller_setup_test.go already registers a
	// controller named "project" in this same test binary. This registers the
	// SAME builder production registers (projectControllerBuilder), not a
	// hand-copied duplicate, so deleting the real edge from SetupWithManager
	// turns this test RED instead of leaving it passing against a stale copy.
	err := projectControllerBuilder(mgr).
		Named("brainstorm-chain-envtest").
		Complete(rec)
	if err != nil {
		t.Fatalf("register the test controller: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan error, 1)
	go func() { started <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-started
	})
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("manager cache did not sync")
	}

	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "chain-scm",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"},
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	if !awaitWake(wakes, key, timeout) {
		t.Fatal("the Project create never reached the reconciler; the manager is not actually running")
	}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "brainstorm-chain-cycle",
			Namespace: testNS,
			Labels:    map[string]string{labelActivity: "brainstorm"},
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj.Name, Kind: "brainstorm", Goal: "propose one issue",
		},
	}
	if err := controllerutil.SetControllerReference(proj, task, scheme.Scheme); err != nil {
		t.Fatalf("set controller reference: %v", err)
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if awaitWake(wakes, key, 2*time.Second) {
		t.Fatal("the Task CREATE woke the Project reconcile; the parent creates these Tasks and would trigger itself")
	}

	task.Status.State = tatarav1alpha1.StateRefined
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("status update to brainstorming: %v", err)
	}
	if awaitWake(wakes, key, 2*time.Second) {
		t.Fatal("a mid-cycle stage write woke the Project reconcile; only a cycle that FINISHES may")
	}

	// THE SKIP: submit_outcome(action="skip") lands the cycle on delivered.
	task.Status.State = tatarav1alpha1.StateDone
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("status update to delivered: %v", err)
	}
	if !awaitWake(wakes, key, timeout) {
		t.Fatalf("a finished brainstorm cycle did not wake the Project reconcile within %s; "+
			"without this edge it would have waited up to %s for the next self-requeue",
			timeout, defaultUnparkDriveInterval)
	}
}
