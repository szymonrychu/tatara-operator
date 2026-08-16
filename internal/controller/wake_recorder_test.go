package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// recordWakesFor returns the recording reconciler an envtest wake test hands to
// the REAL controller builder, plus the channel it records on. It records ONLY
// requests for key; every other request is accepted and discarded.
//
// The filter is on the WRITE side on purpose - see the test below for what
// filtering on read alone costs. awaitWake filters again on read, which is
// redundant and stays that way: a reader that trusts the writer's key is a
// reader that silently passes when the writer's filter is wrong.
func recordWakesFor(key types.NamespacedName) (reconcile.Func, <-chan reconcile.Request) {
	wakes := make(chan reconcile.Request, 32)
	return func(_ context.Context, req reconcile.Request) (reconcile.Result, error) {
		if req.NamespacedName != key {
			return reconcile.Result{}, nil
		}
		// Still non-blocking: a reconciler worker parked on a full channel
		// would never return, and mgr.Start would not shut down.
		select {
		case wakes <- req:
		default:
		}
		return reconcile.Result{}, nil
	}, wakes
}

// A recorder that buffers every request and filters on READ loses its own
// test's wake: this package's tests share one control plane, so the real
// controllerBuilder's initial informer list enqueues every Task and Project an
// earlier test left behind. Measured on the CI head: 204 requests reached the
// recorder and 74 were dropped against a 32-deep buffer. A dropped wake is
// never redelivered - the workqueue considers it delivered - so the test then
// fails with "the manager is not actually running" while the manager is
// running perfectly. That is the TestAdmittedTicketWakesTaskReconcile flake
// (run 31952159425), and it fires whenever the runner is slow enough that the
// buffer fills before the test starts draining.
func TestRecordWakesFor_KeepsItsOwnWakeUnderABurstOfOtherObjects(t *testing.T) {
	key := types.NamespacedName{Namespace: testNS, Name: "mine"}
	rec, wakes := recordWakesFor(key)

	for i := 0; i < 200; i++ {
		other := reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: testNS, Name: fmt.Sprintf("leftover-%d", i),
		}}
		if _, err := rec(context.Background(), other); err != nil {
			t.Fatalf("record leftover %d: %v", i, err)
		}
	}
	if _, err := rec(context.Background(), reconcile.Request{NamespacedName: key}); err != nil {
		t.Fatalf("record the keyed wake: %v", err)
	}

	if !awaitWake(wakes, key, time.Second) {
		t.Fatal("the keyed wake was dropped behind other objects' wakes; the recorder must filter on write, not on read")
	}
}
