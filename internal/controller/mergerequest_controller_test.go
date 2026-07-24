package controller

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// TestReconcile_PermanentForge404MarksClosedInsteadOfLooping is
// tatara-operator#426: a permanently-deleted upstream MR/issue 404s on every
// forge write and the reconciler used to return that error unchanged, so
// controller-runtime requeued with exponential backoff forever. A 404 is a
// PERMANENT response - it never clears on retry - so the reconciler must
// instead mark the mirror closed and stop, exactly like a real forge close.
func TestReconcile_PermanentForge404MarksClosedInsteadOfLooping(t *testing.T) {
	ctx := context.Background()
	task := mdTask("t1", "implement", tatarav1alpha1.StageMerging)
	mr := mdMR(task, "helmfile", 1311)
	now := metav1.Now()
	mr.Status.Ownership = tatarav1alpha1.OwnershipTatara
	mr.Status.OwnershipReason = "takeover-requested-by:alice"
	mr.Status.OwnershipChangedAt = &now

	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("helmfile"), task, mr)
	f := newFakeForge(t)
	f.commentErr = &scm.HTTPError{Status: 404, Path: "/projects/szymonrychu%2Fhelmfile/issues/1311/notes", Body: `{"message":"404 Not found"}`}

	d := mdNewDriver(t, f, c)
	r := mdMRReconciler(c, d)

	res, err := r.Reconcile(ctx, reqFor(mr))
	if err != nil {
		t.Fatalf("reconcile must NOT propagate a permanent 404 into an error requeue, got: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("want a normal cadence requeue, got %+v", res)
	}

	got := mdGetMR(t, c, mr.Name)
	if got.Status.State != "closed" {
		t.Fatalf("want mirror marked closed on a permanent 404, got state=%q", got.Status.State)
	}
}
