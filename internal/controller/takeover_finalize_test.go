package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// takenOverMR builds the exact post-handover mirror state: an MR whose CONTROLLER
// owner is the takeover Task and whose demoted-to-plain owner is the parent review
// Task, exactly what own.HandOverController leaves behind on a maintainer takeover.
func takenOverMR(review, takeover *tatarav1alpha1.Task, repo string, number int) *tatarav1alpha1.MergeRequest {
	mr := mdMR(review, repo, number) // controller = review
	own.AddPlainOwner(mr, takeover)
	if err := own.HandOverController(mr, review, takeover); err != nil {
		panic(err)
	}
	mr.Status.Ownership = tatarav1alpha1.OwnershipTatara
	mr.Status.OwnershipReason = "takeover-requested-by:maintainer"
	return mr
}

// TestTaskTakenOver is the pure-ish predicate test: it distinguishes a completed
// maintainer takeover from every seam that must NOT trip it (the reMintReviewOwner
// no-controller window, a still-owned MR, a dead controller, a non-review kind).
func TestTaskTakenOver(t *testing.T) {
	ctx := context.Background()
	review := mdTask("rev-7", "review", tatarav1alpha1.StageReviewing)
	takeover := mdTask("takeover-7", "takeover", tatarav1alpha1.StageImplementing)
	review.Status.MRRefs = []string{tatarav1alpha1.MergeRequestName("tatara-operator", 7)}

	t.Run("taken over by a different live task", func(t *testing.T) {
		mr := takenOverMR(review, takeover, "tatara-operator", 7)
		c := newMirrorClient(t, mdProject(), mdSecret(), review, takeover, mr)
		over, err := TaskTakenOver(ctx, c, review)
		if err != nil || !over {
			t.Fatalf("expected taken-over=true, got %v err=%v", over, err)
		}
	})

	t.Run("still controller-owned by this task", func(t *testing.T) {
		mr := mdMR(review, "tatara-operator", 7) // controller still = review
		c := newMirrorClient(t, mdProject(), mdSecret(), review, mr)
		over, err := TaskTakenOver(ctx, c, review)
		if err != nil || over {
			t.Fatalf("a still-owned MR is not a takeover; got %v err=%v", over, err)
		}
	})

	t.Run("no-controller window does not misfire", func(t *testing.T) {
		// The mid-handover / RepairZeroController seam: the MR has plain owners but
		// no controller. own.ControllerOwner returns ok=false, so the predicate must
		// refuse rather than finalize.
		mr := mdMR(review, "tatara-operator", 7)
		mr.OwnerReferences = nil
		own.AddPlainOwner(mr, review)
		own.AddPlainOwner(mr, takeover)
		// Guard against a vacuous pass: TaskTakenOver Gets the MR at
		// {task.Namespace, ref}; a namespace mismatch would read as NotFound and
		// return false for the WRONG reason. Prove the fixture actually resolves.
		if mr.Namespace != mdNS || review.Namespace != mdNS {
			t.Fatalf("fixture namespaces diverged: mr=%q task=%q want %q", mr.Namespace, review.Namespace, mdNS)
		}
		c := newMirrorClient(t, mdProject(), mdSecret(), review, takeover, mr)
		var resolved tatarav1alpha1.MergeRequest
		if err := c.Get(ctx, types.NamespacedName{Namespace: review.Namespace, Name: review.Status.MRRefs[0]}, &resolved); err != nil {
			t.Fatalf("the ref'd MR must resolve at the task's namespace, or the subtest passes vacuously: %v", err)
		}
		over, err := TaskTakenOver(ctx, c, review)
		if err != nil || over {
			t.Fatalf("a no-controller MR must not read as taken over; got %v err=%v", over, err)
		}
	})

	t.Run("a missing ref'd MR CR refuses the predicate", func(t *testing.T) {
		// refs [gone, takenOver]: the dangling ref is NOT proof of takeover -
		// external deletion of an MR CR must never finalize the parent, even when
		// another ref IS genuinely taken over.
		mixed := mdTask("rev-mixed", "review", tatarav1alpha1.StageReviewing)
		mixed.Status.MRRefs = []string{
			tatarav1alpha1.MergeRequestName("tatara-operator", 41), // CR deleted externally
			tatarav1alpha1.MergeRequestName("tatara-operator", 42),
		}
		mr := takenOverMR(mixed, takeover, "tatara-operator", 42)
		c := newMirrorClient(t, mdProject(), mdSecret(), mixed, takeover, mr)
		over, err := TaskTakenOver(ctx, c, mixed)
		if err != nil || over {
			t.Fatalf("a missing ref'd MR must refuse the predicate; got %v err=%v", over, err)
		}
	})

	t.Run("dead controller does not misfire", func(t *testing.T) {
		// The controller ref names a Task that no longer exists (a reap dropped the
		// flag, RepairZeroController not yet run): let convergence re-own it.
		mr := takenOverMR(review, takeover, "tatara-operator", 7)
		c := newMirrorClient(t, mdProject(), mdSecret(), review, mr) // takeover NOT seeded
		over, err := TaskTakenOver(ctx, c, review)
		if err != nil || over {
			t.Fatalf("a dead controller must not read as taken over; got %v err=%v", over, err)
		}
	})

	t.Run("non-review kind is never taken over", func(t *testing.T) {
		impl := mdTask("impl-7", "implement", tatarav1alpha1.StageReviewing)
		impl.Status.MRRefs = review.Status.MRRefs
		mr := takenOverMR(impl, takeover, "tatara-operator", 7)
		c := newMirrorClient(t, mdProject(), mdSecret(), impl, takeover, mr)
		over, err := TaskTakenOver(ctx, c, impl)
		if err != nil || over {
			t.Fatalf("only kind=review is finalized on takeover; got %v err=%v", over, err)
		}
	})

	t.Run("no mrRefs is never taken over", func(t *testing.T) {
		bare := mdTask("rev-bare", "review", tatarav1alpha1.StageReviewing)
		c := newMirrorClient(t, mdProject(), mdSecret(), bare)
		over, err := TaskTakenOver(ctx, c, bare)
		if err != nil || over {
			t.Fatalf("a review task with no mrRefs is not taken over; got %v err=%v", over, err)
		}
	})
}

// TestReconcileClocks_FinalizesTakenOverReviewParent reproduces the orphan: a
// kind=review Task sitting in reviewing whose MR was handed to a takeover Task
// controller-owns zero MRs. reconcileClocks (the unconditional #33-shape finalize)
// must retire it at rejected(mr-taken-over) instead of leaving it in reviewing.
func TestReconcileClocks_FinalizesTakenOverReviewParent(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := tsProject(3)
	review := tsTask("rev-7", "review", tatarav1alpha1.StageReviewing, now.Add(-time.Minute))
	takeover := mdTask("takeover-7", "takeover", tatarav1alpha1.StageImplementing)
	review.Status.MRRefs = []string{tatarav1alpha1.MergeRequestName("tatara-operator", 7)}
	mr := takenOverMR(review, takeover, "tatara-operator", 7)

	c := newMirrorClient(t, proj, mdSecret(), review, takeover, mr)
	r := tsReconciler(c)

	_, handled, err := r.reconcileClocks(ctx, proj, review, now)
	if err != nil {
		t.Fatalf("reconcileClocks: %v", err)
	}
	if !handled {
		t.Fatal("reconcileClocks must handle (finalize) the taken-over review parent")
	}
	got := mdGetTask(t, c, "rev-7")
	if got.Status.Stage != tatarav1alpha1.StageRejected || got.Status.StageReason != stage.ReasonMRTakenOver {
		t.Fatalf("stage/reason = %q/%q, want rejected/mr-taken-over", got.Status.Stage, got.Status.StageReason)
	}
}

// TestEnsureStagePod_FinalizesTakenOverReviewParent proves the pre-dispatch guard:
// a taken-over parent must be finalized pod-lessly, spawning NO review pod (which
// could only 400 on submit_outcome and respawn-loop).
func TestEnsureStagePod_FinalizesTakenOverReviewParent(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	review := tsTask("rev-8", "review", tatarav1alpha1.StageReviewing, time.Now())
	takeover := mdTask("takeover-8", "takeover", tatarav1alpha1.StageImplementing)
	review.Status.MRRefs = []string{tatarav1alpha1.MergeRequestName("tatara-operator", 8)}
	mr := takenOverMR(review, takeover, "tatara-operator", 8)

	c := newMirrorClient(t, proj, mdSecret(), review, takeover, mr)
	r := tsReconciler(c)

	skipped, err := r.ensureStagePod(ctx, proj, review)
	if err != nil {
		t.Fatalf("ensureStagePod: %v", err)
	}
	if !skipped {
		t.Fatal("ensureStagePod must report skipped=true after a pod-less finalize")
	}
	var pod corev1.Pod
	err = c.Get(ctx, types.NamespacedName{Namespace: mdNS, Name: agent.PodName(review)}, &pod)
	if err == nil {
		t.Fatal("a review pod was spawned for a taken-over parent; the pre-dispatch guard did not fire")
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected error checking for pod: %v", err)
	}
	got := mdGetTask(t, c, "rev-8")
	if got.Status.Stage != tatarav1alpha1.StageRejected || got.Status.StageReason != stage.ReasonMRTakenOver {
		t.Fatalf("stage/reason = %q/%q, want rejected/mr-taken-over", got.Status.Stage, got.Status.StageReason)
	}
}

// TestTaskTakenOver_DoesNotMisfireOnFreshReviewOwner is the crossing test against
// the reMintReviewOwner seam (#408): after a stand-down re-mint, the FRESH review
// Task is minted straight to CONTROLLER of the MR (own.HandOverController hands the
// flag over atomically, no zero-controller window). Such a task controller-OWNS the
// MR, so both ownedMergeRequests (non-empty) and TaskTakenOver (false) leave it
// alone - it must keep its reviewing lifecycle, never get finalized.
func TestTaskTakenOver_DoesNotMisfireOnFreshReviewOwner(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := tsProject(3)
	fresh := tsTask("rev-remint", "review", tatarav1alpha1.StageReviewing, now.Add(-time.Minute))
	fresh.Status.MRRefs = []string{tatarav1alpha1.MergeRequestName("tatara-operator", 9)}
	mr := mdMR(fresh, "tatara-operator", 9) // fresh IS the controller owner

	c := newMirrorClient(t, proj, mdSecret(), fresh, mr)
	r := tsReconciler(c)

	// The predicate sees it owns the MR: not taken over.
	over, err := TaskTakenOver(ctx, c, fresh)
	if err != nil || over {
		t.Fatalf("a fresh review owner must not read as taken over; got %v err=%v", over, err)
	}
	// And a reconcile leaves it in reviewing (owned, non-terminal MR -> no finalize).
	_, _, err = r.reconcileClocks(ctx, proj, fresh, now)
	if err != nil {
		t.Fatalf("reconcileClocks: %v", err)
	}
	if got := mdGetTask(t, c, "rev-remint"); got.Status.Stage != tatarav1alpha1.StageReviewing {
		t.Fatalf("fresh review owner was finalized to %q; it must stay reviewing", got.Status.Stage)
	}
}
