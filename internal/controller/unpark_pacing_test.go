package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// Stream H (unpark_declined burst, 2026-07-18, tatara-operator#368): driveUnparks
// had no pacing of its own and re-swept the FULL parked-Task backlog on every
// single Project reconcile, whatever cadence that happened to run at (a stuck
// memory stack forced reconciles far faster than the nominal 10s floor - see
// tatara-operator#367). driveUnparksPaced puts a floor under it, independent of
// Reconcile()'s other drivers, mirroring the memoryUnhealthyCycles per-project-map
// idiom (project_controller.go) rather than the CLUSTER-WIDE lastGaugeRecompute
// scalar (wrong shape here: two live Projects must not throttle each other).
func TestDriveUnparksPaced_SkipsWithinFloor_FoldsResidualIntoRequeue(t *testing.T) {
	task := wfParkedTask("t-paced", "review", stage.ReasonAwaitingHuman)
	c := newMirrorClient(t, task)
	metrics := wfMetrics()
	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: metrics, UnparkDriveInterval: 30 * time.Second}
	proj := wfProject()
	ctx := context.Background()

	t0 := time.Now()
	requeue1, err := r.driveUnparksPaced(ctx, proj, t0)
	if err != nil {
		t.Fatalf("driveUnparksPaced (first pass): %v", err)
	}
	if requeue1 != 30*time.Second {
		t.Fatalf("first pass requeue = %v, want the full 30s floor", requeue1)
	}
	if got := testutil.ToFloat64(metrics.UnparkDeclinedCounter(stage.ReasonAwaitingHuman, "rule")); got != 1 {
		t.Fatalf("first pass must run driveUnparks and count one decline, got %v", got)
	}

	// A second call 5s later, well inside the 30s floor: must be skipped - no
	// second sweep, no second decline counted - and the returned duration must
	// be the RESIDUAL wait (25s), not zero and not the full interval, so
	// Reconcile()'s soonestRequeue never starves the eventual re-check past the
	// floor (a real F.6 re-entry still needs to land within 30s of becoming
	// eligible, even if nothing else forces a reconcile in the meantime).
	t1 := t0.Add(5 * time.Second)
	requeue2, err := r.driveUnparksPaced(ctx, proj, t1)
	if err != nil {
		t.Fatalf("driveUnparksPaced (paced pass): %v", err)
	}
	if requeue2 != 25*time.Second {
		t.Fatalf("paced pass residual requeue = %v, want 25s (30s floor - 5s elapsed)", requeue2)
	}
	if got := testutil.ToFloat64(metrics.UnparkDeclinedCounter(stage.ReasonAwaitingHuman, "rule")); got != 1 {
		t.Fatalf("a paced-out pass must not re-count a decline, got %v", got)
	}

	// A third call once the floor has fully elapsed must run again.
	t2 := t0.Add(31 * time.Second)
	requeue3, err := r.driveUnparksPaced(ctx, proj, t2)
	if err != nil {
		t.Fatalf("driveUnparksPaced (post-floor pass): %v", err)
	}
	if requeue3 != 30*time.Second {
		t.Fatalf("post-floor pass requeue = %v, want the full 30s floor again", requeue3)
	}
	if got := testutil.ToFloat64(metrics.UnparkDeclinedCounter(stage.ReasonAwaitingHuman, "rule")); got != 2 {
		t.Fatalf("post-floor pass must run driveUnparks again and count a second decline, got %v", got)
	}
}

// Two live Projects must not throttle each other: driveUnparksPaced is keyed
// per-project (like memoryUnhealthyCycles), not a single cluster-wide clock
// (unlike lastGaugeRecompute, which is correctly scalar because gauge recompute
// really is cluster-wide). Project B's first pass must run in full even though
// Project A was just paced out.
func TestDriveUnparksPaced_PerProjectFloor_DoesNotCrossThrottle(t *testing.T) {
	taskA := wfParkedTask("t-paced-a", "review", stage.ReasonAwaitingHuman)
	taskB := wfParkedTask("t-paced-b", "review", stage.ReasonAwaitingHuman)
	taskB.Spec.ProjectRef = "proj-b"
	c := newMirrorClient(t, taskA, taskB)
	metrics := wfMetrics()
	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: metrics, UnparkDriveInterval: 30 * time.Second}
	projA := wfProject()
	projB := wfProject()
	projB.Name = "proj-b"
	ctx := context.Background()
	t0 := time.Now()

	if _, err := r.driveUnparksPaced(ctx, projA, t0); err != nil {
		t.Fatalf("driveUnparksPaced (project A): %v", err)
	}
	// Project B, 1s later, well inside project A's floor: must still run its
	// own first pass in full.
	requeueB, err := r.driveUnparksPaced(ctx, projB, t0.Add(time.Second))
	if err != nil {
		t.Fatalf("driveUnparksPaced (project B): %v", err)
	}
	if requeueB != 30*time.Second {
		t.Fatalf("project B's first pass requeue = %v, want the full 30s floor (must not inherit project A's clock)", requeueB)
	}
	if got := testutil.ToFloat64(metrics.UnparkDeclinedCounter(stage.ReasonAwaitingHuman, "rule")); got != 2 {
		t.Fatalf("both projects' first passes must each count one decline, got %v", got)
	}
}
