package controller

import (
	"context"
	"testing"
	"time"
)

// Stream #367 (ReapTerminal half): ReapTerminal does a full namespace Task List
// EVERY Reconcile pass. ReapTerminalPaced puts a floor under it on the CALLER
// side (Reconcile()'s call site), mirroring driveUnparksPaced (unpark.go):
// calling it twice inside the floor runs the underlying reap once and returns
// the residual wait; a third call past the floor runs it again. ReapTerminal
// itself (and reapOne/needsDocumenting, WP6) is untouched.
func TestReapTerminalPaced_SkipsWithinFloor_FoldsResidualIntoRequeue(t *testing.T) {
	base := newMirrorClient(t)
	cc := &listCountingClient{Client: base}
	r := &ProjectReconciler{Client: cc, Scheme: base.Scheme()}
	proj := wfProject()
	ctx := context.Background()

	t0 := time.Now()
	requeue1, err := r.ReapTerminalPaced(ctx, proj, t0)
	if err != nil {
		t.Fatalf("ReapTerminalPaced (first pass): %v", err)
	}
	if requeue1 != defaultReapTerminalInterval {
		t.Fatalf("first pass requeue = %v, want the full %v floor", requeue1, defaultReapTerminalInterval)
	}
	if got := cc.ListCount(); got != 1 {
		t.Fatalf("first pass must run ReapTerminal (one Task List), got %d list calls", got)
	}

	// A second call well inside the floor: no second sweep, residual wait returned.
	t1 := t0.Add(5 * time.Second)
	requeue2, err := r.ReapTerminalPaced(ctx, proj, t1)
	if err != nil {
		t.Fatalf("ReapTerminalPaced (paced pass): %v", err)
	}
	want2 := defaultReapTerminalInterval - 5*time.Second
	if requeue2 != want2 {
		t.Fatalf("paced pass residual requeue = %v, want %v", requeue2, want2)
	}
	if got := cc.ListCount(); got != 1 {
		t.Fatalf("a paced-out pass must not re-list, got %d list calls (want 1)", got)
	}

	// A third call once the floor has fully elapsed must run again.
	t2 := t0.Add(defaultReapTerminalInterval + time.Second)
	requeue3, err := r.ReapTerminalPaced(ctx, proj, t2)
	if err != nil {
		t.Fatalf("ReapTerminalPaced (post-floor pass): %v", err)
	}
	if requeue3 != defaultReapTerminalInterval {
		t.Fatalf("post-floor pass requeue = %v, want the full %v floor again", requeue3, defaultReapTerminalInterval)
	}
	if got := cc.ListCount(); got != 2 {
		t.Fatalf("post-floor pass must re-list, got %d list calls (want 2)", got)
	}
}

// Two live Projects must not throttle each other: ReapTerminalPaced is keyed
// per-project (like lastDriveUnparks), not a single cluster-wide clock.
func TestReapTerminalPaced_PerProjectFloor_DoesNotCrossThrottle(t *testing.T) {
	base := newMirrorClient(t)
	cc := &listCountingClient{Client: base}
	r := &ProjectReconciler{Client: cc, Scheme: base.Scheme()}
	projA := wfProject()
	projB := wfProject()
	projB.Name = "proj-b"
	ctx := context.Background()
	t0 := time.Now()

	if _, err := r.ReapTerminalPaced(ctx, projA, t0); err != nil {
		t.Fatalf("ReapTerminalPaced (project A): %v", err)
	}
	requeueB, err := r.ReapTerminalPaced(ctx, projB, t0.Add(time.Second))
	if err != nil {
		t.Fatalf("ReapTerminalPaced (project B): %v", err)
	}
	if requeueB != defaultReapTerminalInterval {
		t.Fatalf("project B's first pass requeue = %v, want the full floor (must not inherit project A's clock)", requeueB)
	}
	if got := cc.ListCount(); got != 2 {
		t.Fatalf("both projects' first passes must each List Task once, got %d list calls", got)
	}
}

// A rapid trigger loop must not turn into an unbounded number of full-namespace
// Lists: 150 calls spanning 149 synthetic seconds cross the 60s floor exactly
// twice, so exactly 3 runs (3 List calls), not 150.
func TestReapTerminalPaced_ListCallsBoundedUnderRapidTriggerLoop(t *testing.T) {
	base := newMirrorClient(t)
	cc := &listCountingClient{Client: base}
	r := &ProjectReconciler{Client: cc, Scheme: base.Scheme()}
	proj := wfProject()
	ctx := context.Background()
	t0 := time.Now()

	for i := 0; i < 150; i++ {
		if _, err := r.ReapTerminalPaced(ctx, proj, t0.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("ReapTerminalPaced (iteration %d): %v", i, err)
		}
	}
	if got := cc.ListCount(); got != 3 {
		t.Fatalf("rapid trigger loop must bound List calls to 3 runs, got %d", got)
	}
}
