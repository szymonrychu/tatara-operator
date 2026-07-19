package controller

import (
	"context"
	"testing"
	"time"

	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// wfParkedTask with reason stage.ReasonStageDeadline (stage.HasReentry is false
// for it) and no PendingEvents makes resumeNoReentryParks take the fast
// "nothing to resume" path after its one Task List - exercising the pacing
// wrapper without needing the full sever+re-mint plumbing (covered elsewhere).

// Stream #367 (resumeNoReentryParks half): resumeNoReentryParks does a full
// namespace Task List EVERY Reconcile pass. resumeNoReentryParksPaced puts a
// floor under it, mirroring driveUnparksPaced (unpark.go): calling it twice
// inside the floor runs the underlying block once and returns the residual
// wait; a third call past the floor runs it again.
func TestResumeNoReentryParksPaced_SkipsWithinFloor_FoldsResidualIntoRequeue(t *testing.T) {
	task := wfParkedTask("t-resume", "clarify", stage.ReasonStageDeadline)
	base := newMirrorClient(t, task)
	cc := &listCountingClient{Client: base}
	r := &ProjectReconciler{Client: cc, Scheme: base.Scheme()}
	proj := wfProject()
	ctx := context.Background()

	t0 := time.Now()
	requeue1, err := r.resumeNoReentryParksPaced(ctx, proj, t0)
	if err != nil {
		t.Fatalf("resumeNoReentryParksPaced (first pass): %v", err)
	}
	if requeue1 != defaultResumeNoReentryInterval {
		t.Fatalf("first pass requeue = %v, want the full %v floor", requeue1, defaultResumeNoReentryInterval)
	}
	if got := cc.ListCount(); got != 1 {
		t.Fatalf("first pass must run resumeNoReentryParks (one Task List), got %d list calls", got)
	}

	// A second call well inside the floor: no second sweep, residual wait returned.
	t1 := t0.Add(5 * time.Second)
	requeue2, err := r.resumeNoReentryParksPaced(ctx, proj, t1)
	if err != nil {
		t.Fatalf("resumeNoReentryParksPaced (paced pass): %v", err)
	}
	want2 := defaultResumeNoReentryInterval - 5*time.Second
	if requeue2 != want2 {
		t.Fatalf("paced pass residual requeue = %v, want %v", requeue2, want2)
	}
	if got := cc.ListCount(); got != 1 {
		t.Fatalf("a paced-out pass must not re-list, got %d list calls (want 1)", got)
	}

	// A third call once the floor has fully elapsed must run again.
	t2 := t0.Add(defaultResumeNoReentryInterval + time.Second)
	requeue3, err := r.resumeNoReentryParksPaced(ctx, proj, t2)
	if err != nil {
		t.Fatalf("resumeNoReentryParksPaced (post-floor pass): %v", err)
	}
	if requeue3 != defaultResumeNoReentryInterval {
		t.Fatalf("post-floor pass requeue = %v, want the full %v floor again", requeue3, defaultResumeNoReentryInterval)
	}
	if got := cc.ListCount(); got != 2 {
		t.Fatalf("post-floor pass must re-list, got %d list calls (want 2)", got)
	}
}

// Two live Projects must not throttle each other: resumeNoReentryParksPaced is
// keyed per-project (like lastDriveUnparks), not a single cluster-wide clock.
func TestResumeNoReentryParksPaced_PerProjectFloor_DoesNotCrossThrottle(t *testing.T) {
	taskA := wfParkedTask("t-resume-a", "clarify", stage.ReasonStageDeadline)
	taskB := wfParkedTask("t-resume-b", "clarify", stage.ReasonStageDeadline)
	taskB.Spec.ProjectRef = "proj-b"
	base := newMirrorClient(t, taskA, taskB)
	cc := &listCountingClient{Client: base}
	r := &ProjectReconciler{Client: cc, Scheme: base.Scheme()}
	projA := wfProject()
	projB := wfProject()
	projB.Name = "proj-b"
	ctx := context.Background()
	t0 := time.Now()

	if _, err := r.resumeNoReentryParksPaced(ctx, projA, t0); err != nil {
		t.Fatalf("resumeNoReentryParksPaced (project A): %v", err)
	}
	requeueB, err := r.resumeNoReentryParksPaced(ctx, projB, t0.Add(time.Second))
	if err != nil {
		t.Fatalf("resumeNoReentryParksPaced (project B): %v", err)
	}
	if requeueB != defaultResumeNoReentryInterval {
		t.Fatalf("project B's first pass requeue = %v, want the full floor (must not inherit project A's clock)", requeueB)
	}
	if got := cc.ListCount(); got != 2 {
		t.Fatalf("both projects' first passes must each List Task once, got %d list calls", got)
	}
}

// A rapid trigger loop must not turn into an unbounded number of full-namespace
// Lists: 150 calls spanning 149 synthetic seconds cross the 60s floor exactly
// twice, so exactly 3 runs (3 List calls), not 150.
func TestResumeNoReentryParksPaced_ListCallsBoundedUnderRapidTriggerLoop(t *testing.T) {
	task := wfParkedTask("t-resume", "clarify", stage.ReasonStageDeadline)
	base := newMirrorClient(t, task)
	cc := &listCountingClient{Client: base}
	r := &ProjectReconciler{Client: cc, Scheme: base.Scheme()}
	proj := wfProject()
	ctx := context.Background()
	t0 := time.Now()

	for i := 0; i < 150; i++ {
		if _, err := r.resumeNoReentryParksPaced(ctx, proj, t0.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("resumeNoReentryParksPaced (iteration %d): %v", i, err)
		}
	}
	if got := cc.ListCount(); got != 3 {
		t.Fatalf("rapid trigger loop must bound List calls to 3 runs, got %d", got)
	}
}
