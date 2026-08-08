package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// DeliverLiveTurn is the live-state entry edge, and under #521 it is NO LONGER
// A TRANSITION AT ALL (the dissolved `conversing` stage's replacement,
// stage.Live(state), is a property every live state already has - there is
// nothing left to "enter"). What is left of the old EnterConversing is the
// accounting and the refusals: it returns (delivered bool, declineReason
// string) and moves NOTHING - the Task's state, park flag and pod are all
// exactly as they were before the call.
func TestDeliverLiveTurnFromRefined(t *testing.T) {
	proj, task, r := ceFixture(t, v1alpha1.StateRefined, "implement")
	now := time.Now()

	delivered, decline := DeliverLiveTurn(context.Background(), r.Client, nil, r.Metrics, proj, task, nil, now)
	if !delivered {
		t.Fatalf("delivered = false (decline %q), want true", decline)
	}
	if decline != "" {
		t.Fatalf("decline = %q, want empty on delivery", decline)
	}

	fresh := ceGetTask(t, r.Client, task.Name)
	if fresh.Status.State != v1alpha1.StateRefined {
		t.Fatalf("state = %q, want unchanged refined: delivery is not a transition", fresh.Status.State)
	}
}

// Entry from reviewing is bounded by humanReviewRounds, exactly like the
// awaiting-human re-entry: each lap can spawn a pod, and a chatty MR thread must
// not spawn one per comment.
func TestDeliverLiveTurnFromReviewing(t *testing.T) {
	proj, task, r := ceFixture(t, v1alpha1.StateAwaitingReview, "review", func(task *v1alpha1.Task) {
		task.Status.HumanReviewRounds = 2
	})
	now := time.Now()

	delivered, decline := DeliverLiveTurn(context.Background(), r.Client, nil, r.Metrics, proj, task, nil, now)
	if !delivered {
		t.Fatalf("delivered = false (decline %q), want true", decline)
	}

	fresh := ceGetTask(t, r.Client, task.Name)
	if fresh.Status.State != v1alpha1.StateAwaitingReview {
		t.Fatalf("state = %q, want unchanged awaiting-review", fresh.Status.State)
	}
	if fresh.Status.HumanReviewRounds != 2 {
		t.Fatalf("HumanReviewRounds = %d, want unchanged at 2: delivery does not spend a round, only ACCOUNTS for the cap", fresh.Status.HumanReviewRounds)
	}
}

// under-implementation is dissolved into the SAME live property as every other
// live state (#521): there is no longer a distinct "conversing" stage a
// mid-change implement pod must be protected from being torn down FOR, because
// a further turn no longer tears the pod down at all - it rides into the
// already-running pod. So, unlike the old EnterConversing (which refused this
// stage outright, by table construction), a qualifying comment while
// under-implementation now DELIVERS.
func TestDeliverLiveTurnFromUnderImplementation(t *testing.T) {
	proj, task, r := ceFixture(t, v1alpha1.StateUnderImplementation, "implement")
	now := time.Now()

	delivered, decline := DeliverLiveTurn(context.Background(), r.Client, nil, r.Metrics, proj, task, nil, now)
	if !delivered {
		t.Fatalf("delivered = false (decline %q), want true: under-implementation is a LIVE state now, not a refused one", decline)
	}

	fresh := ceGetTask(t, r.Client, task.Name)
	if fresh.Status.State != v1alpha1.StateUnderImplementation {
		t.Fatalf("state = %q, want unchanged under-implementation", fresh.Status.State)
	}
}

// The reviewing round cap is spent: no more turns may be delivered into a live
// pod from this Task's review thread.
func TestDeliverLiveTurnRefusesAtTheReviewRoundCap(t *testing.T) {
	proj, task, r := ceFixture(t, v1alpha1.StateAwaitingReview, "review", func(task *v1alpha1.Task) {
		task.Status.HumanReviewRounds = v1alpha1.MaxHumanReviewRounds
	})
	now := time.Now()

	delivered, decline := DeliverLiveTurn(context.Background(), r.Client, nil, r.Metrics, proj, task, nil, now)
	if delivered {
		t.Fatal("delivered = true, want false: the review round cap is spent")
	}
	if decline != LiveEntryDeclineRoundsExhausted {
		t.Fatalf("decline = %q, want %q", decline, LiveEntryDeclineRoundsExhausted)
	}

	fresh := ceGetTask(t, r.Client, task.Name)
	if fresh.Status.State != v1alpha1.StateAwaitingReview {
		t.Fatalf("state = %q, want unchanged awaiting-review", fresh.Status.State)
	}
	if fresh.Status.HumanReviewRounds != v1alpha1.MaxHumanReviewRounds {
		t.Fatalf("HumanReviewRounds = %d, want unchanged at %d", fresh.Status.HumanReviewRounds, v1alpha1.MaxHumanReviewRounds)
	}
}

// #511: a maintainer's take-over comment on a stood-down (Ownership==external)
// MR must still be delivered even with the round cap spent - the cap
// bounds ordinary review ping-pong, not a take-over request.
func TestDeliverLiveTurnBypassesRoundCapOnExternallyOwnedMR(t *testing.T) {
	proj, task, r := ceFixture(t, v1alpha1.StateAwaitingReview, "review", func(task *v1alpha1.Task) {
		task.Status.HumanReviewRounds = v1alpha1.MaxHumanReviewRounds
	})
	now := time.Now()
	mrs := []v1alpha1.MergeRequest{{Status: v1alpha1.MergeRequestStatus{Ownership: v1alpha1.OwnershipExternal}}}

	delivered, decline := DeliverLiveTurn(context.Background(), r.Client, nil, r.Metrics, proj, task, mrs, now)
	if !delivered {
		t.Fatalf("delivered = false (decline %q), want true: an externally-owned MR must bypass the spent round cap", decline)
	}

	fresh := ceGetTask(t, r.Client, task.Name)
	if fresh.Status.HumanReviewRounds != v1alpha1.MaxHumanReviewRounds {
		t.Fatalf("HumanReviewRounds = %d, want unchanged at %d (this delivery does not spend a round)",
			fresh.Status.HumanReviewRounds, v1alpha1.MaxHumanReviewRounds)
	}
}

// A done Task delivers nothing: its work is over.
func TestDeliverLiveTurnDeclinesForADoneTask(t *testing.T) {
	proj, task, r := ceFixture(t, v1alpha1.StateDone, "implement")
	now := time.Now()

	delivered, decline := DeliverLiveTurn(context.Background(), r.Client, nil, r.Metrics, proj, task, nil, now)
	if delivered {
		t.Fatal("delivered = true, want false: a done Task's work is over")
	}
	if decline != LiveEntryDeclineTerminal {
		t.Fatalf("decline = %q, want %q", decline, LiveEntryDeclineTerminal)
	}
}

// A parked Task holds no pod: park is what takes it down.
func TestDeliverLiveTurnDeclinesForAParkedTask(t *testing.T) {
	proj, task, r := ceFixture(t, v1alpha1.StateRefined, "implement")
	task.Status.ParkReason = "awaiting-human"
	now := time.Now()

	delivered, decline := DeliverLiveTurn(context.Background(), r.Client, nil, r.Metrics, proj, task, nil, now)
	if delivered {
		t.Fatal("delivered = true, want false: a parked Task holds no pod")
	}
	if decline != LiveEntryDeclineParked {
		t.Fatalf("decline = %q, want %q", decline, LiveEntryDeclineParked)
	}
}

// merged runs no agent pod: it is operator-driven, not live.
func TestDeliverLiveTurnDeclinesForANonLiveState(t *testing.T) {
	proj, task, r := ceFixture(t, v1alpha1.StateMerged, "implement")
	now := time.Now()

	delivered, decline := DeliverLiveTurn(context.Background(), r.Client, nil, r.Metrics, proj, task, nil, now)
	if delivered {
		t.Fatal("delivered = true, want false: merged runs no agent pod")
	}
	if decline != LiveEntryDeclineNotLive {
		t.Fatalf("decline = %q, want %q", decline, LiveEntryDeclineNotLive)
	}
}

// The 2026-08-07 measurement this vocabulary exists to fix: EVERY one of
// ~27 declines on the live cluster carried the literal reason "unresolved",
// which cannot say which of several guards refused. controller.LiveEntryDeclineReasons
// is the closed, named vocabulary that replaced it - this asserts none of its
// members is that placeholder, so a future decline can never regress to it.
func TestLiveEntryDeclined_NeverEmitsAnUnresolvedReason(t *testing.T) {
	if len(LiveEntryDeclineReasons) == 0 {
		t.Fatal("LiveEntryDeclineReasons is empty: the vocabulary must name every decline this package can return")
	}
	seen := map[string]bool{}
	for _, r := range LiveEntryDeclineReasons {
		if r == "" {
			t.Fatal("LiveEntryDeclineReasons contains an empty string, which collides with DeliverLiveTurn's own success value")
		}
		if r == "unresolved" {
			t.Fatal(`LiveEntryDeclineReasons contains "unresolved": the exact defect this vocabulary exists to make impossible`)
		}
		if seen[r] {
			t.Fatalf("LiveEntryDeclineReasons contains a duplicate: %q", r)
		}
		seen[r] = true
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// ceFixture builds a Task at stg (with a live wrapper pod, mirroring a real
// pod-bearing state) plus a reconciler carrying real metrics, the same
// fake-client idiom as newLiveTurnFixture (livepods_turn_test.go, Task
// 8). setup, if given, mutates task BEFORE the fake client is built: the vanilla
// fake.NewClientBuilder deep-copies every WithObjects() argument once, at Build
// (see newLiveTurnFixture's liveTaskClient doc), so a field set on the
// returned task pointer AFTER this call is invisible to a fresh Get - a test
// that wants a seeded value to round-trip through a real persisted write must
// seed it here, not after.
func ceFixture(t *testing.T, stg, kind string, setup ...func(*v1alpha1.Task)) (*v1alpha1.Project, *v1alpha1.Task, *TaskReconciler) {
	t.Helper()
	now := time.Now()
	proj := tsStablyReadyProject(3)
	task := tsTask("ce-1", kind, stg, now.Add(-time.Hour))
	podAt := metav1.NewTime(now.Add(-10 * time.Minute))
	task.Status.PodStartedAt = &podAt
	for _, fn := range setup {
		fn(task)
	}

	c := newMirrorClient(t, proj, mdSecret(), task, tsReadyPod(task))
	r := tsReconciler(c)
	return proj, task, r
}

func ceGetTask(t *testing.T, c client.Client, name string) *v1alpha1.Task {
	t.Helper()
	return mdGetTask(t, c, name)
}

// Compile-time reminder that obs.OperatorMetrics is the type DeliverLiveTurn
// expects; tsReconciler already wires one, but a future refactor of the fixture
// must not silently drop it.
var _ = (*obs.OperatorMetrics)(nil)
