package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// poisonedTaskStatusFailure returns interceptor.Funcs whose SubResourceUpdate
// fails EVERY status write for the Task named victimName and lets every other
// Task's status write through untouched. That is the fixture-level shape of
// #564's "one permanently-erroring victim": exactly which line inside
// liveHandoffAndPark first hits the failure (the handoff note write, or the
// post-stop clearLastTurn patch) does not matter for this test, only that the
// SAME named Task's eviction attempt reliably errors every single time while
// its siblings' do not - which a single shared session-level error
// (fakeSession.submitErr) cannot express, since it fails every Task the
// fixture's one session serves, not one victim among several.
//
// calls is bumped on every injected failure - ONE evictFn attempt on the
// poisoned Task can trip this more than once (SubmitHandoffTurn's own note
// append fails, which falls through to writeSyntheticNote, which fails
// again), so tests compare calls BEFORE and AFTER a pass rather than assert
// an absolute count: unchanged means the pass skipped the victim outright
// (backoff), increased means it was actually retried.
func poisonedTaskStatusFailure(victimName string, calls *int) interceptor.Funcs {
	return interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if subResourceName == "status" {
				if tk, ok := obj.(*tatarav1alpha1.Task); ok && tk.Name == victimName {
					*calls++
					return fmt.Errorf("injected #564 fixture failure: status update for %s always fails", victimName)
				}
			}
			return c.SubResource(subResourceName).Update(ctx, obj, opts...)
		},
	}
}

// TestEnforceLivePodCeiling_PoisonedVictimDoesNotStarveThePass is the #564
// regression test.
//
// BEFORE THE FIX: enforceLivePodCeiling's eviction loop only ever tried
// candidates[0:evict) (evict capped at maxLivePodEvictionsPerPass, which is
// 1) and kept no memory of a failure across calls. A candidate that errored
// left firstErr set in a local variable, the function returned it, and the
// very next pass called evictionCandidates from scratch and re-sorted the
// SAME permanently-erroring Task back to the front - so it was retried, and
// only it, every single pass, while every other overflow Task sat untouched
// forever.
//
// Three live Tasks against a ceiling of 1 (overflow=2). "poisoned" is the
// longest-idle (sorts first) and every attempt to evict it is made to fail;
// "second" and "third" are ordinary legal victims.
func TestEnforceLivePodCeiling_PoisonedVictimDoesNotStarveThePass(t *testing.T) {
	now := time.Now()
	proj := evictionCeilingProject(1)

	poisoned := liveStateTask("poisoned", "infrastructure", now.Add(-40*time.Minute))
	second := liveStateTask("second", "infrastructure", now.Add(-20*time.Minute))
	third := liveStateTask("third", "infrastructure", now.Add(-10*time.Minute))

	var failures int
	r := newLiveCapacityFixtureIntercepted(t, poisonedTaskStatusFailure("poisoned", &failures), proj, poisoned, second, third)

	assertParked := func(name string, task *tatarav1alpha1.Task, want bool) {
		t.Helper()
		var got tatarav1alpha1.Task
		if err := r.Get(context.Background(), objectKeyOf(task), &got); err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		if tatarav1alpha1.Parked(&got) != want {
			t.Fatalf("%s parked=%v, want %v", name, tatarav1alpha1.Parked(&got), want)
		}
	}

	// Pass 1: poisoned errors, but the pass must still make progress by
	// falling through to "second" WITHIN THE SAME CALL - the whole point of
	// #564. The error is still surfaced for observability.
	if _, err := r.enforceLivePodCeiling(context.Background(), proj, now); err == nil {
		t.Fatal("enforceLivePodCeiling (pass 1): want a non-nil error (poisoned's own failure is still surfaced), got nil")
	}
	if failures == 0 {
		t.Fatal("poisoned's eviction was never attempted in pass 1")
	}
	failuresAfterPass1 := failures
	assertParked("poisoned", poisoned, false)
	assertParked("second", second, true)
	assertParked("third", third, false)

	// Pass 2, same instant: poisoned is now serving its failure backoff, so
	// this pass must NOT re-attempt it at all (failures stays exactly where
	// pass 1 left it) - it must be skipped outright and progress must instead
	// land on "third".
	if _, err := r.enforceLivePodCeiling(context.Background(), proj, now); err != nil {
		t.Fatalf("enforceLivePodCeiling (pass 2): %v", err)
	}
	if failures != failuresAfterPass1 {
		t.Fatalf("poisoned was re-attempted in pass 2 (failures %d -> %d): its backoff should have skipped the attempt entirely, not just failed again", failuresAfterPass1, failures)
	}
	assertParked("poisoned", poisoned, false)
	assertParked("third", third, true)
}

// TestEnforceLivePodCeiling_PoisonedVictimBackoffEventuallyElapses proves the
// other half of #564: a permanently-erroring victim is retried EVENTUALLY,
// not banned forever. midTurn is excluded from evictionCandidates entirely
// (stage.TurnInFlight, the B4 guard), so poisoned is the ONLY legal candidate
// every pass - there is nothing else for the pass to fall through to, which
// is exactly the shape that must not wedge: the ceiling stays over budget
// until poisoned's backoff elapses and it is tried again.
func TestEnforceLivePodCeiling_PoisonedVictimBackoffEventuallyElapses(t *testing.T) {
	now := time.Now()
	proj := evictionCeilingProject(1)

	poisoned := liveStateTask("poisoned", "infrastructure", now.Add(-40*time.Minute))
	midTurn := liveStateTask("mid-turn", "infrastructure", now.Add(-30*time.Minute))
	midTurn.Annotations = map[string]string{tatarav1alpha1.AnnCurrentTurn: "turn-1"}

	var failures int
	r := newLiveCapacityFixtureIntercepted(t, poisonedTaskStatusFailure("poisoned", &failures), proj, poisoned, midTurn)

	// Pass 1 at t0: poisoned is the only legal candidate and it errors.
	if _, err := r.enforceLivePodCeiling(context.Background(), proj, now); err == nil {
		t.Fatal("enforceLivePodCeiling (pass 1): want a non-nil error, got nil")
	}
	if failures == 0 {
		t.Fatal("poisoned's eviction was never attempted in pass 1")
	}
	failuresAfterPass1 := failures

	// Pass 2, 5s later: still inside evictionFailureBackoffBase (30s) - must
	// be skipped outright, not re-attempted at all. Overflow remains, so the
	// fast per-pass requeue still applies.
	requeue, err := r.enforceLivePodCeiling(context.Background(), proj, now.Add(5*time.Second))
	if err != nil {
		t.Fatalf("enforceLivePodCeiling (pass 2): %v", err)
	}
	if requeue != livePodEvictionRequeue {
		t.Fatalf("requeue (pass 2) = %v, want livePodEvictionRequeue: overflow remains and a backed-off victim is worth a fast retry", requeue)
	}
	if failures != failuresAfterPass1 {
		t.Fatalf("poisoned was re-attempted in pass 2 (failures %d -> %d): its backoff should have skipped the attempt entirely", failuresAfterPass1, failures)
	}

	// Pass 3, past the backoff: poisoned is eligible again and IS retried - it
	// is still genuinely poisoned so it fails again, but the retry itself is
	// the proof this is a backoff and not a permanent ban.
	if _, err := r.enforceLivePodCeiling(context.Background(), proj, now.Add(evictionFailureBackoffBase+time.Second)); err == nil {
		t.Fatal("enforceLivePodCeiling (pass 3): want a non-nil error, got nil")
	}
	if failures <= failuresAfterPass1 {
		t.Fatalf("failures did not increase in pass 3 (%d -> %d): a backed-off victim must be retried once its backoff elapses, not banned forever", failuresAfterPass1, failures)
	}
}

// TestEvictionBackoffAfter pins evictionBackoffAfter's growth curve directly:
// it doubles from evictionFailureBackoffBase per consecutive failure and caps
// at evictionFailureBackoffMax.
func TestEvictionBackoffAfter(t *testing.T) {
	tests := []struct {
		count int
		want  time.Duration
	}{
		{count: 1, want: evictionFailureBackoffBase},
		{count: 2, want: 2 * evictionFailureBackoffBase},
		{count: 3, want: 4 * evictionFailureBackoffBase},
		{count: 4, want: 8 * evictionFailureBackoffBase},
		{count: 10, want: evictionFailureBackoffMax},
		{count: 100, want: evictionFailureBackoffMax},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("count=%d", tc.count), func(t *testing.T) {
			if got := evictionBackoffAfter(tc.count); got != tc.want {
				t.Fatalf("evictionBackoffAfter(%d) = %v, want %v", tc.count, got, tc.want)
			}
		})
	}
}
