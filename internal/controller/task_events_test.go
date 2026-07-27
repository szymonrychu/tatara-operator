package controller

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// TestAppendAgentTaskEvent_AccumulatesAcrossConsecutiveRounds is the D7 counter
// regression: composing the shared AppendTaskEvent (which unconditionally
// resets BotRounds=0 - correct for its two HUMAN-only callers) with a separate
// increment would zero-then-rebump BotRounds to exactly 1 on EVERY consecutive
// bot round, capping the fleet-wide observability counter at 1 forever and
// defeating the entire point of a CONSECUTIVE round counter (the 2026-06
// incident was 40+ rounds). AppendAgentTaskEvent must increment in the SAME
// transaction as the append, never reset, so three consecutive bot rounds with
// no human comment in between must read 1, 2, 3.
func TestAppendAgentTaskEvent_AccumulatesAcrossConsecutiveRounds(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-rounds", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{Kind: "review", ProjectRef: "p", Goal: "g"},
	}
	c := newMirrorClient(t, task)
	ctx := context.Background()

	for i, want := range []int{1, 2, 3} {
		ev := tatarav1alpha1.TaskEvent{Kind: "mr_comment", Repo: "r", Number: 1, Author: "tatara-bot", Body: "round"}
		rounds, err := AppendAgentTaskEvent(ctx, c, task, ev)
		if err != nil {
			t.Fatalf("round %d: AppendAgentTaskEvent: %v", i+1, err)
		}
		if rounds != want {
			t.Fatalf("round %d: BotRounds = %d, want %d - the counter must accumulate, not cap at 1", i+1, rounds, want)
		}
	}

	// A human comment through the ordinary AppendTaskEvent funnel resets it,
	// proving the two functions compose correctly end to end.
	humanEv := tatarav1alpha1.TaskEvent{Kind: "mr_comment", Repo: "r", Number: 1, Author: "maintainer", Body: "a human weighs in"}
	if err := AppendTaskEvent(ctx, c, task, humanEv); err != nil {
		t.Fatalf("AppendTaskEvent: %v", err)
	}
	if task.Status.BotRounds != 0 {
		t.Fatalf("BotRounds after a human event = %d, want 0", task.Status.BotRounds)
	}
}
