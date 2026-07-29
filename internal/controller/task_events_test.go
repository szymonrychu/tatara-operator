package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

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
		rounds, err := AppendAgentTaskEvent(ctx, c, task, ev, true)
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

// TestAppendTaskEvent_ClampsOversizedBody pins the #495 fix at the FUNNEL
// rather than at one call site. TaskEvent.Body is CRD-capped at 4096 bytes and
// there are four construction sites (two webhook comment paths, the webhook
// issue_edited path, and the sweep's comment redelivery), none of which clamped.
// AppendTaskEvent's own doc calls itself the one funnel every TaskEvent passes
// through, so the clamp belongs here: a fifth call site cannot reintroduce the
// 422. Runs against envtest, so the CRD MaxLength is genuinely enforced.
func TestAppendTaskEvent_ClampsOversizedBody(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		body string
	}{
		{"one byte over", strings.Repeat("a", tatarav1alpha1.TaskEventBodyMaxBytes+1)},
		{"far over", strings.Repeat("a", 100_000)},
		{"multi-byte rune split at the cut", strings.Repeat("a", tatarav1alpha1.TaskEventBodyMaxBytes-1) + strings.Repeat("é", 64)},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			name := fmt.Sprintf("t-clamp-%d", i)
			mkTaskWithKind(t, name, "p", "", "review")
			task := getTask(t, name)
			ev := tatarav1alpha1.TaskEvent{
				At: metav1.Now(), Kind: "mr_comment", Repo: "r", Number: 1, Author: "alice", Body: tc.body,
			}
			if err := AppendTaskEvent(ctx, k8sClient, task, ev); err != nil {
				t.Fatalf("AppendTaskEvent must clamp, not 422: %v", err)
			}
			got := getTask(t, name).Status.PendingEvents
			if len(got) != 1 {
				t.Fatalf("pending events = %d, want 1", len(got))
			}
			if len(got[0].Body) > tatarav1alpha1.TaskEventBodyMaxBytes {
				t.Fatalf("stored body = %d bytes, want <= %d", len(got[0].Body), tatarav1alpha1.TaskEventBodyMaxBytes)
			}
			if !utf8.ValidString(got[0].Body) {
				t.Fatalf("stored body is not valid UTF-8")
			}
			if !strings.Contains(got[0].Body, taskEventTruncatedMarker) {
				t.Fatalf("a clamped body must carry the truncation marker")
			}
			// The caller's own copy must reflect what was persisted: callers go on
			// to inspect task.Status (reverifyParked passes the event straight on).
			if task.Status.PendingEvents[0].Body != got[0].Body {
				t.Fatalf("in-place task update does not match the persisted body")
			}
		})
	}
}

// TestAppendAgentTaskEvent_ClampsOversizedBody is the same clamp on the D4
// cross-kind path, which appends through the identical capped-append and was
// equally exposed: a multi-KB agent comment 422-ed the whole status write,
// taking the botRounds increment down with it.
func TestAppendAgentTaskEvent_ClampsOversizedBody(t *testing.T) {
	ctx := context.Background()
	mkTaskWithKind(t, "t-clamp-agent", "p", "", "review")
	task := getTask(t, "t-clamp-agent")
	ev := tatarav1alpha1.TaskEvent{
		At: metav1.Now(), Kind: "mr_comment", Repo: "r", Number: 1, Author: "tatara-bot",
		Body: strings.Repeat("a", 50_000),
	}
	rounds, err := AppendAgentTaskEvent(ctx, k8sClient, task, ev, true)
	if err != nil {
		t.Fatalf("AppendAgentTaskEvent must clamp, not 422: %v", err)
	}
	if rounds != 1 {
		t.Fatalf("rounds = %d, want 1", rounds)
	}
	got := getTask(t, "t-clamp-agent").Status.PendingEvents
	if len(got) != 1 || len(got[0].Body) > tatarav1alpha1.TaskEventBodyMaxBytes {
		t.Fatalf("agent event body not clamped: %d events, %d bytes", len(got), len(got[0].Body))
	}
}

// TestClampTaskEventBody_LeavesShortBodiesAlone is the negative half: a body
// that fits must be byte-identical, marker and all - drainRenderedEvents
// identifies a rendered event by its full value tuple (body included), so a
// clamp that rewrote every body would break the webhook-vs-drain race guard.
func TestClampTaskEventBody_LeavesShortBodiesAlone(t *testing.T) {
	for _, body := range []string{"", "a short human reply", strings.Repeat("a", tatarav1alpha1.TaskEventBodyMaxBytes)} {
		ev := tatarav1alpha1.TaskEvent{Kind: "mr_comment", Repo: "r", Number: 7, Body: body}
		if got := clampTaskEventBody(ev); got.Body != body {
			t.Fatalf("a %d-byte body must pass through untouched, got %d bytes", len(body), len(got.Body))
		}
	}
}
