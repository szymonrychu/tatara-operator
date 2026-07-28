package controller

import (
	"context"
	"fmt"
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// maxPendingEvents caps Task.Status.PendingEvents (contract E.3), applied
// Go-side, drop-oldest, BEFORE every write. The CRD's MaxItems=25 is a
// backstop only: an API-server 422 is not retried by retry.RetryOnConflict and
// would hot-loop webhook redelivery, so the cap here must stay strictly below
// it.
const maxPendingEvents = 20

// AppendTaskEvent appends ev to task.Status.PendingEvents (contract E.3),
// capping Go-side at maxPendingEvents, drop-oldest, BEFORE the write. The
// CRD's MaxItems=25 is a backstop only: an API-server 422 is NOT retried by
// retry.RetryOnConflict and would hot-loop webhook redelivery, so the cap
// here must stay strictly below it.
//
// It clamps ev.Body to TaskEvent.Body's MaxLength for the same reason and in
// the same place: the COUNT was capped here and the LENGTH was not, so a forge
// comment over 4 KB 422-ed the whole status update and the comment never
// reached the Task (issue #495). The clamp lives at this funnel rather than at
// the four TaskEvent construction sites because this is the one function every
// TaskEvent passes through - a fifth call site cannot reintroduce the 422.
//
// The E.3 enqueue filter (drop a bot-authored event) is the CALLER's
// responsibility, applied BEFORE this function is ever invoked - a
// bot-authored ev must never reach it.
//
// On success task is updated in place to the freshly persisted object, so a
// caller that goes on to inspect task.Status sees the write it just made.
//
// Relocated here from internal/webhook/pending_events.go (OP12): the sweep's
// comment-cursor redelivery (redeliverMRComments) needs to append the exact
// same capped mr_comment TaskEvent the webhook fast path does, and the two
// paths must never duplicate this logic - one function, two callers.
func AppendTaskEvent(ctx context.Context, c client.Client, task *tatarav1alpha1.Task, ev tatarav1alpha1.TaskEvent) error {
	ev = clampTaskEventBody(ev)
	key := client.ObjectKeyFromObject(task)
	fresh := &tatarav1alpha1.Task{}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh = &tatarav1alpha1.Task{}
		if err := c.Get(ctx, key, fresh); err != nil {
			return err
		}
		fresh.Status.PendingEvents = appendEventCapped(fresh.Status.PendingEvents, ev, maxPendingEvents)
		// THE IDLE CLOCK RESET. Every queued event is, by definition, the
		// conversation not being idle. It is stamped here rather than at the
		// webhook because this is the ONE funnel every TaskEvent passes through, so
		// no future event source can forget it. Harmless on a Task that is not
		// conversing: nothing reads the field outside that stage.
		now := metav1.Now()
		fresh.Status.ConversationLastEventAt = &now
		// A HUMAN spoke. The consecutive-agent-round streak is over. AppendTaskEvent
		// is only ever reached with a bot-authored event through the narrow D4
		// cross-kind path, which bumps the counter itself AFTER this call, so
		// resetting here is correct for both callers.
		fresh.Status.BotRounds = 0
		return c.Status().Update(ctx, fresh)
	})
	if err != nil {
		return fmt.Errorf("task_events: append task event on %s: %w", task.Name, err)
	}
	*task = *fresh
	return nil
}

// AppendAgentTaskEvent is the D4 cross-kind path's counterpart to
// AppendTaskEvent, for the one caller (deliverAgentComment) that queues a
// BOT-authored event. It INCREMENTS Task.status.botRounds instead of
// resetting it, in the SAME Get-mutate-Update transaction, and returns the new
// count. It clamps ev.Body exactly as AppendTaskEvent does (issue #495): an
// agent comment is no likelier to fit 4096 bytes than a human's, and a 422 here
// takes the botRounds increment down with the append.
//
// enqueue controls whether ev is actually appended to Status.PendingEvents.
// It must be false for a SAME-KIND agent comment: the only consumer of
// PendingEvents while conversing is the follow-up-turn check in
// reconcilePodStage (task_stage.go), which keys on nothing but
// len(PendingEvents) > 0 - it cannot tell a genuine cross-kind wake from an
// agent's own comment reflected back at it. Queueing a same-kind event
// unconditionally fed that comment straight back into the SAME live pod that
// wrote it: the 2026-06 forty-duplicate-comment incident shape, reproduced by
// this feature instead of guarded against by it (2026-07-28 final review
// CRITICAL 1). Cross-kind callers pass true so the event still reaches the
// live pod's next turn - that delivery is the whole point of D4.
//
// It NEVER stamps ConversationLastEventAt, in either enqueue mode: only a
// genuine human comment (AppendTaskEvent) may reset the idle clock. task_types.go's
// own doc comment on the field says the clock is "RE-STAMPED by AppendTaskEvent
// on every non-bot event" - a bot round, cross-kind or not, is not a human
// weighing in, and resetting it here is what disabled the idle backstop: a
// conversation stuck reflecting a bot comment back at itself (or ping-ponging
// across kinds) never went idle and never released its concurrency slot,
// bounded only by maxTurnsPerTask (300), with no absolute lifetime ceiling by
// design (decision D6, which assumes the idle clock is real).
//
// It deliberately does NOT compose as AppendTaskEvent(...) followed by a
// separate bump: AppendTaskEvent's human-comment reset runs on every call
// regardless of caller, because AppendTaskEvent has no signal to tell a
// bot-authored append from a human one. Composing the two calls in either
// order would either zero-then-rebump BotRounds to exactly 1 on EVERY
// consecutive round (Append first) or bump-then-immediately-zero it (Bump
// first) - both cap the counter at a fixed value and defeat the entire point of
// a CONSECUTIVE round counter: the 2026-06 incident was 40+ rounds, not 1.
// A single atomic transaction that increments instead of resetting is the only
// way this path counts correctly. NOTHING reads the returned count to make a
// decision (decision D7) - it is observability only.
func AppendAgentTaskEvent(ctx context.Context, c client.Client, task *tatarav1alpha1.Task, ev tatarav1alpha1.TaskEvent, enqueue bool) (int, error) {
	ev = clampTaskEventBody(ev)
	key := client.ObjectKeyFromObject(task)
	fresh := &tatarav1alpha1.Task{}
	rounds := 0
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh = &tatarav1alpha1.Task{}
		if err := c.Get(ctx, key, fresh); err != nil {
			return err
		}
		if enqueue {
			fresh.Status.PendingEvents = appendEventCapped(fresh.Status.PendingEvents, ev, maxPendingEvents)
		}
		fresh.Status.BotRounds++
		rounds = fresh.Status.BotRounds
		return c.Status().Update(ctx, fresh)
	})
	if err != nil {
		return 0, fmt.Errorf("task_events: append agent task event on %s: %w", task.Name, err)
	}
	*task = *fresh
	return rounds, nil
}

// taskEventTruncatedMarker ends a clamped body. It is not decoration: without
// it an agent reading the event cannot tell a human's short reply from the
// first 4 KB of a long one, and would answer a question whose second half it
// never saw. The full text is NOT lost - AppendCommentToMirror has already put
// it (up to the mirror's own 8192 cap) on the MergeRequest/Issue mirror the
// event names, which is where the marker points.
const taskEventTruncatedMarker = "\n...(truncated by tatara; full comment on the mirrored thread"

// clampTaskEventBody fits ev.Body inside TaskEvent.Body's CRD MaxLength,
// cutting on a RUNE boundary and marking the cut. A body that already fits is
// returned byte-identical: drainRenderedEvents matches a rendered event by its
// whole value tuple, body included, so an unconditional rewrite would break the
// webhook-vs-drain race guard.
//
// A byte-slice of a UTF-8 string cuts mid-rune and the API server's JSON
// encoder rejects invalid UTF-8, so the rune boundary is load-bearing, exactly
// as in mirror.go's truncateCommentBody.
func clampTaskEventBody(ev tatarav1alpha1.TaskEvent) tatarav1alpha1.TaskEvent {
	if len(ev.Body) <= tatarav1alpha1.TaskEventBodyMaxBytes {
		return ev
	}
	marker := taskEventTruncatedMarker + ")"
	if ev.Repo != "" && ev.Number > 0 {
		marker = fmt.Sprintf("%s %s#%d)", taskEventTruncatedMarker, ev.Repo, ev.Number)
	}
	ev.Body = tatarav1alpha1.TruncateUTF8(ev.Body, tatarav1alpha1.TaskEventBodyMaxBytes-len(marker)) + marker
	return ev
}

// appendEventCapped appends ev to events, keeping at most max entries by
// dropping the oldest. It never mutates the input slice's backing array.
func appendEventCapped(events []tatarav1alpha1.TaskEvent, ev tatarav1alpha1.TaskEvent, max int) []tatarav1alpha1.TaskEvent {
	out := make([]tatarav1alpha1.TaskEvent, 0, len(events)+1)
	out = append(out, events...)
	out = append(out, ev)
	if max > 0 && len(out) > max {
		out = out[len(out)-max:]
	}
	return out
}

// drainRenderedEvents removes exactly the entries of rendered from current -
// by VALUE, one occurrence each, wherever they sit in current - leaving
// everything else (in particular anything appended after render) untouched.
//
// This is the fix for the webhook-vs-drain race: AppendTaskEvent's caller,
// the webhook handler, runs on every replica regardless of leader election
// (webhook.HandlerRunnable.NeedLeaderElection() is false - the reconcile
// loop's per-object-key workqueue + leader election is what makes
// reconcile-vs-reconcile impossible, but that guarantee does not reach the
// webhook). A turn's SubmitTurn is a real network round trip; a comment can
// land in status.pendingEvents while it is in flight. Unconditionally nil-ing
// pendingEvents in the drain would silently erase that comment - it was never
// rendered into any turn, and nothing would ever resend it. current has no
// unique event id, so identity is the full value tuple (at/kind/repo/number/
// author/body), which is what a real webhook-originated event is unique on in
// practice.
//
// current is also NOT assumed to hold rendered as a strict prefix: the
// maxPendingEvents cap (drop-oldest) can in principle evict an already-
// rendered entry before this runs. A rendered entry no longer present is
// simply not found and not removed - harmless, since there is nothing left to
// remove.
func drainRenderedEvents(current, rendered []tatarav1alpha1.TaskEvent) []tatarav1alpha1.TaskEvent {
	if len(rendered) == 0 || len(current) == 0 {
		return current
	}
	toRemove := make([]tatarav1alpha1.TaskEvent, len(rendered))
	copy(toRemove, rendered)

	out := make([]tatarav1alpha1.TaskEvent, 0, len(current))
	for _, ev := range current {
		matched := false
		for i, r := range toRemove {
			if reflect.DeepEqual(ev, r) {
				toRemove = append(toRemove[:i], toRemove[i+1:]...)
				matched = true
				break
			}
		}
		if !matched {
			out = append(out, ev)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
