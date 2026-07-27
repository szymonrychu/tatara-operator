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
		return c.Status().Update(ctx, fresh)
	})
	if err != nil {
		return fmt.Errorf("task_events: append task event on %s: %w", task.Name, err)
	}
	*task = *fresh
	return nil
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
