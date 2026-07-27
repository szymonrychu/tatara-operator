package controller

import (
	"context"
	"fmt"

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
