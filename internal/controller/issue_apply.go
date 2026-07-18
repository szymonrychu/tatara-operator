package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ApplyIssueClosedStop is the WS3-I3 stop edge: a human closed the driving issue
// mid-flight, so the operator stops the Task at rejected(issue-closed), severs
// (and DELETES) the closed Issue mirror, and tears down the wrapper pod. The
// existing terminal reaper then closes the bot PR with its standard note.
//
// LEADER-ONLY: it is driven from the IssueReconciler (leader-only), never from
// the webhook goroutine, so it adds no new webhook-goroutine stage mutation
// (#353 / F6-1). It mirrors review_apply.go's applier shape.
//
// It re-reads the Task live inside RetryOnConflict and re-checks the live-stage
// gate: an approval or a park that landed since the caller's read must not be
// overwritten. It returns stopped=false (no error) when the Task is no longer in
// a live source stage (raced past, or the operator's own C.4 deploying-close).
func ApplyIssueClosedStop(ctx context.Context, c client.Client, task *tatarav1alpha1.Task,
	issueName string, now time.Time) (stopped bool, err error) {

	key := client.ObjectKeyFromObject(task)
	var prevStage string
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		stopped = false
		fresh := &tatarav1alpha1.Task{}
		if err := c.Get(ctx, key, fresh); err != nil {
			return err
		}
		if !stage.AllowsIssueClosedStop(fresh.Status.Stage) {
			return nil // raced off a live source stage (approval/park/deploying); fold
		}
		prevStage = fresh.Status.Stage
		if err := stage.Enter(fresh, nil, tatarav1alpha1.StageRejected, stage.ReasonIssueClosed, now); err != nil {
			return nil // guard refused; leave untouched
		}
		if err := c.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*task = *fresh
		stopped = true
		return nil
	}); err != nil {
		return false, fmt.Errorf("issue-closed: stop task %s: %w", task.Name, err)
	}
	if !stopped {
		return false, nil
	}

	// Sever the closed issue: clear IssueRefs FIRST, then DELETE the mirror CR (no
	// leak, no split state). The reopen mint re-creates it via SyncIssue off the
	// live open event.
	if err := SeverIssueFromTask(ctx, c, task, issueName, SeverDeleteCR); err != nil {
		return true, err
	}

	// Tear the wrapper pod down inline, leader-safe (same idiom as the review
	// appliers). The in-flight turn is abandoned; no half-finished branch is pushed.
	if stage.AgentKindFor(prevStage) != "" {
		if err := agent.DeleteWrapper(ctx, c, task.Namespace, task); err != nil {
			return true, fmt.Errorf("issue-closed: delete wrapper pod for %s: %w", task.Name, err)
		}
	}
	log.FromContext(ctx).Info("issue closed mid-flight: stopped the task",
		"action", "issue_closed_stop", "resource_id", task.Name,
		"from_stage", prevStage, "issue", issueName)
	return true, nil
}
