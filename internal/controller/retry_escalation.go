// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// escalateExhaustedRetry ends an UnparkRetry lane LOUDLY.
//
// THE FAILURE IT REMOVES. Before the lane, a technical blocker that reached a
// park stayed parked: nothing released it, nothing said so, and the only way it
// surfaced was a human noticing days later that an approved issue had never
// shipped. The lane gives the machine MaxUnparkRetries backed-off laps to clear
// its own blocker, and this is what happens when it does not - the Task is
// re-parked under retry-exhausted (UnparkHuman, so a comment resumes it), a
// comment naming the blocker and every lap goes on the owning issue, and an
// alertable counter moves.
//
// THE ORDER IS COMMENT, LATCH, REPARK, and each position is load-bearing:
//
//   - the comment is best-effort and is NOT allowed to block the repark. The
//     repark is the correctness-critical half: while the reason is still a retry
//     reason with no schedule armed, this function runs on EVERY 30s pass, so a
//     forge outage that blocked the repark would turn one escalation into a
//     comment attempt every thirty seconds for as long as the outage lasts.
//   - the latch is stamped only after the comment LANDS, so an outage costs a
//     retry next pass rather than a silently lost escalation.
//   - the latch is keyed on the park's own parkedAt, so it silences THIS
//     escalation and not the next blocker's.
func (r *ProjectReconciler) escalateExhaustedRetry(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, now time.Time) error {

	l := log.FromContext(ctx)
	reason := t.Status.ParkReason
	state := t.Status.State
	attempts := t.Status.RetryAttempts
	stamp := parkStamp(t)

	if t.Annotations[tatarav1alpha1.AnnRetryExhaustedCommented] != stamp {
		if issueRef, ok := r.exhaustedRetryIssueRef(ctx, t); ok {
			body := retryExhaustedComment(reason, attempts, parkedAt(t), now)
			if err := r.commentOnIssue(ctx, proj, issueRef, body); err != nil {
				// Logged and swallowed: see the ordering argument above.
				l.Error(err, "retry lane: the escalation comment failed; the park still lands",
					"action", "retry_exhausted_comment_failed", "resource_id", t.Name,
					"reason", reason, "issue_ref", issueRef)
			} else if err := r.stampRetryExhaustedCommented(ctx, t, stamp); err != nil {
				return err
			} else {
				l.Info("retry lane: escalated a spent lane to the humans on its issue",
					"action", "retry_exhausted_commented", "resource_id", t.Name,
					"reason", reason, "issue_ref", issueRef, "attempts", attempts)
			}
		} else {
			l.Info("retry lane: spent, and the Task owns no issue to say so on",
				"action", "retry_exhausted_no_issue", "resource_id", t.Name,
				"reason", reason, "attempts", attempts)
		}
	}

	if err := r.applyRetryExhaustion(ctx, t, now); err != nil {
		return err
	}
	l.Info("retry lane spent: the blocker outlived the machine's budget and now belongs to a human",
		"action", "retry_exhausted", "resource_id", t.Name, "reason", reason,
		"state", state, "attempts", attempts, "max_attempts", tatarav1alpha1.MaxUnparkRetries)
	r.Metrics.TaskRetryExhausted(reason, state)
	return nil
}

// parkStamp is the latch VALUE: the instant this park was written, which is what
// scopes the escalation latch to one park rather than to the Task's lifetime.
func parkStamp(t *tatarav1alpha1.Task) string {
	return parkedAt(t).UTC().Format(time.RFC3339)
}

// applyRetryExhaustion persists stage.ExhaustRetry under optimistic concurrency,
// re-reading through the UNCACHED APIReader and re-checking the park under the
// retry, exactly as the other appliers in unpark.go do.
func (r *ProjectReconciler) applyRetryExhaustion(ctx context.Context, t *tatarav1alpha1.Task, now time.Time) error {
	getter := client.Reader(r.APIReader)
	if getter == nil {
		getter = r.Client
	}
	key := client.ObjectKeyFromObject(t)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := getter.Get(ctx, key, fresh); err != nil {
			return err
		}
		if fresh.Status.ParkReason != t.Status.ParkReason {
			return nil // raced past this park; the escalation is already recorded
		}
		if _, err := stage.ExhaustRetry(fresh, now); err != nil {
			return fmt.Errorf("unpark: exhaust the retry lane on %s: %w", key.Name, err)
		}
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*t = *fresh
		return nil
	})
}

// stampRetryExhaustedCommented writes the latch. A METADATA patch, not a status
// write: the two subresources are independent, so this cannot be lost by the
// status update that follows it.
func (r *ProjectReconciler) stampRetryExhaustedCommented(ctx context.Context, t *tatarav1alpha1.Task, stamp string) error {
	patch := client.MergeFrom(t.DeepCopy())
	if t.Annotations == nil {
		t.Annotations = map[string]string{}
	}
	t.Annotations[tatarav1alpha1.AnnRetryExhaustedCommented] = stamp
	if err := r.Patch(ctx, t, patch); err != nil {
		return fmt.Errorf("unpark: stamp %s on %s: %w", tatarav1alpha1.AnnRetryExhaustedCommented, t.Name, err)
	}
	return nil
}

// exhaustedRetryIssueRef resolves where to say it: the first OPEN owned Issue,
// as "<owner>/<repo>#<number>". A Task can own several; the escalation is about
// the TASK, so one comment on one live thread is the right volume - and an
// upgrade or documentation Task legitimately owns none at all, which is why the
// caller treats a miss as "no target" rather than as an error.
func (r *ProjectReconciler) exhaustedRetryIssueRef(ctx context.Context, t *tatarav1alpha1.Task) (string, bool) {
	l := log.FromContext(ctx)
	issues, err := loadTaskIssues(ctx, r.Client, t)
	if err != nil {
		l.Error(err, "retry lane: could not load the owned issues to escalate on",
			"action", "retry_exhausted_issue_lookup_failed", "resource_id", t.Name)
		return "", false
	}
	for i := range issues {
		iss := &issues[i]
		if iss.Status.State != "" && iss.Status.State != "open" {
			continue
		}
		var repo tatarav1alpha1.Repository
		key := client.ObjectKey{Namespace: t.Namespace, Name: iss.Spec.RepositoryRef}
		if err := r.Get(ctx, key, &repo); err != nil {
			continue
		}
		slug, err := scm.RepoSlugFromURL(repo.Spec.URL)
		if err != nil {
			continue
		}
		return fmt.Sprintf("%s#%d", slug, iss.Spec.Number), true
	}
	return "", false
}

// commentOnIssue posts one comment with the Project's own SCM credentials.
func (r *ProjectReconciler) commentOnIssue(ctx context.Context, proj *tatarav1alpha1.Project,
	issueRef, body string) error {

	writer, token, err := r.scanWriter(ctx, proj)
	if err != nil {
		return fmt.Errorf("unpark: resolve scm writer for the retry escalation: %w", err)
	}
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	cerr := writer.Comment(ctx, token, issueRef, body)
	RecordSCM(r.Metrics, provider, "comment", cerr)
	if cerr != nil {
		return fmt.Errorf("unpark: comment on %s: %w", issueRef, cerr)
	}
	return nil
}

// retryExhaustedComment is the escalation body. It names the blocker by the
// reason the operator itself recorded, lists the schedule the laps were spent
// on, and says what a human is being asked to do - because "tatara gave up" with
// no blocker named is a message that sends somebody reading logs.
func retryExhaustedComment(reason string, attempts int, parkedAt, now time.Time) string {
	waits := make([]string, 0, attempts)
	for i := 0; i < attempts; i++ {
		waits = append(waits, stage.RetryWait(i).String())
	}
	var b strings.Builder
	fmt.Fprintf(&b, "tatara stopped retrying this task and is asking for a human.\n\n")
	fmt.Fprintf(&b, "- blocker: `%s`\n", reason)
	fmt.Fprintf(&b, "- attempts: %d of %d, waiting %s between them\n",
		attempts, tatarav1alpha1.MaxUnparkRetries, strings.Join(waits, ", "))
	if !parkedAt.IsZero() {
		fmt.Fprintf(&b, "- blocked since: %s (%s ago)\n",
			parkedAt.UTC().Format(time.RFC3339), now.Sub(parkedAt).Round(time.Minute))
	}
	fmt.Fprintf(&b, "\nThe blocker is one a machine is normally expected to clear on its own "+
		"(a pipeline finishing, a branch rebasing), so it was retried with a growing backoff rather than "+
		"handed straight to a person. It is still standing. Look at the merge request's checks and "+
		"mergeability; a comment on this issue resumes the task.\n")
	return b.String()
}
