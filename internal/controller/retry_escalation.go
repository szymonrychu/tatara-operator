// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
//
//   - THE LATCH WRITE IS SWALLOWED FOR THE SAME REASON, and it was not: a failed
//     latch used to return before the repark, which left the Task in the lane at
//     the cap and brought the next pass straight back here to comment AGAIN. One
//     transient apiserver error was a duplicate "tatara stopped retrying" comment
//     on a human's issue; a persistent one was a comment every thirty seconds.
//     Falling through costs nothing that matters: the repark moves the reason to
//     retry-exhausted, which is UnparkHuman, so the lane never looks at this Task
//     again and the latch it failed to write has nothing left to guard.
//
//     THAT ARGUMENT ONLY HOLDS WHERE THE REPARK LANDS, and the two writes go to
//     the same object on the same apiserver, so their failures are CORRELATED:
//     during a blip the latch fails, the repark fails too, the Task stays in the
//     lane at the cap, and the next pass posts the comment again - the exact
//     storm the swallow was supposed to have prevented. So a repark that fails
//     RE-TRIES the latch for a comment that did land, best-effort, before
//     returning. It cannot be one write instead: the latch is metadata and the
//     repark is the status subresource, and every persist path behind
//     internal/stage ends in Status().Update, which discards annotations.
//
//   - the latch is stamped only after the comment LANDS, so an outage costs a
//     retry next pass rather than a silently lost escalation.
//
//   - the latch is keyed on the park's own parkedAt, so it silences THIS
//     escalation and not the next blocker's.
//
// A FAILED LOOKUP IS THE ONE THING THAT DOES STOP THE PASS. "Where do I say
// this" failing is not "there is nowhere to say it": returning the error skips
// the repark, so the Task stays in the lane at the cap and the next 30s pass
// tries the whole escalation again. Re-parking through a transient apiserver
// error would move the reason to retry-exhausted and make the escalation
// permanently silent - precisely the failure this file exists to prevent.
//
// IT HAS TWO ENTRANCES, and everything above is common to both. driveRetryLane
// brings the lane's own exhaustion (the budget is spent and a live read has just
// confirmed the blocker standing); driveUnparks brings a lane that the
// AGENT-STOP RE-ARM CAP took over after the lane had already released it - see
// stage.LaneStranded. Only two things differ, and both are read off the Task
// rather than passed in, so no caller can get the pair wrong: which reason the
// comment names as the blocker, and which of stage's two terminals ends it.
func (r *ProjectReconciler) escalateExhaustedRetry(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, now time.Time) error {

	l := log.FromContext(ctx)
	reason := t.Status.ParkReason
	state := t.Status.State
	attempts := t.Status.RetryAttempts
	stamp := parkStamp(t)
	// stranded is read BEFORE the repark, which moves the park reason out from
	// under stage.LaneStranded and would make every read after it answer false.
	stranded := stage.LaneStranded(t)
	blocker := reason
	if stranded {
		blocker = t.Status.RetryBlocker
	}

	// unlatched is "a comment for THIS park has landed and nothing records that
	// yet". It is what the repark's failure path re-tries the latch for.
	unlatched := false
	if t.Annotations[tatarav1alpha1.AnnRetryExhaustedCommented] != stamp {
		ref, ok, err := r.exhaustedRetryTarget(ctx, proj, t)
		if err != nil {
			return err
		}
		if ok {
			body := retryExhaustedComment(blocker, attempts, parkedAt(t), now)
			if stranded {
				body = strandedLaneComment(blocker, attempts, t.Status.Stats.AgentStops, parkedAt(t), now)
			}
			if cerr := r.commentOnIssue(ctx, proj, ref, body); cerr != nil {
				// Logged and swallowed: see the ordering argument above.
				l.Error(cerr, "retry lane: the escalation comment failed; the park still lands",
					"action", "retry_exhausted_comment_failed", "resource_id", t.Name,
					"reason", reason, "issue_ref", ref)
			} else if serr := r.stampRetryExhaustedCommented(ctx, t, stamp); serr != nil {
				unlatched = true
				l.Error(serr, "retry lane: the escalation latch failed to write; the park still lands",
					"action", "retry_exhausted_latch_failed", "resource_id", t.Name,
					"reason", reason, "issue_ref", ref)
			} else {
				l.Info("retry lane: escalated a spent lane to the humans on its issue",
					"action", "retry_exhausted_commented", "resource_id", t.Name,
					"reason", reason, "issue_ref", ref, "attempts", attempts)
			}
		} else {
			l.Info("retry lane: spent, and the Task owns no open issue or merge request to say so on",
				"action", "retry_exhausted_no_issue", "resource_id", t.Name,
				"reason", reason, "attempts", attempts)
		}
	}

	if err := r.applyRetryExhaustion(ctx, t, now); err != nil {
		if unlatched {
			// The repark did NOT move the reason, so this Task comes back here on
			// the next pass and would comment again. One more attempt at the latch,
			// best-effort: the failures are correlated, so it usually fails too -
			// but a blip that cleared between the two writes is common enough that
			// this is the difference between one comment and one per thirty seconds.
			if serr := r.stampRetryExhaustedCommented(ctx, t, stamp); serr != nil {
				l.Error(serr, "retry lane: the escalation is delivered but neither latched nor re-parked; "+
					"the next pass may re-post it",
					"action", "retry_exhausted_latch_failed", "resource_id", t.Name, "reason", reason)
			}
		}
		return err
	}
	l.Info("retry lane spent: the blocker outlived the machine's budget and now belongs to a human",
		"action", "retry_exhausted", "resource_id", t.Name, "reason", reason, "blocker", blocker,
		"stranded", stranded, "state", state, "attempts", attempts,
		"max_attempts", tatarav1alpha1.MaxUnparkRetries)
	// THE PARK REASON, NOT THE BLOCKER, is the label. The two entrances are
	// different failures and an operator has to be able to tell them apart:
	// reason="ci-failed" is a pipeline that stayed red for the whole budget,
	// reason="no-outcome" is the agent-stop cap taking a lane over after the
	// release - same alert, different thing to go and look at. It is a new VALUE
	// on an existing label rather than a new series, so every alert on
	// operator_task_retry_exhausted_total keeps firing unchanged.
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
		if err := endRetryLane(fresh, now); err != nil {
			return fmt.Errorf("unpark: exhaust the retry lane on %s: %w", key.Name, err)
		}
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*t = *fresh
		return nil
	})
}

// endRetryLane re-parks t as retry-exhausted from whichever end of the lane it
// reached. The discrimination is made on the FRESH copy inside the conflict
// retry, not on the one the escalation was decided from, so a Task another
// writer moved between the two reads ends up under the terminal that matches
// what it actually is.
func endRetryLane(t *tatarav1alpha1.Task, now time.Time) error {
	if stage.LaneStranded(t) {
		_, err := stage.StrandRetryLane(t, now)
		return err
	}
	_, err := stage.ExhaustRetry(t, now)
	return err
}

// stampRetryExhaustedCommented writes the latch. A METADATA patch, not a status
// write: the two subresources are independent, so this cannot be lost by the
// status update that follows it.
//
// t IS NOT MUTATED UNTIL THE PATCH LANDS, unlike the other stampers here, and
// this one has a caller that retries it: the repark's failure path. Mutating
// first would leave the in-memory Task carrying an annotation the apiserver
// never got, so MergeFrom would compute an EMPTY diff on the retry and the
// second attempt would silently write nothing at all.
func (r *ProjectReconciler) stampRetryExhaustedCommented(ctx context.Context, t *tatarav1alpha1.Task, stamp string) error {
	stamped := t.DeepCopy()
	if stamped.Annotations == nil {
		stamped.Annotations = map[string]string{}
	}
	stamped.Annotations[tatarav1alpha1.AnnRetryExhaustedCommented] = stamp
	if err := r.Patch(ctx, stamped, client.MergeFrom(t)); err != nil {
		return fmt.Errorf("unpark: stamp %s on %s: %w", tatarav1alpha1.AnnRetryExhaustedCommented, t.Name, err)
	}
	*t = *stamped
	return nil
}

// exhaustedRetryTarget resolves WHERE to say it: the first OPEN owned Issue,
// falling back to the first OPEN owned merge request. A Task can own several;
// the escalation is about the TASK, so one comment on one live thread is the
// right volume.
//
// THE MERGE-REQUEST FALLBACK IS NOT A NICETY - IT IS THE POPULATION THIS LANE
// IS WRITTEN FOR. Cron-minted upgrade Tasks and documentation batches own ZERO
// Issue CRs by construction ("their deliverable IS the merge request", as
// stage.go's own awaiting-human arm puts it), and they go through the merge
// corridor, which is exactly where ci-failed and merge-conflict-retry are
// written. Issue-only, those Tasks exhausted in total silence on the forge and
// only the metric moved.
//
// AN ERROR IS NOT "NO TARGET". A failed List/Get returns it so the caller can
// skip the repark and try again in 30s; only a genuinely absent or unparseable
// target answers (false, nil), and a mirror whose Repository CR is gone is
// SKIPPED rather than fatal - the mirror is not authoritative (loadTaskIssues
// makes the same call).
func (r *ProjectReconciler) exhaustedRetryTarget(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task) (string, bool, error) {

	l := log.FromContext(ctx)
	issues, err := loadTaskIssues(ctx, r.Client, t)
	if err != nil {
		return "", false, fmt.Errorf("unpark: load the owned issues to escalate on %s: %w", t.Name, err)
	}
	for i := range issues {
		iss := &issues[i]
		if iss.Status.State != "" && iss.Status.State != "open" {
			continue
		}
		slug, ok, serr := r.repoSlugFor(ctx, t.Namespace, iss.Spec.RepositoryRef)
		if serr != nil {
			return "", false, serr
		}
		if !ok {
			continue
		}
		return fmt.Sprintf("%s#%d", slug, iss.Spec.Number), true, nil
	}
	mrs, err := loadTaskMRs(ctx, r.Client, t)
	if err != nil {
		return "", false, fmt.Errorf("unpark: load the owned merge requests to escalate on %s: %w", t.Name, err)
	}
	// The separator is the PROVIDER's, not a formatting choice: GitLab routes a
	// '!' ref to the merge-request note endpoint and a '#' ref to the issue one,
	// so an issue-shaped ref would post the escalation onto whatever issue
	// happens to carry the merge request's iid. GitHub shares one endpoint and
	// takes '#' for both.
	sep := "#"
	if proj.Spec.Scm != nil && proj.Spec.Scm.Provider == "gitlab" {
		sep = "!"
	}
	for i := range mrs {
		mr := &mrs[i]
		if mr.Status.State != "" && mr.Status.State != "open" {
			continue
		}
		slug, ok, serr := r.repoSlugFor(ctx, t.Namespace, mr.Spec.RepositoryRef)
		if serr != nil {
			return "", false, serr
		}
		if !ok {
			continue
		}
		l.Info("retry lane: the Task owns no open issue; escalating on its merge request instead",
			"action", "retry_exhausted_mr_target", "resource_id", t.Name,
			"repo", mr.Spec.RepositoryRef, "pr", mr.Spec.Number)
		return fmt.Sprintf("%s%s%d", slug, sep, mr.Spec.Number), true, nil
	}
	return "", false, nil
}

// repoSlugFor resolves a Repository CR name to its forge slug. ok=false means
// this mirror cannot address the forge (its Repository is gone, or its URL does
// not parse) and the caller should try the next candidate; an error means the
// LOOKUP failed and the pass must be retried rather than concluded.
func (r *ProjectReconciler) repoSlugFor(ctx context.Context, ns, name string) (string, bool, error) {
	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &repo); err != nil {
		if apierrors.IsNotFound(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("unpark: get repository %s: %w", name, err)
	}
	slug, err := scm.RepoSlugFromURL(repo.Spec.URL)
	if err != nil {
		log.FromContext(ctx).Error(err, "retry lane: a repository URL does not yield a forge slug",
			"action", "retry_exhausted_slug_failed", "repo", name, "url", repo.Spec.URL)
		return "", false, nil
	}
	return slug, true, nil
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

// strandedLaneComment is the OTHER escalation body, and it must not reuse the
// one above: that one ends "It is still standing", and here the blocker is the
// one thing that is NOT standing. The lane read the forge, found the blocker
// gone and released the task on purpose; what failed afterwards is whatever came
// next. Naming the cleared blocker anyway is still the right context - it is
// what the task was parked on and where its merge corridor stopped - but the ask
// is different, so the text is.
//
// TWO WRITERS REACH IT, not just the agent-stop cap, so the middle line asks the
// counter rather than assuming it. reconcileCaps parks the same no-outcome for
// the UN-GRACEFUL version - a pod that became Ready and then ended without an
// outcome - and stats.agentStops is 0 there. "the agent asked to stop 0 times"
// is the kind of sentence that makes a human distrust the whole comment.
//
// The closing claim that part of the merge order has landed is the CALLER's
// (driveUnparks escalates only on stage.Unpark's own merged-mr decline), not
// something this function could check.
func strandedLaneComment(blocker string, attempts, stops int, parkedAt, now time.Time) string {
	var b strings.Builder
	fmt.Fprintf(&b, "tatara stopped working on this task and is asking for a human.\n\n")
	fmt.Fprintf(&b, "- was retried for: `%s` (%d of %d attempts spent before it cleared)\n",
		blocker, attempts, tatarav1alpha1.MaxUnparkRetries)
	if stops > 0 {
		fmt.Fprintf(&b, "- then: the agent asked to stop %d times in a row without submitting an outcome\n", stops)
	} else {
		fmt.Fprintf(&b, "- then: the pod ended without submitting an outcome\n")
	}
	if !parkedAt.IsZero() {
		fmt.Fprintf(&b, "- stopped since: %s (%s ago)\n",
			parkedAt.UTC().Format(time.RFC3339), now.Sub(parkedAt).Round(time.Minute))
	}
	fmt.Fprintf(&b, "\nPart of this task's merge order has already landed, so tatara will not re-implement "+
		"it: that would re-propose merged code. It will not retry either - the agent had nothing left to "+
		"say. Look at what is still unmerged and why; a comment on this issue resumes the task.\n")
	return b.String()
}
