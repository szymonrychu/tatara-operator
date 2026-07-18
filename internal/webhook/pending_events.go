package webhook

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// deliverPendingEvent is the contract E.3 side channel: it runs on every
// issue_comment/mr_comment webhook delivery that has already passed the
// bot-actor and reporter-allowlist gates in handleIssueComment, and it is
// best-effort - any failure here is logged, never surfaced to the SCM as a
// non-2xx.
//
// Three things happen, in order:
//  1. the comment is mirrored onto the owning Issue/MergeRequest CR
//     immediately (does not wait for the sweep's cadence sync);
//  2. a non-bot event is queued onto the owning Task's pendingEvents,
//     capped and drop-oldest;
//  3. if that Task is parked(identity-unverified), the approval grammar is
//     re-run right now - syncing that issue's thread from the forge FIRST
//     (fix M11) - so a maintainer's "go ahead" un-parks in one comment
//     instead of waiting on the daily mirror cadence.
func (s *Server) deliverPendingEvent(ctx context.Context, proj tatarav1.Project, repo *tatarav1.Repository, ev scm.WebhookEvent) {
	if repo == nil {
		return
	}
	obj, kind := s.resolveMirrorTarget(ctx, repo, ev)
	if obj == nil {
		return
	}

	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	isBot := botLogin != "" && ev.ActorLogin != "" && ev.ActorLogin == botLogin

	sp := s.cfg.SpillerFor(&proj)
	if sp != nil {
		cmt := tatarav1.Comment{
			ExternalID: strconv.Itoa(ev.CommentID),
			Author:     ev.ActorLogin,
			Body:       ev.CommentBody,
			CreatedAt:  metav1.Now(),
			IsBot:      isBot,
		}
		if err := controller.AppendCommentToMirror(ctx, s.cfg.Client, sp, obj, cmt); err != nil {
			s.log.ErrorContext(ctx, "pendingEvents: mirror comment append failed", "error", err, "kind", kind)
		}
	} else {
		s.log.ErrorContext(ctx, "pendingEvents: no Spiller configured; mirror comment append skipped", "kind", kind)
	}

	// E.3 enqueue filter: a BOT-authored event is NEVER enqueued. Without it the
	// operator's own park comment would land in pendingEvents and un-park the
	// Task the operator just parked - a fully autonomous
	// hallucinated-approval-to-prod path.
	if isBot {
		return
	}

	ownerName, ok := own.ControllerOwner(obj)
	if !ok {
		return // no owning Task yet; the sweep (B.4) adopts this artifact later.
	}
	task := &tatarav1.Task{}
	if err := s.cfg.Client.Get(ctx, objKey(s.cfg.Namespace, ownerName), task); err != nil {
		if !apierrors.IsNotFound(err) {
			s.log.ErrorContext(ctx, "pendingEvents: get owning task failed", "error", err, "task", ownerName)
		}
		return
	}

	taskEv := tatarav1.TaskEvent{
		At:     metav1.Now(),
		Kind:   kind,
		Repo:   repo.Name,
		Number: ev.Number,
		Author: ev.ActorLogin,
		Body:   ev.CommentBody,
	}
	if err := AppendTaskEvent(ctx, s.cfg.Client, task, taskEv); err != nil {
		s.log.ErrorContext(ctx, "pendingEvents: append task event failed", "error", err, "task", task.Name)
		return
	}
	s.log.InfoContext(ctx, "pendingEvents: queued task event",
		"action", "pending_event_queued", "task", task.Name, "kind", kind, "repo", repo.Name, "number", ev.Number)

	if task.Status.Stage == tatarav1.StageParked && task.Status.StageReason == stage.ReasonIdentityUnverified {
		s.reverifyParked(ctx, &proj, task, taskEv)
	}
	if task.Status.Stage == tatarav1.StageParked &&
		(task.Status.StageReason == stage.ReasonAwaitingHuman || task.Status.StageReason == stage.ReasonBacklogSweep) {
		s.driveCommentUnpark(ctx, &proj, task)
	}
}

// driveCommentUnpark is the F.6 comment-driven re-entry for parked(awaiting-human)
// and parked(backlog-sweep): a non-bot pendingEvent (already enqueued above) may
// promote them PROMPTLY, instead of waiting on the project reconcile cadence.
// Unlike identity-unverified it needs no grammar and no forge sync - stage.Unpark
// reads the enqueued pendingEvents directly - so it shares the operator's single
// ApplyUnpark, which re-checks the maxOpenTasks cap at re-entry (H8: a promotion
// is not a mint). The project-reconcile driveUnparks loop backstops this.
func (s *Server) driveCommentUnpark(ctx context.Context, proj *tatarav1.Project, task *tatarav1.Task) {
	active, err := controller.CountActiveTasks(ctx, s.cfg.Client, proj)
	if err != nil {
		s.log.ErrorContext(ctx, "pendingEvents: count active tasks failed", "error", err, "task", task.Name)
		return
	}
	maxOpen := proj.Spec.MaxOpenTasks
	if maxOpen <= 0 {
		maxOpen = 6
	}
	target, decline, err := controller.ApplyUnpark(ctx, s.cfg.Client, s.cfg.APIReader, proj, task, active, maxOpen, false, time.Now())
	if err != nil {
		s.log.ErrorContext(ctx, "pendingEvents: comment-driven unpark failed", "error", err, "task", task.Name)
		return
	}
	if decline != controller.DeclineNone {
		s.cfg.Metrics.UnparkDeclined(task.Status.StageReason, string(decline))
	}
	if target == "" {
		// NOT an error (a decline is a normal outcome of stage.Unpark), but this
		// call site fires in direct reaction to a human comment the operator was
		// just asked to act on, so a silent decline here is exactly what hid the
		// cache-lag race (fresh.Status.PendingEvents read stale-empty) for a full
		// day with zero errors and zero "unparked" logs to explain the silence.
		// Both GUARD and RULE declines are logged here (unlike driveUnparks,
		// which only surfaces GUARD): this fires once, in direct reaction to a
		// human action, where silence - of either kind - is anomalous.
		s.log.InfoContext(ctx, "pendingEvents: comment-driven unpark declined",
			"action", "pending_event_unpark_declined", "task", task.Name, "stage_reason", task.Status.StageReason,
			"decline_kind", string(decline))
		return
	}
	s.log.InfoContext(ctx, "pendingEvents: unparked task on human comment",
		"action", "pending_event_unpark", "task", task.Name, "stage", target, "reason_from", task.Status.StageReason)
}

// resolveMirrorTarget maps a webhook event onto its mirror CR (Issue or
// MergeRequest), by the deterministic name - never a field-indexed List - so
// no field index needs registering for this lookup. A miss (no CR minted yet)
// returns (nil, "") and the caller treats it as nothing-to-do, not an error.
func (s *Server) resolveMirrorTarget(ctx context.Context, repo *tatarav1.Repository, ev scm.WebhookEvent) (client.Object, string) {
	if ev.IsPR {
		mr := &tatarav1.MergeRequest{}
		if err := s.cfg.Client.Get(ctx, objKey(s.cfg.Namespace, tatarav1.MergeRequestName(repo.Name, ev.Number)), mr); err != nil {
			if !apierrors.IsNotFound(err) {
				s.log.ErrorContext(ctx, "pendingEvents: get mergerequest failed", "error", err)
			}
			return nil, ""
		}
		return mr, "mr_comment"
	}
	iss := &tatarav1.Issue{}
	if err := s.cfg.Client.Get(ctx, objKey(s.cfg.Namespace, tatarav1.IssueName(repo.Name, ev.Number)), iss); err != nil {
		if !apierrors.IsNotFound(err) {
			s.log.ErrorContext(ctx, "pendingEvents: get issue failed", "error", err)
		}
		return nil, ""
	}
	return iss, "issue_comment"
}

// reverifyParked is the F.6/C3-3 un-park path for stageReason=identity-
// unverified, wired to Task 10's ReVerifyParked (which syncs the issue thread
// from the forge FIRST, then re-runs the C.6 grammar) and Task 9's
// stage.Unpark. On a grammar pass with every owned Issue approved, the Task
// enters implementing; on a fail, or if some owned Issue is still
// unapproved, it stays parked and pendingEvents is retained (never dropped).
func (s *Server) reverifyParked(ctx context.Context, proj *tatarav1.Project, task *tatarav1.Task, ev tatarav1.TaskEvent) {
	sp := s.cfg.SpillerFor(proj)
	if sp == nil {
		s.log.ErrorContext(ctx, "pendingEvents: no Spiller configured; skipping identity-unverified reverify", "task", task.Name)
		return
	}
	reader, err := s.scmReader(ctx, proj)
	if err != nil {
		s.log.ErrorContext(ctx, "pendingEvents: build scm reader failed", "error", err, "task", task.Name)
		return
	}
	passed, err := controller.ReVerifyParked(ctx, s.cfg.Client, sp, reader, proj, task, ev, s.cfg.Metrics)
	if err != nil {
		s.log.ErrorContext(ctx, "pendingEvents: reverify parked failed", "error", err, "task", task.Name)
		return
	}

	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	key := client.ObjectKeyFromObject(task)
	updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1.Task{}
		if err := s.cfg.Client.Get(ctx, key, fresh); err != nil {
			return err
		}
		if fresh.Status.Stage != tatarav1.StageParked || fresh.Status.StageReason != stage.ReasonIdentityUnverified {
			return nil // raced past this un-park by another writer already
		}
		issues, err := s.loadOwnedIssues(ctx, fresh)
		if err != nil {
			return err
		}
		target, ok := stage.Unpark(stage.UnparkInput{
			Task:          fresh,
			Issues:        issues,
			BotLogin:      botLogin,
			GrammarPassed: passed,
			Now:           time.Now(),
		})
		if !ok {
			return nil
		}
		if err := s.cfg.Client.Status().Update(ctx, fresh); err != nil {
			return err
		}
		s.log.InfoContext(ctx, "pendingEvents: unparked task",
			"action", "pending_event_unpark", "task", fresh.Name, "stage", target, "grammar_passed", passed)
		return nil
	})
	if updateErr != nil {
		s.log.ErrorContext(ctx, "pendingEvents: unpark task failed", "error", updateErr, "task", task.Name)
	}
}

// loadOwnedIssues resolves task's owned Issue CRs for the F.6 empty-set and
// allApproved checks. A ref that no longer resolves (deleted/renamed) is
// skipped, not an error - stage.Unpark's own scope check then runs against
// whatever survives.
func (s *Server) loadOwnedIssues(ctx context.Context, task *tatarav1.Task) ([]tatarav1.Issue, error) {
	issues := make([]tatarav1.Issue, 0, len(task.Status.IssueRefs))
	for _, name := range task.Status.IssueRefs {
		var iss tatarav1.Issue
		if err := s.cfg.Client.Get(ctx, objKey(s.cfg.Namespace, name), &iss); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("pendingEvents: get issue %s: %w", name, err)
		}
		issues = append(issues, iss)
	}
	return issues, nil
}

// scmReader builds a token-bound scm.SCMReader for proj, on demand: the
// webhook package holds no long-lived forge client, matching every other
// webhook path's on-demand secret read (see webhookSecret).
func (s *Server) scmReader(ctx context.Context, proj *tatarav1.Project) (scm.SCMReader, error) {
	if proj.Spec.Scm == nil {
		return nil, fmt.Errorf("pendingEvents: project %s has no scm spec", proj.Name)
	}
	var sec corev1.Secret
	if err := s.cfg.Client.Get(ctx, objKey(s.cfg.Namespace, proj.Spec.ScmSecretRef), &sec); err != nil {
		return nil, fmt.Errorf("pendingEvents: get scm secret %s: %w", proj.Spec.ScmSecretRef, err)
	}
	reader, err := s.cfg.ReaderFor(proj.Spec.Scm.Provider, string(sec.Data["token"]))
	if err != nil {
		return nil, fmt.Errorf("pendingEvents: build scm reader: %w", err)
	}
	return reader, nil
}

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
func AppendTaskEvent(ctx context.Context, c client.Client, task *tatarav1.Task, ev tatarav1.TaskEvent) error {
	key := client.ObjectKeyFromObject(task)
	fresh := &tatarav1.Task{}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh = &tatarav1.Task{}
		if err := c.Get(ctx, key, fresh); err != nil {
			return err
		}
		fresh.Status.PendingEvents = appendEventCapped(fresh.Status.PendingEvents, ev, maxPendingEvents)
		return c.Status().Update(ctx, fresh)
	})
	if err != nil {
		return fmt.Errorf("webhook: append task event on %s: %w", task.Name, err)
	}
	*task = *fresh
	return nil
}

// ClearDeliveredEvents removes exactly the delivered events from
// task.Status.PendingEvents - a SET-DIFFERENCE keyed on (Kind, Repo, Number,
// At), inside RetryOnConflict, NEVER a blind PendingEvents = nil.
//
// Every RetryOnConflict attempt re-Gets the Task before subtracting, so a
// webhook that queues a NEW event between the caller's bundle render and this
// call is not lost: if that append lands (and commits) before this function's
// Update, the Update conflicts, the retry re-Gets the now-appended state, and
// the subtraction runs against a base that already contains the new event -
// which survives. Only events actually named in delivered are ever removed.
func ClearDeliveredEvents(ctx context.Context, c client.Client, task *tatarav1.Task, delivered []tatarav1.TaskEvent) error {
	key := client.ObjectKeyFromObject(task)
	fresh := &tatarav1.Task{}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh = &tatarav1.Task{}
		if err := c.Get(ctx, key, fresh); err != nil {
			return err
		}
		fresh.Status.PendingEvents = subtractEvents(fresh.Status.PendingEvents, delivered)
		return c.Status().Update(ctx, fresh)
	})
	if err != nil {
		return fmt.Errorf("webhook: clear delivered events on %s: %w", task.Name, err)
	}
	*task = *fresh
	return nil
}

// appendEventCapped appends ev to events, keeping at most max entries by
// dropping the oldest. It never mutates the input slice's backing array.
func appendEventCapped(events []tatarav1.TaskEvent, ev tatarav1.TaskEvent, max int) []tatarav1.TaskEvent {
	out := make([]tatarav1.TaskEvent, 0, len(events)+1)
	out = append(out, events...)
	out = append(out, ev)
	if max > 0 && len(out) > max {
		out = out[len(out)-max:]
	}
	return out
}

// eventKey is the delivery identity contract E.3's clear step keys on:
// (Kind, Repo, Number, At). At is normalized through Rfc3339Copy - the
// second-precision truncation the API server itself applies on a real
// round-trip - so a key computed from a freshly-constructed TaskEvent matches
// the same event read back after being persisted.
func eventKey(ev tatarav1.TaskEvent) [4]string {
	return [4]string{ev.Kind, ev.Repo, strconv.Itoa(ev.Number), ev.At.Rfc3339Copy().UTC().Format(time.RFC3339)}
}

// subtractEvents returns cur with every event whose key matches one in
// delivered removed. Pure set-difference; order of the survivors is
// preserved.
func subtractEvents(cur, delivered []tatarav1.TaskEvent) []tatarav1.TaskEvent {
	if len(delivered) == 0 {
		return cur
	}
	remove := make(map[[4]string]struct{}, len(delivered))
	for _, ev := range delivered {
		remove[eventKey(ev)] = struct{}{}
	}
	out := make([]tatarav1.TaskEvent, 0, len(cur))
	for _, ev := range cur {
		if _, ok := remove[eventKey(ev)]; ok {
			continue
		}
		out = append(out, ev)
	}
	return out
}
