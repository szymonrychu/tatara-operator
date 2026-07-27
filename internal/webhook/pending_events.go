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
	"github.com/szymonrychu/tatara-operator/internal/obs"
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

	// A folded pull_request_review carries review.id, NOT a comment id (F5-2):
	// key the mirror comment and the TaskEvent on the review so multiple folded
	// reviews neither collide on ExternalID "0" nor mis-tag as a plain comment.
	externalID := strconv.Itoa(ev.CommentID)
	if ev.IsReview {
		kind = "mr_review"
		if ev.ReviewID != "" {
			externalID = "review-" + ev.ReviewID
		}
	}

	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	isBot := botLogin != "" && ev.ActorLogin != "" && ev.ActorLogin == botLogin

	sp := s.cfg.SpillerFor(&proj)
	if sp != nil {
		cmt := tatarav1.Comment{
			ExternalID: externalID,
			Author:     ev.ActorLogin,
			Body:       ev.CommentBody,
			CreatedAt:  metav1.Now(),
			IsBot:      isBot,
		}
		if err := controller.AppendCommentToMirror(ctx, s.cfg.Client, sp, obj, cmt); err != nil {
			// The function continues past this and still enqueues the TaskEvent
			// below, so a HUMAN comment is lost from the mirror while the unpark
			// event still fires - the agent gets unparked by a comment its bundle
			// does not contain.
			obs.MirrorWriteDroppedTotal.WithLabelValues(proj.Name, kind, "comment_append").Inc()
			s.log.WarnContext(ctx, "pendingEvents: mirror comment append failed; comment lost from mirror but the unpark event still fires",
				"error", err, "kind", kind, "project", proj.Name)
		}
	} else {
		// Same consequence as the AppendCommentToMirror error above: no Spiller
		// means the comment never lands on the mirror, but the TaskEvent below is
		// still enqueued and can still unpark the Task.
		obs.MirrorWriteDroppedTotal.WithLabelValues(proj.Name, kind, "comment_append").Inc()
		s.log.WarnContext(ctx, "pendingEvents: no Spiller configured; mirror comment append skipped, comment lost from mirror but the unpark event still fires",
			"kind", kind, "project", proj.Name)
	}

	// E.3 enqueue filter: a BOT-authored event is NEVER enqueued. Without it the
	// operator's own park comment would land in pendingEvents and un-park the
	// Task the operator just parked - a fully autonomous
	// hallucinated-approval-to-prod path.
	if isBot {
		return
	}

	task := s.resolveOwningTask(ctx, &proj, repo, obj, ev)
	if task == nil {
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
	if err := controller.AppendTaskEvent(ctx, s.cfg.Client, task, taskEv); err != nil {
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
	// A LIVE conversational stage. The event is already queued above, but a
	// clarifying/reviewing Task whose pod has finished its turn is deaf to it
	// until something moves: conversing is what gives the comment a live agent on
	// the other end and an idle clock that resets per event.
	s.driveConversingEntry(ctx, &proj, task)
}

// deliverAgentComment is the D4 cross-kind path: an agent-authored comment on an
// owned thread, delivered to a DIFFERENT agent kind than the one that wrote it.
//
// Everything about it is deliberately narrower than deliverPendingEvent:
//   - the authoring kind comes from the operator's own comment ledger, never from
//     the actor login, and an unresolved author is refused;
//   - the reacting kind comes from the Task's stage, so a settled, delivered or
//     merging Task starts nothing (decision D2);
//   - same-kind is refused BY CONSTRUCTION, so an agent can never wake itself -
//     though the round still gets COUNTED (below), because the D7 counter tracks
//     every agent-authored round on a live conversation, not only the ones that
//     happen to open a fresh one;
//   - the C.6 approval grammar is never run and no un-park rule is evaluated: a
//     bot comment cannot approve anything, ever.
//
// The consecutive-round counter (AppendAgentTaskEvent) is maintained here and is
// NEVER acted on (decision D7): there is no ping-pong cap.
func (s *Server) deliverAgentComment(ctx context.Context, proj tatarav1.Project, repo *tatarav1.Repository, ev scm.WebhookEvent) {
	if repo == nil || !ev.IsComment {
		return
	}
	obj, kind := s.resolveMirrorTarget(ctx, repo, ev)
	if obj == nil {
		return
	}
	task := s.resolveOwningTask(ctx, &proj, repo, obj, ev)
	if task == nil {
		return
	}

	authorKind := controller.ResolveCommentAgentKind(obj, strconv.Itoa(ev.CommentID))
	reactingKind := controller.ReactingAgentKind(task)
	if authorKind == "" || reactingKind == "" {
		// FAIL CLOSED on either side: an unresolved author is a comment whose
		// provenance the operator cannot vouch for (no ledger entry, predates the
		// ledger, or was never operator-authored at all), and an unresolved
		// reacting kind is a Task that is not in a live conversational state
		// (settled, delivered, merging, or parked for a non-conversational reason).
		// Neither case counts as a round: there is nothing here the operator can
		// attribute to an agent landing on a live conversation.
		s.log.InfoContext(ctx, "pendingEvents: agent comment unresolved; refusing",
			"action", "agent_comment_declined", "task", task.Name, "project", proj.Name,
			"author_kind", authorKind, "reacting_kind", reactingKind)
		s.cfg.Metrics.ConversingEntryDeclined(proj.Name, "unresolved")
		return
	}

	taskEv := tatarav1.TaskEvent{
		At: metav1.Now(), Kind: kind, Repo: repo.Name, Number: ev.Number,
		Author: ev.ActorLogin, Body: ev.CommentBody,
	}
	rounds, err := controller.AppendAgentTaskEvent(ctx, s.cfg.Client, task, taskEv)
	if err != nil {
		s.log.ErrorContext(ctx, "pendingEvents: append agent task event failed", "error", err, "task", task.Name)
		return
	}
	s.cfg.Metrics.SetBotRounds(proj.Name, float64(rounds))

	if !controller.CrossKindTriggers(authorKind, reactingKind) {
		// Same-kind: refused BY CONSTRUCTION. The round is already counted above;
		// nothing further happens - no conversing entry, no re-drive.
		s.log.InfoContext(ctx, "pendingEvents: agent comment landed same-kind; round counted, no trigger",
			"action", "agent_comment_same_kind", "task", task.Name, "project", proj.Name,
			"author_kind", authorKind, "reacting_kind", reactingKind, "bot_rounds", rounds)
		s.cfg.Metrics.ConversingEntryDeclined(proj.Name, "same-kind")
		return
	}

	s.log.InfoContext(ctx, "pendingEvents: queued a cross-kind agent event",
		"action", "agent_comment_queued", "task", task.Name, "project", proj.Name,
		"author_kind", authorKind, "reacting_kind", reactingKind, "bot_rounds", rounds)

	// driveConversingEntry is the ONLY re-entry attempted here, deliberately.
	// stage.Unpark's F.6 rules (driveCommentUnpark's target, for parked(awaiting-
	// human)) and reverifyParked's C.6 grammar (parked(identity-unverified)) are
	// NEVER called from this path: both structurally require hasNonBotEvent - a
	// GENUINE human-authored pendingEvent - and a bot-authored event can never
	// satisfy that (by construction, not by an extra check here), so calling them
	// would only ever decline. reverifyParked in particular must never even be
	// attempted: it feeds the comment TEXT into the C.6 approval grammar, and a
	// bot-authored comment must never be fed into that grammar - "a bot comment
	// cannot approve anything, ever". A parked Task's queued event rides along,
	// counted above, and waits for a genuine human comment or the daily sweep.
	// driveConversingEntry itself is a harmless no-op on a non-live-stage Task
	// (ConversingEntryEligible), so this call is exactly as narrow for a parked
	// Task as it is active for one in clarifying/reviewing.
	s.driveConversingEntry(ctx, &proj, task)
}

// driveConversingEntry moves a live clarifying/reviewing Task into conversing on
// a qualifying comment, so the comment reaches an agent instead of only sitting
// in pendingEvents. Best-effort, like every other side effect in this file: a
// failure is logged and never surfaced to the SCM as a non-2xx.
func (s *Server) driveConversingEntry(ctx context.Context, proj *tatarav1.Project, task *tatarav1.Task) {
	// The cheap check FIRST: most comments land on a Task that could never
	// qualify (delivered, merging, implementing, parked-for-a-non-conversational
	// reason, ...), and this is a free map lookup versus ConversingHasRoom's
	// namespace List - which used to run unconditionally, on EVERY webhook
	// comment, before this stage even mattered (2026-07-28 security review
	// IMPORTANT 5).
	if !controller.ConversingEntryEligible(task.Status.Stage) {
		return
	}
	room, err := controller.ConversingHasRoom(ctx, s.reader(), proj)
	if err != nil {
		s.log.ErrorContext(ctx, "pendingEvents: conversing capacity check failed", "error", err, "task", task.Name)
		return
	}
	if !room {
		// The ceiling is real and the event is NOT lost: it stays in pendingEvents
		// and rides into the Task's next turn whenever one happens. The metric is
		// what makes the ceiling visible to the operator (there is no
		// acknowledgement layer, per decision D5).
		s.log.InfoContext(ctx, "pendingEvents: conversing ceiling reached; the event stays queued",
			"action", "conversing_entry_declined", "task", task.Name,
			"project", proj.Name, "decline", "over-ceiling")
		s.cfg.Metrics.ConversingEntryDeclined(proj.Name, "over-ceiling")
		return
	}
	sp := s.cfg.SpillerFor(proj)
	mrs, err := controller.LoadTaskMRsFor(ctx, s.reader(), task)
	if err != nil {
		s.log.ErrorContext(ctx, "pendingEvents: load owned MRs failed", "error", err, "task", task.Name)
		return
	}
	entered, err := controller.EnterConversing(ctx, s.cfg.Client, sp, s.cfg.Metrics, proj, task, mrs, time.Now())
	if err != nil {
		s.log.ErrorContext(ctx, "pendingEvents: conversing entry failed", "error", err, "task", task.Name)
		return
	}
	if !entered {
		return
	}
	s.log.InfoContext(ctx, "pendingEvents: opened a conversation on a human comment",
		"action", "pending_event_conversing", "task", task.Name, "project", proj.Name)
}

// resolveOwningTask maps a mirror CR onto the Task the pending event belongs
// to. The normal path is the mirror's controller ownerRef. The fallback is the
// 2026-07-19 deadlock fix (task mt-r-tatara-operator-388-...): a mint
// interrupted between the Task create and the bind leaves the mirror CR an
// UNOWNED stub, and the old silent early return here dropped the very human
// comment the watchdog's park notice asked for - the parked Task stayed parked
// forever. When the mirror has no controller owner, resolve the Task by the
// SAME deterministic natural key intake mints under (IntakeTaskName with the
// sweep kind for this artifact type) - the identity that produced this exact
// stub. Only a LIVE Task is returned: parked counts as live (delivering the
// event is precisely what can unpark it); failed/rejected/delivered do not
// (they have no F.6 re-entry). A miss keeps the early return, but at INFO
// instead of silently.
func (s *Server) resolveOwningTask(ctx context.Context, proj *tatarav1.Project,
	repo *tatarav1.Repository, obj client.Object, ev scm.WebhookEvent) *tatarav1.Task {

	if ownerName, ok := own.ControllerOwner(obj); ok {
		task := &tatarav1.Task{}
		if err := s.cfg.Client.Get(ctx, objKey(s.cfg.Namespace, ownerName), task); err != nil {
			if !apierrors.IsNotFound(err) {
				s.log.ErrorContext(ctx, "pendingEvents: get owning task failed", "error", err, "task", ownerName)
			}
			return nil
		}
		return task
	}

	intakeKind := controller.SweepIssueKind
	if ev.IsPR {
		intakeKind = controller.SweepReviewKind
	}
	name := tatarav1.IntakeTaskName(proj.Name, intakeKind, repo.Name, ev.Number)
	task := &tatarav1.Task{}
	if err := s.cfg.Client.Get(ctx, objKey(s.cfg.Namespace, name), task); err != nil {
		if apierrors.IsNotFound(err) {
			s.log.InfoContext(ctx, "pendingEvents: mirror has no controller owner and no intake task matches; dropping event",
				"action", "pending_event_owner_fallback_miss", "mirror", obj.GetName(), "task", name,
				"repo", repo.Name, "number", ev.Number)
		} else {
			s.log.ErrorContext(ctx, "pendingEvents: get fallback intake task failed", "error", err, "task", name)
		}
		return nil
	}
	if task.DeletionTimestamp != nil || task.Status.Stage == tatarav1.StageFailed ||
		task.Status.Stage == tatarav1.StageRejected || task.Status.Stage == tatarav1.StageDelivered {
		s.log.InfoContext(ctx, "pendingEvents: mirror has no controller owner and its intake task is not live; dropping event",
			"action", "pending_event_owner_fallback_miss", "mirror", obj.GetName(), "task", name,
			"stage", task.Status.Stage)
		return nil
	}
	// The natural key encodes (project, kind, repo, number), but the Task under
	// that name is only trusted if its OWN source identity agrees with the
	// event: a Task minted by something other than intake (or with a stale
	// source) must not receive deliveries it never asked for. Source carries no
	// repo field (IssueRef is a URL), so number is the comparable part.
	if task.Spec.Source == nil || task.Spec.Source.Number != ev.Number {
		s.log.InfoContext(ctx, "pendingEvents: mirror has no controller owner and its natural-key task does not match the event source; dropping event",
			"action", "pending_event_owner_fallback_miss", "mirror", obj.GetName(), "task", name,
			"repo", repo.Name, "number", ev.Number)
		return nil
	}
	s.log.InfoContext(ctx, "pendingEvents: mirror has no controller owner; routed to its intake task by natural key",
		"action", "pending_event_owner_fallback", "mirror", obj.GetName(), "task", name,
		"repo", repo.Name, "number", ev.Number, "stage", task.Status.Stage)
	return task
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
	// backlog-sweep never consults ConversingHasRoom (stage.go's ReasonBacklogSweep
	// branch), so the capacity List only runs for the one reason that needs it
	// here - awaiting-human - and a transient failure degrades to "no room"
	// (the safe fallback) rather than aborting this comment-driven unpark
	// entirely (2026-07-28 security review IMPORTANT 4).
	conversingRoom := false
	if task.Status.StageReason == stage.ReasonAwaitingHuman {
		room, roomErr := controller.ConversingHasRoom(ctx, s.reader(), proj)
		if roomErr != nil {
			s.log.ErrorContext(ctx, "pendingEvents: conversing capacity check failed; treating as no room", "error", roomErr, "task", task.Name)
		} else {
			conversingRoom = room
		}
	}
	target, decline, err := controller.ApplyUnpark(ctx, s.cfg.Client, s.cfg.APIReader, proj, task, active, maxOpen, false, conversingRoom, time.Now())
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
	passed, evidence, err := controller.ReVerifyParkedDetailed(ctx, s.cfg.Client, sp, reader, proj, task, ev, s.cfg.Metrics)
	if err != nil {
		s.log.ErrorContext(ctx, "pendingEvents: reverify parked failed", "error", err, "task", task.Name)
		return
	}

	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	// Task 9's conversing branch of the identity-unverified F.6 rule (a comment
	// that fails the grammar opens a conversation instead of a dead end) needs
	// the SAME ceiling answer ApplyUnpark's other two callers compute - reused
	// via controller.ConversingHasRoom, never reimplemented, off the uncached
	// reader like every other read on this path. A failure here degrades to "no
	// room" (the safe fallback) rather than returning early: this reverify's
	// whole point is that the ApprovalVerdict write below must land WHETHER OR
	// NOT the un-park succeeds, and bailing out here before that write would
	// throw away a genuine grammar pass over a transient capacity-check error
	// (2026-07-28 security review IMPORTANT 4).
	conversingRoom, err := controller.ConversingHasRoom(ctx, s.reader(), proj)
	if err != nil {
		s.log.ErrorContext(ctx, "pendingEvents: conversing capacity check failed; treating as no room", "error", err, "task", task.Name)
		conversingRoom = false
	}
	key := client.ObjectKeyFromObject(task)
	var declined controller.UnparkDecline
	updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		declined = controller.DeclineNone
		fresh := &tatarav1.Task{}
		if err := s.cfg.Client.Get(ctx, key, fresh); err != nil {
			return err
		}
		if fresh.Status.Stage != tatarav1.StageParked || fresh.Status.StageReason != stage.ReasonIdentityUnverified {
			declined = controller.DeclineGuard
			return nil // raced past this un-park by another writer already
		}
		// THE VERDICT IS PERSISTED WHETHER OR NOT THE UN-PARK SUCCEEDS. That is the
		// whole point: when the un-park loses a cache race, driveUnparks re-derives
		// the owned-Issue half from live state and re-enters using this record,
		// because it can never re-run the grammar itself.
		if passed {
			fresh.Status.ApprovalVerdict = verdictFrom(evidence, time.Now())
		}
		issues, err := s.loadOwnedIssues(ctx, fresh)
		if err != nil {
			return err
		}
		// MRs feed anyMerged() in the ReasonIdentityUnverified conversing branch's
		// kind=review guard (Task 9 IMPORTANT 2): without this, a kind=review Task
		// parked(identity-unverified) whose owned MR is already merged would still
		// open a conversing pod on the next stray comment - anyMerged(nil) is
		// always false, so the guard was structurally inert on this ONE path,
		// even though driveUnparks' ApplyUnpark (which does load MRs) enforced it
		// correctly (2026-07-28 security review NEW-2). Not a security bypass:
		// GUARD 1 still blocks review-kind from implementing/merging/approved -
		// the impact was bounded pod waste on an already-merged PR.
		mrs, err := controller.LoadTaskMRsFor(ctx, s.reader(), fresh)
		if err != nil {
			return err
		}
		// ActiveTasks/MaxOpenTasks are deliberately left unset: they are read ONLY
		// by ReasonBacklogSweep (stage.go), and this function only ever drives
		// ReasonIdentityUnverified - the guard above (fresh.Status.StageReason !=
		// stage.ReasonIdentityUnverified) already refused any other reason before
		// this point is reached. See unparkFires' field-by-field audit
		// (internal/controller/reaper.go) for the equivalent reasoning against
		// every UnparkInput field and all three production builders.
		target, code := stage.UnparkDetailed(stage.UnparkInput{
			Task:              fresh,
			Issues:            issues,
			MRs:               mrs,
			BotLogin:          botLogin,
			GrammarPassed:     passed,
			ConversingHasRoom: conversingRoom,
			Now:               time.Now(),
		})
		if target == "" {
			declined = controller.DeclineFor(code)
			// The verdict write still has to land, so this is an Update, not a
			// return: without it the backstop has nothing to retry with.
			if !passed {
				return nil
			}
			return s.cfg.Client.Status().Update(ctx, fresh)
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
		return
	}
	if declined != controller.DeclineNone {
		// NOT an error, and NOT silent. This call site fires in DIRECT reaction to a
		// maintainer comment on the single narrowest, highest-stakes re-entry rule
		// in the F.6 table. The bare `if !ok { return nil }` this replaces is what
		// made the 2026-07-27 stall undiagnosable: the approval label was visible on
		// the forge and no log line anywhere said why the Task had not moved.
		s.log.InfoContext(ctx, "pendingEvents: identity-unverified reverify declined",
			"action", "pending_event_unpark_declined", "task", task.Name,
			"stage_reason", stage.ReasonIdentityUnverified, "decline_kind", string(declined),
			"grammar_passed", passed)
		s.cfg.Metrics.UnparkDeclined(stage.ReasonIdentityUnverified, string(declined))
	}
}

// verdictFrom collapses the per-Issue approval evidence into the Task's single
// durable ApprovalVerdict. When several owned Issues approved, the NEWEST
// evidence wins: the verdict answers "has a maintainer approved this Task", and
// the newest approving comment is the one whose recency the backstop cares about.
// Auto-approval evidence carries no CommentID and that is recorded faithfully.
func verdictFrom(evidence map[string]*tatarav1.ApprovalEvidence, now time.Time) *tatarav1.ApprovalVerdict {
	var bestName string
	var best *tatarav1.ApprovalEvidence
	for name, e := range evidence {
		if e == nil {
			continue
		}
		if best == nil || e.CreatedAt.After(best.CreatedAt.Time) {
			best, bestName = e, name
		}
	}
	if best == nil {
		return nil
	}
	return &tatarav1.ApprovalVerdict{
		At:                metav1.NewTime(now),
		IssueRef:          bestName,
		CommentExternalID: best.CommentID,
		Author:            best.Login,
		Phrase:            best.Phrase,
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
		// UNCACHED (s.reader()), not s.cfg.Client. reverifyParked calls this
		// microseconds after VerifyApprovalDetailed's objbudget.FitIssue wrote
		// Status.Status="approved" to this very Issue CR, and a cached Get is not
		// guaranteed to observe a write made through the same client - the informer
		// converges only once the watch stream delivers it. A stale snapshot made
		// allApproved false and the un-park declined silently, permanently, for a
		// Task a maintainer had just approved (2026-07-27, gitlab helmfile#26). This
		// is the escape hatch, not the fix: the fix is the driveUnparks backstop.
		if err := s.reader().Get(ctx, objKey(s.cfg.Namespace, name), &iss); err != nil {
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
