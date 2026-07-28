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
// Four things happen, in order:
//  1. the comment is mirrored onto the owning Issue/MergeRequest CR
//     immediately (does not wait for the sweep's cadence sync);
//  2. a non-bot event is queued onto the owning Task's pendingEvents,
//     capped and drop-oldest;
//  3. if the comment landed on an Issue this Task owns, that Issue's whole
//     thread is re-synced from the forge right now (fix M11, decision D2), so
//     the mirror both sides of the approval gate read is current instead of up
//     to a day stale;
//  4. a parked Task with an F.6 comment-driven re-entry rule (awaiting-human,
//     backlog-sweep, identity-unverified) is driven through the ONE shared
//     ApplyUnpark, so a maintainer's reply moves it in one comment instead of
//     waiting on the project reconcile cadence.
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

	// D2: the comment mirror is the ONLY place both the turn-0 bundle's
	// external_id and the operator's own citation check read from, and NOTHING
	// else on this path writes it (mirror_refresh.go touches Body/Title only,
	// and the parked mirror cadence is daily). One forge read here - exactly the
	// read the deleted identity-unverified limb already paid for, now bought for
	// ANY live or parked Task rather than only parked(identity-unverified) - is
	// what makes the cited comment visible to BOTH sides of the gate.
	//
	// kind, not ev.Kind: scm.WebhookEvent.Kind is the coarse delivery kind
	// ("issue"/"mr"/"push"). kind is the mirror kind resolveMirrorTarget already
	// derived, and it is the SAME value stamped on taskEv.Kind, which is what
	// the grammar's own issue_comment check reads.
	if kind == "issue_comment" && ev.Number > 0 && controller.TaskOwnsIssue(task, repo.Name, ev.Number) {
		s.syncOwnedIssueThread(ctx, &proj, task, repo.Name, ev.Number)
	}

	if task.Status.Stage == tatarav1.StageParked &&
		(task.Status.StageReason == stage.ReasonAwaitingHuman ||
			task.Status.StageReason == stage.ReasonBacklogSweep ||
			task.Status.StageReason == stage.ReasonIdentityUnverified) {
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

	// trigger decides BOTH whether this round may open/wake a conversation AND
	// whether it is even allowed into PendingEvents at all. A same-kind comment
	// must never be queued: PendingEvents is what the conversing follow-up-turn
	// check (reconcilePodStage, task_stage.go) drains on nothing but non-empty,
	// so an unconditional enqueue here fed a live agent its own comment as a
	// follow-up turn - the 2026-06 forty-comment loop, reproduced by this
	// feature (2026-07-28 final review CRITICAL 1). The round is still counted
	// either way; only the enqueue is gated.
	trigger := controller.CrossKindTriggers(authorKind, reactingKind)

	taskEv := tatarav1.TaskEvent{
		At: metav1.Now(), Kind: kind, Repo: repo.Name, Number: ev.Number,
		Author: ev.ActorLogin, Body: ev.CommentBody,
	}
	rounds, err := controller.AppendAgentTaskEvent(ctx, s.cfg.Client, task, taskEv, trigger)
	if err != nil {
		s.log.ErrorContext(ctx, "pendingEvents: append agent task event failed", "error", err, "task", task.Name)
		return
	}

	if !trigger {
		// Same-kind: refused BY CONSTRUCTION. The round is already counted above;
		// nothing further happens - no conversing entry, no re-drive, and (as of
		// the fix above) no queued event for a live pod to mistake for a fresh
		// human turn.
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
	// stage.Unpark's F.6 comment-driven rules (driveCommentUnpark's target:
	// awaiting-human, backlog-sweep, identity-unverified) are NEVER called from
	// this path: every one of them structurally requires hasNonBotEvent - a
	// GENUINE human-authored pendingEvent - and a bot-authored event can never
	// satisfy that (by construction, not by an extra check here), so calling them
	// would only ever decline. The on-demand issue sync is skipped here for the
	// same reason: nothing on this path can approve anything, so there is no
	// citation to make visible and no forge read worth buying. A parked Task's
	// queued event rides along, counted above, and waits for a genuine human
	// comment or the daily sweep.
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

// driveCommentUnpark is THE F.6 comment-driven re-entry, for all three reasons
// that have one: parked(awaiting-human), parked(backlog-sweep) and
// parked(identity-unverified). A non-bot pendingEvent (already enqueued above)
// may promote them PROMPTLY, instead of waiting on the project reconcile cadence.
//
// identity-unverified used to have a bespoke limb of its own here, on the
// reasoning that it needed a re-run grammar the shared path could not compute.
// It did not need one at all: no grammar verdict feeds stage.Unpark any more,
// so a non-bot comment on a parked(identity-unverified) Task opens a
// CONVERSATION (conversing, room permitting) and the approval decision itself
// happens later, live, in restapi's verifyApprovalScope on a
// submit_outcome(decision=implement). The shared path is also strictly better
// on the axis that limb existed for: its in-loop Get uses the uncached
// APIReader, which is exactly the read that lost the 2026-07-27 cache race. One
// path, one maxOpenTasks re-check at re-entry (H8: a promotion is not a mint),
// one set of decline metrics. The project-reconcile driveUnparks loop backstops
// this.
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
	// backlog-sweep never consults ConversingHasRoom, but awaiting-human AND
	// identity-unverified both do (controller.NeedsConversingRoom is the single
	// definition, shared with driveUnparks). Gating on awaiting-human alone
	// would leave the identity-unverified conversing edge STRUCTURALLY INERT on
	// this webhook fast path. A transient List failure degrades to "no room"
	// (the safe fallback) rather than aborting this comment-driven unpark
	// entirely (2026-07-28 security review IMPORTANT 4).
	conversingRoom := false
	if controller.NeedsConversingRoom(task.Status.StageReason) {
		room, roomErr := controller.ConversingHasRoom(ctx, s.reader(), proj)
		if roomErr != nil {
			s.log.ErrorContext(ctx, "pendingEvents: conversing capacity check failed; treating as no room", "error", roomErr, "task", task.Name)
		} else {
			conversingRoom = room
		}
	}
	target, decline, err := controller.ApplyUnpark(ctx, s.cfg.Client, s.cfg.APIReader, proj, task, active, maxOpen, conversingRoom, time.Now())
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
		// Both GUARD and RULE declines are logged here (driveUnparks surfaces
		// only GUARD plus the one no-conversing-room rule decline): this fires
		// once, in direct reaction to a human action, where silence - of either
		// kind - is anomalous.
		s.log.InfoContext(ctx, "pendingEvents: comment-driven unpark declined",
			"action", "pending_event_unpark_declined", "task", task.Name, "stage_reason", task.Status.StageReason,
			"decline_kind", string(decline))
		return
	}
	s.log.InfoContext(ctx, "pendingEvents: unparked task on human comment",
		"action", "pending_event_unpark", "task", task.Name, "stage", target, "reason_from", task.Status.StageReason)
}

// syncOwnedIssueThread is D2's one forge read: it re-syncs the owned Issue's
// thread from the forge onto the mirror, NOW, on the comment that triggered it.
//
// It is not an optimisation. Issue.Status.Comments is the single field BOTH
// halves of the approval gate read: the agent reads the comment's external_id
// out of the turn-0 bundle rendered from it, and the operator re-verifies the
// agent's citation against it. The webhook's own AppendCommentToMirror covers
// only THIS comment; the rest of the thread converges on a DAILY cadence. A
// Task whose mirror is a day stale therefore has an agent citing ids that are
// not there yet.
//
// Every failure here is best-effort and LOUD: the unpark below still queues and
// drives the event and the daily cadence still converges, but a silent miss
// means the agent cannot cite and the gate refuses.
func (s *Server) syncOwnedIssueThread(ctx context.Context, proj *tatarav1.Project, task *tatarav1.Task, repoRef string, number int) {
	key := controller.IssueKey(repoRef, number)
	fail := func(msg string, err error) {
		obs.MirrorWriteDroppedTotal.WithLabelValues(proj.Name, "Issue", "issue_sync").Inc()
		attrs := []any{"action", "pending_event_issue_sync_failed",
			"task", task.Name, "project", proj.Name, "issue", key}
		if err != nil {
			// The no-Spiller branch has no error to report; an "error": null field
			// on a JSON log line reads like a nil deref that was swallowed.
			attrs = append(attrs, "error", err)
		}
		s.log.ErrorContext(ctx, "pendingEvents: "+msg+"; the cited comment may not be visible to the gate", attrs...)
	}
	sp := s.cfg.SpillerFor(proj)
	if sp == nil {
		fail("no Spiller configured; on-demand issue sync skipped", nil)
		return
	}
	reader, err := s.scmReader(ctx, proj)
	if err != nil {
		fail("build scm reader failed", err)
		return
	}
	if err := controller.SyncIssueOnDemand(ctx, s.cfg.Client, sp, reader, proj, key); err != nil {
		fail("on-demand issue sync failed", err)
		return
	}
	s.log.InfoContext(ctx, "pendingEvents: synced the owned issue thread on demand",
		"action", "pending_event_issue_synced", "task", task.Name, "project", proj.Name, "issue", key)
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
