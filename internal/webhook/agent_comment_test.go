package webhook

// Task 12 coverage for deliverAgentComment, the D4 narrow cross-kind path: an
// agent-authored comment (resolved via the operator's own ledger, never the
// actor login) may wake a DIFFERENT agent kind than the one that wrote it, and
// only that. Same-kind is refused BY CONSTRUCTION. Every landed round on a live
// conversational Task is counted (BotRounds), whether or not it crosses kinds.

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// TestAgentCommentTriggersOnlyAcrossKinds is the acceptance table for the
// maintainer's rule: a bot comment may trigger a DIFFERENT agent kind, never
// its own, and only on a Task in a live conversational state - but the round
// counter still counts a same-kind landing, because D7 tracks every
// agent-authored round on a live conversation, not only the ones that manage to
// open a fresh one.
func TestAgentCommentTriggersOnlyAcrossKinds(t *testing.T) {
	cases := []struct {
		name string
		// taskState is the Task's live/terminal state (v1alpha1.State*). parkReason,
		// when non-empty, parks the Task at that state via stage.Park - state is
		// then WHERE the Task parks, not a separate "conversing" holding stage: #521
		// dissolved that stage, so a parked Task's reacting kind resolves off
		// ParkedFromState instead.
		taskState  string
		parkReason string
		ledgerKind string // Comment.AgentKind stamped on the inbound comment's ledger entry
		// wantRoundBump: BotRounds increments. True whenever BOTH the author and
		// the reacting Task are resolved - i.e. the Task is genuinely live AND
		// the comment is a ledgered agent comment - regardless of whether it
		// crosses kinds: D7 counts every agent-authored round on a live
		// conversation.
		wantRoundBump bool
		// wantDeclineNotLive: operator_live_entry_declined_total{not-a-live-state}
		// increments. #521 collapsed the old separate "same-kind" decline label
		// into this generic one - deliverAgentComment now records it BOTH when
		// either kind fails to resolve at all AND when both resolve but are
		// equal (CrossKindTriggers returns false either way) - so it is no longer
		// a same-kind-specific signal, only "this comment triggered nothing".
		wantDeclineNotLive bool
		// wantPendingEvents: the comment lands in Task.status.PendingEvents.
		// ONLY true when the kinds resolved AND differ (a genuine cross-kind
		// wake) - never for same-kind, which must never queue the event at all
		// (CRITICAL 1, 2026-07-28 final review): PendingEvents is exactly what
		// the live-turn follow-up check (reconcilePodStage) drains on nothing
		// but non-empty, so a queued same-kind event would feed a live agent its
		// own comment as a follow-up turn.
		wantPendingEvents int
	}{
		{name: "reviewing same-kind: no cross-kind trigger, round still counted, event NEVER queued",
			taskState: tatarav1.StateAwaitingReview, ledgerKind: stage.AgentReview,
			wantRoundBump: true, wantDeclineNotLive: true, wantPendingEvents: 0},
		{name: "reviewing cross-kind: triggers a real live turn, round counted, event queued",
			taskState: tatarav1.StateAwaitingReview, ledgerKind: stage.AgentImplement,
			wantRoundBump: true, wantDeclineNotLive: false, wantPendingEvents: 1},
		{name: "parked awaiting-human cross-kind: round counted, event queued, structurally cannot un-park off a bot event",
			taskState: tatarav1.StateRefined, parkReason: stage.ReasonAwaitingHuman, ledgerKind: stage.AgentReview,
			wantRoundBump: true, wantDeclineNotLive: false, wantPendingEvents: 1},
		{name: "parked stage-deadline: not a live conversational state, no trigger, no round, no queue",
			taskState: tatarav1.StateRefined, parkReason: stage.ReasonStageDeadline, ledgerKind: stage.AgentReview,
			wantRoundBump: false, wantDeclineNotLive: true, wantPendingEvents: 0},
		{name: "delivered: settled, no trigger, no round, no queue",
			taskState: tatarav1.StateDone, ledgerKind: stage.AgentReview,
			wantRoundBump: false, wantDeclineNotLive: true, wantPendingEvents: 0},
		{name: "reviewing with no ledger entry: FAILS CLOSED, no trigger, no round, no queue",
			taskState: tatarav1.StateAwaitingReview, ledgerKind: "",
			wantRoundBump: false, wantDeclineNotLive: true, wantPendingEvents: 0},
		{name: "parked identity-unverified cross-kind: round counted, event queued, but NEVER unparked - re-entry needs a NON-BOT pendingEvent of any kind, and the operator's own agents are not that",
			taskState: tatarav1.StateRefined, parkReason: stage.ReasonIdentityUnverified, ledgerKind: stage.AgentReview,
			wantRoundBump: true, wantDeclineNotLive: false, wantPendingEvents: 1},
		// A Task that STARTS the delivery already live is deliberately NOT a case
		// here: under #521 there is no separate "conversing" state to distinguish
		// it by - it is simply refined/under-implementation/awaiting-review, which
		// every other row already covers. TestAgentComment_ConversingSameKind_NoFollowUpTurnNoIdleReset
		// below covers the same-kind-on-an-already-live-Task shape directly instead
		// of forcing it into this table's abstraction.
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := peTask("t-agent", tc.taskState, "")
			if tc.parkReason != "" {
				if err := stage.Park(task, tc.parkReason, time.Now()); err != nil {
					t.Fatalf("park: %v", err)
				}
			}
			comments := []tatarav1.Comment{
				{ExternalID: "500", Author: "tatara-bot", IsBot: true, AgentKind: tc.ledgerKind, CreatedAt: metav1.Now()},
			}
			iss := peIssue(7, task, comments...)
			task.Status.IssueRefs = []string{iss.Name}
			proj := peProject("tatara-bot", "maintainer")
			c := peClient(t, proj, peRepo(), task, iss)
			s := peServer(c, &stubSpiller{}, nil)

			ev := scm.WebhookEvent{
				IsComment: true, IssueRef: "o/r#7", Number: 7,
				ActorLogin: "tatara-bot", CommentID: 500, CommentBody: "an agent comment",
			}
			s.deliverAgentComment(context.Background(), *proj, peRepo(), ev)

			gotTask := getPETask(t, c, task.Name)
			wantRounds := 0
			if tc.wantRoundBump {
				wantRounds = 1
			}
			if gotTask.Status.BotRounds != wantRounds {
				t.Errorf("BotRounds = %d, want %d", gotTask.Status.BotRounds, wantRounds)
			}
			if got := len(gotTask.Status.PendingEvents); got != tc.wantPendingEvents {
				t.Errorf("len(PendingEvents) = %d, want %d - a same-kind agent comment must never reach a live pod as a follow-up turn", got, tc.wantPendingEvents)
			}
			wantDecline := 0.0
			if tc.wantDeclineNotLive {
				wantDecline = 1
			}
			if got := testutil.ToFloat64(s.cfg.Metrics.LiveEntryDeclinedCounter(proj.Name, "not-a-live-state")); got != wantDecline {
				t.Errorf("operator_live_entry_declined_total{not-a-live-state} = %v, want %v", got, wantDecline)
			}
		})
	}
}

// TestAgentComment_SameKind_BreaksSelfLoop is the discrimination proof required
// by this task: it proves the kind comparison, not just the shape of the code,
// by breaking CrossKindTriggers-equivalent behavior would be caught here. A
// review-kind Task in reviewing receiving its OWN kind's comment must NEVER
// enter (or re-enter) conversing - that is exactly the self-loop the 2026-06
// incident hit.
func TestAgentComment_SameKind_BreaksSelfLoop(t *testing.T) {
	task := peTask("t-selfloop", tatarav1.StateAwaitingReview, "")
	comments := []tatarav1.Comment{
		{ExternalID: "9", Author: "tatara-bot", IsBot: true, AgentKind: stage.AgentReview, CreatedAt: metav1.Now()},
	}
	iss := peIssue(7, task, comments...)
	task.Status.IssueRefs = []string{iss.Name}
	proj := peProject("tatara-bot", "maintainer")
	c := peClient(t, proj, peRepo(), task, iss)
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{
		IsComment: true, IssueRef: "o/r#7", Number: 7,
		ActorLogin: "tatara-bot", CommentID: 9, CommentBody: "review's own comment, echoed back",
	}
	s.deliverAgentComment(context.Background(), *proj, peRepo(), ev)

	gotTask := getPETask(t, c, task.Name)
	// Un-park/live-turn delivery never moves state under #521 (there is no
	// distinct "conversing" state left to slip into): the self-loop guard is
	// proven entirely by state staying put and nothing landing in PendingEvents.
	if gotTask.Status.State != tatarav1.StateAwaitingReview {
		t.Fatalf("state = %q, want unchanged (awaiting-review)", gotTask.Status.State)
	}
	if len(gotTask.Status.PendingEvents) != 0 {
		t.Fatalf("pendingEvents = %d, want 0 - a same-kind comment must never be queued at all", len(gotTask.Status.PendingEvents))
	}
}

// TestAgentComment_ConversingSameKind_NoFollowUpTurnNoIdleReset is the
// CRITICAL 1 discrimination proof (2026-07-28 final review): a LIVE conversing
// Task's own agent posting its own comment must not (a) land in PendingEvents
// - the only thing task_stage.go's conversing follow-up-turn check consults,
// so a queued same-kind event feeds the live pod its own comment as a further
// turn - and must not (b) reset ConversationLastEventAt, the conversing idle
// clock's ONLY base: resetting it on a self-authored round would keep a
// self-looping conversation permanently "not idle" and never release its
// concurrency slot, with no absolute lifetime ceiling by design (D6, which
// assumes the clock is real).
//
// This is the exact shape of the 2026-06 forty-duplicate-comment incident:
// task_types.go's own doc comment says the clock is re-stamped "on every
// non-bot event" - this test proves the code now matches that, where the
// unfixed code queued the event and reset the clock on every pass.
func TestAgentComment_ConversingSameKind_NoFollowUpTurnNoIdleReset(t *testing.T) {
	before := metav1.NewTime(time.Now().Add(-30 * time.Minute).Truncate(time.Second))
	task := peTask("t-conversing-selfloop", tatarav1.StateRefined, "")
	task.Status.ConversationLastEventAt = &before
	comments := []tatarav1.Comment{
		// AgentImplement is the reacting kind a refined, kind=implement Task
		// resolves to (stage.AgentKindFor(StateRefined, "implement")) - clarify
		// was folded into implement by #521, so this is the implement agent's
		// OWN comment landing back on its own live conversation.
		{ExternalID: "42", Author: "tatara-bot", IsBot: true, AgentKind: stage.AgentImplement, CreatedAt: metav1.Now()},
	}
	iss := peIssue(7, task, comments...)
	task.Status.IssueRefs = []string{iss.Name}
	proj := peProject("tatara-bot", "maintainer")
	c := peClient(t, proj, peRepo(), task, iss)
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{
		IsComment: true, IssueRef: "o/r#7", Number: 7,
		ActorLogin: "tatara-bot", CommentID: 42, CommentBody: "clarify's own comment, echoed back",
	}
	s.deliverAgentComment(context.Background(), *proj, peRepo(), ev)

	gotTask := getPETask(t, c, task.Name)
	if len(gotTask.Status.PendingEvents) != 0 {
		t.Fatalf("SELF-LOOP: pendingEvents = %d, want 0 - the live pod would take this as a follow-up turn on its own comment", len(gotTask.Status.PendingEvents))
	}
	if gotTask.Status.ConversationLastEventAt == nil || !gotTask.Status.ConversationLastEventAt.Time.Equal(before.Time) {
		got := "nil"
		if gotTask.Status.ConversationLastEventAt != nil {
			got = gotTask.Status.ConversationLastEventAt.String()
		}
		t.Fatalf("IDLE CLOCK RESET BY A BOT EVENT: ConversationLastEventAt = %s, want unchanged %s - the idle backstop is disabled for exactly this loop shape", got, before.Time)
	}
	// The round is still counted (D7): observation only, never acted on.
	if gotTask.Status.BotRounds != 1 {
		t.Fatalf("BotRounds = %d, want 1 - the round must still be counted even though nothing else fires", gotTask.Status.BotRounds)
	}
}

// TestAgentComment_UnresolvableLedger_TriggersNothing is the second required
// discrimination proof: a bot comment whose ExternalID carries no ledger entry
// (predates the feature, or a mirror resync lost it) must trigger nothing at
// all - not even the round counter - because the operator cannot vouch for who
// wrote it.
func TestAgentComment_UnresolvableLedger_TriggersNothing(t *testing.T) {
	task := peTask("t-unresolvable", tatarav1.StateAwaitingReview, "")
	// The Issue mirror carries NO comment with ExternalID "77": a bot comment
	// with no ledger entry at all.
	iss := peIssue(7, task)
	task.Status.IssueRefs = []string{iss.Name}
	proj := peProject("tatara-bot", "maintainer")
	c := peClient(t, proj, peRepo(), task, iss)
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{
		IsComment: true, IssueRef: "o/r#7", Number: 7,
		ActorLogin: "tatara-bot", CommentID: 77, CommentBody: "an untraceable bot comment",
	}
	s.deliverAgentComment(context.Background(), *proj, peRepo(), ev)

	gotTask := getPETask(t, c, task.Name)
	if gotTask.Status.State != tatarav1.StateAwaitingReview {
		t.Fatalf("state = %q, want unchanged (awaiting-review) - an unresolvable ledger entry must trigger nothing", gotTask.Status.State)
	}
	if gotTask.Status.BotRounds != 0 {
		t.Fatalf("BotRounds = %d, want 0 - an unresolvable comment must not even be counted as a round", gotTask.Status.BotRounds)
	}
	if len(gotTask.Status.PendingEvents) != 0 {
		t.Fatalf("pendingEvents = %d, want 0", len(gotTask.Status.PendingEvents))
	}
}

// TestHumanCommentResetsBotRounds: a Task mid-streak (BotRounds==4) receiving a
// non-bot comment through the ordinary deliverPendingEvent path ends with
// BotRounds==0 - any human comment ends the consecutive-agent-round streak.
func TestHumanCommentResetsBotRounds(t *testing.T) {
	task := peTask("t-human-reset", tatarav1.StateRefined, "")
	task.Status.BotRounds = 4
	iss := peIssue(7, task)
	task.Status.IssueRefs = []string{iss.Name}
	proj := peProject("tatara-bot", "maintainer")
	c := peClient(t, proj, peRepo(), task, iss)
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{
		IsComment: true, IssueRef: "o/r#7", Number: 7,
		ActorLogin: "maintainer", CommentID: 200, CommentBody: "a human weighs in",
	}
	s.deliverPendingEvent(context.Background(), *proj, peRepo(), ev)

	gotTask := getPETask(t, c, task.Name)
	if gotTask.Status.BotRounds != 0 {
		t.Fatalf("BotRounds = %d, want 0 - a human comment must reset the consecutive-round streak", gotTask.Status.BotRounds)
	}
}
