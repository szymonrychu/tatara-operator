package webhook

// Task 12 coverage for deliverAgentComment, the D4 narrow cross-kind path: an
// agent-authored comment (resolved via the operator's own ledger, never the
// actor login) may wake a DIFFERENT agent kind than the one that wrote it, and
// only that. Same-kind is refused BY CONSTRUCTION. Every landed round on a live
// conversational Task is counted (BotRounds), whether or not it crosses kinds.

import (
	"context"
	"testing"

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
		name       string
		taskStage  string
		taskReason string
		ledgerKind string // Comment.AgentKind stamped on the inbound comment's ledger entry
		// wantConverse: the Task actually reaches StageConversing as a DIRECT
		// result of this one delivery. Only true for a LIVE-STAGE source
		// (clarifying/reviewing) crossing kinds - EnterConversing's own eligible-
		// stage set (Task 9) never included parked, and Task 12 does not widen it:
		// a parked Task's re-entry needs a GENUINE human event (stage.Unpark's
		// hasNonBotEvent gate), which a bot-authored event can never satisfy by
		// construction, not by an extra check here.
		wantConverse bool
		// wantRoundBump: BotRounds increments and the event lands in
		// pendingEvents. True whenever BOTH the author and the reacting Task are
		// resolved - i.e. the Task is genuinely live AND the comment is a
		// ledgered agent comment - regardless of whether it crosses kinds: D7
		// counts every agent-authored round on a live conversation.
		wantRoundBump bool
		// wantSameKindDecline: the "same-kind" ConversingEntryDeclined reason
		// fires - true only when both kinds resolved AND are EQUAL.
		wantSameKindDecline bool
	}{
		{name: "reviewing same-kind: no cross-kind trigger, round still counted",
			taskStage: tatarav1.StageReviewing, ledgerKind: stage.AgentReview,
			wantConverse: false, wantRoundBump: true, wantSameKindDecline: true},
		{name: "reviewing cross-kind: triggers a real conversing entry, round counted",
			taskStage: tatarav1.StageReviewing, ledgerKind: stage.AgentImplement,
			wantConverse: true, wantRoundBump: true, wantSameKindDecline: false},
		{name: "parked awaiting-human cross-kind: round counted, structurally cannot un-park off a bot event",
			taskStage: tatarav1.StageParked, taskReason: stage.ReasonAwaitingHuman, ledgerKind: stage.AgentReview,
			wantConverse: false, wantRoundBump: true, wantSameKindDecline: false},
		{name: "parked stage-deadline: not a live conversational state, no trigger, no round",
			taskStage: tatarav1.StageParked, taskReason: stage.ReasonStageDeadline, ledgerKind: stage.AgentReview,
			wantConverse: false, wantRoundBump: false, wantSameKindDecline: false},
		{name: "delivered: settled, no trigger, no round",
			taskStage: tatarav1.StageDelivered, ledgerKind: stage.AgentReview,
			wantConverse: false, wantRoundBump: false, wantSameKindDecline: false},
		{name: "reviewing with no ledger entry: FAILS CLOSED, no trigger, no round",
			taskStage: tatarav1.StageReviewing, ledgerKind: "",
			wantConverse: false, wantRoundBump: false, wantSameKindDecline: false},
		{name: "parked identity-unverified cross-kind: round counted, but NEVER unparked - the only re-entry path runs the C.6 grammar, which a bot comment must never feed",
			taskStage: tatarav1.StageParked, taskReason: stage.ReasonIdentityUnverified, ledgerKind: stage.AgentReview,
			wantConverse: false, wantRoundBump: true, wantSameKindDecline: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := peTask("t-agent", tc.taskStage, tc.taskReason)
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
			gotConverse := gotTask.Status.Stage == tatarav1.StageConversing
			if gotConverse != tc.wantConverse {
				t.Errorf("stage = %q, converse = %v, want converse = %v", gotTask.Status.Stage, gotConverse, tc.wantConverse)
			}
			wantRounds := 0
			if tc.wantRoundBump {
				wantRounds = 1
			}
			if gotTask.Status.BotRounds != wantRounds {
				t.Errorf("BotRounds = %d, want %d", gotTask.Status.BotRounds, wantRounds)
			}
			if gotGauge := testutil.ToFloat64(s.cfg.Metrics.BotRoundsGauge(proj.Name)); tc.wantRoundBump && gotGauge != float64(wantRounds) {
				t.Errorf("operator_bot_rounds{%s} = %v, want %v", proj.Name, gotGauge, wantRounds)
			}
			wantSameKind := 0.0
			if tc.wantSameKindDecline {
				wantSameKind = 1
			}
			if got := testutil.ToFloat64(s.cfg.Metrics.ConversingEntryDeclinedCounter(proj.Name, "same-kind")); got != wantSameKind {
				t.Errorf("operator_conversing_entry_declined_total{same-kind} = %v, want %v", got, wantSameKind)
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
	task := peTask("t-selfloop", tatarav1.StageReviewing, "")
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
	if gotTask.Status.Stage == tatarav1.StageConversing {
		t.Fatalf("SELF-LOOP: a review-kind comment on a reviewing Task must never open a conversation, got stage=%q", gotTask.Status.Stage)
	}
	if gotTask.Status.Stage != tatarav1.StageReviewing {
		t.Fatalf("stage = %q, want unchanged (reviewing)", gotTask.Status.Stage)
	}
}

// TestAgentComment_UnresolvableLedger_TriggersNothing is the second required
// discrimination proof: a bot comment whose ExternalID carries no ledger entry
// (predates the feature, or a mirror resync lost it) must trigger nothing at
// all - not even the round counter - because the operator cannot vouch for who
// wrote it.
func TestAgentComment_UnresolvableLedger_TriggersNothing(t *testing.T) {
	task := peTask("t-unresolvable", tatarav1.StageReviewing, "")
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
	if gotTask.Status.Stage != tatarav1.StageReviewing {
		t.Fatalf("stage = %q, want unchanged (reviewing) - an unresolvable ledger entry must trigger nothing", gotTask.Status.Stage)
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
	task := peTask("t-human-reset", tatarav1.StageConversing, "")
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
