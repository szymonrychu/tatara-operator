package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// RISK 1: reaper-phase-race-pod-blip (2026-07-11, live incident). The reaper
// deleted a LIVE wrapper pod mid-continuation because the old model let a Task
// look terminal while its pod was still working; one Task's pod churned through
// 6 UIDs in 24h with zero container crashes. conversing is NON-TERMINAL, so
// orphanReason's TaskDone gate never fires for it - and the pod's agent-kind
// label matches the stage's, so the superseded check never fires either.
func TestRisk_ReaperDoesNotCollectALiveConversingPod(t *testing.T) {
	task := conversingTask("c1", "infrastructure", time.Now())
	task.UID = "uid-1"
	task.Annotations = map[string]string{
		annTurnStartedAt: time.Now().Add(-1 * time.Minute).UTC().Format(time.RFC3339),
	}

	pod := &corev1.Pod{}
	pod.Namespace = "tatara"
	pod.Name = agent.PodName(task)
	pod.CreationTimestamp = metav1.NewTime(time.Now().Add(-10 * time.Minute))
	pod.Labels = map[string]string{
		agent.LabelTask:      task.Name,
		agent.LabelTaskUID:   string(task.UID),
		agent.LabelAgentKind: stage.AgentKindFor(tatarav1alpha1.StageConversing),
	}

	srv := &CallbackServer{ReaperGrace: time.Minute, IdlePodReapAfter: time.Hour}
	tasks := map[string]*tatarav1alpha1.Task{task.Name: task}

	if reason, orphan := srv.orphanReason(pod, tasks); orphan {
		t.Fatalf("the reaper would collect a live conversing pod: %q", reason)
	}
}

// RISK 1b: the same test with the pod's LABEL stamped for the previous stage.
// It MUST be reaped: a surviving clarifying-stage pod under the per-Task pod name
// would be silently reused by conversing, running the wrong kind, model and
// skills. The superseded check is what stops that, and it must keep working.
func TestRisk_ReaperStillCollectsASupersededPodUnderConversing(t *testing.T) {
	task := conversingTask("c2", "infrastructure", time.Now())
	task.UID = "uid-2"

	pod := &corev1.Pod{}
	pod.Namespace = "tatara"
	pod.Name = agent.PodName(task)
	pod.CreationTimestamp = metav1.NewTime(time.Now().Add(-10 * time.Minute))
	pod.Labels = map[string]string{
		agent.LabelTask:      task.Name,
		agent.LabelTaskUID:   string(task.UID),
		agent.LabelAgentKind: stage.AgentImplement, // stamped for a DIFFERENT stage
	}

	srv := &CallbackServer{ReaperGrace: time.Minute}
	tasks := map[string]*tatarav1alpha1.Task{task.Name: task}

	if _, orphan := srv.orphanReason(pod, tasks); !orphan {
		t.Fatal("a superseded pod survived under conversing: the next turn would run the wrong agent kind")
	}
}

// RISK 2: #83, "stop Conversation tasks re-posting Silent hold comments". An
// author-blind reactivation looped on the bot's OWN comment and posted 40+
// duplicate hold comments live in production. The cross-kind rule makes that
// impossible by construction, and BotRounds makes any residual cycle visible.
func TestRisk_AnAgentCommentNeverWakesItsOwnKind(t *testing.T) {
	task := &tatarav1alpha1.Task{}
	task.Status.Stage = tatarav1alpha1.StageReviewing
	reacting := ReactingAgentKind(task)
	if reacting != stage.AgentReview {
		t.Fatalf("ReactingAgentKind(reviewing) = %q, want review", reacting)
	}
	if CrossKindTriggers(stage.AgentReview, reacting) {
		t.Fatal("the review agent's own comment woke the review agent: this is the 2026-06 forty-comment loop")
	}
}

// RISK 2b (2026-07-28 final review CRITICAL 1): CrossKindTriggers alone is not
// the whole guarantee - the webhook's deliverAgentComment used to call
// AppendAgentTaskEvent (which queues the event into PendingEvents and, before
// this fix, reset the idle clock) BEFORE consulting CrossKindTriggers, so a
// same-kind comment was already queued by the time the same-kind refusal ran.
// task_stage.go's conversing follow-up-turn check keys on nothing but
// len(PendingEvents) > 0 - it cannot tell a genuine cross-kind wake from an
// agent's own comment reflected back at it. This proves the fix at the
// choke point itself: AppendAgentTaskEvent's enqueue parameter must gate the
// append, and the function must never stamp ConversationLastEventAt at all
// (only a genuine human comment, through AppendTaskEvent, may reset the idle
// clock - task_types.go's own doc comment says so).
func TestRisk_SameKindAgentEventNeverEntersPendingEventsOrResetsTheIdleClock(t *testing.T) {
	before := metav1.NewTime(time.Now().Add(-30 * time.Minute).Truncate(time.Second))
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-risk2b", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{Kind: "review", ProjectRef: "p", Goal: "g"},
		Status:     tatarav1alpha1.TaskStatus{ConversationLastEventAt: &before},
	}
	c := newMirrorClient(t, task)
	ctx := context.Background()

	ev := tatarav1alpha1.TaskEvent{Kind: "mr_comment", Repo: "r", Number: 1, Author: "tatara-bot", Body: "own comment, reflected"}
	// enqueue=false is what a same-kind CrossKindTriggers()==false result must
	// pass: reproduces the same-kind branch of deliverAgentComment.
	rounds, err := AppendAgentTaskEvent(ctx, c, task, ev, false)
	if err != nil {
		t.Fatalf("AppendAgentTaskEvent: %v", err)
	}
	if rounds != 1 {
		t.Fatalf("rounds = %d, want 1 - the round is still counted (D7)", rounds)
	}
	if len(task.Status.PendingEvents) != 0 {
		t.Fatalf("SELF-LOOP: PendingEvents = %d, want 0 - a same-kind event must never reach the follow-up-turn check", len(task.Status.PendingEvents))
	}
	if task.Status.ConversationLastEventAt == nil || !task.Status.ConversationLastEventAt.Time.Equal(before.Time) {
		t.Fatalf("IDLE CLOCK RESET BY A BOT EVENT: ConversationLastEventAt changed, want unchanged %s", before.Time)
	}

	// A CROSS-kind event (enqueue=true) DOES reach PendingEvents - that
	// delivery is the whole point of D4 - but it must ALSO never reset the
	// idle clock: only a human comment may.
	rounds, err = AppendAgentTaskEvent(ctx, c, task, ev, true)
	if err != nil {
		t.Fatalf("AppendAgentTaskEvent (cross-kind): %v", err)
	}
	if rounds != 2 {
		t.Fatalf("rounds = %d, want 2 (accumulates across the two calls)", rounds)
	}
	if len(task.Status.PendingEvents) != 1 {
		t.Fatalf("cross-kind event was not queued: PendingEvents = %d, want 1", len(task.Status.PendingEvents))
	}
	if task.Status.ConversationLastEventAt == nil || !task.Status.ConversationLastEventAt.Time.Equal(before.Time) {
		t.Fatalf("IDLE CLOCK RESET BY A CROSS-KIND BOT EVENT: ConversationLastEventAt changed, want unchanged %s", before.Time)
	}
}

// RISK 3: laneOccupancy frees terminal/Conversation lifecycle tasks (e19bedb).
// Conversation Tasks were counted against the per-repo concurrency lane FOREVER,
// which with maxPerRepo=1 wedged mrScan/issueScan recovery entirely (lanes at
// 29/10/6 against a cap of 1). The fix is cited by name in a current code
// comment: operator-laneoccupancy-starves-recovery-2026-06-15,
// internal/controller/queue_controller.go's queueTaskHoldsSlot doc.
//
// conversing must hold a slot WHILE conversing and RELEASE it the instant it
// parks. Both halves, or the leak comes back.
func TestRisk_ConversingHoldsALaneAndReleasesItOnPark(t *testing.T) {
	task := &tatarav1alpha1.Task{}
	task.Status.Stage = tatarav1alpha1.StageConversing
	if !queueTaskHoldsSlot(task) {
		t.Fatal("a conversing Task holds no slot: its pod runs off the MaxConcurrentAgents books")
	}
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = stage.ReasonAwaitingHuman
	if queueTaskHoldsSlot(task) {
		t.Fatal("a parked Task still holds a slot: this is operator-laneoccupancy-starves-recovery-2026-06-15")
	}
}

// RISK 4: "exclude Conversation lifecycle state from creation budget" (5b6a3a4).
// A Task idling in Conversation was double-counted against MaxOpenTasks and
// starved brainstorm/scan work, and the fix was an ad hoc, hand-maintained
// exclusion list. conversing must be counted exactly ONCE, by the closed
// StageActive predicate, with no bespoke list anywhere.
func TestRisk_ConversingIsCountedOnceByTheClosedPredicate(t *testing.T) {
	task := &tatarav1alpha1.Task{}
	task.Status.Stage = tatarav1alpha1.StageConversing
	if !StageActive(task) {
		t.Fatal("a conversing Task is not ACTIVE: maxOpenTasks would over-admit against it")
	}
	if tatarav1alpha1.TaskDone(task) {
		t.Fatal("a conversing Task is DONE: it would be counted as finished AND hold a pod")
	}
	// The whole point of the closed tables: no bespoke exclusion list exists to
	// drift. If conversing ever needs one, this assertion is where the argument
	// has to be made.
	if tatarav1alpha1.StagePodless(tatarav1alpha1.StageConversing) {
		t.Fatal("conversing is podless: it would be excluded from the pod accounting while running a pod")
	}
}

// RISK 5: fix(webhook) interjection redelivery (3fd9480). A GitHub webhook
// redelivery appended the SAME interjection twice into a live session and needed
// a body-equality dedup bolted on afterwards. The turn-boundary model makes the
// double-delivery harmless by construction - a redelivered event is rendered into
// the SAME bundle, not typed into a live PTY - but the operator must still never
// submit a second turn while one is in flight.
func TestRisk_RedeliveryDoesNotDoubleSubmitIntoALiveConversation(t *testing.T) {
	proj, task, r, sess := newConversingTurnFixture(t)
	task.Annotations = map[string]string{
		annStageTurn0:   turn0Marker(task),
		annCurrentTurn:  "turn-0",
		annTurnComplete: time.Now().UTC().Format(time.RFC3339),
	}
	ev := tatarav1alpha1.TaskEvent{
		At: metav1.Now(), Kind: "issue_comment", Repo: "helmfile", Number: 26,
		Author: "szymonrychu", Body: "same body twice",
	}
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{ev, ev}

	if _, err := r.reconcilePodStage(context.Background(), proj, task, "clarify", time.Now()); err != nil {
		t.Fatalf("reconcilePodStage: %v", err)
	}
	if sess.submitted != 1 {
		t.Fatalf("SubmitTurn called %d times for a redelivered pair, want 1", sess.submitted)
	}

	// The second reconcile, with the delta already spent, must submit nothing.
	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(task), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	fresh.Annotations = task.Annotations
	if _, err := r.reconcilePodStage(context.Background(), proj, fresh, "clarify", time.Now()); err != nil {
		t.Fatalf("reconcilePodStage (second pass): %v", err)
	}
	if sess.submitted != 1 {
		t.Fatalf("SubmitTurn called %d times after the delta was spent, want 1", sess.submitted)
	}
}
