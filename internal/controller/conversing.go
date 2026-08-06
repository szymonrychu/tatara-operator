package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// conversingHandoffAndPark ends a conversation the way G.7 ends a pod: the agent
// gets ONE handoff turn to write everything the next pod needs, the operator
// writes a synthetic handoff note if the agent cannot, and only THEN does the
// Task park at awaiting-human (which tears the pod down through the ordinary
// choke point).
//
// The order is the whole point. Notes ARE the continuation state, so a park that
// deletes the pod first leaves the journal empty and the next pod starts from
// nothing, redoes the work and burns maxTurnsPerTask. The eviction path (the
// per-project conversing ceiling) is a ROUTINE mechanism rather than a rare edge
// case, so this sequence runs often and must be correct.
//
// cause is "idle" or "evicted" and lands on the log line and the metric; it is
// not otherwise behavioural. The park reason is awaiting-human either way: a
// conversation that ended without a decision is a Task waiting on a human, and
// awaiting-human already has the F.6 re-entry rule that resumes it on the next
// comment.
func (r *TaskReconciler) conversingHandoffAndPark(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest, cause string, now time.Time) error {

	var sp objbudget.Spiller
	if r.SpillerFor != nil {
		sp = r.SpillerFor(proj)
	}
	// A conversing Task with no pod (the pod died, or none was ever admitted) has
	// nothing to hand off from. Park it directly: the handoff is best-effort, the
	// park is not.
	if task.Status.PodStartedAt != nil {
		stopper := &agent.TTLStopper{
			Client:  r.Client,
			Session: r.Session,
			Notes: &agent.FitNoteAppender{
				Client:    r.Client,
				Spiller:   sp,
				Namespace: task.Namespace,
			},
			Namespace: task.Namespace,
			Record:    obs.AgentPodTTLExpired,
		}
		in := agent.TTLStopInput{
			BaseURL:     agent.BaseURL(task, task.Namespace),
			CallbackURL: r.callbackURL(),
			AgentKind:   stage.AgentKindFor(task.Status.Stage),
			// The deadline is NOW: the conversation is over, so the hard cap the
			// stopper computes from it (now + 2*turnTimeout + 60s) bounds this
			// sequence rather than any pod TTL.
			Deadline:    now,
			TurnTimeout: time.Duration(proj.Spec.Agent.TurnTimeoutSeconds) * time.Second,
		}
		// Same material as the TTL path: without it the synthetic note is a
		// placeholder (issue #527).
		if lt := task.Status.LastTurn; lt != nil {
			in.LastFinalText, in.PushedRepos = lt.FinalText, lt.PushedRepos
		}
		res, err := stopper.StopWithHandoff(ctx, task, in)
		if err != nil {
			return fmt.Errorf("conversing: handoff stop on %s: %w", task.Name, err)
		}
		logTTLStop(ctx,
			"conversation handed off",
			"conversation ended with NO continuation state captured; this pod's work is unrecorded",
			"conversing_handoff", res, "resource_id", task.Name, "cause", cause)
	}

	if err := EnterStage(ctx, r.Client, sp, r.Metrics, task, mrs,
		tatarav1alpha1.StageParked, stage.ReasonAwaitingHuman, now, nil); err != nil {
		return fmt.Errorf("conversing: park %s after handoff: %w", task.Name, err)
	}
	log.FromContext(ctx).Info("conversation closed",
		"action", "conversing_closed", "resource_id", task.Name, "cause", cause,
		"project", task.Spec.ProjectRef)
	if r.Metrics != nil {
		r.Metrics.ConversingClosed(task.Spec.ProjectRef, cause)
	}
	return nil
}

// countConversing returns proj's live conversing Tasks in no particular
// order - callers that care about idle order sort it themselves. r is the
// caller's reader: the webhook passes the UNCACHED APIReader (it is deciding
// whether to open a conversation right now, off a cache that may not have
// observed the last one), the project reconcile passes its cached client (it
// sweeps on a cadence and a one-pass lag is harmless).
func countConversing(ctx context.Context, r client.Reader, proj *tatarav1alpha1.Project) ([]tatarav1alpha1.Task, error) {
	var tl tatarav1alpha1.TaskList
	if err := r.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		return nil, fmt.Errorf("conversing: list tasks: %w", err)
	}
	out := make([]tatarav1alpha1.Task, 0, len(tl.Items))
	for i := range tl.Items {
		t := &tl.Items[i]
		if t.Spec.ProjectRef != proj.Name || t.Status.Stage != tatarav1alpha1.StageConversing {
			continue
		}
		if t.DeletionTimestamp != nil {
			continue
		}
		out = append(out, *t)
	}
	return out, nil
}

// ConversingHasRoom reports whether proj is under its conversing ceiling right
// now. A conversation is cheap to START and long to HOLD, and it holds a real
// concurrency slot, so without this bound a handful of chatty threads would
// occupy every agent slot the project has.
func ConversingHasRoom(ctx context.Context, r client.Reader, proj *tatarav1alpha1.Project) (bool, error) {
	live, err := countConversing(ctx, r, proj)
	if err != nil {
		return false, err
	}
	return len(live) < tatarav1alpha1.MaxConversingPods(proj), nil
}

// conversationIdleSince is the instant a conversation last saw an event. A Task
// with no stamp falls back to the zero time, i.e. maximally idle - it has never
// had an event and is the least likely to be mid-exchange.
func conversationIdleSince(t *tatarav1alpha1.Task) time.Time {
	if t.Status.ConversationLastEventAt == nil {
		return time.Time{}
	}
	return t.Status.ConversationLastEventAt.Time
}

// sortByIdleThenName orders live longest-idle first (ConversationLastEventAt
// ascending), breaking an EXACT tie by Task name. Extracted from
// enforceConversingCeiling so the tie-break's order-independence is directly
// testable against a hand-built slice, rather than only indirectly through
// whatever order a List call happens to return - which for both the fake
// client and a real apiserver tends to already come back name-sorted, and
// would let a test "pass" even with no tie-break logic at all.
//
// The tie is real, not contrived: metav1.Time round-trips at whole-second
// precision, so two conversations idle since the same second is an ordinary
// collision, not an edge case that can't happen through the API server.
func sortByIdleThenName(live []tatarav1alpha1.Task) {
	sort.Slice(live, func(i, j int) bool {
		ti, tj := conversationIdleSince(&live[i]), conversationIdleSince(&live[j])
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		return live[i].Name < live[j].Name
	})
}

// enforceConversingCeiling parks the longest-idle conversations until proj is
// back under its ceiling, each through the handoff-turn sequence. It runs from
// the project reconcile rather than from the webhook so it is LEADER-ONLY and
// level-triggered: the webhook's ConversingHasRoom check is the fast path that
// usually keeps the count in range, and this is the backstop that converges it
// whatever raced it (a second replica's stale cache, two comments landing on
// two different Tasks in the same instant, ...).
//
// Eviction is not a failure. The evicted Task parks at awaiting-human, which
// has an F.6 re-entry rule, so its next comment cold-spawns it and the
// conversation resumes from the handoff note. What it costs is warmth, not
// work.
//
// LONGEST-IDLE FIRST: the sort key is ConversationLastEventAt, never
// StageEnteredAt/PodStartedAt/StageWorkStartedAt - a pod TTL rotation
// re-stamps those on a conversation that is very much still alive, so sorting
// on them would evict a busy conversation instead of an idle one. Ties (two
// conversations idle since the exact same whole second - metav1.Time
// round-trips at whole-second precision, so this is a real collision, not a
// contrived one) break on Task name so the outcome never depends on List's
// return order, which Kubernetes does not guarantee and Go's map iteration
// would actively randomise if this were keyed by one.
//
// A project at the ceiling with every conversing pod genuinely busy is left
// alone: this only ever evicts the OVERFLOW (len(live) - ceiling Tasks), never
// more, so a full-but-not-over project is untouched and a new conversation
// simply queues behind ConversingHasRoom until one closes on its own. Evicting
// a mid-turn pod to force room for a new conversation would thrash the busiest
// projects hardest, which is worse than making the new conversation wait.
//
// maxConversingEvictionsPerPass BOUNDS how much of the overflow one call
// evicts (2026-07-28 final review CRITICAL 2). Each eviction runs
// StopWithHandoff, which blocks on real timers up to ~2*TurnTimeoutSeconds+60s
// (about 61 minutes at the 1800s default) per Task, and ProjectReconciler runs
// MaxConcurrentReconciles=1 ACROSS EVERY PROJECT - so evicting a large overflow
// serially in one call (a bulk maintainer comment pass against a since-lowered
// ceiling is exactly the shape that produces one) would wedge every project's
// reconcile - driveUnparks, ReapTerminal, every gauge - for hours. Capped at 1:
// the caller requeues quickly (conversingEvictionRequeue) whenever overflow
// remains after the cap, so convergence spreads across several short passes
// instead of one long blocking one.
const maxConversingEvictionsPerPass = 1

// conversingEvictionRequeue is how soon the caller re-drives
// enforceConversingCeiling when this pass's eviction cap left overflow
// remaining, so a large overflow converges in several short passes rather
// than waiting for whatever cadence next triggers this Project's reconcile.
const conversingEvictionRequeue = 5 * time.Second

// enforceConversingCeiling returns a non-zero requeue duration when this
// pass's per-pass eviction cap left overflow remaining: the caller should
// requeue that soon rather than wait for Reconcile()'s own cadence.
func (r *ProjectReconciler) enforceConversingCeiling(ctx context.Context, proj *tatarav1alpha1.Project, now time.Time) (time.Duration, error) {
	live, err := countConversing(ctx, r.Client, proj)
	if err != nil {
		return 0, err
	}
	if r.Metrics != nil {
		r.Metrics.SetConversingPods(proj.Name, float64(len(live)))
	}
	ceiling := tatarav1alpha1.MaxConversingPods(proj)
	if len(live) <= ceiling {
		return 0, nil
	}
	// r.Tasks is the *TaskReconciler this eviction path hands off through
	// (project_controller.go's own doc comment on the field says "Nil ... is
	// never dereferenced" - a nil r.Tasks here would panic on the very next
	// line, contradicting that claim; fail loud instead, 2026-07-28 final
	// review M4).
	if r.Tasks == nil {
		err := fmt.Errorf("conversing: ceiling exceeded (%d live over %d) on project %s but no TaskReconciler is wired to evict through", len(live), ceiling, proj.Name)
		log.FromContext(ctx).Error(err, "conversing: eviction skipped, TaskReconciler unwired",
			"action", "conversing_evict_error", "project", proj.Name)
		return 0, err
	}
	sortByIdleThenName(live)

	overflow := len(live) - ceiling
	evict := overflow
	if evict > maxConversingEvictionsPerPass {
		evict = maxConversingEvictionsPerPass
	}

	var firstErr error
	for i := 0; i < evict; i++ {
		t := &live[i]
		mrs, mErr := ownedMergeRequests(ctx, r.Client, t)
		if mErr != nil {
			log.FromContext(ctx).Error(mErr, "conversing: load owned MRs for eviction failed",
				"action", "conversing_evict_error", "resource_id", t.Name, "project", proj.Name)
			if firstErr == nil {
				firstErr = mErr
			}
			continue
		}
		log.FromContext(ctx).Info("conversing ceiling reached; evicting the longest-idle conversation",
			"action", "conversing_evicting", "resource_id", t.Name, "project", proj.Name,
			"live", len(live), "ceiling", ceiling,
			"idle_since", conversationIdleSince(t).UTC().Format(time.RFC3339))
		if eErr := r.Tasks.conversingHandoffAndPark(ctx, proj, t, mrs, "evicted", now); eErr != nil {
			log.FromContext(ctx).Error(eErr, "conversing: eviction failed",
				"action", "conversing_evict_error", "resource_id", t.Name, "project", proj.Name)
			if firstErr == nil {
				firstErr = eErr
			}
		}
	}
	if firstErr != nil {
		return 0, firstErr
	}
	if evict < overflow {
		return conversingEvictionRequeue, nil
	}
	return 0, nil
}

// conversingEntryStages is the CLOSED set of live stages a qualifying comment
// may move into conversing. It is a table, not a switch with a default, for the
// same reason every other table in the stage machine is: a stage that is not
// named here cannot acquire this behaviour by accident.
//
// implementing is deliberately ABSENT. An implement pod is mid-change; taking it
// down to hold a conversation would lose in-flight work, and a comment arriving
// then already rides into its next turn through the ordinary pendingEvents path.
var conversingEntryStages = map[string]bool{
	tatarav1alpha1.StageClarifying: true,
	tatarav1alpha1.StageReviewing:  true,
}

// ConversingEntryEligible reports whether stg is one of the live stages
// EnterConversing may move into conversing - the SAME closed set
// conversingEntryStages holds, exported so a caller outside this package (the
// webhook's driveConversingEntry) can do this free map lookup BEFORE paying
// for ConversingHasRoom's namespace List, instead of after. Every webhook
// comment on every Task otherwise cost one List regardless of whether the
// Task's stage could ever qualify (2026-07-28 security review IMPORTANT 5).
func ConversingEntryEligible(stg string) bool {
	return conversingEntryStages[stg]
}

// EnterConversing applies the live-stage entry edge into conversing. It
// returns (false, nil) - not an error - when the Task is in a stage that may
// not enter conversing, or when the reviewing round cap is already spent:
// both are ordinary steady-state outcomes, and the caller falls back to
// queueing the event and nothing else.
//
// The idle clock's base (status.conversationLastEventAt) is armed by
// stage.Enter itself, unconditionally on every entry into conversing - see
// its doc comment - not here: a second call site stamping the same field
// would only be redundant-until-someone-forgets-it, and stage.Enter is the
// ONE choke point every entry route (this one, and stage.UnparkDetailed's
// pure enter() closure for the parked(awaiting-human)/
// parked(identity-unverified) edges) already goes through.
//
// The previous stage's pod IS torn down, by EnterStage's ordinary choke-point
// teardown. There is no carve-out and there must not be one: the pod name is
// per-TASK, so a surviving pod would be silently reused by conversing while still
// running the previous stage's kind, model and skills. The conversing pod's
// turn-0 bundle carries the whole mirror thread, the notes journal and <events>,
// so nothing is lost - the FIRST comment pays one cold spawn and every subsequent
// comment in that conversation reaches the live pod warm.
//
// Entry from reviewing increments humanReviewRounds and is capped by it, exactly
// like the awaiting-human re-entry and for the same reason: each lap can spawn a
// pod, and a chatty MR thread must not spawn one per comment.
func EnterConversing(ctx context.Context, c client.Client, sp objbudget.Spiller, m *obs.OperatorMetrics,
	proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest,
	now time.Time) (bool, error) {

	if !conversingEntryStages[task.Status.Stage] {
		return false, nil
	}
	// Captured BEFORE EnterStage mutates task.Status.Stage to conversing: the
	// success log below needs the stage this Task actually entered FROM.
	// task.Status.ParkedFromStage is the wrong field for this - it is only ever
	// stamped on entry into PARKED, so on this live-stage edge it is either
	// stale (left over from an earlier park) or empty, not "clarifying" or
	// "reviewing" (2026-07-28 security review Minor).
	fromStage := task.Status.Stage
	fromReviewing := task.Status.Stage == tatarav1alpha1.StageReviewing
	// #511: an externally-owned MR (a stand-down) is the one state a
	// maintainer's "take over" comment can arrive in. The round cap bounds
	// ordinary review ping-pong; it must not swallow a take-over request, so
	// this entry is exempted from the cap and does not spend a round.
	takeoverCandidate := anyExternallyOwnedMR(mrs)
	if fromReviewing && !takeoverCandidate && task.Status.HumanReviewRounds >= tatarav1alpha1.MaxHumanReviewRounds {
		log.FromContext(ctx).Info("conversing entry refused: the human review round cap is spent",
			"action", "conversing_entry_declined", "resource_id", task.Name,
			"stage", task.Status.Stage, "decline", "rounds-exhausted")
		return false, nil
	}

	err := EnterStage(ctx, c, sp, m, task, mrs, tatarav1alpha1.StageConversing, "", now,
		func(t *tatarav1alpha1.Task) {
			// ABSOLUTE ASSIGNMENT only: FitTask re-runs this closure to size the
			// write and again on every conflict retry, so an increment here would
			// multiply. The rounds value is computed once, outside.
			if fromReviewing && !takeoverCandidate {
				t.Status.HumanReviewRounds = task.Status.HumanReviewRounds + 1
			}
		})
	if err != nil {
		return false, err
	}
	log.FromContext(ctx).Info("conversation opened",
		"action", "conversing_entered", "resource_id", task.Name,
		"from", fromStage, "human_review_rounds", task.Status.HumanReviewRounds)
	return true, nil
}

// anyExternallyOwnedMR reports whether any of mrs is currently
// Ownership==external - see stage.go's identically-purposed anyExternallyOwned
// (#511). Duplicated rather than shared: this package cannot import the
// unexported helper from internal/stage, and the check is a two-line loop.
func anyExternallyOwnedMR(mrs []tatarav1alpha1.MergeRequest) bool {
	for i := range mrs {
		if mrs[i].Status.Ownership == tatarav1alpha1.OwnershipExternal {
			return true
		}
	}
	return false
}
