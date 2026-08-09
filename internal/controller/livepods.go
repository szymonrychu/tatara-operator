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

// liveHandoffAndPark ends a conversation the way G.7 ends a pod: the agent
// gets ONE handoff turn to write everything the next pod needs, the operator
// writes a synthetic handoff note if the agent cannot, and only THEN does the
// Task park at awaiting-human (which tears the pod down through the ordinary
// park choke point).
//
// The order is the whole point. Notes ARE the continuation state, so a park that
// deletes the pod first leaves the journal empty and the next pod starts from
// nothing, redoes the work and burns maxTurnsPerTask. The eviction path (the
// per-project live-pod ceiling) is a ROUTINE mechanism rather than a rare edge
// case, so this sequence runs often and must be correct.
//
// cause is "idle" or "evicted". It lands on the log line and the metric and
// gates NOTHING (#561): the pod dies and the handoff turn is offered the same
// way for both, and the exit is chosen by what the AGENT did, not by which
// clock or ceiling brought us here.
//
// AWAITING-HUMAN IS FOR TASKS THAT ACTUALLY ASKED SOMETHING. It is UnparkHuman,
// so it resumes only on a non-bot comment - and a Task whose pod died without the
// agent writing a handoff note never posed a question, so no reply is owed and
// none arrives. Measured live: 45 Tasks parked awaiting-human, only 20 with a
// real agent handoff; the other 25 were pod-death wreckage sitting on a human
// gate nobody knew existed, released only when an unrelated comment happened to
// land on their issue. Those re-arm for a fresh pod instead, bounded by the
// pod-recreation budget (stage.ReArmAfterPodLoss). A Task that DID hand off
// parks exactly as before: the Task does NOT move state - park is orthogonal to
// state (#521) - so the un-park drops it straight back into the live state it
// was talking in.
//
// #557 applied that to the idle exit only and left the eviction path parking
// unconditionally. #561 is the bill for that: seconds-old Tasks evicted by the
// ceiling, having said nothing, each parked behind a human gate nobody knew it
// was waiting on - the exact wreckage shape #557 named, arriving through the one
// door #557 left open.
func (r *TaskReconciler) liveHandoffAndPark(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest, cause string, now time.Time) error {

	var sp objbudget.Spiller
	if r.SpillerFor != nil {
		sp = r.SpillerFor(proj)
	}
	// A live Task with no pod (the pod died, or none was ever admitted) has
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
			Namespace:            task.Namespace,
			Record:               obs.AgentPodTTLExpired,
			RecordEmptySynthetic: obs.AgentSyntheticHandoffEmpty,
		}
		in := agent.TTLStopInput{
			BaseURL:     agent.BaseURL(task, task.Namespace),
			CallbackURL: r.callbackURL(),
			AgentKind:   stage.AgentKindFor(task.Status.State, task.Spec.Kind),
			// The deadline is NOW: the conversation is over, so the hard cap the
			// stopper computes from it (now + 2*turnTimeout + 60s) bounds this
			// sequence rather than any pod TTL.
			Deadline:    now,
			TurnTimeout: time.Duration(proj.Spec.Agent.TurnTimeoutSeconds) * time.Second,
			// #527: the persisted last-turn continuation state, so the synthetic
			// note carries what the agent actually did instead of "(none)".
			LastFinalText: task.Status.LastTurnFinalText,
			PushedRepos:   task.Status.LastTurnPushedRepos,
		}
		outcome, err := stopper.StopWithHandoff(ctx, task, in)
		if err != nil {
			return fmt.Errorf("livepods: handoff stop on %s: %w", task.Name, err)
		}
		log.FromContext(ctx).Info("conversation handed off",
			"action", "conversing_handoff", "resource_id", task.Name,
			"cause", cause, "outcome", outcome)
	}

	// THE HUMAN-GATE TEST, and it applies to BOTH causes (#561). It used to
	// exempt an eviction on the grounds that "the ceiling deliberately chose to
	// take this pod's slot, and handing it straight back would defeat the
	// mechanism" - but the mechanism the exemption protects is the POD teardown,
	// which has already happened above, unconditionally, for both causes. What
	// the exemption actually bought was a park reason, and awaiting-human is the
	// wrong one for a Task that never asked anything: it is UnparkHuman, so it
	// clears on a non-bot comment and on nothing else, and nobody replies to a
	// question nobody posed. Measured in the 45 minutes before #561 was filed:
	// 20 unpark_declined events carrying park_reason=awaiting-human, on top of
	// 30 fabricated parks already on the books.
	//
	// Re-arming does not hand the slot back for free. It costs one
	// podRecreations (stage.ReArmAfterPodLoss, which deliberately does not reset
	// the counter), so a conversation the ceiling keeps choosing spends its
	// budget and terminates at pod-recreation-exhausted. And it re-enters as
	// UNSTARTED, which evictionCandidates excludes, so the very next pass picks
	// a different victim instead of grinding this one - the loop moves, it does
	// not spin.
	asked, err := r.agentAskedSomething(ctx, task)
	if err != nil {
		return err
	}
	if !asked {
		return r.reArmWithoutHandoff(ctx, proj, task, sp, cause, now)
	}

	if err := ParkTask(ctx, r.Client, sp, r.Metrics, task, stage.ReasonAwaitingHuman, now, nil); err != nil {
		return fmt.Errorf("livepods: park %s after handoff: %w", task.Name, err)
	}
	log.FromContext(ctx).Info("conversation closed",
		"action", "live_closed", "resource_id", task.Name, "cause", cause,
		"project", task.Spec.ProjectRef)
	if r.Metrics != nil {
		r.Metrics.LiveClosed(task.Spec.ProjectRef, cause)
	}
	return nil
}

// causeIdle / causeEvicted are liveHandoffAndPark's two callers.
const (
	causeIdle    = "idle"
	causeEvicted = "evicted"
)

// agentAskedSomething re-reads the Task from the API server and reports whether
// an AGENT-authored handoff note exists. It re-reads deliberately: the stop
// sequence that just ran may have appended one microseconds ago, and the caller's
// copy predates it.
func (r *TaskReconciler) agentAskedSomething(ctx context.Context, task *tatarav1alpha1.Task) (bool, error) {
	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
		return false, fmt.Errorf("livepods: reload %s for the handoff check: %w", task.Name, err)
	}
	return agent.HasAgentHandoffNote(fresh), nil
}

// reArmWithoutHandoff is the non-park exit: the pod ended without the agent
// saying anything, so the Task gets a replacement pod rather than a fabricated
// human gate. The pod-recreation budget bounds it, and spending that budget
// terminates at parked(pod-recreation-exhausted) - which is the correct terminal
// for repeated pod death and already carries no re-entry.
func (r *TaskReconciler) reArmWithoutHandoff(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, sp objbudget.Spiller, cause string, now time.Time) error {

	var exhausted bool
	if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		_, terminal := stage.ReArmAfterPodLoss(fresh, taskMaxPodRecreations(proj), now)
		exhausted = terminal
		return !terminal
	}); err != nil {
		return fmt.Errorf("livepods: re-arm %s after a handoff-less pod: %w", task.Name, err)
	}
	l := log.FromContext(ctx)
	if exhausted {
		l.Info("pod ended with no agent handoff and the recreation budget is spent; parking",
			"action", "live_rearm_exhausted", "resource_id", task.Name, "cause", cause,
			"pod_recreations", task.Status.Stats.PodRecreations)
		if err := ParkTask(ctx, r.Client, sp, r.Metrics, task,
			stage.ReasonPodRecreationExhausted, now, nil); err != nil {
			return fmt.Errorf("livepods: park %s after the recreation budget: %w", task.Name, err)
		}
		return nil
	}
	l.Info("pod ended with no agent handoff; re-arming for a replacement pod instead of waiting on a human",
		"action", "live_rearm_no_handoff", "resource_id", task.Name, "cause", cause,
		"state", task.Status.State, "project", task.Spec.ProjectRef)
	return nil
}

// countLive returns proj's LIVE Tasks in no particular order - callers that care
// about idle order sort it themselves. r is the caller's reader: the webhook
// passes the UNCACHED APIReader (it is deciding whether to open a conversation
// right now, off a cache that may not have observed the last one), the project
// reconcile passes its cached client (it sweeps on a cadence and a one-pass lag
// is harmless).
//
// BOTH CLAUSES ARE LOAD-BEARING AND THIS PREDICATE FAILS SILENTLY. `live` alone
// counts a parked Task whose pod is already gone and refuses a legitimate new
// conversation. `!parked` alone counts every Task in the project. And a
// predicate that tried to be cleverer - "was it ever live", "did it come from a
// live state" - under-counts a Task that re-entered awaiting-review from merged,
// which is a legal edge, and the ceiling then stops bounding pods at all: a cost
// blowout with no error and no counter anywhere. Keep it to the two clauses.
func countLive(ctx context.Context, r client.Reader, proj *tatarav1alpha1.Project) ([]tatarav1alpha1.Task, error) {
	var tl tatarav1alpha1.TaskList
	if err := r.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		return nil, fmt.Errorf("livepods: list tasks: %w", err)
	}
	out := make([]tatarav1alpha1.Task, 0, len(tl.Items))
	for i := range tl.Items {
		t := &tl.Items[i]
		if t.Spec.ProjectRef != proj.Name || !stage.Live(t.Status.State) || tatarav1alpha1.Parked(t) {
			continue
		}
		if t.DeletionTimestamp != nil {
			continue
		}
		out = append(out, *t)
	}
	return out, nil
}

// LiveHasRoom reports whether proj is under its live-pod ceiling right now. A
// conversation is cheap to START and long to HOLD, and it holds a real
// concurrency slot, so without this bound a handful of chatty threads would
// occupy every agent slot the project has.
func LiveHasRoom(ctx context.Context, r client.Reader, proj *tatarav1alpha1.Project) (bool, error) {
	live, err := countLive(ctx, r, proj)
	if err != nil {
		return false, err
	}
	return len(live) < tatarav1alpha1.MaxLivePods(proj), nil
}

// conversationIdleSince is the instant a conversation last saw an event, and it
// falls back TOWARDS NOW, never towards the zero time.
//
// READ THE NEXT PARAGRAPH BEFORE "FIXING" THIS BACK (#561). This function used
// to answer time.Time{} for a Task with no ConversationLastEventAt, and the doc
// comment argued the case plainly: such a Task "has never had an event and is
// the least likely to be mid-exchange", i.e. maximally idle. That reasoning is
// true of an ANCIENT conversation that never got a reply, and exactly INVERTED
// for a Task created moments ago that has not had time to produce one. The zero
// time cannot tell the two apart, and sortByIdleThenName orders ASCENDING, so
// the never-stamped Task sorted FIRST and the newest work in the project was
// sacrificed before anything else. "Never had an event" is not evidence of age.
//
// The fallback chain is therefore three claims of NEWNESS, weakest last:
// the conversation stamp, then the live-state entry stamp (stage.stampEnter
// writes both, so in practice the first always exists on a live Task and this is
// belt-and-braces), then the object's own creation instant. Only a Task carrying
// none of the three - which the API server does not produce - can still reach
// the zero time.
func conversationIdleSince(t *tatarav1alpha1.Task) time.Time {
	if t.Status.ConversationLastEventAt != nil {
		return t.Status.ConversationLastEventAt.Time
	}
	if t.Status.StateEnteredAt != nil {
		return t.Status.StateEnteredAt.Time
	}
	return t.CreationTimestamp.Time
}

// conversationStarted reports whether this Task's CURRENT live state ever got a
// READY pod. status.stateWorkStartedAt is the pod-ready stamp and stampEnter
// clears it on every state entry, so a nil one means: no turn 0 was ever
// submitted in this state, the agent has said nothing and been asked nothing.
//
// It is an ETERNAL FACT ABOUT THE PAST, not a snapshot - a Task does not become
// unstarted again once it has started - so it cannot flap and cannot wedge. The
// two ways out of it are both already bounded: the pod becomes ready (clock 2,
// the never-Ready budget, terminates at pod-recreation-exhausted) or the Task
// never gets one (clock 1, AdmissionStarvedBudget, terminates at
// admission-starved).
func conversationStarted(t *tatarav1alpha1.Task) bool {
	return t.Status.StateWorkStartedAt != nil
}

// sortByIdleThenName orders live longest-idle first (ConversationLastEventAt
// ascending), breaking an EXACT tie by Task name. Extracted from
// enforceLivePodCeiling so the tie-break's order-independence is directly
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

// evictionCandidates returns the subset of live that may legally be chosen as
// an eviction victim - longest-idle first, with sortByIdleThenName's exact
// tie-break - EXCLUDING two shapes: a Task with a turn in flight
// (stage.TurnInFlight), and a Task whose conversation has never started
// (conversationStarted).
//
// UNSTARTED IS NOT IDLE, AND THIS IS THE SHAPE THAT WAS ACTUALLY MEASURED
// (#561). The zero-time fallback below conversationIdleSince is real but it is
// NOT what produced the live evictions: stampEnter writes
// ConversationLastEventAt on entry into every live state, so a live Task always
// carries one and the nil branch is unreachable through the API server. The
// victims were picked with a perfectly good stamp - their OWN, seconds old.
//
// Counted off the operator's logs for the eviction storm of 2026-08-08: 74
// live_evicting lines, median lag from the victim's own idle_since to its
// eviction 53.5 SECONDS, 38 of the 74 inside sixty seconds. Nothing evicted that
// day was genuinely idle. The clearest single case, verbatim:
//
//	06:43:57.933  stage_transition (create) -> new     mt-r-tatara-cli-97-...
//	06:43:57.941  stage_transition new -> awaiting-review
//	06:43:58.339  queue_admit
//	06:44:00.382  live_evicting  idle_since=06:43:57Z  live=4  ceiling=2
//	06:44:00.412  conversing_handoff  cause=evicted  outcome=force_deleted
//
// Two and a half seconds old, never having held a turn in its life, logged as
// "the longest-idle conversation". idle_since is its own state-entry instant,
// because that is the only thing a Task that has never conversed HAS. Note also
// that the mid-turn exclusion above cannot cover this: measured on the same
// logs, the operator held ZERO turns in flight for the whole eviction window (a
// 2.95-second cascade expired all 44 of them nine seconds before the first
// eviction), so every live Task was a candidate and the guard had nothing to
// exclude.
//
// A Task whose pod has never become ready has submitted no turn, said nothing
// and asked nothing. Ending its conversation reclaims no warmth because there is
// none: it destroys the newest work in the project along with the admission slot
// the queue granted it seconds earlier, and the Task simply re-queues for the
// pod it never got - so the ceiling is not converged either, only churned.
//
// EXCLUDED, NOT MERELY SORTED LAST (2026-08-07 adversarial review B4, HIGH).
// Before this predicate existed, enforceLivePodCeiling picked its victim by
// conversationIdleSince alone, and an agent 40 minutes into an ordinary turn
// has ConversationLastEventAt still pinned to its state-entry stamp - the
// OLDEST timestamp in the project - so it sorted FIRST and was evicted
// mid-turn. That is ROUTINE, not a race: LiveHasRoom gates only the webhook
// and un-park paths, forward stage.Enter into the three live states consults
// no ceiling at all, and #521 widened `live` from one stage to three against
// a default ceiling of 2.
//
// Sorting mid-turn Tasks to the END of the slice instead of excluding them
// would fix the common case (an idle Task is preferred whenever one exists)
// but not the shape this predicate exists to rule out: a project sitting at,
// say, one idle Task and two mid-turn Tasks, two over ceiling, would still
// reach into the mid-turn tail for its second victim once the idle one is
// spent. Excluding them outright means a mid-turn Task is NEVER a victim,
// full stop, whatever the overflow looks like.
//
// The cost is that a project can sit over its ceiling for a while when every
// live Task happens to be mid-turn - see enforceLivePodCeiling's handling of
// evict==0. That is acceptable and cannot wedge: enforceLivePodCeiling is
// level-triggered and reruns on the project's ordinary reconcile cadence, and
// every turn is itself bounded (the turn stall timeout, ResidencyExceeded,
// maxTurnsPerTask), so "wait for a turn to end" cannot become "wait forever".
// Evicting a mid-turn pod to force room instead would thrash the busiest
// projects hardest, which is exactly the outcome this file's own doc comment
// on enforceLivePodCeiling already says is worse than making something wait.
func evictionCandidates(live []tatarav1alpha1.Task) []tatarav1alpha1.Task {
	out := make([]tatarav1alpha1.Task, 0, len(live))
	for i := range live {
		if stage.TurnInFlight(&live[i]) || !conversationStarted(&live[i]) {
			continue
		}
		out = append(out, live[i])
	}
	sortByIdleThenName(out)
	return out
}

// maxLivePodEvictionsPerPass BOUNDS how much of the overflow one call evicts
// (2026-07-28 final review CRITICAL 2). Each eviction runs StopWithHandoff,
// which blocks on real timers up to ~2*TurnTimeoutSeconds+60s (about 61 minutes
// at the 1800s default) per Task, and ProjectReconciler runs
// MaxConcurrentReconciles=1 ACROSS EVERY PROJECT - so evicting a large overflow
// serially in one call (a bulk maintainer comment pass against a since-lowered
// ceiling is exactly the shape that produces one) would wedge every project's
// reconcile - driveUnparks, ReapTerminal, every gauge - for hours. Capped at 1:
// the caller requeues quickly (livePodEvictionRequeue) whenever overflow
// remains after the cap, so convergence spreads across several short passes
// instead of one long blocking one.
const maxLivePodEvictionsPerPass = 1

// livePodEvictionRequeue is how soon the caller re-drives
// enforceLivePodCeiling when this pass's eviction cap left overflow
// remaining, so a large overflow converges in several short passes rather
// than waiting for whatever cadence next triggers this Project's reconcile.
const livePodEvictionRequeue = 5 * time.Second

// enforceLivePodCeiling parks the longest-idle live Tasks until proj is back
// under its ceiling, each through the handoff-turn sequence. It runs from the
// project reconcile rather than from the webhook so it is LEADER-ONLY and
// level-triggered: the webhook's LiveHasRoom check is the fast path that usually
// keeps the count in range, and this is the backstop that converges it whatever
// raced it (a second replica's stale cache, two comments landing on two
// different Tasks in the same instant, ...).
//
// Eviction is not a failure. The evicted Task parks at awaiting-human, which has
// a re-entry rule, so its next comment cold-spawns it and the conversation
// resumes from the handoff note IN THE SAME STATE. What it costs is warmth, not
// work.
//
// LONGEST-IDLE FIRST: the sort key is ConversationLastEventAt, never
// StateEnteredAt/PodStartedAt/StateWorkStartedAt - a pod TTL rotation
// re-stamps those on a conversation that is very much still alive, so sorting
// on them would evict a busy conversation instead of an idle one. Ties (two
// conversations idle since the exact same whole second - metav1.Time
// round-trips at whole-second precision, so this is a real collision, not a
// contrived one) break on Task name so the outcome never depends on List's
// return order, which Kubernetes does not guarantee and Go's map iteration
// would actively randomise if this were keyed by one.
//
// NEVER A MID-TURN VICTIM (B4): ConversationLastEventAt is stamped on ENTRY
// into a live state and does not move again until a human comments, so a Task
// 40 minutes into an ordinary agent turn looks like the MOST idle Task in the
// project, not the least - "longest idle" and "mid-turn" are not the same
// thing, and conflating them is exactly what used to evict a working agent.
// evictionCandidates (below) excludes any Task with stage.TurnInFlight true
// before this function ever sorts or picks, so a mid-turn Task cannot be
// selected however old its idle stamp looks.
//
// NEVER AN UNSTARTED VICTIM EITHER (#561): the same helper excludes a Task
// whose pod has never become ready. Mid-turn and unstarted are the two ends of
// the same misreading - "longest idle" means neither "busy" nor "brand new" -
// and the second one is what the 2026-08-08 logs actually caught, evicting Tasks
// a median of 53.5 seconds old, one of them 2.45 seconds after it was created.
//
// A project at the ceiling with every live pod genuinely busy is left alone:
// this only ever evicts the OVERFLOW (len(live) - ceiling Tasks), never more, so
// a full-but-not-over project is untouched and a new conversation simply queues
// behind LiveHasRoom until one closes on its own. Evicting a mid-turn pod to
// force room for a new conversation would thrash the busiest projects hardest,
// which is worse than making the new conversation wait - and now that this
// applies to genuine overflow too, a project can sit OVER ceiling, not just
// at it, when every overflow Task happens to be mid-turn; see the evict==0
// handling below for why that cannot wedge.
//
// It returns a non-zero requeue duration when this pass's per-pass eviction cap
// left IDLE overflow remaining: the caller should requeue that soon rather than
// wait for Reconcile()'s own cadence. It returns zero when nothing was evicted
// because no legal (non-mid-turn) victim existed - retrying in 5s would not
// help there, since nothing changes until a turn ends, so that case rides the
// project's ordinary level-triggered reconcile instead.
func (r *ProjectReconciler) enforceLivePodCeiling(ctx context.Context, proj *tatarav1alpha1.Project, now time.Time) (time.Duration, error) {
	live, err := countLive(ctx, r.Client, proj)
	if err != nil {
		return 0, err
	}
	if r.Metrics != nil {
		r.Metrics.SetLivePods(proj.Name, float64(len(live)))
	}
	ceiling := tatarav1alpha1.MaxLivePods(proj)
	if len(live) <= ceiling {
		return 0, nil
	}
	// r.Tasks is the *TaskReconciler this eviction path hands off through
	// (project_controller.go's own doc comment on the field says "Nil ... is
	// never dereferenced" - a nil r.Tasks here would panic on the very next
	// line, contradicting that claim; fail loud instead, 2026-07-28 final
	// review M4).
	if r.Tasks == nil {
		err := fmt.Errorf("livepods: ceiling exceeded (%d live over %d) on project %s but no TaskReconciler is wired to evict through", len(live), ceiling, proj.Name)
		log.FromContext(ctx).Error(err, "livepods: eviction skipped, TaskReconciler unwired",
			"action", "live_evict_error", "project", proj.Name)
		return 0, err
	}
	overflow := len(live) - ceiling
	candidates := evictionCandidates(live)

	// desiredEvict is how much of the overflow COULD be evicted if the
	// per-pass cap did not exist: bounded by the overflow itself and by how
	// many legal (non-mid-turn) victims actually exist. evict is then that,
	// further capped by maxLivePodEvictionsPerPass. The two are compared
	// again below to decide whether the fast per-pass-cap requeue applies.
	desiredEvict := overflow
	if desiredEvict > len(candidates) {
		desiredEvict = len(candidates)
	}
	evict := desiredEvict
	if evict > maxLivePodEvictionsPerPass {
		evict = maxLivePodEvictionsPerPass
	}
	if evict == 0 {
		// Every overflow Task is mid-turn or has never started: there is no
		// legal victim this pass. Not an error, not a spin - see the doc
		// comment above.
		log.FromContext(ctx).Info("live-pod ceiling exceeded but no overflow conversation is a legal victim (each is mid-turn or has never started); evicting nothing this pass",
			"action", "live_evict_skipped", "project", proj.Name,
			"live", len(live), "ceiling", ceiling)
		return 0, nil
	}

	var firstErr error
	for i := 0; i < evict; i++ {
		t := &candidates[i]
		mrs, mErr := ownedMergeRequests(ctx, r.Client, t)
		if mErr != nil {
			log.FromContext(ctx).Error(mErr, "livepods: load owned MRs for eviction failed",
				"action", "live_evict_error", "resource_id", t.Name, "project", proj.Name)
			if firstErr == nil {
				firstErr = mErr
			}
			continue
		}
		log.FromContext(ctx).Info("live-pod ceiling reached; evicting the longest-idle conversation",
			"action", "live_evicting", "resource_id", t.Name, "project", proj.Name,
			"live", len(live), "ceiling", ceiling,
			"idle_since", conversationIdleSince(t).UTC().Format(time.RFC3339))
		if eErr := r.Tasks.liveHandoffAndPark(ctx, proj, t, mrs, causeEvicted, now); eErr != nil {
			log.FromContext(ctx).Error(eErr, "livepods: eviction failed",
				"action", "live_evict_error", "resource_id", t.Name, "project", proj.Name)
			if firstErr == nil {
				firstErr = eErr
			}
		}
	}
	if firstErr != nil {
		return 0, firstErr
	}
	// A fast requeue is for the per-pass CAP, not for waiting on a turn: only
	// ask for one when idle-and-evictable overflow is still left after this
	// pass's cap (evict < desiredEvict). If desiredEvict itself fell short of
	// overflow, the rest is mid-turn Tasks that a fast retry cannot touch
	// either - that overflow rides the level-triggered reconcile instead.
	if evict < desiredEvict {
		return livePodEvictionRequeue, nil
	}
	return 0, nil
}

// The CLOSED vocabulary of live-entry decline reasons, the `reason` label on
// operator_live_entry_declined_total.
//
// IT EXISTS BECAUSE THE OLD ONE COULD NOT NAME ITS CONDITION. Measured on the
// live cluster 2026-08-07, operator_conversing_entry_declined_total had ~27
// declines and EVERY ONE was labelled with the literal "unresolved" - a counter
// that cannot say which of several guards refused, which is the exact defect the
// SweepSkip and UnparkDecline vocabularies exist to prevent.
const (
	// LiveEntryDeclineNotLive: the Task is not in a state that carries a live
	// agent pod, so there is nothing to deliver a further turn into.
	LiveEntryDeclineNotLive = "not-a-live-state"
	// LiveEntryDeclineParked: the Task is parked. A parked Task holds no pod;
	// the un-park rules decide whether the comment resumes it.
	LiveEntryDeclineParked = "task-parked"
	// LiveEntryDeclineCeilingFull: the project is at its live-pod ceiling. The
	// event stays queued and rides the Task's next turn.
	LiveEntryDeclineCeilingFull = "live-ceiling-full"
	// LiveEntryDeclineRoundsExhausted: humanReviewRounds is spent.
	LiveEntryDeclineRoundsExhausted = "rounds-exhausted"
	// LiveEntryDeclineTerminal: the Task is done or rejected.
	LiveEntryDeclineTerminal = "task-done"
)

// LiveEntryDeclineReasons is that vocabulary as data, for the metric's label
// allow-list and for the test that asserts none of them is "unresolved".
var LiveEntryDeclineReasons = []string{
	LiveEntryDeclineNotLive,
	LiveEntryDeclineParked,
	LiveEntryDeclineCeilingFull,
	LiveEntryDeclineRoundsExhausted,
	LiveEntryDeclineTerminal,
}

// LiveEntryEligible reports whether a Task in `state` can take a further turn on
// a live pod. It is the free map lookup a caller outside this package (the
// webhook's driveLiveEntry) does BEFORE paying for LiveHasRoom's namespace List,
// instead of after: every webhook comment on every Task otherwise cost one List
// regardless of whether the Task's state could ever qualify (2026-07-28 security
// review IMPORTANT 5).
//
// It is stage.Live and nothing more. The old conversingEntryStages table was a
// SECOND, parallel table naming clarifying and reviewing; entry into a live
// state is now the liveness property's business, and one table is the point.
func LiveEntryEligible(state string) bool { return stage.Live(state) }

// DeliverLiveTurn is the live-state entry edge, and under #521 it is no longer a
// TRANSITION at all: the Task is ALREADY in a live state, so a qualifying comment
// just rides into the pod that is already there (or cold-spawns one). What is
// left of the old EnterConversing is the accounting and the refusals.
//
// It returns (false, reason) - never an error - for the ordinary steady-state
// refusals, and the caller falls back to queueing the event and nothing else.
//
// Entry from awaiting-review increments humanReviewRounds and is capped by it,
// exactly like the awaiting-human re-entry and for the same reason: each lap can
// spawn a pod, and a chatty MR thread must not spawn one per comment.
func DeliverLiveTurn(ctx context.Context, c client.Client, sp objbudget.Spiller, m *obs.OperatorMetrics,
	proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest,
	now time.Time) (bool, string) {

	if tatarav1alpha1.TaskDone(task) {
		return false, LiveEntryDeclineTerminal
	}
	if tatarav1alpha1.Parked(task) {
		return false, LiveEntryDeclineParked
	}
	if !stage.Live(task.Status.State) {
		return false, LiveEntryDeclineNotLive
	}
	// #511: an externally-owned MR (a stand-down) is the one state a
	// maintainer's "take over" comment can arrive in. The round cap bounds
	// ordinary review ping-pong; it must not swallow a take-over request, so
	// this entry is exempted from the cap and does not spend a round.
	fromReview := task.Status.State == tatarav1alpha1.StateAwaitingReview
	takeoverCandidate := anyExternallyOwnedMR(mrs)
	if fromReview && !takeoverCandidate && task.Status.HumanReviewRounds >= tatarav1alpha1.MaxHumanReviewRounds {
		log.FromContext(ctx).Info("live turn refused: the human review round cap is spent",
			"action", "live_entry_declined", "resource_id", task.Name,
			"state", task.Status.State, "decline", LiveEntryDeclineRoundsExhausted)
		return false, LiveEntryDeclineRoundsExhausted
	}
	log.FromContext(ctx).Info("live turn delivered",
		"action", "live_turn", "resource_id", task.Name,
		"state", task.Status.State, "human_review_rounds", task.Status.HumanReviewRounds)
	return true, ""
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

// repairParkedWithLivePod is THE PARK/LIVE INVARIANT REPAIR.
//
// parkReason != "" AND a live pod is a state the design calls transient, and it
// IS - ParkTask stamps the flag and deletes the pod in the same call. Nothing
// bounds the gap, though: a crash, a conflict retry that loses the pod delete,
// or an eviction between the two leaves a parked Task holding a pod that burns
// an admission slot with NO clock armed. ArmedClock switches to the park
// RETENTION clock the instant the flag lands, ResidencyExceeded does not apply
// to a parked Task, and the pod's own TTL only rotates it into a replacement.
// That is a permanent, silent slot leak, and the two mechanisms are orthogonal
// so nothing else notices it.
//
// Repair it on sight and COUNT it: if
// operator_task_parked_with_live_pod_repaired_total is ever non-zero at a
// steady rate, the transient is not transient.
//
// handled is true when it repaired something, so the caller requeues rather
// than reconciling a Task it has just mutated.
func (r *TaskReconciler) repairParkedWithLivePod(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task) (bool, error) {

	if !tatarav1alpha1.Parked(task) || task.Status.PodName == "" {
		return false, nil
	}
	log.FromContext(ctx).Info("parked task still holds a live pod; stopping it",
		"action", "parked_live_pod_repaired", "resource_id", task.Name,
		"park_reason", task.Status.ParkReason, "pod", task.Status.PodName)
	if r.Metrics != nil {
		r.Metrics.ParkedWithLivePodRepaired(task.Spec.ProjectRef, task.Status.ParkReason)
	}
	if err := agent.DeleteWrapper(ctx, r.Client, task.Namespace, task); err != nil {
		return true, err
	}
	var sp objbudget.Spiller
	if r.SpillerFor != nil && proj != nil {
		sp = r.SpillerFor(proj)
	}
	key := client.ObjectKeyFromObject(task)
	if err := objbudget.FitTask(ctx, r.Client, sp, key, func(t *tatarav1alpha1.Task) {
		t.Status.PodName = ""
		t.Status.PodStartedAt = nil
		t.Status.StateWorkStartedAt = nil
	}); err != nil {
		return true, fmt.Errorf("livepods: clear pod ref on %s: %w", key.Name, err)
	}
	task.Status.PodName = ""
	task.Status.PodStartedAt = nil
	task.Status.StateWorkStartedAt = nil
	return true, nil
}
