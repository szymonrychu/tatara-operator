package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// INACTIVITY MEANS ASK THE AGENT, NOT KILL THE TURN.
//
// The old rule was a single branch: max(turn-started-at, turn-last-activity-at)
// older than turnTimeoutSeconds + grace => stalledTurnStop. It has exactly one
// input, silence, and silence is not evidence. A parent agent blocked on a Task
// tool call is silent; an agent inside one long tool call is silent; an agent
// that has genuinely wedged is silent. O1 closed the first of those by teaching
// the operator to read the subagent transcripts, and closing it exposed the
// shape of the remaining two: they are not distinguishable from OUTSIDE, and no
// amount of extra clock arithmetic will make them so.
//
// So the operator asks. `POST /v1/probe` writes a question straight into the
// agent's PTY - it is not a turn, so it never 409s against the in-flight one -
// and the answer, or the shape of its absence, is the actual evidence:
//
//	answered              the agent is alive. The turn continues with NO BOUND.
//	                      Every answer re-bases the inactivity anchor, so a live
//	                      agent that keeps answering keeps working indefinitely.
//	                      That is the entire point of this phase.
//	delivered, unanswered the agent CONSUMED the message and did not reply. It is
//	                      running and ignoring us.
//	pending (never
//	  delivered)          the `enqueue` line has no matching `remove`: the agent
//	                      never reached a tool-call boundary, i.e. it is blocked
//	                      INSIDE one tool call. Verified: a 70s sleep buffered a
//	                      probe 58.2s; an infinite one buffers it forever.
//
// The last two escalate, and escalation is ESC FIRST. `stalledTurnStop` almost
// always lands on the synthetic-note fallback today, because `POST /v1/messages`
// 409s for as long as the hung turn is in flight and the operator had no cancel
// primitive - so the "graceful" stop was graceful in name and a teardown in
// practice. `POST /v1/interrupt` is that primitive: measured to interrupt
// synchronously mid-tool in ~40ms, with session, sessionId, JSONL, workspace and
// context all intact afterwards. With the turn actually cancelled, the handoff
// turn the stop wants to submit becomes submittable, and the agent gets to write
// its own continuation state instead of the operator guessing at it.
//
// AND AN OLD WRAPPER MUST BE BYTE-IDENTICAL TO TODAY. The operator and the
// wrapper roll in different helm releases, so new-operator + old-wrapper is a
// guaranteed window on every train (#544 was 56 minutes of exactly that). Every
// path out of this file that cannot get an answer from the probe endpoints falls
// through to the SAME `stalledTurnStop` call the old branch made, with the same
// arguments - not a degraded variant of it.

// stallProbeText is the probe body. The TATARA-ALIVE marker is the wrapper's
// answered-detection contract (it matches an assistant message that STARTS with
// the literal), and the one-sentence request is what makes the answer worth
// reading in the log line rather than a bare ack.
const stallProbeText = "TATARA STALL PROBE. You are not being asked to stop or to change what you are doing - " +
	"keep working immediately afterwards. Reply with exactly TATARA-ALIVE " +
	"<one sentence about what you are doing right now>."

// Defaults for the two CRD knobs, applied when a Project predates the schema (the
// field decodes as zero) or carries a value outside the validated range. They
// mirror the kubebuilder defaults on AgentSpec, deliberately: a Project object
// created before the CRD gained the fields has no defaulted value at all, and
// falling back to zero would make the grace window instant and the attempt
// ladder empty - escalating on the FIRST unanswered poll, which is strictly more
// aggressive than the behaviour this phase replaces.
const (
	defaultStallProbeGraceSeconds = 300
	defaultStallProbeMaxAttempts  = 2
	maxStallProbeMaxAttempts      = 5
)

// stallProbeGrace is how long one probe gets to be answered.
func stallProbeGrace(proj *tatarav1alpha1.Project) time.Duration {
	s := proj.Spec.Agent.StallProbeGraceSeconds
	if s <= 0 {
		s = defaultStallProbeGraceSeconds
	}
	return time.Duration(s) * time.Second
}

// stallProbeMaxAttempts is how many probes a stall episode sends before the ESC.
func stallProbeMaxAttempts(proj *tatarav1alpha1.Project) int {
	n := proj.Spec.Agent.StallProbeMaxAttempts
	if n <= 0 {
		n = defaultStallProbeMaxAttempts
	}
	if n > maxStallProbeMaxAttempts {
		n = maxStallProbeMaxAttempts
	}
	return n
}

// taskStallProbeInFlight reports whether a probe is outstanding on this Task.
func taskStallProbeInFlight(task *tatarav1alpha1.Task) bool {
	return task.Annotations[annStallProbeID] != ""
}

// stallProbeAttempts reads the persisted attempt count, treating anything
// unparseable as zero - a corrupt count must restart the ladder, never skip it
// straight to the ESC.
func stallProbeAttempts(task *tatarav1alpha1.Task) int {
	n, err := strconv.Atoi(task.Annotations[annStallProbeAttempts])
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// turnIsStalled is the inactivity predicate the ladder starts from: the O1
// subagent-aware anchor, unchanged.
func turnIsStalled(proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task) bool {
	return turnTimedOut(task.Annotations[annTurnStartedAt],
		task.Annotations[annTurnLastActivity], proj.Spec.Agent.TurnTimeoutSeconds)
}

// reconcileStall replaces the old `turnTimedOut -> stalledTurnStop` branch.
//
// It returns handled=false ONLY when there is nothing for the stall machinery to
// do, in which case the caller carries on with ordinary stage work exactly as if
// this function did not exist. Every other return is terminal for the pass.
func (r *TaskReconciler) reconcileStall(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, agentKind string, now time.Time) (ctrl.Result, bool, error) {

	l := log.FromContext(ctx)
	inFlight := taskStallProbeInFlight(task)

	if !turnIsStalled(proj, task) {
		// The turn is reporting activity again. If a probe was outstanding, the
		// agent came back on its own between passes - retire the episode so the
		// NEXT stall starts a fresh ladder rather than inheriting a spent one.
		if !inFlight {
			return ctrl.Result{}, false, nil
		}
		if err := r.clearStallProbe(ctx, task); err != nil {
			return ctrl.Result{}, true, err
		}
		l.Info("stall probe retired: the turn resumed reporting activity on its own",
			"action", "stall_probe_retired", "resource_id", task.Name,
			"turn_id", task.Annotations[annCurrentTurn], "probe_id", task.Annotations[annStallProbeID])
		return ctrl.Result{}, false, nil
	}

	if !inFlight {
		res, err := r.sendStallProbe(ctx, proj, task, agentKind, now, stallProbeAttempts(task)+1)
		return res, true, err
	}
	res, err := r.evaluateStallProbe(ctx, proj, task, agentKind, now)
	return res, true, err
}

// sendStallProbe writes one probe and stamps the episode state.
//
// A probe that cannot be sent ends the episode immediately on the OLD path.
// ErrProbeUnsupported is the expected mid-train case (this wrapper has no such
// endpoint); any other error means the wrapper is unreachable or broken, which
// is the strongest stall evidence there is. Both fall back to `stalledTurnStop`,
// which is precisely what this Task would have got before this phase existed -
// the fallback is never worse than today, and is never a new terminal.
func (r *TaskReconciler) sendStallProbe(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, agentKind string, now time.Time, attempt int) (ctrl.Result, error) {

	l := log.FromContext(ctx)
	probeID, err := r.Session.Probe(ctx, agent.BaseURL(task, task.Namespace), stallProbeText)
	if err != nil {
		return r.stallProbeFallback(ctx, proj, task, agentKind, now, err, "probe")
	}

	if err := r.patchTaskAnnotations(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations[annStallProbeID] = probeID
		fresh.Annotations[annStallProbeAt] = now.UTC().Format(time.RFC3339)
		fresh.Annotations[annStallProbeAttempts] = strconv.Itoa(attempt)
		return true
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("stamp stall probe %s: %w", task.Name, err)
	}

	obs.StallProbe(obs.StallProbeSent)
	grace := stallProbeGrace(proj)
	l.Info("turn inactive past its window; asked the agent whether it is alive",
		"action", "stall_probe_sent", "resource_id", task.Name,
		"turn_id", task.Annotations[annCurrentTurn], "probe_id", probeID,
		"attempt", attempt, "max_attempts", stallProbeMaxAttempts(proj),
		"grace_seconds", int(grace.Seconds()), "agent_kind", agentKind)
	return ctrl.Result{RequeueAfter: grace}, nil
}

// evaluateStallProbe reads the outstanding probe and moves the ladder one step.
func (r *TaskReconciler) evaluateStallProbe(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, agentKind string, now time.Time) (ctrl.Result, error) {

	l := log.FromContext(ctx)
	probeID := task.Annotations[annStallProbeID]
	res, err := r.Session.ProbeStatus(ctx, agent.BaseURL(task, task.Namespace), probeID)
	if err != nil {
		return r.stallProbeFallback(ctx, proj, task, agentKind, now, err, "probe_status")
	}

	// THE ANSWER. Everything else in this file exists to make this branch
	// reachable: the agent is alive, so the turn is not stalled, so the turn
	// simply continues - and it continues with no bound at all, because the
	// activity stamp re-bases the inactivity anchor and the next verdict is a full
	// turnTimeoutSeconds away. An agent that answers every probe works forever.
	if res.Answered() {
		if err := r.recordProbeAnswer(ctx, task, now); err != nil {
			return ctrl.Result{}, err
		}
		obs.StallProbe(obs.StallProbeAnswered)
		l.Info("stall probe answered; the turn is alive and continues",
			"action", "stall_probe_answered", "resource_id", task.Name,
			"turn_id", task.Annotations[annCurrentTurn], "probe_id", probeID,
			"answer", res.Answer, "agent_kind", agentKind)
		return ctrl.Result{RequeueAfter: stageRequeue}, nil
	}

	// Still inside the grace window. The probe is delivered at the agent's next
	// TOOL-CALL BOUNDARY, so "no answer yet" from a healthy agent inside a long
	// tool call is the NORMAL reading here, not a verdict.
	if sentAt, ok := parseStallProbeSentAt(task); ok {
		if deadline := sentAt.Add(stallProbeGrace(proj)); now.Before(deadline) {
			return ctrl.Result{RequeueAfter: deadline.Sub(now)}, nil
		}
	}

	// The grace window is spent. WHICH failure it is decides nothing here - both
	// escalate - but they are counted apart because they are different faults with
	// different fixes, and telling them apart in the fleet is the only way to know
	// whether the probe mechanism is working at all.
	outcome := obs.StallProbeUnanswered
	if res.State != agent.ProbeStateDelivered {
		outcome = obs.StallProbeNeverDelivered
	}
	obs.StallProbe(outcome)

	attempts := stallProbeAttempts(task)
	maxAttempts := stallProbeMaxAttempts(proj)
	if attempts < maxAttempts {
		l.Info("stall probe went unanswered; asking again",
			"action", "stall_probe_unanswered", "resource_id", task.Name,
			"turn_id", task.Annotations[annCurrentTurn], "probe_id", probeID,
			"probe_state", res.State, "outcome", outcome,
			"attempt", attempts, "max_attempts", maxAttempts, "agent_kind", agentKind)
		return r.sendStallProbe(ctx, proj, task, agentKind, now, attempts+1)
	}

	l.Info("stall probe ladder exhausted; interrupting the turn and stopping the pod",
		"action", "stall_escalation", "resource_id", task.Name,
		"turn_id", task.Annotations[annCurrentTurn], "probe_id", probeID,
		"probe_state", res.State, "outcome", outcome,
		"attempts", attempts, "agent_kind", agentKind)
	return r.escalateStall(ctx, proj, task, agentKind, now)
}

// escalateStall is the end of the ladder: ESC, then the existing G.7 stop.
//
// THE ESC IS NOT OPTIONAL AND IT MUST PRECEDE THE STOP. `stalledTurnStop` opens
// by waiting for the wrapper to go idle and only then submits the handoff turn -
// and a wedged turn never goes idle, so that wait times out, `POST /v1/messages`
// would 409 anyway, and the sequence falls through to the operator's synthetic
// note. That is what happens today, on essentially every stall. Sending ESC
// first CANCELS the in-flight turn (measured ~40ms, mid-tool, with session,
// sessionId, JSONL, workspace and context all surviving), so the wrapper reaches
// idle for real and the agent gets to write its OWN handoff - which is the whole
// difference between continuation state and a guess.
//
// The wait the plan calls for is `stalledTurnStop`'s own: it is already bounded
// by StalledTurnHandoffWait (MaxWait), which is exactly the "seconds-wide race
// where the turn completes" window the ESC has just made near-certain to close.
// A second, blocking StalledTurnHandoffWait here would double the reconcile's
// block for nothing.
//
// An Interrupt that fails is NOT fatal. The stop below is the same stop this
// Task got before this phase, and it copes with a turn that never clears - by
// definition, since that is the path it has been taking in production all along.
func (r *TaskReconciler) escalateStall(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, agentKind string, now time.Time) (ctrl.Result, error) {

	l := log.FromContext(ctx)
	if err := r.Session.Interrupt(ctx, agent.BaseURL(task, task.Namespace)); err != nil {
		l.Info("stall interrupt failed; stopping without it (the stop tolerates a turn that never clears)",
			"action", "stall_interrupt_failed", "resource_id", task.Name,
			"turn_id", task.Annotations[annCurrentTurn], "error", err.Error(),
			"unsupported", agent.IsProbeUnsupported(err), "agent_kind", agentKind)
	} else {
		l.Info("interrupted the stalled turn so its handoff can be submitted",
			"action", "stall_interrupted", "resource_id", task.Name,
			"turn_id", task.Annotations[annCurrentTurn], "agent_kind", agentKind)
	}
	// stalledTurnStop clears the turn annotations, and the probe trio goes with
	// them (clearTurnAnnotations owns all seven).
	return r.stalledTurnStop(ctx, proj, task, agentKind, now)
}

// stallProbeFallback is THE SAFETY PROPERTY, and it is why this phase can deploy
// ahead of the new wrapper image.
//
// A wrapper with no probe endpoints answers 404/405, which agent maps to
// ErrProbeUnsupported. A wrapper that is unreachable, or 5xx-ing, or timing out,
// answers with an ordinary error. In BOTH cases the operator has no evidence
// about the agent, which is the exact epistemic position the pre-probe operator
// was in permanently - so it does what the pre-probe operator did, calling the
// SAME `stalledTurnStop` with the SAME arguments. Not a variant of it, not a
// park, not a failure: an old wrapper under a new operator behaves
// byte-identically to an old wrapper under an old operator.
func (r *TaskReconciler) stallProbeFallback(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, agentKind string, now time.Time, cause error, call string) (ctrl.Result, error) {

	outcome := obs.StallProbeError
	if agent.IsProbeUnsupported(cause) {
		outcome = obs.StallProbeUnsupported
	}
	obs.StallProbe(outcome)
	log.FromContext(ctx).Info("stall probe unavailable; falling back to the pre-probe stalled-turn stop",
		"action", "stall_probe_fallback", "resource_id", task.Name,
		"turn_id", task.Annotations[annCurrentTurn], "call", call,
		"outcome", outcome, "error", cause.Error(), "agent_kind", agentKind)
	return r.stalledTurnStop(ctx, proj, task, agentKind, now)
}

// recordProbeAnswer retires the probe AND stamps the activity the answer proves.
//
// Both in ONE patch, because they are one fact. Clearing the probe without
// re-basing the anchor would leave the turn still past its window, so the very
// next reconcile would open a fresh ladder against an agent that just told us it
// is working - and the ladder would spend its attempts and escalate anyway.
func (r *TaskReconciler) recordProbeAnswer(ctx context.Context, task *tatarav1alpha1.Task, now time.Time) error {
	if err := r.patchTaskAnnotations(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations[annTurnLastActivity] = now.UTC().Format(time.RFC3339)
		delete(fresh.Annotations, annStallProbeID)
		delete(fresh.Annotations, annStallProbeAt)
		delete(fresh.Annotations, annStallProbeAttempts)
		return true
	}); err != nil {
		return fmt.Errorf("record stall probe answer %s: %w", task.Name, err)
	}
	return nil
}

// clearStallProbe retires an episode WITHOUT touching the activity anchor: the
// caller has already established that the anchor moved on its own.
func (r *TaskReconciler) clearStallProbe(ctx context.Context, task *tatarav1alpha1.Task) error {
	if err := r.patchTaskAnnotations(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		if !taskStallProbeInFlight(fresh) && fresh.Annotations[annStallProbeAttempts] == "" {
			return false
		}
		delete(fresh.Annotations, annStallProbeID)
		delete(fresh.Annotations, annStallProbeAt)
		delete(fresh.Annotations, annStallProbeAttempts)
		return true
	}); err != nil {
		return fmt.Errorf("clear stall probe %s: %w", task.Name, err)
	}
	return nil
}

// parseStallProbeSentAt reads the outstanding probe's send time. An absent or
// unparseable stamp reports false, and the caller then treats the grace as
// ALREADY spent - the conservative direction, since the alternative is a probe
// whose window never expires and a stall that is never escalated.
func parseStallProbeSentAt(task *tatarav1alpha1.Task) (time.Time, bool) {
	raw := task.Annotations[annStallProbeAt]
	if raw == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// THE AGENT EXPLICITLY ASKING TO BE STOPPED NEEDS NO NEW MECHANISM.
//
// It already exists and agents already use it: `task_note(kind=handoff)` is an
// agent saying "here is everything the next pod needs". The gap was that nothing
// ACTED on it - the pod carried on existing until some clock (the pod TTL, the
// conversation idle budget, the idle reaper) happened to come round, up to hours
// later, holding a concurrency slot for an agent that had already finished
// talking.
//
// Two guards, both load-bearing:
//
//   - NO TURN IN FLIGHT. A note written mid-turn is the agent narrating, not
//     finishing; the turn is entitled to run to its end.
//   - WRITTEN BY THIS POD (`HasAgentHandoffNoteSince(task, podStartedAt)`). The
//     journal is per-Task and every graceful stop ends by putting a handoff note
//     in it, so a CONTINUATION pod is born into a Task that already carries one.
//     An unscoped check would kill every replacement pod at boot, forever.
func (r *TaskReconciler) agentAskedToBeStopped(task *tatarav1alpha1.Task) bool {
	if task.Status.PodStartedAt == nil || taskHasInflightTurn(task) {
		return false
	}
	return agent.HasAgentHandoffNoteSince(task, task.Status.PodStartedAt.Time)
}

// stopAfterAgentHandoff tears the pod down NOW because the agent asked it to.
//
// It deliberately does NOT run the G.7 stop sequence. That sequence exists to
// EXTRACT a handoff from an agent that has not given one - wait for idle, submit
// the one handoff turn, fall back to a synthetic note. Here the agent has
// already written it, unprompted, so every step of that sequence would spend a
// real turn's worth of tokens and wall time re-asking a question that has been
// answered.
//
// ONE step of it is still owed, and it is the one the agent structurally cannot
// perform: the failed-repos note. See AppendFailedReposNote. This is also the
// path it is needed on MOST - it fires whenever an agent ends its work
// properly, where the TTL path fires only when a clock came round first.
func (r *TaskReconciler) stopAfterAgentHandoff(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, agentKind string, now time.Time) (ctrl.Result, error) {

	l := log.FromContext(ctx)
	// Best-effort: a wrapper that is already gone, wedged or unreachable must not
	// keep the pod alive. The Delete below is what actually ends it.
	if err := r.Session.DeleteSession(ctx, agent.BaseURL(task, task.Namespace)); err != nil {
		l.Info("agent-requested stop: session delete failed; deleting the pod anyway",
			"action", "agent_stop_session_delete_failed", "resource_id", task.Name, "error", err.Error())
	}
	if err := r.deleteWrapper(ctx, task); err != nil {
		return ctrl.Result{}, fmt.Errorf("agent-requested stop %s: %w", task.Name, err)
	}
	if err := clearTurnAnnotations(ctx, r.Client, task); err != nil {
		return ctrl.Result{}, fmt.Errorf("agent-requested stop clear %s: %w", task.Name, err)
	}
	// BEFORE the clear below, and not covered by the agent's note: the agent had
	// no way to know a push failed at turn end (the wrapper reports it to pod
	// stdout only), so this is the one fact its handoff cannot carry.
	// Best-effort, and ordered ahead of the patch because the report is worth more
	// than the tidy state: if the patch then fails, respawnLostPod picks the Task
	// up with the payload deliberately un-retired, and the note is already
	// written. AppendFailedReposNote is idempotent on the body, so that second
	// pass does not double it - and the body names its turn, so a LATER turn
	// losing the same repos is still recorded rather than swallowed by the older
	// note that this unconditional clearLastTurn is about to make unrecoverable.
	agent.AppendFailedReposNote(ctx,
		&agent.FitNoteAppender{Client: r.Client, Spiller: r.spiller(proj), Namespace: task.Namespace},
		task, task.Status.LastTurnFailedRepos, task.Status.LastTurnReposTurnID, now)
	if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		fresh.Status.PodStartedAt = nil
		fresh.Status.StateWorkStartedAt = nil
		// The handoff note the agent wrote IS the continuation state now, so the
		// last-turn payload has been spent (#527) and must not be re-used by a
		// later synthetic note.
		clearLastTurn(fresh)
		return true
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("agent-requested stop re-arm %s: %w", task.Name, err)
	}
	l.Info("agent asked to be stopped (handoff note, no turn in flight); pod stopped",
		"action", "agent_requested_stop", "resource_id", task.Name,
		"state", task.Status.State, "agent_kind", agentKind)
	return ctrl.Result{RequeueAfter: agentBootRequeue}, nil
}
