package stage

import (
	"time"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// liveStates are the states that carry a LIVE agent pod - one whose clock is
// the IDLE timer on status.conversationLastEventAt rather than a work budget,
// and into which a queued event is delivered as a further turn. It is the
// `conversing` stage's mechanism, promoted to a property so it composes with
// parkReason instead of competing with it.
//
// A parked Task is NEVER live, whatever its state says: park is what takes the
// pod down. That is enforced by the reconciler (see repairParkedWithLivePod),
// not by this table, because the table is pure and the pod is not.
var liveStates = map[string]bool{
	v1alpha1.StateRefined:             true,
	v1alpha1.StateUnderImplementation: true,
	v1alpha1.StateAwaitingReview:      true,
}

// Live reports whether state carries a live agent pod.
func Live(state string) bool { return liveStates[state] }

// operatorDriven replaces podlessStages. Two members, not eight: new runs the
// triage pass and then leaves immediately, done/rejected run nothing and are
// reaped, and the three live states are covered by Live. What is left is the
// two states where the OPERATOR does the work - merging and deploying.
var operatorDriven = map[string]bool{
	v1alpha1.StateMerged:   true,
	v1alpha1.StateDeployed: true,
}

// OperatorDriven reports whether the operator, not an agent, advances state.
func OperatorDriven(state string) bool { return operatorDriven[state] }

// AllStates returns the 8 members of the state enum, in lifecycle order.
func AllStates() []string {
	return []string{
		v1alpha1.StateNew, v1alpha1.StateRefined, v1alpha1.StateUnderImplementation,
		v1alpha1.StateAwaitingReview, v1alpha1.StateMerged, v1alpha1.StateDeployed,
		v1alpha1.StateDone, v1alpha1.StateRejected,
	}
}

// originAgentKinds maps Task.spec.kind (the ORIGIN) to the agent kind that runs
// the work states. It is DATA, not a switch with a default: an origin kind not
// in it maps to "", which fails closed at pod spawn.
var originAgentKinds = map[string]string{
	"implement":     AgentImplement,
	"takeover":      AgentImplement,
	"review":        AgentReview,
	"brainstorm":    AgentBrainstorm,
	"incident":      AgentIncident,
	"refine":        AgentRefine,
	"documentation": AgentDocumentation,
}

// AgentKindFor is the F.2 table, now keyed on (state, spec.kind) because the
// state enum is kind-agnostic.
//
// The old table mapped stage -> kind and worked only because stages were
// kind-specific (brainstorming, investigating, refining, clarifying). With
// eight generic states it cannot: a brainstorm Task in refined needs a
// brainstorm agent, not an implement one.
//
// It returns "" for an operator-driven state, for `new` (operator triage, which
// leaves immediately), for the two terminals, and for an origin kind that is
// not in originAgentKinds - the last of which is a FAIL-CLOSED, not an
// oversight.
func AgentKindFor(state, specKind string) string {
	switch state {
	case v1alpha1.StateRefined, v1alpha1.StateUnderImplementation:
		return originAgentKinds[specKind]
	case v1alpha1.StateAwaitingReview:
		return AgentReview
	default:
		return ""
	}
}

// TurnInFlight reports whether an agent turn is running right now: a turn was
// submitted and no completion has been recorded for it.
//
// It is the ONE predicate that separates "the conversation is waiting on a
// human" from "the agent is working", and the idle clock needs it because
// Status.ConversationLastEventAt only ever moves on a HUMAN comment. Reading the
// two turn annotations does not make this package impure - they are declared in
// api/v1alpha1 alongside the status fields and are as much a part of the Task's
// observable state as Status is. The controller's taskHasInflightTurn delegates
// here so the two can never drift apart.
func TurnInFlight(t *v1alpha1.Task) bool { return turnInFlight(t) }

func turnInFlight(t *v1alpha1.Task) bool {
	return t.Annotations[v1alpha1.AnnCurrentTurn] != "" &&
		t.Annotations[v1alpha1.AnnTurnComplete] == ""
}

// idleBase is the instant the current silence began: the LATEST of the last
// human conversation event, the moment this pod became ready, and the end of the
// agent's last turn. Every one of the three is something that gave the human
// fresh reason to be silent.
//
// THE TURN END. A turn that ran for three hours means the human has had nothing
// to reply to for three hours. Charging that silence to the conversation parks
// the Task the instant the agent finishes speaking - the exact moment the human
// is first able to answer.
//
// THE POD-READY STAMP. ConversationLastEventAt is stamped on ENTRY into a live
// state, and a Task can then wait hours in the admission queue. Without this term
// the idle clock is spent before the pod exists, so the Task parks on the first
// reconcile after pod-ready and turn 0 is never submitted: the agent never
// speaks, and the human never gets anything to be silent about.
//
// THE RATCHET THIS ADMITS, stated rather than discovered later. Both extending
// terms are re-stamped by a pod ROTATION (the G.7 TTL stop nils
// stateWorkStartedAt and the replacement re-stamps it; the replacement's turn 0
// completes), so an ABANDONED conversation on a project whose
// agentPodTTLSeconds is at or below its conversationIdleMinutes can rotate a pod
// instead of parking, repeatedly. Since O3 it is bounded ONE way and that way is
// the only reason the ratchet is still acceptable: ResidencyExceeded, cumulative,
// at ResidencyCapAll. The turn and pod-recreation budgets that used to be the
// other two bounds are gone; the rotation churn they used to cap is now watched
// by the operator_pod_recreations_total alert instead. The alternative - a base
// that only a human can move - is the #521 regression itself, which killed
// working agents at 60 minutes.
//
// An unparseable or absent turn stamp falls back to the other two: a malformed
// annotation must never widen a deadline. ok is false when there is no
// conversation stamp at all, which is what Enter exists to prevent.
func idleBase(t *v1alpha1.Task) (time.Time, bool) {
	if t.Status.ConversationLastEventAt == nil {
		return time.Time{}, false
	}
	base := t.Status.ConversationLastEventAt.Time
	if t.Status.StateWorkStartedAt != nil && t.Status.StateWorkStartedAt.After(base) {
		base = t.Status.StateWorkStartedAt.Time
	}
	if raw := t.Annotations[v1alpha1.AnnTurnComplete]; raw != "" {
		if at, err := time.Parse(time.RFC3339, raw); err == nil && at.After(base) {
			base = at
		}
	}
	return base, true
}

// ResidencyCapAll is THE ONE SURVIVING CEILING, and it is a CONSTANT.
//
// The stall-detection rework (O3) deleted maxTurnsPerTask, maxReviewRounds and
// maxPodRecreations as terminals: every one of them killed agents that were
// working, because a turn count, a review round and a pod recreation are all
// proxies for "stuck" and none of them measures it. What replaced them is the
// wrapper's probe/interrupt state machine (W3/W4, O2), which asks the agent
// whether it is alive instead of counting how much it has done.
//
// Residency is what is left: a DEAD-MAN SWITCH, not a work budget. It exists so
// that a Task the probe machinery cannot reach at all - a wedged pod, a boot
// crash loop, an operator bug - still has a terminus. "No feature should exceed
// 24h ever" is the platform invariant it encodes.
//
// IT IS A CONSTANT AND NOT A CRD FIELD, deliberately. A CRD field can be
// silently dropped by structural-schema pruning when the schema and the values
// fall out of lockstep, and the failure mode of a silently-pruned dead-man
// switch is no dead-man switch at all. A constant cannot be pruned, cannot be
// mis-set per project, and cannot drift between the operator and helmfile.
const ResidencyCapAll = 24 * time.Hour

// residencyCaps is the ABSOLUTE bound on time spent in a live state, whatever
// the idle clock says. Only the live states have one: an operator-driven state
// already has a work budget, and new/done/rejected are not where an agent runs.
//
// All three are ResidencyCapAll. They used to be the old kind-specific stage
// budgets (clarifying 24h, implementing 6h, reviewing 4h) and those two shorter
// ones were exactly the "working agent killed by a clock that measures the wrong
// thing" shape O3 removed everywhere else - a chatty review round-trip or a long
// healthy coding run is not a stall at 4h or 6h. The known cost is that a
// genuinely wedged Task holds a concurrency slot up to four times longer; the
// compensation is the +2 on maxConcurrentAgents that ships with it.
var residencyCaps = map[string]time.Duration{
	v1alpha1.StateRefined:             ResidencyCapAll,
	v1alpha1.StateUnderImplementation: ResidencyCapAll,
	v1alpha1.StateAwaitingReview:      ResidencyCapAll,
}

// ResidencyExceeded is THE MANDATORY MITIGATION for the one genuine regression
// in promoting liveness to a property: a live state arms the IDLE clock instead
// of the WORK clock, so under-implementation loses the absolute residency bound
// the old `implementing` stage had (6h). An agent ping-ponging with a reviewer
// would otherwise sit forever, because every comment re-stamps the idle base.
//
// It is a SEPARATE check, not a second return value from ArmedClock: ArmedClock
// returns exactly one clock by construction and that is the property that makes
// the three-clock model auditable. Whichever of the two fires first wins.
//
// It requires StateWorkStartedAt != nil (fix B3). StateEnteredAt is stamped the
// instant the Task enters the state - which is when its QueuedEvent is
// ENQUEUED, not when a pod exists - so a Task that has never been admitted can
// sit in the admission queue for hours with no pod at all. Charging that queue
// time against the 6h/4h live-state caps parks it at stage-deadline
// (UnparkNever) well before its actual 24h admission budget (clock 1) has run,
// and it ages out and gets reaped for the crime of waiting for a slot. This is
// verbatim the failure StateWorkStartedAt's own doc comment
// (api/v1alpha1/task_types.go) exists to prevent for clock 2; residency is
// clock 2's absolute-bound sibling and needs the same admission gate. Once work
// has actually started, the bound applies exactly as before.
//
// It reads StateElapsedSeconds, so once armed it is cumulative across a
// park/un-park round trip - deliberately. A Task that spends six hours in
// under-implementation across three re-entries has spent six hours there, and
// buying a fresh cap per re-entry is the unbounded-loop shape #480 killed for
// merging.
func ResidencyExceeded(t *v1alpha1.Task, now time.Time) bool {
	capacity, ok := residencyCaps[t.Status.State]
	if !ok {
		return false
	}
	if t.Status.StateEnteredAt == nil {
		return false
	}
	if t.Status.StateWorkStartedAt == nil {
		return false
	}
	return StateElapsedSeconds(t, now) > capacity.Seconds()
}

// ResidencyCap exposes the absolute bound for state, for reporting. ok is false
// for a state that has none.
func ResidencyCap(state string) (time.Duration, bool) {
	d, ok := residencyCaps[state]
	return d, ok
}
