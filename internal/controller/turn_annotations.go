package controller

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// THE TURN ANNOTATIONS ARE POD-SCOPED, AND THAT IS THE WHOLE INVARIANT.
//
// current-turn / turn-started-at / turn-last-activity-at / turn-complete
// describe ONE turn running inside ONE wrapper pod. submitStageTurn is their
// only writer and it writes them against a live pod. When that pod goes away the
// turn it was running is over by construction, and the annotations must go with
// it: a Task with no pod that still says "a turn is in flight" is a lie every
// reader of those annotations then acts on.
//
// WHAT THE LIE COSTS, because it is not just log volume (issue #566):
//
//   - turnTimedOut() anchors on max(turn-started-at, turn-last-activity-at).
//     Both are frozen the moment the pod dies, so the predicate is permanently
//     true. PollOnce evaluates it every 30s and logs turn_timeout forever - 1109
//     lines in 30 minutes across 19 Tasks, 100% of the operator's INFO volume,
//     measured live on v2.1.5.
//   - stage.TurnInFlight() is true forever, which (a) disarms the idle clock in
//     ArmedClock, (b) makes the Task permanently ineligible as an eviction
//     victim in evictionCandidates, (c) exempts its pod from the idle-pod reaper,
//     and (d) gags the conversation follow-up turn in reconcilePodStage, which
//     silently swallows every subsequent human comment.
//
// So the clear is not a log-silencer. It is what keeps four other mechanisms
// honest.
//
// #551 is where the invariant broke. It moved the stalled-turn teardown out of
// PollOnce (which hard-deleted the pod with no handoff) into
// TaskReconciler.stalledTurnStop, which does the graceful G.7 stop AND clears
// these four - but only ever runs for a Task with a live pod
// (reconcilePodStage's turnTimedOut branch sits behind
// `task.Status.PodStartedAt != nil`, and reconcileStage returns early for a
// parked Task before it). For every OTHER way a pod ends there was no clear at
// all: stalledTurnStop was the single site in the repo that cleared them.
//
// EVERY CALLER OF THIS HELPER IS A SITE WHERE THE POD ENDED. Adding a new
// pod-teardown or pod-clock re-arm path without calling it re-opens #566.
func clearTurnAnnotations(ctx context.Context, c client.Client, task *tatarav1alpha1.Task) error {
	return patchTaskAnnotations(ctx, c, task, func(fresh *tatarav1alpha1.Task) bool {
		if !taskCarriesTurnAnnotations(fresh) {
			return false // nothing to clear: skip the write entirely
		}
		delete(fresh.Annotations, tatarav1alpha1.AnnCurrentTurn)
		delete(fresh.Annotations, tatarav1alpha1.AnnTurnStartedAt)
		delete(fresh.Annotations, tatarav1alpha1.AnnTurnLastActivity)
		delete(fresh.Annotations, tatarav1alpha1.AnnTurnComplete)
		return true
	})
}

// taskCarriesTurnAnnotations reports whether any of the four turn annotations is
// present. It is the "is a write needed" half of clearTurnAnnotations, split out
// so the no-op case costs zero API writes - PollOnce runs this over every Task
// in the namespace every 30 seconds.
func taskCarriesTurnAnnotations(t *tatarav1alpha1.Task) bool {
	if len(t.Annotations) == 0 {
		return false
	}
	for _, k := range []string{
		tatarav1alpha1.AnnCurrentTurn,
		tatarav1alpha1.AnnTurnStartedAt,
		tatarav1alpha1.AnnTurnLastActivity,
		tatarav1alpha1.AnnTurnComplete,
	} {
		if _, ok := t.Annotations[k]; ok {
			return true
		}
	}
	return false
}
