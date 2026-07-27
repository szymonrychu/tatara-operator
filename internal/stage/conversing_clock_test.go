package stage_test

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// The conversing clock is an IDLE timer, reset per event. It must NOT measure
// from stageWorkStartedAt: ttlStop nils that stamp and PodWatchReconciler
// re-stamps it at the replacement pod's readiness, so a pod rotation would
// silently reset the idle timer and an idle conversation would never park.
//
// This test simulates the ACTUAL rotation sequence, not a static snapshot: it
// nils PodStartedAt/StageWorkStartedAt (as ttlStop does) and then re-stamps
// BOTH to a time AFTER ConversationLastEventAt (as PodWatchReconciler does at
// the replacement pod's readiness). Pod stamps newer than the last event is
// the one arrangement that would expose a regression - e.g. a future
// max(lastEvent, stageWorkStartedAt) fallback - because the freshly rotated
// pod would otherwise look "alive" even though the conversation has been
// silent well past budget.
func TestConversingClockMeasuresIdleNotPodWork(t *testing.T) {
	entered := time.Date(2026, 7, 27, 9, 0, 0, 0, time.UTC)
	lastEvent := entered.Add(5 * time.Minute)

	task := &v1alpha1.Task{}
	task.Status.Stage = v1alpha1.StageConversing
	task.Status.StageEnteredAt = &metav1.Time{Time: entered}
	task.Status.ConversationLastEventAt = &metav1.Time{Time: lastEvent}

	// The rotation: ttlStop nils both pod stamps, takes a handoff turn, deletes
	// the pod. PodWatchReconciler then re-stamps both at the REPLACEMENT pod's
	// readiness - well after lastEvent, since the pod rotated (TTL elapsed)
	// precisely because the conversation had already gone quiet.
	task.Status.PodStartedAt = nil
	task.Status.StageWorkStartedAt = nil
	rotatedAt := lastEvent.Add(65 * time.Minute)
	task.Status.PodStartedAt = &metav1.Time{Time: rotatedAt}
	task.Status.StageWorkStartedAt = &metav1.Time{Time: rotatedAt}

	clock, since, budget, edge := stage.ArmedClock(task, false)
	if clock != stage.ClockWork {
		t.Fatalf("clock = %q, want %q", clock, stage.ClockWork)
	}
	if !since.Equal(lastEvent) {
		t.Fatalf("since = %v, want the last event at %v (NOT the freshly rotated pod stamp %v)", since, lastEvent, rotatedAt)
	}
	if budget != v1alpha1.ConversationIdleDefault {
		t.Errorf("budget = %v, want %v", budget, v1alpha1.ConversationIdleDefault)
	}
	if edge.To != v1alpha1.StageParked || edge.Reason != stage.ReasonAwaitingHuman {
		t.Errorf("edge = %s(%s), want parked(awaiting-human)", edge.To, edge.Reason)
	}

	// Just short of budget, measured from the LAST EVENT, is still live -
	// regardless of the (later) rotated pod stamps sitting in the fixture.
	if _, fired := stage.Elapsed(task, false, lastEvent.Add(59*time.Minute)); fired {
		t.Error("the conversation aged out 59 minutes after the last event: the idle timer is not being reset per event")
	}

	// ONE MINUTE after the rotation, the replacement pod looks brand new - but
	// the conversation has been silent for 66 minutes (past the 60-minute
	// default budget), so it must still be idle-expired. A clock that measured
	// pod age, or fell back to it when newer, would wrongly call this live.
	now := rotatedAt.Add(1 * time.Minute)
	if edge, fired := stage.Elapsed(task, false, now); !fired ||
		edge.To != v1alpha1.StageParked || edge.Reason != stage.ReasonAwaitingHuman {
		t.Fatalf("Elapsed 1m after the pod rotation (66m after the last event) = (%v, %v), want parked(awaiting-human), true: "+
			"a pod rotation must not reset the idle timer", edge, fired)
	}
}

// A conversing Task with no last-event stamp has never been armed. It must not
// fall through into the generic pod-stage clock selector, which would read a
// stale podStartedAt and TTL-shape the conversation.
func TestConversingWithNoLastEventIsUnarmed(t *testing.T) {
	task := &v1alpha1.Task{}
	task.Status.Stage = v1alpha1.StageConversing
	task.Status.StageEnteredAt = &metav1.Time{Time: time.Now()}
	if clock, _, _, _ := stage.ArmedClock(task, false); clock != stage.ClockNone {
		t.Fatalf("clock = %q, want none", clock)
	}
}

// A paused project must not shred conversations either: the pause carve-out
// covers the admission clock, and conversing runs a work clock, so a paused
// project's conversing Task still ages out normally. Assert the current,
// deliberate behaviour so a later change is a decision, not an accident.
func TestConversingClockIgnoresPause(t *testing.T) {
	last := time.Date(2026, 7, 27, 11, 0, 0, 0, time.UTC)
	task := &v1alpha1.Task{}
	task.Status.Stage = v1alpha1.StageConversing
	task.Status.StageEnteredAt = &metav1.Time{Time: last}
	task.Status.ConversationLastEventAt = &metav1.Time{Time: last}
	if clock, _, _, _ := stage.ArmedClock(task, true); clock != stage.ClockWork {
		t.Fatalf("clock under pause = %q, want work", clock)
	}
}
