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
func TestConversingClockMeasuresIdleNotPodWork(t *testing.T) {
	entered := time.Date(2026, 7, 27, 9, 0, 0, 0, time.UTC)
	lastEvent := time.Date(2026, 7, 27, 11, 0, 0, 0, time.UTC)

	task := &v1alpha1.Task{}
	task.Status.Stage = v1alpha1.StageConversing
	task.Status.StageEnteredAt = &metav1.Time{Time: entered}
	task.Status.PodStartedAt = &metav1.Time{Time: entered}
	task.Status.StageWorkStartedAt = &metav1.Time{Time: entered}
	task.Status.ConversationLastEventAt = &metav1.Time{Time: lastEvent}

	clock, since, budget, edge := stage.ArmedClock(task, false)
	if clock != stage.ClockWork {
		t.Fatalf("clock = %q, want %q", clock, stage.ClockWork)
	}
	if !since.Equal(lastEvent) {
		t.Errorf("since = %v, want the last event at %v", since, lastEvent)
	}
	if budget != v1alpha1.ConversationIdleDefault {
		t.Errorf("budget = %v, want %v", budget, v1alpha1.ConversationIdleDefault)
	}
	if edge.To != v1alpha1.StageParked || edge.Reason != stage.ReasonAwaitingHuman {
		t.Errorf("edge = %s(%s), want parked(awaiting-human)", edge.To, edge.Reason)
	}

	// 59 minutes after the LAST EVENT (two hours after pod start) is still live.
	if _, fired := stage.Elapsed(task, false, lastEvent.Add(59*time.Minute)); fired {
		t.Error("the conversation aged out 59 minutes after the last event: the idle timer is not being reset per event")
	}
	// 61 minutes after the last event is idle.
	if edge, fired := stage.Elapsed(task, false, lastEvent.Add(61*time.Minute)); !fired ||
		edge.To != v1alpha1.StageParked || edge.Reason != stage.ReasonAwaitingHuman {
		t.Errorf("Elapsed at +61m = (%v, %v), want parked(awaiting-human), true", edge, fired)
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
