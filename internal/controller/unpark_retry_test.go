package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// retryParkedTask builds a Task parked in the retry lane at an explicit state.
// It does not go through wfParkedTask, which infers the state from (kind,
// reason): a retry park is written from the merge corridor and from
// awaiting-review, and which one it is decides whether the live-pod ceiling has
// anything to say about the release.
func retryParkedTask(name, state, reason string) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: mdNS, UID: types.UID("uid-" + name)},
		Spec:       tatarav1alpha1.TaskSpec{Kind: "implement", ProjectRef: "proj", Goal: "g"},
		Status: tatarav1alpha1.TaskStatus{
			State:           state,
			ParkReason:      reason,
			ParkedFromState: state,
			ParkedAt:        &metav1.Time{Time: time.Now().Add(-time.Hour)},
			StateEnteredAt:  &metav1.Time{Time: time.Now().Add(-time.Hour)},
		},
	}
}

// TestDriveUnparksArmsAnUnarmedRetryPark: a retry park whose schedule is nil is
// ARMED, not released. That self-heals a park written by a path that forgot the
// schedule (or by a build that predates it) instead of stranding it, and it
// costs exactly one backoff of latency.
func TestDriveUnparksArmsAnUnarmedRetryPark(t *testing.T) {
	task := retryParkedTask("t-retry-unarmed", tatarav1alpha1.StateMerged, stage.ReasonCIFailed)
	c := newMirrorClient(t, task)
	r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics()}
	now := time.Now()

	if err := r.driveUnparks(context.Background(), wfProject(), now); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	got := mdGetTask(t, c, task.Name)
	if !tatarav1alpha1.Parked(got) {
		t.Fatalf("an unarmed retry park was RELEASED on sight; it must serve a backoff first")
	}
	if got.Status.RetryAttempts != 1 {
		t.Fatalf("retryAttempts = %d, want 1: arming spends the lap it schedules", got.Status.RetryAttempts)
	}
	if got.Status.RetryNextAt == nil {
		t.Fatalf("retryNextAt is still nil; the park is stranded exactly as before")
	}
	if want := now.Add(tatarav1alpha1.UnparkRetryBackoffBase); got.Status.RetryNextAt.Time.Before(want.Add(-time.Second)) {
		t.Fatalf("retryNextAt = %s, want the base backoff at %s", got.Status.RetryNextAt.Time, want)
	}
}

// TestDriveUnparksHonoursRetryNextAt is the backoff itself: waiting means
// waiting, and the pass that finds the schedule elapsed releases the park.
func TestDriveUnparksHonoursRetryNextAt(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name         string
		next         time.Time
		wantParked   bool
		wantAttempts int
	}{
		{"the backoff has not elapsed", now.Add(5 * time.Minute), true, 2},
		{"the backoff has elapsed", now.Add(-time.Second), false, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := retryParkedTask("t-retry-sched", tatarav1alpha1.StateMerged, stage.ReasonMergeConflictRetry)
			task.Status.RetryAttempts = 2
			task.Status.RetryNextAt = &metav1.Time{Time: tc.next}
			c := newMirrorClient(t, task)
			r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics()}

			if err := r.driveUnparks(context.Background(), wfProject(), now); err != nil {
				t.Fatalf("driveUnparks: %v", err)
			}
			got := mdGetTask(t, c, task.Name)
			if parked := tatarav1alpha1.Parked(got); parked != tc.wantParked {
				t.Fatalf("parked = %v (%q), want %v", parked, got.Status.ParkReason, tc.wantParked)
			}
			if got.Status.RetryAttempts != tc.wantAttempts {
				t.Fatalf("retryAttempts = %d, want %d: neither waiting nor releasing may charge a lap",
					got.Status.RetryAttempts, tc.wantAttempts)
			}
			if !tc.wantParked && got.Status.State != tatarav1alpha1.StateMerged {
				t.Fatalf("state = %s, want merged: an un-park never moves the Task", got.Status.State)
			}
		})
	}
}

// TestRetryParkDoesNotSpendLiveRoomWhileWaiting is the sharpest trap in the
// lane: liveRoomBudget is computed ONCE per pass and spent per Task that
// actually resumes a live state. A retry park that is still serving its backoff
// resumes nothing, so it must leave the budget for the human park behind it -
// otherwise one waiting retry silently starves every comment-driven un-park in
// the project for as long as its blocker stands.
func TestRetryParkDoesNotSpendLiveRoomWhileWaiting(t *testing.T) {
	now := time.Now()
	waiting := retryParkedTask("t-retry-waiting", tatarav1alpha1.StateAwaitingReview, stage.ReasonCIFailed)
	waiting.Status.RetryAttempts = 1
	waiting.Status.RetryNextAt = &metav1.Time{Time: now.Add(10 * time.Minute)}

	human := wfParkedTask("t-human-behind-it", "review", stage.ReasonAwaitingHuman)
	human.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "human", Body: "any news?",
	}}

	proj := wfProject()
	proj.Spec.MaxLivePods = 1
	c := newMirrorClient(t, proj, waiting, human)
	r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics()}

	if err := r.driveUnparks(context.Background(), proj, now); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	if got := mdGetTask(t, c, waiting.Name); !tatarav1alpha1.Parked(got) {
		t.Fatalf("the waiting retry park was released before its backoff elapsed")
	}
	if got := mdGetTask(t, c, human.Name); tatarav1alpha1.Parked(got) {
		t.Fatalf("the human park stayed parked(%s): the waiting retry consumed the live-pod budget it never used",
			got.Status.ParkReason)
	}
}

// TestDriveUnparksDoesNotChargeALapWhenThereIsNoLiveRoom: capacity is checked
// BEFORE the budget is charged. A project at its live ceiling is the operator's
// own constraint, not the blocker's, and spending the Task's retry laps on it
// would escalate a queue to a human as though the pipeline had failed.
func TestDriveUnparksDoesNotChargeALapWhenThereIsNoLiveRoom(t *testing.T) {
	now := time.Now()
	parked := retryParkedTask("t-retry-noroom", tatarav1alpha1.StateAwaitingReview, stage.ReasonCIFailed)
	occupant := retryParkedTask("t-occupant", tatarav1alpha1.StateUnderImplementation, "")
	occupant.Status.ParkReason = ""
	occupant.Status.ParkedFromState = ""

	proj := wfProject()
	proj.Spec.MaxLivePods = 1
	c := newMirrorClient(t, proj, parked, occupant)
	r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics()}

	if err := r.driveUnparks(context.Background(), proj, now); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	got := mdGetTask(t, c, parked.Name)
	if got.Status.RetryAttempts != 0 {
		t.Fatalf("retryAttempts = %d, want 0: a capacity refusal must spend no lap", got.Status.RetryAttempts)
	}
	if got.Status.RetryNextAt != nil {
		t.Fatalf("retryNextAt was armed against a ceiling refusal")
	}
}

// The lane must not touch a park it does not own.
func TestDriveUnparksLeavesANonRetryParkAlone(t *testing.T) {
	task := wfParkedTask("t-declined", "implement", stage.ReasonImplementDeclined)
	c := newMirrorClient(t, task)
	r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics()}

	if err := r.driveUnparks(context.Background(), wfProject(), time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.RetryAttempts != 0 || got.Status.RetryNextAt != nil {
		t.Fatalf("implement-declined was armed into the retry lane: attempts=%d next=%v",
			got.Status.RetryAttempts, got.Status.RetryNextAt)
	}
	if got.Status.ParkReason != stage.ReasonImplementDeclined {
		t.Fatalf("parkReason = %q, want unchanged implement-declined", got.Status.ParkReason)
	}
}
