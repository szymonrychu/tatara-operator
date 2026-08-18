package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
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

// rlForge is the retry lane's forge: the three calls the release gate and the
// escalation make, and nothing else. scm.SCMWriter is embedded nil so any other
// call panics.
type rlForge struct {
	scm.SCMWriter

	mergeState map[int]scm.MergeState
	prState    map[int]scm.PRState
	mergeErr   error
	prErr      error
	comments   []mbComment
	mergeCalls int
	prCalls    int
}

func (f *rlForge) GetMergeState(_ context.Context, _, _ string, number int) (scm.MergeState, error) {
	f.mergeCalls++
	if f.mergeErr != nil {
		return "", f.mergeErr
	}
	if ms, ok := f.mergeState[number]; ok {
		return ms, nil
	}
	return scm.MergeStateClean, nil
}

func (f *rlForge) GetPRState(_ context.Context, _, _ string, number int) (scm.PRState, error) {
	f.prCalls++
	if f.prErr != nil {
		return scm.PRState{}, f.prErr
	}
	if st, ok := f.prState[number]; ok {
		return st, nil
	}
	return scm.PRState{CIStatus: "success"}, nil
}

func (f *rlForge) Comment(_ context.Context, token, issueRef, body string) error {
	f.comments = append(f.comments, mbComment{token, issueRef, body})
	return nil
}

// rlReconciler wires a ProjectReconciler with a forge, which the release gate
// needs: without one it cannot confirm a blocker and falls back to releasing.
func rlReconciler(c client.Client, f *rlForge, m *obs.OperatorMetrics) *ProjectReconciler {
	return &ProjectReconciler{
		Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: m,
		SCMFor: func(string) (scm.SCMWriter, error) { return f, nil },
	}
}

// rlProject is wfProject plus the SCM secret the live blocker read needs.
func rlProject() *tatarav1alpha1.Project {
	p := wfProject()
	p.Spec.ScmSecretRef = "scm-secret"
	return p
}

// rlDueTask is a retry park whose backoff has elapsed: the pass that has to
// decide between a release and another lap.
func rlDueTask(name, state, reason string, attempts int) *tatarav1alpha1.Task {
	t := retryParkedTask(name, state, reason)
	t.Status.RetryAttempts = attempts
	t.Status.RetryBlocker = reason
	t.Status.RetryNextAt = &metav1.Time{Time: time.Now().Add(-time.Second)}
	t.Status.MRRefs = []string{tatarav1alpha1.MergeRequestName("repo-a", 7)}
	return t
}

// TestArmingARacedRetryParkIsAHandledNoOp is the CRITICAL one. driveUnparks
// Lists through the CACHED client and armRetryPark re-reads through the
// UNCACHED APIReader, so any writer in between - the webhook's ApplyUnpark,
// reconcileParkedExternalTerminal, a restapi takeover, a lagging informer -
// leaves the two disagreeing. The race branch used to return nil WITHOUT
// assigning the fresh copy, so the caller logged next_at off a status the
// unarmed path guarantees is nil and panicked in the leader's driver loop, and
// counted a scheduled lap for an arming that never happened.
func TestArmingARacedRetryParkIsAHandledNoOp(t *testing.T) {
	task := retryParkedTask("t-retry-raced", tatarav1alpha1.StateMerged, stage.ReasonCIFailed)
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	c := newMirrorClientIntercepted(t, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey,
			obj client.Object, opts ...client.GetOption) error {
			if err := cl.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			// The writer that got there first: the park is already armed by the
			// time the uncached read lands.
			if tk, ok := obj.(*tatarav1alpha1.Task); ok && tk.Name == "t-retry-raced" {
				tk.Status.RetryNextAt = &metav1.Time{Time: time.Now().Add(time.Minute)}
			}
			return nil
		},
	}, rlProject(), mdSecret(), task)
	r := rlReconciler(c, &rlForge{}, m)

	if err := r.driveUnparks(context.Background(), rlProject(), time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	if n := testutil.ToFloat64(m.TaskRetryScheduledCounter(stage.ReasonCIFailed)); n != 0 {
		t.Fatalf("operator_task_retry_scheduled_total = %v, want 0: no lap was armed on the race path", n)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.RetryAttempts != 0 {
		t.Fatalf("retryAttempts = %d, want 0: the raced pass must charge nothing", got.Status.RetryAttempts)
	}
}

// TestADueRetryLapCostsNoPodWhileTheBlockerStands is finding 3, and it is the
// claim MaxUnparkRetries=5 is justified by. merge-conflict-retry is parked at
// awaiting-review, where NOTHING in reconcileClocks covers mergeability - so a
// release there mints a review pod that the conflict sweep kills again up to
// five minutes later. The lane confirms the blocker itself instead.
func TestADueRetryLapCostsNoPodWhileTheBlockerStands(t *testing.T) {
	tests := []struct {
		name       string
		reason     string
		forge      *rlForge
		wantParked bool
	}{
		{
			name:   "the branch still conflicts",
			reason: stage.ReasonMergeConflictRetry,
			forge: &rlForge{mergeState: map[int]scm.MergeState{7: scm.MergeStateDirty},
				prState: map[int]scm.PRState{7: {CIStatus: "success"}}},
			wantParked: true,
		},
		{
			name:       "the conflict is gone",
			reason:     stage.ReasonMergeConflictRetry,
			forge:      &rlForge{mergeState: map[int]scm.MergeState{7: scm.MergeStateClean}},
			wantParked: false,
		},
		{
			name:       "the pipeline is still red at the reviewed head",
			reason:     stage.ReasonCIFailed,
			forge:      &rlForge{prState: map[int]scm.PRState{7: {CIStatus: "failure"}}},
			wantParked: true,
		},
		{
			name:       "the pipeline went green",
			reason:     stage.ReasonCIFailed,
			forge:      &rlForge{prState: map[int]scm.PRState{7: {CIStatus: "success"}}},
			wantParked: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			proj := rlProject()
			task := rlDueTask("t-retry-blocker", tatarav1alpha1.StateAwaitingReview, tc.reason, 1)
			mr := mdMR(task, "repo-a", 7)
			c := newMirrorClient(t, proj, mdSecret(), mdRepo("repo-a"), mr, task)
			m := obs.NewOperatorMetrics(prometheus.NewRegistry())
			r := rlReconciler(c, tc.forge, m)

			if err := r.driveUnparks(context.Background(), proj, now); err != nil {
				t.Fatalf("driveUnparks: %v", err)
			}
			got := mdGetTask(t, c, task.Name)
			if parked := tatarav1alpha1.Parked(got); parked != tc.wantParked {
				t.Fatalf("parked = %v (%q), want %v", parked, got.Status.ParkReason, tc.wantParked)
			}
			if !tc.wantParked {
				return
			}
			if got.Status.RetryAttempts != 2 {
				t.Fatalf("retryAttempts = %d, want 2: a standing blocker costs the NEXT lap, not a pod",
					got.Status.RetryAttempts)
			}
			if got.Status.RetryNextAt == nil || !got.Status.RetryNextAt.After(now) {
				t.Fatalf("retryNextAt = %v, want a fresh backoff in the future", got.Status.RetryNextAt)
			}
			if n := testutil.ToFloat64(m.TaskRetryScheduledCounter(tc.reason)); n != 1 {
				t.Fatalf("operator_task_retry_scheduled_total{reason=%s} = %v, want 1", tc.reason, n)
			}
		})
	}
}

// A read that FAILS is neither verdict: nothing is charged, nothing is
// released, and the still-due park is re-read on the next pass.
func TestAFailedBlockerReadChargesNothingAndReleasesNothing(t *testing.T) {
	proj := rlProject()
	task := rlDueTask("t-retry-readfail", tatarav1alpha1.StateAwaitingReview, stage.ReasonMergeConflictRetry, 1)
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("repo-a"), mdMR(task, "repo-a", 7), task)
	r := rlReconciler(c, &rlForge{mergeErr: context.DeadlineExceeded}, wfMetrics())

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err == nil {
		t.Fatalf("driveUnparks swallowed the failed blocker read")
	}
	got := mdGetTask(t, c, task.Name)
	if !tatarav1alpha1.Parked(got) {
		t.Fatalf("the park was released on a read that never answered")
	}
	if got.Status.RetryAttempts != 1 {
		t.Fatalf("retryAttempts = %d, want 1: the operator's own read failure is not the blocker's lap",
			got.Status.RetryAttempts)
	}
}

// TestAnArmedDueRetryParkRefusedOnCapacityStillEscalates: the exhaustion check
// used to sit ONLY on the unarmed branch, so a spent lane whose release the
// live-pod ceiling refused stayed armed and due forever - nothing charged,
// nothing escalated, operator_task_retry_exhausted_total flat for as long as
// the project stayed busy.
func TestAnArmedDueRetryParkRefusedOnCapacityStillEscalates(t *testing.T) {
	proj := rlProject()
	proj.Spec.MaxLivePods = 1
	task := rlDueTask("t-retry-capped", tatarav1alpha1.StateAwaitingReview,
		stage.ReasonMergeConflictRetry, tatarav1alpha1.MaxUnparkRetries)
	task.Status.IssueRefs = []string{tatarav1alpha1.IssueName("repo-a", 42)}
	occupant := retryParkedTask("t-occupant", tatarav1alpha1.StateUnderImplementation, "")
	occupant.Status.ParkReason = ""
	occupant.Status.ParkedFromState = ""

	c := newMirrorClient(t, proj, mdSecret(), mdRepo("repo-a"),
		mdMR(task, "repo-a", 7), mdIssue(task, "repo-a", 42), task, occupant)
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := rlReconciler(c, &rlForge{mergeState: map[int]scm.MergeState{7: scm.MergeStateClean}}, m)

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.ParkReason != stage.ReasonRetryExhausted {
		t.Fatalf("parkReason = %q, want retry-exhausted: a spent lane must not sit armed and due forever",
			got.Status.ParkReason)
	}
	if n := testutil.ToFloat64(m.TaskRetryExhaustedCounter(stage.ReasonMergeConflictRetry,
		tatarav1alpha1.StateAwaitingReview)); n != 1 {
		t.Fatalf("operator_task_retry_exhausted_total = %v, want 1", n)
	}
}

// TestAWaitingRetryParkMovesItsDeclineCounter: the driver short-circuits before
// driveUnparks' shared decline arm, so without moving the counter here
// operator_unpark_declined_total{decline="retry-not-due"} never moved for the
// driver's own waiting parks - which is every waiting retry park there is.
func TestAWaitingRetryParkMovesItsDeclineCounter(t *testing.T) {
	now := time.Now()
	task := retryParkedTask("t-retry-waiting-metric", tatarav1alpha1.StateMerged, stage.ReasonCIFailed)
	task.Status.RetryAttempts = 1
	task.Status.RetryBlocker = stage.ReasonCIFailed
	task.Status.RetryNextAt = &metav1.Time{Time: now.Add(10 * time.Minute)}
	c := newMirrorClient(t, rlProject(), mdSecret(), task)
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := rlReconciler(c, &rlForge{}, m)

	if err := r.driveUnparks(context.Background(), rlProject(), now); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	if n := testutil.ToFloat64(m.UnparkDeclinedCounter(stage.ReasonCIFailed,
		string(DeclineRetryNotDue))); n != 1 {
		t.Fatalf("operator_unpark_declined_total{decline=retry-not-due} = %v, want 1", n)
	}
}

// TestRetryLaneMigrationMovesTheParksThatMotivatedTheLane: ci-red and
// merge-conflict lost their park writers to the lane, so without this every
// Task ALREADY carrying one - helmfile#27/#32, ansible!17/!18, terraform!221 -
// keeps a verdict from a rule that no longer exists and ages out silently at
// ParkRetention. It RE-PARKS, never un-parks: every guard the lane has still
// applies afterwards.
func TestRetryLaneMigrationMovesTheParksThatMotivatedTheLane(t *testing.T) {
	tests := []struct {
		from, to string
	}{
		{stage.ReasonCIRed, stage.ReasonCIFailed},
		{stage.ReasonMergeConflict, stage.ReasonMergeConflictRetry},
	}
	for _, tc := range tests {
		t.Run(tc.from, func(t *testing.T) {
			proj := rlProject()
			task := retryParkedTask("t-migrate-"+tc.from, tatarav1alpha1.StateMerged, tc.from)
			c := newMirrorClient(t, proj, mdSecret(), task)
			r := rlReconciler(c, &rlForge{}, wfMetrics())

			if err := r.driveRetryLaneMigration(context.Background(), proj, time.Now()); err != nil {
				t.Fatalf("driveRetryLaneMigration: %v", err)
			}
			got := mdGetTask(t, c, task.Name)
			if got.Status.ParkReason != tc.to {
				t.Fatalf("parkReason = %q, want %q", got.Status.ParkReason, tc.to)
			}
			if got.Status.State != tatarav1alpha1.StateMerged {
				t.Fatalf("state = %q, want merged: a migration re-parks, it never un-parks", got.Status.State)
			}
			if got.Annotations[tatarav1alpha1.AnnRetryLaneMigrated] == "" {
				t.Fatalf("the once-only latch was never stamped")
			}
			// Idempotent forever: a second pass (and a re-park by a mid-rollout
			// replica) must not migrate it again.
			got.Status.ParkReason = tc.from
			if err := c.Status().Update(context.Background(), got); err != nil {
				t.Fatalf("re-park: %v", err)
			}
			if err := r.driveRetryLaneMigration(context.Background(), proj, time.Now()); err != nil {
				t.Fatalf("driveRetryLaneMigration (second pass): %v", err)
			}
			if again := mdGetTask(t, c, task.Name); again.Status.ParkReason != tc.from {
				t.Fatalf("parkReason = %q, want the latch to have refused a second migration", again.Status.ParkReason)
			}
		})
	}
}

// Past ParkRetention the reaper is about to collect the Task; waking it to
// escalate is noise, and the un-migrated park ages out exactly as before.
func TestRetryLaneMigrationLeavesAParkPastParkRetentionAlone(t *testing.T) {
	proj := rlProject()
	task := retryParkedTask("t-migrate-ancient", tatarav1alpha1.StateMerged, stage.ReasonCIRed)
	task.Status.ParkedAt = &metav1.Time{Time: time.Now().Add(-tatarav1alpha1.ParkRetention - time.Hour)}
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := rlReconciler(c, &rlForge{}, wfMetrics())

	if err := r.driveRetryLaneMigration(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveRetryLaneMigration: %v", err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.ParkReason != stage.ReasonCIRed {
		t.Fatalf("parkReason = %q, want ci-red left to age out", got.Status.ParkReason)
	}
	if got.Annotations[tatarav1alpha1.AnnRetryLaneMigrated] != "" {
		t.Fatalf("a skipped park must not be latched: the window is an age test, not a one-shot")
	}
}

// TestAdvancingTheMergeCursorRefundsTheRetryBudget is finding 4's corridor
// half. stampEnter launders the budget on a genuine TRANSITION, but the merge
// corridor advances status.mergeCursor WITHOUT one - it stays in `merging`
// across the whole of spec.mergeOrder. So with [A,B,C,D,E] one brief red
// pipeline per repo escalated on E having in fact cleared four blockers and
// delivered four repos.
func TestAdvancingTheMergeCursorRefundsTheRetryBudget(t *testing.T) {
	task := mdTask("t-cursor-refund", "implement", tatarav1alpha1.StateMerged)
	task.Status.MergeCursor = 1
	task.Status.RetryAttempts = 4
	task.Status.RetryBlocker = stage.ReasonCIFailed
	task.Status.RetryNextAt = &metav1.Time{Time: time.Now()}
	c := newMirrorClient(t, mdProject(), task)
	d := &StageDriver{Client: c, Now: func() time.Time { return time.Now() }}

	// The cursor MOVES: repo 1 landed and the corridor is on repo 2.
	if err := d.enterStageWithCursor(context.Background(), mdProject(), task, "", "", nil, 2); err != nil {
		t.Fatalf("enterStageWithCursor: %v", err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.MergeCursor != 2 {
		t.Fatalf("mergeCursor = %d, want 2", got.Status.MergeCursor)
	}
	if got.Status.RetryAttempts != 0 || got.Status.RetryBlocker != "" || got.Status.RetryNextAt != nil {
		t.Fatalf("the retry budget survived a cursor advance: attempts=%d blocker=%q next=%v",
			got.Status.RetryAttempts, got.Status.RetryBlocker, got.Status.RetryNextAt)
	}

	// A pass that does NOT move the cursor refunds nothing: the blocker the laps
	// were spent on is still the one in front of the corridor.
	got.Status.RetryAttempts = 3
	got.Status.RetryBlocker = stage.ReasonCIFailed
	if err := c.Status().Update(context.Background(), got); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := d.enterStageWithCursor(context.Background(), mdProject(), got, "", "", nil, 2); err != nil {
		t.Fatalf("enterStageWithCursor (no advance): %v", err)
	}
	if again := mdGetTask(t, c, task.Name); again.Status.RetryAttempts != 3 {
		t.Fatalf("retryAttempts = %d, want 3: a poll that moves nothing refunds nothing", again.Status.RetryAttempts)
	}
}
