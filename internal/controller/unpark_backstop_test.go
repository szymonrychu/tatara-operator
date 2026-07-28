package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// newUnparkTestReconciler builds a ProjectReconciler backed by a fake client
// seeded with objs, with APIReader pointed at the SAME fake client (the
// uncached-reader idiom ApplyUnpark relies on - see unpark.go's ApplyUnpark
// doc comment) and a fresh metrics registry so UnparkDeclined calls do not
// panic on a nil Metrics.
func newUnparkTestReconciler(t *testing.T, objs ...client.Object) *ProjectReconciler {
	t.Helper()
	c := newMirrorClient(t, objs...)
	return &ProjectReconciler{
		Client:    c,
		Scheme:    c.Scheme(),
		APIReader: c,
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}
}

// objectKeyOf is client.ObjectKeyFromObject, named for readability at the call
// site in this file's tests (fresh := &Task{}; r.Get(ctx, objectKeyOf(task), fresh)).
func objectKeyOf(o client.Object) client.ObjectKey {
	return client.ObjectKeyFromObject(o)
}

// TestApplyUnpark_VerdictIsNotConsulted is the Step B guarantee. Before this
// change a durable Task.status.approvalVerdict could carry a parked
// identity-unverified Task into implementing without any live re-check: the
// deleted grammarPassedFor read the record off the Task and handed it to
// stage.Unpark as UnparkInput.GrammarPassed. After it, the ONLY grant path into
// implementing is restapi's verifyApprovalScope (internal/restapi/outcome.go),
// which runs the LIVE grammar against a live pod's submit_outcome - so a Task
// carrying a PASSING, in-scope verdict AND an approved owned Issue, exactly the
// shape the old fast path re-entered on, must still land on conversing.
func TestApplyUnpark_VerdictIsNotConsulted(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxOpenTasks = 6

	parkedAt := time.Now().Add(-10 * time.Minute)
	task := &tatarav1alpha1.Task{}
	task.Namespace = "tatara"
	task.Name = "t-step-b-verdict-inert"
	task.Spec.ProjectRef = "infrastructure"
	task.Spec.Kind = "clarify"
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = stage.ReasonIdentityUnverified
	task.Status.StageEnteredAt = &metav1.Time{Time: parkedAt}
	task.Status.IssueRefs = []string{"iss-helmfile-26"}
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Repo: "helmfile", Number: 26,
		Author: "szymonrychu", Body: "go ahead",
	}}
	// A verdict that would have satisfied every scoping check grammarPassedFor
	// applied: non-empty Author, and stamped well AFTER this park began.
	task.Status.ApprovalVerdict = &tatarav1alpha1.ApprovalVerdict{
		At:                metav1.Time{Time: parkedAt.Add(5 * time.Minute)},
		IssueRef:          "iss-helmfile-26",
		CommentExternalID: "3606943691",
		Author:            "szymonrychu",
		Phrase:            "go ahead",
	}

	// Owned AND approved live: the other half of the old fast path's condition.
	iss := &tatarav1alpha1.Issue{}
	iss.Namespace = "tatara"
	iss.Name = "iss-helmfile-26"
	iss.Status.State = "open"
	iss.Status.Status = "approved"

	c := newMirrorClient(t, proj, task, iss)

	target, decline, err := ApplyUnpark(context.Background(), c, c, proj, task, 1, 6, true, time.Now())
	if err != nil {
		t.Fatalf("ApplyUnpark: %v", err)
	}
	if target == tatarav1alpha1.StageImplementing {
		t.Fatal("target = implementing: a parked identity-unverified Task must NEVER reach implementing from unpark, " +
			"whatever verdict its status happens to carry")
	}
	if target != tatarav1alpha1.StageConversing || decline != DeclineNone {
		t.Fatalf("target = %q decline = %q, want conversing/none", target, decline)
	}
}

// The SAME guarantee as TestApplyUnpark_VerdictIsNotConsulted, one level up:
// through the PACED reconcile driver rather than ApplyUnpark directly, so the
// conversingRoomBudget hoist and the NeedsConversingRoom gate are exercised too
// (a batch driver that never computed room for identity-unverified would leave
// this edge structurally inert and the Task parked). A human DID comment, the
// operator can no longer read any comment as an approval on this path, and
// Task 9 reads that as a conversation rather than a dead end: the Task lands in
// conversing. It must NEVER land in implementing - that is the fully autonomous
// hallucinated-approval-to-prod path, and this is what stays impossible. The
// REAL guarantee against an unverified implement is restapi's
// verifyApprovalScope (internal/restapi/outcome.go), which judges every owned
// Issue live on every submit_outcome(decision=implement) - NOT LegalFor's kind
// guard, which keys on Task.Spec.Kind == "review" and has nothing to say about
// a kind=clarify Task like this one. A clarify agent standing in conversing CAN
// still move this Task toward implementing, via a GENUINE decision=implement
// that passes that live check - see
// TestOutcome_Conversing_ApprovalVerdictIsNeverConsulted (internal/restapi).
func TestDriveUnparks_IdentityUnverifiedOpensConversationNeverImplementing(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxOpenTasks = 6

	task := &tatarav1alpha1.Task{}
	task.Namespace = "tatara"
	task.Name = "t"
	task.Spec.ProjectRef = "infrastructure"
	task.Spec.Kind = "clarify"
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = stage.ReasonIdentityUnverified
	task.Status.StageEnteredAt = &metav1.Time{Time: time.Now()}
	task.Status.IssueRefs = []string{"iss-helmfile-26"}
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "szymonrychu", Body: "go ahead",
	}}

	iss := &tatarav1alpha1.Issue{}
	iss.Namespace = "tatara"
	iss.Name = "iss-helmfile-26"
	iss.Status.State = "open"
	iss.Status.Status = "approved"

	r := newUnparkTestReconciler(t, proj, task, iss)

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}

	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(task), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if fresh.Status.Stage == tatarav1alpha1.StageImplementing {
		t.Fatal("stage = implementing: the reconcile driver must never re-enter implementing from parked(identity-unverified)")
	}
	if fresh.Status.Stage != tatarav1alpha1.StageConversing {
		t.Fatalf("stage = %s, want conversing: a human comment on a parked identity-unverified Task opens a conversation", fresh.Status.Stage)
	}
}

// 2026-07-28 security review CRITICAL 1: unparkFires is a THIRD UnparkInput
// builder (after ApplyUnpark and stage.UnparkDetailed's own callers), and it
// used to leave ConversingHasRoom at its zero value (false)
// unconditionally. The window: parked(identity-unverified), a non-bot event,
// and the conversing ceiling has room. driveUnparks' ApplyUnpark, called with
// room=true, sends the Task to conversing and saves it; unparkFires, called
// with room=false (the bug), returns DeclineGrammarNotPassed and fires=false -
// so reapParked, once past
// ParkRetention, would delete a Task the driver was about to save out from
// under it. driveUnparksPaced and ReapTerminalPaced are paced
// INDEPENDENTLY, so a pass where the driver is throttled and the reaper is
// not is a real production window.
//
// This proves the probe and the driver reach the SAME verdict given the SAME
// room=true answer: unparkFires(room=true) must return fires=true on exactly
// the input where ApplyUnpark's own stage.UnparkDetailed call would enter
// conversing (proven by TestUnpark_IdentityUnverifiedWithoutGrammarConversesWhenThereIsRoom
// directly against the pure function).
func TestUnparkFires_AgreesWithApplyUnparkOnConversingHasRoom(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxOpenTasks = 6

	task := &tatarav1alpha1.Task{}
	task.Namespace = "tatara"
	task.Name = "t-crit1-conversing-window"
	task.Spec.ProjectRef = "infrastructure"
	task.Spec.Kind = "clarify"
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = stage.ReasonIdentityUnverified
	task.Status.StageEnteredAt = &metav1.Time{Time: time.Now().Add(-time.Hour)}
	// GrammarPassed is unset by both builders now, so room is the only variable.
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "szymonrychu", Body: "not the magic phrase",
	}}

	r := newUnparkTestReconciler(t, proj, task)

	firesNoRoom, err := r.unparkFires(context.Background(), proj, task, time.Now(), false)
	if err != nil {
		t.Fatalf("unparkFires(room=false): %v", err)
	}
	if firesNoRoom {
		t.Fatal("unparkFires(room=false) = true: a full ceiling must fall back to the SAME non-firing decline as before Task 9, not invent a new re-entry")
	}

	firesRoom, err := r.unparkFires(context.Background(), proj, task, time.Now(), true)
	if err != nil {
		t.Fatalf("unparkFires(room=true): %v", err)
	}
	if !firesRoom {
		t.Fatal("unparkFires(room=true) = false: the reaper disagrees with what ApplyUnpark would do with room - " +
			"exactly the CRITICAL 1 window, where the reaper would delete a Task the driver was about to save into conversing")
	}
}

// 2026-07-28 security review NEW-1: unparkFires never set
// UnparkInput.MaxTurnsPerTask, so for a parked(no-outcome) Task,
// ReasonNoOutcome's Stats.Turns >= in.MaxTurnsPerTask read Turns >= 0, which
// is true for ANY real turn count. The probe always declined turns-exhausted
// regardless of how far under its actual cap the Task was, so reapParked -
// once past ParkRetention - deleted every parked(no-outcome) Task whether or
// not driveUnparks' ApplyUnpark (which threads taskMaxTurns correctly) would
// have re-entered it. Identical failure class to CRITICAL 1, on
// MaxTurnsPerTask instead of ConversingHasRoom.
//
// This Task is parked(no-outcome) from implementing, with Turns(10) far below
// the default cap (300, since neither the Task nor the Project overrides it) -
// exactly the shape ApplyUnpark would re-enter into implementing. The probe
// must agree.
func TestUnparkFires_AgreesWithApplyUnparkOnMaxTurnsPerTask(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxOpenTasks = 6

	task := &tatarav1alpha1.Task{}
	task.Namespace = "tatara"
	task.Name = "t-new1-no-outcome-under-cap"
	task.Spec.ProjectRef = "infrastructure"
	task.Spec.Kind = "clarify"
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = stage.ReasonNoOutcome
	task.Status.ParkedFromStage = tatarav1alpha1.StageImplementing
	task.Status.StageEnteredAt = &metav1.Time{Time: time.Now().Add(-time.Hour)}
	task.Status.Stats.Turns = 10 // far below the default cap of 300

	r := newUnparkTestReconciler(t, proj, task)

	fires, err := r.unparkFires(context.Background(), proj, task, time.Now(), false)
	if err != nil {
		t.Fatalf("unparkFires: %v", err)
	}
	if !fires {
		t.Fatal("unparkFires = false: a parked(no-outcome) Task at 10/300 turns must agree with ApplyUnpark that it re-enters - " +
			"MaxTurnsPerTask was left at its zero value, so Turns >= 0 always declined turns-exhausted regardless of the real count")
	}
}
