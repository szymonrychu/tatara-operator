package webhook

// Fix for recovery path (a) of the 2026-07-19 production deadlock (task
// mt-r-tatara-operator-388-6e7958617d9d0119): a human's retry comment on the
// PR resolved the mirror MR CR, but the interrupted mint had left that CR an
// UNOWNED stub, so deliverPendingEvent's own.ControllerOwner check
// early-returned silently and driveCommentUnpark never ran - the parked Task
// the comment was meant to rescue stayed parked forever. deliverPendingEvent
// must fall back to the deterministic intake identity (IntakeTaskName - the
// natural key the mint that produced this exact stub used) and route the
// event to that Task when it is live.

import (
	"context"
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// peMRStub is the interrupted mint's residue: a mirror MergeRequest CR with
// NO ownerReferences and empty status.
func peMRStub(number int) *tatarav1.MergeRequest {
	return &tatarav1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1.MergeRequestName("pe-repo", number), Namespace: peNS},
		Spec: tatarav1.MergeRequestSpec{
			RepositoryRef: "pe-repo", Number: number, ProjectRef: "pe-proj",
			URL: fmt.Sprintf("https://github.com/o/r/pull/%d", number),
		},
	}
}

// TestDeliverPendingEvent_UnownedMRStub_CommentDrivesUnpark: the mirror MR CR
// exists but has no controller owner; the parked(awaiting-human) intake Task
// that SHOULD own it is resolvable by IntakeTaskName. A human comment must be
// enqueued onto that Task and drive the F.6 comment unpark - not silently
// dropped at the ControllerOwner early return.
func TestDeliverPendingEvent_UnownedMRStub_CommentDrivesUnpark(t *testing.T) {
	proj := peProject("tatara-bot", "maintainer")
	name := tatarav1.IntakeTaskName("pe-proj", controller.SweepReviewKind, "pe-repo", 88)
	task := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: peNS},
		Spec: tatarav1.TaskSpec{
			Kind: controller.SweepReviewKind, ProjectRef: "pe-proj", Goal: "g",
			Source: &tatarav1.TaskSource{
				Provider: "github", IssueRef: "https://github.com/o/r/pull/88",
				Number: 88, IsPR: true,
			},
		},
		Status: tatarav1.TaskStatus{State: tatarav1.StateAwaitingReview},
	}
	if err := stage.Park(task, stage.ReasonAwaitingHuman, time.Now()); err != nil {
		t.Fatalf("park: %v", err)
	}
	c := peClient(t, proj, peRepo(), task, peMRStub(88))
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{
		IsComment: true, IsPR: true, Number: 88,
		ActorLogin: "maintainer", CommentID: 5, CommentBody: "please retry",
	}
	s.deliverPendingEvent(context.Background(), *proj, peRepo(), ev)

	got := getPETask(t, c, name)
	if len(got.Status.PendingEvents) != 1 {
		t.Fatalf("pendingEvents = %d, want 1 (event routed via the intake natural key)", len(got.Status.PendingEvents))
	}
	// Un-parked and STILL in the same state (awaiting-review): un-park never
	// moves state under #521 (there is no separate "conversing" stage to land
	// in any more). This assertion used to check the Task moved to a
	// conversing stage; what actually mattered - and still does - is that the
	// human comment drove the F.6 unpark rather than being silently dropped.
	if tatarav1.Parked(got) {
		t.Fatalf("still parked(%s): the human comment must drive the unpark", got.Status.ParkReason)
	}
	if got.Status.State != tatarav1.StateAwaitingReview {
		t.Fatalf("state = %q, want unchanged awaiting-review", got.Status.State)
	}
}

// TestDeliverPendingEvent_UnownedMRStub_NoMatchingTask: same unowned stub but
// NO intake Task exists under the natural key. The comment still mirrors, the
// delivery early-returns without panicking, and no Task is invented.
func TestDeliverPendingEvent_UnownedMRStub_NoMatchingTask(t *testing.T) {
	proj := peProject("tatara-bot", "maintainer")
	mr := peMRStub(89)
	c := peClient(t, proj, peRepo(), mr)
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{
		IsComment: true, IsPR: true, Number: 89,
		ActorLogin: "maintainer", CommentID: 6, CommentBody: "anyone home?",
	}
	s.deliverPendingEvent(context.Background(), *proj, peRepo(), ev)

	var gotMR tatarav1.MergeRequest
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: peNS, Name: mr.Name}, &gotMR); err != nil {
		t.Fatalf("get mr: %v", err)
	}
	if len(gotMR.Status.Comments) != 1 {
		t.Fatalf("comments = %d, want 1: the mirror append still happens", len(gotMR.Status.Comments))
	}
	var tl tatarav1.TaskList
	if err := c.List(context.Background(), &tl, client.InNamespace(peNS)); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tl.Items) != 0 {
		t.Fatalf("tasks = %d, want 0: the fallback never mints", len(tl.Items))
	}
}

// TestDeliverPendingEvent_UnownedMRStub_DoneTaskNotResurrected: the natural-key
// twin exists but is DONE - not live. The fallback must not deliver into it
// (a done Task has no F.6 re-entry; appending events to a corpse is noise).
//
// #521 dissolved the separate `failed` terminal this fixture used to build:
// TaskDone is now EXACTLY state in {done, rejected}, and that is precisely the
// gate resolveOwningTask's fallback checks (tatarav1.TaskDone(task)) - a Task
// merely PARKED (whatever the reason, "failed"'s closest new-model analogue)
// is NOT TaskDone and is deliberately treated as live by that same gate
// ("parked counts as live: delivering the event is precisely what can unpark
// it"), so building this fixture parked would exercise a different property
// than the one this test names. A matching Source is set (unlike the sibling
// mismatch test below) so the TaskDone gate - not a source mismatch - is what
// actually stops the delivery.
func TestDeliverPendingEvent_UnownedMRStub_DoneTaskNotResurrected(t *testing.T) {
	proj := peProject("tatara-bot", "maintainer")
	name := tatarav1.IntakeTaskName("pe-proj", controller.SweepReviewKind, "pe-repo", 90)
	task := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: peNS},
		Spec: tatarav1.TaskSpec{
			Kind: controller.SweepReviewKind, ProjectRef: "pe-proj", Goal: "g",
			Source: &tatarav1.TaskSource{
				Provider: "github", IssueRef: "https://github.com/o/r/pull/90",
				Number: 90, IsPR: true,
			},
		},
		Status: tatarav1.TaskStatus{State: tatarav1.StateDone},
	}
	c := peClient(t, proj, peRepo(), task, peMRStub(90))
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{
		IsComment: true, IsPR: true, Number: 90,
		ActorLogin: "maintainer", CommentID: 7, CommentBody: "retry",
	}
	s.deliverPendingEvent(context.Background(), *proj, peRepo(), ev)

	got := getPETask(t, c, name)
	if len(got.Status.PendingEvents) != 0 {
		t.Fatalf("pendingEvents = %d, want 0: a done task never receives fallback deliveries", len(got.Status.PendingEvents))
	}
	if got.Status.State != tatarav1.StateDone {
		t.Fatalf("state = %s, want untouched done", got.Status.State)
	}
}

// TestDeliverPendingEvent_UnownedMRStub_SourceMismatchNotDelivered: a Task
// exists under the natural key but its OWN source identity disagrees with the
// event (nil Source, or a different number). The fallback must treat that as a
// miss - a name collision is not an ownership claim.
func TestDeliverPendingEvent_UnownedMRStub_SourceMismatchNotDelivered(t *testing.T) {
	tests := []struct {
		name   string
		source *tatarav1.TaskSource
	}{
		{name: "nil source", source: nil},
		{name: "wrong number", source: &tatarav1.TaskSource{
			Provider: "github", IssueRef: "https://github.com/o/r/pull/999",
			Number: 999, IsPR: true,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proj := peProject("tatara-bot", "maintainer")
			taskName := tatarav1.IntakeTaskName("pe-proj", controller.SweepReviewKind, "pe-repo", 91)
			task := &tatarav1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: taskName, Namespace: peNS},
				Spec: tatarav1.TaskSpec{
					Kind: controller.SweepReviewKind, ProjectRef: "pe-proj", Goal: "g",
					Source: tt.source,
				},
				Status: tatarav1.TaskStatus{State: tatarav1.StateAwaitingReview},
			}
			if err := stage.Park(task, stage.ReasonAwaitingHuman, time.Now()); err != nil {
				t.Fatalf("park: %v", err)
			}
			c := peClient(t, proj, peRepo(), task, peMRStub(91))
			s := peServer(c, &stubSpiller{}, nil)

			ev := scm.WebhookEvent{
				IsComment: true, IsPR: true, Number: 91,
				ActorLogin: "maintainer", CommentID: 8, CommentBody: "retry",
			}
			s.deliverPendingEvent(context.Background(), *proj, peRepo(), ev)

			got := getPETask(t, c, taskName)
			if len(got.Status.PendingEvents) != 0 {
				t.Fatalf("pendingEvents = %d, want 0: a source-mismatched task never receives fallback deliveries",
					len(got.Status.PendingEvents))
			}
			if !tatarav1.Parked(got) {
				t.Fatalf("state = %s, want untouched (still parked)", got.Status.State)
			}
		})
	}
}
