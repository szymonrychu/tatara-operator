package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ---------------------------------------------------------------------------
// THE #521 TERMINAL-RESET GUARD.
//
// #521 removed status.stage from the CRD and CRD structural pruning applies on
// the READ path, so every pre-#521 Task comes back from the API server with NO
// state at all and re-derives through the create edge. That reset is ACCEPTED.
// What is NOT accepted is a Task that already FINISHED re-entering the live
// lifecycle: it would reopen issues, respawn agents and redo merge requests.
//
// terminalResetTarget is the guard. Every fixture below is built out of fields
// that SURVIVE pruning - status.deliveredAt, status.documentedBy and the owned
// Issue mirrors, which are separate objects and are not pruned at all.
// ---------------------------------------------------------------------------

func rgTask(name string, mutate func(*tatarav1alpha1.Task)) *tatarav1alpha1.Task {
	t := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: mdNS, UID: types.UID("uid-" + name)},
		Spec:       tatarav1alpha1.TaskSpec{Kind: "implement", ProjectRef: "proj", Goal: "g"},
	}
	if mutate != nil {
		mutate(t)
	}
	return t
}

// rgIssue builds an Issue mirror CONTROLLER-owned by task and also listed in the
// Task's status.issueRefs, which is how a live mirror is bound on both sides.
func rgIssue(task *tatarav1alpha1.Task, name, scmState, platformStatus string) *tatarav1alpha1.Issue {
	task.Status.IssueRefs = append(task.Status.IssueRefs, name)
	yes := true
	return &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: mdNS,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: tatarav1alpha1.GroupVersion.String(),
				Kind:       "Task", Name: task.Name, UID: task.UID, Controller: &yes,
			}},
		},
		Spec:   tatarav1alpha1.IssueSpec{RepositoryRef: "repo", Number: 1, ProjectRef: "proj"},
		Status: tatarav1alpha1.IssueStatus{State: scmState, Status: platformStatus},
	}
}

func rgTarget(t *testing.T, task *tatarav1alpha1.Task, issues ...*tatarav1alpha1.Issue) (string, string) {
	t.Helper()
	objs := []client.Object{task}
	for _, iss := range issues {
		objs = append(objs, iss)
	}
	c := newMirrorClient(t, objs...)
	state, reason, err := terminalResetTarget(context.Background(), c, task)
	if err != nil {
		t.Fatalf("terminalResetTarget: %v", err)
	}
	return state, reason
}

// THE DELIVERY STAMP. status.deliveredAt is written in exactly two places, both
// of them in the same call as EnterStage(..., done, ...), and nothing clears it.
func TestTerminalResetTarget_DeliveredAtIsDone(t *testing.T) {
	stamp := metav1.NewTime(time.Now().Add(-48 * time.Hour))
	task := rgTask("delivered", func(t *tatarav1alpha1.Task) { t.Status.DeliveredAt = &stamp })

	state, reason := rgTarget(t, task)
	if state != tatarav1alpha1.StateDone || reason != "" {
		t.Fatalf("a Task carrying deliveredAt reset to %q(%q), want done with no reason", state, reason)
	}
}

// THE DOC-BATCH COVER STAMP. status.documentedBy is only ever written over
// batch.spec.documentsTasks, and mintDocBatch builds that list exclusively from
// Tasks it read at state done. Its presence therefore PROVES the operator once
// observed this Task as done - independently of deliveredAt.
func TestTerminalResetTarget_DocumentedByIsDone(t *testing.T) {
	task := rgTask("covered", func(t *tatarav1alpha1.Task) { t.Status.DocumentedBy = "proj-docs-batch" })

	state, reason := rgTarget(t, task)
	if state != tatarav1alpha1.StateDone || reason != "" {
		t.Fatalf("a Task carrying documentedBy reset to %q(%q), want done with no reason", state, reason)
	}
}

// THE DELIVERY STAMP ON THE ISSUE. CloseIssuesOnDelivery is the only writer of
// Issue.status.status = "done", and it writes it on every owned issue of a Task
// it is about to deliver.
func TestTerminalResetTarget_EveryOwnedIssueStampedDoneIsDone(t *testing.T) {
	task := rgTask("shipped", nil)
	a := rgIssue(task, "iss-repo-1", "closed", "done")
	b := rgIssue(task, "iss-repo-2", "closed", "done")

	state, reason := rgTarget(t, task, a, b)
	if state != tatarav1alpha1.StateDone || reason != "" {
		t.Fatalf("a Task whose every owned issue carries the delivery stamp reset to %q(%q), want done", state, reason)
	}
}

// THE DECLINE STAMP. recordProposalDecline is the only writer of
// Issue.status.status = "rejected", and it runs on the issue-closed stop path.
func TestTerminalResetTarget_AnyOwnedIssueDeclinedIsRejected(t *testing.T) {
	task := rgTask("declined", nil)
	a := rgIssue(task, "iss-repo-3", "closed", "rejected")
	b := rgIssue(task, "iss-repo-4", "closed", "done")

	state, reason := rgTarget(t, task, a, b)
	if state != tatarav1alpha1.StateRejected || reason != stage.ReasonDeclined {
		t.Fatalf("a Task carrying a declined owned issue reset to %q(%q), want rejected(%s)",
			state, reason, stage.ReasonDeclined)
	}
}

// CLOSED ON THE FORGE, NEVER STAMPED BY THE PLATFORM. A human closed it; the
// ordinary machine would have stopped the Task at rejected(issue-closed).
func TestTerminalResetTarget_EveryOwnedIssueClosedWithoutAPlatformStampIsRejectedIssueClosed(t *testing.T) {
	task := rgTask("human-closed", nil)
	a := rgIssue(task, "iss-repo-5", "closed", "approved")

	state, reason := rgTarget(t, task, a)
	if state != tatarav1alpha1.StateRejected || reason != stage.ReasonIssueClosed {
		t.Fatalf("a Task whose owned issue a human closed reset to %q(%q), want rejected(%s)",
			state, reason, stage.ReasonIssueClosed)
	}
}

// THE CONSERVATIVE BIAS, stated as a test. ONE open issue is enough to make the
// evidence ambiguous, and ambiguous means stateless: re-triage is noisy and
// harmless, a wrong terminal strands real work.
func TestTerminalResetTarget_OneOpenOwnedIssueLeavesTheTaskStateless(t *testing.T) {
	task := rgTask("still-open", nil)
	a := rgIssue(task, "iss-repo-6", "closed", "done")
	b := rgIssue(task, "iss-repo-7", "open", "approved")

	state, reason := rgTarget(t, task, a, b)
	if state != "" || reason != "" {
		t.Fatalf("a Task with one open owned issue reset to %q(%q), want stateless", state, reason)
	}
}

// A Task that owns NO issue mirrors carries no issue evidence at all, and an
// absence is not proof. It re-triages.
func TestTerminalResetTarget_NoOwnedIssuesLeavesTheTaskStateless(t *testing.T) {
	task := rgTask("bare", nil)

	state, reason := rgTarget(t, task)
	if state != "" || reason != "" {
		t.Fatalf("a Task owning no issue mirrors reset to %q(%q), want stateless", state, reason)
	}
}

// WORK EVIDENCE IS NOT TERMINAL EVIDENCE, and this is the whole accepted reset.
// mrRefs, notes, pod stamps and review counters all prove the Task was DOING
// something when its stage was pruned; none of them proves it FINISHED. It
// re-derives through the create edge, which is now INTENDED behaviour.
func TestTerminalResetTarget_WorkEvidenceAloneLeavesTheTaskStateless(t *testing.T) {
	task := rgTask("in-flight", func(t *tatarav1alpha1.Task) {
		t.Status.MRRefs = []string{"mr-tatara-operator-42"}
		t.Status.MergeCursor = 1
		t.Status.PodName = "agent-in-flight"
		t.Status.HumanReviewRounds = 2
		t.Status.Notes = []tatarav1alpha1.Note{{
			ID: "n1", At: metav1.Now(), Agent: "implement", Kind: "plan", Body: "b",
		}}
	})

	state, reason := rgTarget(t, task)
	if state != "" || reason != "" {
		t.Fatalf("a Task carrying only work evidence reset to %q(%q), want stateless", state, reason)
	}
}

// An issue mirror named in status.issueRefs whose CR is GONE is not evidence of
// anything - the mirror is rebuildable and its absence must not be read as
// closure. With no surviving mirror at all the Task stays stateless.
func TestTerminalResetTarget_AVanishedIssueMirrorIsNotEvidence(t *testing.T) {
	task := rgTask("ghost-ref", nil)
	task.Status.IssueRefs = []string{"iss-repo-gone"}

	state, reason := rgTarget(t, task)
	if state != "" || reason != "" {
		t.Fatalf("a Task whose only issue mirror is gone reset to %q(%q), want stateless", state, reason)
	}
}

// ---------------------------------------------------------------------------
// The guard AT THE CREATE EDGE, which is where it runs.
// ---------------------------------------------------------------------------

// A pre-#521 DELIVERED Task boots stateless (its stage was pruned) and must land
// on done, NOT be re-triaged into `new` where it would reopen its issues,
// respawn an agent and redo merge requests that already landed.
func TestCreateEdge_DoesNotResurrectADeliveredTask(t *testing.T) {
	stamp := metav1.NewTime(time.Now().Add(-30 * time.Hour))
	task := rgTask("pruned-delivered", func(t *tatarav1alpha1.Task) {
		t.Status.DeliveredAt = &stamp
		t.Status.MRRefs = []string{"mr-tatara-operator-42"}
	})
	c := newMirrorClient(t, tsProject(3), mdSecret(), task)
	r := tsReconciler(c)

	got := tsReconcile(t, r, tsProject(3), task, time.Now())
	if got.Status.State != tatarav1alpha1.StateDone {
		t.Fatalf("a delivered Task re-derived as %q, want done", got.Status.State)
	}
	if got.Status.ParkReason != "" {
		t.Fatalf("the reset guard parked a terminal Task: %q", got.Status.ParkReason)
	}
}

// The rejected half of the same edge.
func TestCreateEdge_DoesNotResurrectARejectedTask(t *testing.T) {
	task := rgTask("pruned-rejected", nil)
	iss := rgIssue(task, "iss-repo-42", "closed", "rejected")
	c := newMirrorClient(t, tsProject(3), mdSecret(), task, iss)
	r := tsReconciler(c)

	got := tsReconcile(t, r, tsProject(3), task, time.Now())
	if got.Status.State != tatarav1alpha1.StateRejected {
		t.Fatalf("a declined Task re-derived as %q, want rejected", got.Status.State)
	}
	if got.Status.StateReason != stage.ReasonDeclined {
		t.Fatalf("rejected without the declining reason: %q", got.Status.StateReason)
	}
}

// THE ACCEPTED RESET, pinned as intended behaviour rather than as the H5 defect
// it was called while a migrator was still on the table. A stateless Task with
// only WORK evidence re-derives through the ordinary create edge into `new`.
func TestCreateEdge_AStatelessTaskWithOnlyWorkEvidenceReTriages(t *testing.T) {
	task := rgTask("pruned-in-flight", func(t *tatarav1alpha1.Task) {
		t.Status.MRRefs = []string{"mr-tatara-operator-7"}
		t.Status.MergeCursor = 1
		t.Status.HumanReviewRounds = 2
	})
	c := newMirrorClient(t, tsProject(3), mdSecret(), task)
	r := tsReconciler(c)

	got := tsReconcile(t, r, tsProject(3), task, time.Now())
	if got.Status.State != tatarav1alpha1.StateNew {
		t.Fatalf("a stateless in-flight Task re-derived as %q, want new", got.Status.State)
	}
}

// IDEMPOTENCE. The guard runs on ordinary reconcile, so it runs many times. A
// second pass over an already-stamped Task must not re-enter, re-stamp or move
// anything: the create edge is gated on state == "" and a terminal Task takes
// the reaper's early return.
func TestCreateEdge_TheResetGuardIsIdempotent(t *testing.T) {
	stamp := metav1.NewTime(time.Now().Add(-30 * time.Hour))
	task := rgTask("pruned-twice", func(t *tatarav1alpha1.Task) { t.Status.DeliveredAt = &stamp })
	c := newMirrorClient(t, tsProject(3), mdSecret(), task)
	r := tsReconciler(c)

	first := tsReconcile(t, r, tsProject(3), task, time.Now())
	second := tsReconcile(t, r, tsProject(3), first, time.Now().Add(time.Hour))
	if second.Status.State != tatarav1alpha1.StateDone {
		t.Fatalf("second pass moved the Task to %q", second.Status.State)
	}
	if !first.Status.StateEnteredAt.Equal(second.Status.StateEnteredAt) {
		t.Fatalf("second pass re-stamped stateEnteredAt: %v -> %v",
			first.Status.StateEnteredAt, second.Status.StateEnteredAt)
	}
}
