package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// THE RED-CI GATE (issue #476). These cover both halves of the defect:
//
//	(a) reviewing -> merging promoted a change whose CI was ALREADY red;
//	(b) merging then re-read that red check every 60s until the 4h budget.

// ciRedFixture builds the incident's shape: an implement Task on ONE repo whose
// review approved sha-a, with the forge reporting ciStatus at headSHA. A
// non-nil pendingReview is armed BEFORE the fake client is built, so the drain
// path sees it on its first read.
func ciRedFixture(t *testing.T, stg, ciStatus, headSHA string, pending bool) (
	*tatarav1alpha1.Task, *tatarav1alpha1.MergeRequest, *fakeForge, client.Client) {
	t.Helper()
	task := mdTask("t1", "implement", stg)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.ReviewedSHA = "sha-a"
	mr.Status.Status = "approved"
	if pending {
		pr := pendingReviewFixture("approve", 1, "sha-a")
		pr.Findings = nil
		mr.Status.PendingReview = pr
	}
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	f.head[7] = "sha-a"
	f.state[7] = scm.PRState{CIStatus: ciStatus, HeadSHA: headSHA}
	return task, mr, f, c
}

// (a) THE PROMOTION THAT MUST NOT HAPPEN. helmfile!1358 was approved 2m51s after
// its pytest job failed, and the advance did not look. It routes back to
// implementing - the only stage that can produce the commit that fixes it.
func TestReviewAdvanceRefusesMergingWhenCIIsRed(t *testing.T) {
	_, mr, f, c := ciRedFixture(t, tatarav1alpha1.StateAwaitingReview, "failure", "sha-a", true)
	d := mdNewDriver(t, f, c)
	if err := d.DrainPendingReview(context.Background(), mdGetMR(t, c, mr.Name)); err != nil {
		t.Fatalf("DrainPendingReview: %v", err)
	}
	// The review itself is NOT suppressed: the verdict is real and belongs on the
	// forge. Only the promotion changes.
	if f.postReviewCalls != 1 {
		t.Fatalf("PostReview calls = %d, want 1: the approve still posts", f.postReviewCalls)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.State != tatarav1alpha1.StateUnderImplementation || got.Status.StateReason != stage.ReasonCIRed {
		t.Fatalf("stage = %q(%q), want implementing(ci-red)", got.Status.State, got.Status.StateReason)
	}
	if got.Status.CIRedReentries != 1 {
		t.Fatalf("ciRedReentries = %d, want 1", got.Status.CIRedReentries)
	}
}

// PENDING IS NOT A VERDICT. Checks that are merely still running (or absent
// entirely) must promote exactly as before, or every Task bounces on a race with
// its own CI.
func TestReviewAdvanceStillMergesUnlessCIActuallyFailed(t *testing.T) {
	for _, ci := range []string{"pending", "success", ""} {
		t.Run("ci="+ci, func(t *testing.T) {
			_, mr, f, c := ciRedFixture(t, tatarav1alpha1.StateAwaitingReview, ci, "sha-a", true)
			d := mdNewDriver(t, f, c)
			if err := d.DrainPendingReview(context.Background(), mdGetMR(t, c, mr.Name)); err != nil {
				t.Fatalf("DrainPendingReview: %v", err)
			}
			if got := mdGetTask(t, c, "t1"); got.Status.State != tatarav1alpha1.StateMerged {
				t.Fatalf("stage = %q(%q), want merging", got.Status.State, got.Status.ParkReason)
			}
		})
	}
}

// A head that MOVED off the reviewed SHA is red about code nobody reviewed. That
// is the head-moved bounce's business (merging -> reviewing), not this gate's.
func TestReviewAdvanceIgnoresRedCIOnAnUnreviewedHead(t *testing.T) {
	_, mr, f, c := ciRedFixture(t, tatarav1alpha1.StateAwaitingReview, "failure", "sha-MOVED", true)
	d := mdNewDriver(t, f, c)
	if err := d.DrainPendingReview(context.Background(), mdGetMR(t, c, mr.Name)); err != nil {
		t.Fatalf("DrainPendingReview: %v", err)
	}
	if got := mdGetTask(t, c, "t1"); got.Status.State != tatarav1alpha1.StateMerged {
		t.Fatalf("stage = %q(%q), want merging: the red head is not the reviewed head",
			got.Status.State, got.Status.ParkReason)
	}
}

// THE GATE FAILS OPEN. reconcileClocks evaluates the reviewing advance BEFORE
// the HandoffDeadline and BEFORE stage.ArmedClock, so an error here would be a
// Task that reaches neither. A forge blip costs one pointless promotion instead
// - and merging re-checks the same status within 60s.
func TestReviewAdvanceFailsOpenWhenCICannotBeRead(t *testing.T) {
	_, mr, f, c := ciRedFixture(t, tatarav1alpha1.StateAwaitingReview, "failure", "sha-a", true)
	f.prStateErr = errors.New("502 bad gateway")
	d := mdNewDriver(t, f, c)
	if err := d.DrainPendingReview(context.Background(), mdGetMR(t, c, mr.Name)); err != nil {
		t.Fatalf("DrainPendingReview: %v", err)
	}
	if got := mdGetTask(t, c, "t1"); got.Status.State != tatarav1alpha1.StateMerged {
		t.Fatalf("stage = %q(%q), want merging: an unreadable CI must not wedge the advance",
			got.Status.State, got.Status.ParkReason)
	}
}

// THE IMPLEMENT POD HAS TO KNOW WHY. The bundle renders mr.status.ciStatus - the
// STALE mirror value this whole gate exists because of - so the bounce carries an
// operator note naming the repo, PR and reviewed SHA, exactly as the
// request_changes path carries reviewBeltNote.
func TestCIRedLeavesANoteForTheImplementPod(t *testing.T) {
	task, _, f, c := ciRedFixture(t, tatarav1alpha1.StateMerged, "failure", "sha-a", false)
	d := mdNewDriver(t, f, c)
	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	got := mdGetTask(t, c, "t1")
	found := ""
	for _, n := range got.Status.Notes {
		if n.Agent == "operator" && strings.Contains(n.Body, "CI is RED") {
			found = n.Body
		}
	}
	if found == "" {
		t.Fatalf("no operator note on the ci-red bounce; notes = %+v", got.Status.Notes)
	}
	for _, want := range []string{"tatara-operator!7", "sha-a", "attempt 1 of 3"} {
		if !strings.Contains(found, want) {
			t.Fatalf("note %q does not name %q", found, want)
		}
	}
}

// (b) THE 4h POLL THAT MUST NOT HAPPEN. A Task already sitting in merging on a
// red check leaves on the FIRST pass, with no merge attempted and no requeue.
func TestMergingLeavesImmediatelyWhenCIIsRed(t *testing.T) {
	obs.CIRedExitTotal.Reset()
	task, _, f, c := ciRedFixture(t, tatarav1alpha1.StateMerged, "failure", "sha-a", false)
	d := mdNewDriver(t, f, c)

	res, err := d.ReconcileMerging(context.Background(), mdProject(), task)
	if err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want 0: a failed check never resolves by waiting", res.RequeueAfter)
	}
	if f.mergeCalls != 0 {
		t.Fatalf("merge calls = %d, want 0", f.mergeCalls)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.State != tatarav1alpha1.StateUnderImplementation || got.Status.StateReason != stage.ReasonCIRed {
		t.Fatalf("stage = %q(%q), want implementing(ci-red)", got.Status.State, got.Status.StateReason)
	}
	if got.Status.CIRedReentries != 1 {
		t.Fatalf("ciRedReentries = %d, want 1", got.Status.CIRedReentries)
	}
	if n := testutil.ToFloat64(obs.CIRedExitTotal.WithLabelValues(
		"tatara-operator", tatarav1alpha1.StateMerged, tatarav1alpha1.StateUnderImplementation)); n != 1 {
		t.Fatalf("operator_ci_red_exit_total = %v, want 1", n)
	}
	// The per-task gauge must not outlive the stage it measures (K.1 cardinality).
	if n := testutil.ToFloat64(obs.MergeCursorStalledSeconds.WithLabelValues("t1", "tatara-operator")); n != 0 {
		t.Fatalf("merge cursor gauge = %v, want cleared", n)
	}
}

// Everything that CAN go green on its own still stalls on the 60s poll: the gate
// narrows the wait, it does not replace it.
func TestMergingStillStallsWhileCIIsPending(t *testing.T) {
	task, _, f, c := ciRedFixture(t, tatarav1alpha1.StateMerged, "pending", "sha-a", false)
	d := mdNewDriver(t, f, c)

	res, err := d.ReconcileMerging(context.Background(), mdProject(), task)
	if err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	if res.RequeueAfter != mergeRequeue {
		t.Fatalf("RequeueAfter = %v, want %v", res.RequeueAfter, mergeRequeue)
	}
	if f.mergeCalls != 0 {
		t.Fatalf("merge calls = %d, want 0", f.mergeCalls)
	}
	if got := mdGetTask(t, c, "t1"); got.Status.State != tatarav1alpha1.StateMerged {
		t.Fatalf("stage = %q, want merging (still waiting)", got.Status.State)
	}
}

// CYCLE 5 IS BOUNDED. Three laps of "go fix the tests" is generous; the fourth
// is refused, exactly like every other re-entry cycle.
func TestCIRedBoundedAtMaxReentries(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StateMerged)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	task.Status.CIRedReentries = tatarav1alpha1.MaxCIRedReentries
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.ReviewedSHA = "sha-a"
	mr.Status.Status = "approved"
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	f.head[7] = "sha-a"
	f.state[7] = scm.PRState{CIStatus: "failure", HeadSHA: "sha-a"}
	d := mdNewDriver(t, f, c)

	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.State != tatarav1alpha1.StateMerged || !tatarav1alpha1.Parked(got) || got.Status.ParkReason != stage.ReasonCIBlocked {
		t.Fatalf("state/park = %q/%q, want merged, parked(ci-blocked)", got.Status.State, got.Status.ParkReason)
	}
	if got.Status.CIRedReentries != tatarav1alpha1.MaxCIRedReentries {
		t.Fatalf("ciRedReentries = %d, want it NOT incremented past the cap", got.Status.CIRedReentries)
	}
}

// THE MERGED BOUNDARY. Once part of mergeOrder has landed, re-implementing would
// re-propose merged code and recreate deleted branches. It parks instead, and
// parked(ci-red) has no F.6 re-entry: a human decides.
func TestCIRedParksWhenAnEarlierRepoAlreadyMerged(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StateMerged)
	task.Spec.MergeOrder = []string{"tatara-cli", "tatara-operator"}
	first := mdMR(task, "tatara-cli", 5)
	first.Status.State = "merged"
	second := mdMR(task, "tatara-operator", 7)
	second.Status.ReviewedSHA = "sha-a"
	second.Status.Status = "approved"
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-cli"), mdRepo("tatara-operator"),
		task, first, second)

	f := newFakeForge(t)
	f.head[7] = "sha-a"
	f.state[7] = scm.PRState{CIStatus: "failure", HeadSHA: "sha-a"}
	d := mdNewDriver(t, f, c)

	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.State != tatarav1alpha1.StateMerged || !tatarav1alpha1.Parked(got) || got.Status.ParkReason != stage.ReasonCIRed {
		t.Fatalf("state/park = %q/%q, want merged, parked(ci-red)", got.Status.State, got.Status.ParkReason)
	}
	if got.Status.CIRedReentries != 0 {
		t.Fatalf("ciRedReentries = %d, want 0: the park is not a re-entry", got.Status.CIRedReentries)
	}
	if class, _ := stage.UnparkClassFor(stage.ReasonCIRed); class != stage.UnparkNever {
		t.Fatal("parked(ci-red) must have no F.6 re-entry: a human decides")
	}
}

// THE MERGED BOUNDARY IS READ LIVE TOO. anyMerged() reads the MIRROR, and the
// mirror sweep is hourly, so an MR merged out of band (the C.9 accepted risk)
// still reads "open" there. Left alone, a red sibling would route the Task to
// implementing and re-propose merged code. The gate folds the live observation
// onto the in-memory copy so the routing sees the truth.
func TestCIRedParksOnAnMRMergedOutOfBandWhileTheMirrorStillSaysOpen(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StateAwaitingReview)
	task.Spec.MergeOrder = []string{"tatara-cli", "tatara-operator"}
	// The mirror says open for BOTH; only the forge knows tatara-cli!5 landed.
	first := mdMR(task, "tatara-cli", 5)
	first.Status.ReviewedSHA = "sha-cli"
	first.Status.Status = "approved"
	second := mdMR(task, "tatara-operator", 7)
	second.Status.ReviewedSHA = "sha-a"
	second.Status.Status = "approved"
	pr := pendingReviewFixture("approve", 1, "sha-a")
	pr.Findings = nil
	second.Status.PendingReview = pr
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-cli"), mdRepo("tatara-operator"),
		task, first, second)

	f := newFakeForge(t)
	f.head[5], f.head[7] = "sha-cli", "sha-a"
	f.state[5] = scm.PRState{Merged: true, HeadSHA: "sha-cli"}
	f.state[7] = scm.PRState{CIStatus: "failure", HeadSHA: "sha-a"}
	d := mdNewDriver(t, f, c)

	if err := d.DrainPendingReview(context.Background(), mdGetMR(t, c, second.Name)); err != nil {
		t.Fatalf("DrainPendingReview: %v", err)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.State != tatarav1alpha1.StateAwaitingReview || !tatarav1alpha1.Parked(got) || got.Status.ParkReason != stage.ReasonCIRed {
		t.Fatalf("state/park = %q/%q, want awaiting-review, parked(ci-red): a merged sibling outranks the red one",
			got.Status.State, got.Status.ParkReason)
	}
}
