package controller

import (
	"context"
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
	_, mr, f, c := ciRedFixture(t, tatarav1alpha1.StageReviewing, "failure", "sha-a", true)
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
	if got.Status.Stage != tatarav1alpha1.StageImplementing || got.Status.StageReason != stage.ReasonCIRed {
		t.Fatalf("stage = %q(%q), want implementing(ci-red)", got.Status.Stage, got.Status.StageReason)
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
			_, mr, f, c := ciRedFixture(t, tatarav1alpha1.StageReviewing, ci, "sha-a", true)
			d := mdNewDriver(t, f, c)
			if err := d.DrainPendingReview(context.Background(), mdGetMR(t, c, mr.Name)); err != nil {
				t.Fatalf("DrainPendingReview: %v", err)
			}
			if got := mdGetTask(t, c, "t1"); got.Status.Stage != tatarav1alpha1.StageMerging {
				t.Fatalf("stage = %q(%q), want merging", got.Status.Stage, got.Status.StageReason)
			}
		})
	}
}

// A head that MOVED off the reviewed SHA is red about code nobody reviewed. That
// is the head-moved bounce's business (merging -> reviewing), not this gate's.
func TestReviewAdvanceIgnoresRedCIOnAnUnreviewedHead(t *testing.T) {
	_, mr, f, c := ciRedFixture(t, tatarav1alpha1.StageReviewing, "failure", "sha-MOVED", true)
	d := mdNewDriver(t, f, c)
	if err := d.DrainPendingReview(context.Background(), mdGetMR(t, c, mr.Name)); err != nil {
		t.Fatalf("DrainPendingReview: %v", err)
	}
	if got := mdGetTask(t, c, "t1"); got.Status.Stage != tatarav1alpha1.StageMerging {
		t.Fatalf("stage = %q(%q), want merging: the red head is not the reviewed head",
			got.Status.Stage, got.Status.StageReason)
	}
}

// (b) THE 4h POLL THAT MUST NOT HAPPEN. A Task already sitting in merging on a
// red check leaves on the FIRST pass, with no merge attempted and no requeue.
func TestMergingLeavesImmediatelyWhenCIIsRed(t *testing.T) {
	obs.CIRedExitTotal.Reset()
	task, _, f, c := ciRedFixture(t, tatarav1alpha1.StageMerging, "failure", "sha-a", false)
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
	if got.Status.Stage != tatarav1alpha1.StageImplementing || got.Status.StageReason != stage.ReasonCIRed {
		t.Fatalf("stage = %q(%q), want implementing(ci-red)", got.Status.Stage, got.Status.StageReason)
	}
	if got.Status.CIRedReentries != 1 {
		t.Fatalf("ciRedReentries = %d, want 1", got.Status.CIRedReentries)
	}
	if n := testutil.ToFloat64(obs.CIRedExitTotal.WithLabelValues(
		"tatara-operator", tatarav1alpha1.StageMerging, tatarav1alpha1.StageImplementing)); n != 1 {
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
	task, _, f, c := ciRedFixture(t, tatarav1alpha1.StageMerging, "pending", "sha-a", false)
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
	if got := mdGetTask(t, c, "t1"); got.Status.Stage != tatarav1alpha1.StageMerging {
		t.Fatalf("stage = %q, want merging (still waiting)", got.Status.Stage)
	}
}

// CYCLE 5 IS BOUNDED. Three laps of "go fix the tests" is generous; the fourth
// is refused, exactly like every other re-entry cycle.
func TestCIRedBoundedAtMaxReentries(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageMerging)
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
	if got.Status.Stage != tatarav1alpha1.StageFailed || got.Status.StageReason != stage.ReasonCIBlocked {
		t.Fatalf("stage = %q(%q), want failed(ci-blocked)", got.Status.Stage, got.Status.StageReason)
	}
	if got.Status.CIRedReentries != tatarav1alpha1.MaxCIRedReentries {
		t.Fatalf("ciRedReentries = %d, want it NOT incremented past the cap", got.Status.CIRedReentries)
	}
}

// THE MERGED BOUNDARY. Once part of mergeOrder has landed, re-implementing would
// re-propose merged code and recreate deleted branches. It parks instead, and
// parked(ci-red) has no F.6 re-entry: a human decides.
func TestCIRedParksWhenAnEarlierRepoAlreadyMerged(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageMerging)
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
	if got.Status.Stage != tatarav1alpha1.StageParked || got.Status.StageReason != stage.ReasonCIRed {
		t.Fatalf("stage = %q(%q), want parked(ci-red)", got.Status.Stage, got.Status.StageReason)
	}
	if got.Status.CIRedReentries != 0 {
		t.Fatalf("ciRedReentries = %d, want 0: the park is not a re-entry", got.Status.CIRedReentries)
	}
	if stage.HasReentry(stage.ReasonCIRed) {
		t.Fatal("parked(ci-red) must have no F.6 re-entry: a human decides")
	}
}
