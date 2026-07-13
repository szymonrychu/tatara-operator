package controller

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// THE DRIVER HAS CALLERS NOW (gap G2).
//
// StageDriver's five entry points were wired to NOTHING: cmd/manager/wire.go
// never built one, and neither mirror reconciler ever drained. reviewing never
// advanced and no review was ever posted. These tests hold the wiring in place.

func mdReq(name string) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: mdNS, Name: name}}
}

// mdMRReconciler builds the MergeRequest reconciler with the drain wired and the
// cadence mirror sync OFF (ReaderFor nil): the drain is what is under test.
func mdMRReconciler(c client.Client, d *StageDriver) *MergeRequestReconciler {
	return &MergeRequestReconciler{Client: c, Driver: d}
}

func mdIssueReconciler(c client.Client, d *StageDriver) *IssueReconciler {
	return &IssueReconciler{Client: c, Driver: d}
}

// The MergeRequest reconciler DRAINS pendingReview: the review lands on the
// forge and the Task advances off reviewing. Without this wiring /outcome writes
// an intent nobody ever performs.
func TestMergeRequestReconcilerDrainsPendingReview(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageReviewing)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	mr := mdMR(task, "tatara-operator", 7)
	pr := pendingReviewFixture("approve", 1, "sha-a")
	pr.Findings = nil
	mr.Status.PendingReview = pr
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	f.head[7] = "sha-a"
	r := mdMRReconciler(c, mdNewDriver(t, f, c))

	if _, err := r.Reconcile(context.Background(), mdReq(mr.Name)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if f.postReviewCalls != 1 {
		t.Fatalf("PostReview calls = %d, want 1: the reconciler must drain pendingReview", f.postReviewCalls)
	}
	if gm := mdGetMR(t, c, mr.Name); gm.Status.PendingReview != nil {
		t.Fatalf("pendingReview not cleared")
	}
	if got := mdGetTask(t, c, "t1"); got.Status.Stage != tatarav1alpha1.StageMerging {
		t.Fatalf("stage = %q, want merging: the review drain is what advances the Task", got.Status.Stage)
	}
}

// A kind=review Task NEVER reaches merging, through the WIRED path. The fake
// forge's Merge PANICS.
func TestMergeRequestReconcilerReviewKindNeverMerges(t *testing.T) {
	task := mdTask("t1", "review", tatarav1alpha1.StageReviewing)
	mr := mdMR(task, "tatara-operator", 7)
	pr := pendingReviewFixture("approve", 1, "sha-a")
	pr.Findings = nil
	mr.Status.PendingReview = pr
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	f.mergePanics = true
	f.head[7] = "sha-a"
	d := mdNewDriver(t, f, c)

	if _, err := mdMRReconciler(c, d).Reconcile(context.Background(), mdReq(mr.Name)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.Stage != tatarav1alpha1.StageParked {
		t.Fatalf("stage = %q, want parked: a human's PR is merged by a HUMAN", got.Status.Stage)
	}
	// And the stage reconciler refuses to drive it into merging even if asked.
	sr := &StageReconciler{Client: c, Driver: d}
	if _, err := sr.Reconcile(context.Background(), mdReq("t1")); err != nil {
		t.Fatalf("StageReconciler.Reconcile: %v", err)
	}
	if f.mergeCalls != 0 {
		t.Fatalf("merge calls = %d, want 0", f.mergeCalls)
	}
}

// The Issue reconciler DRAINS pendingComments: an agent's issue_write lands on
// the forge.
func TestIssueReconcilerDrainsPendingComments(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageClarifying)
	iss := mdIssue(task, "tatara-operator", 41)
	iss.Status.PendingComments = []tatarav1alpha1.PendingComment{
		{RequestID: "req-1", Action: "comment", Body: "clarifying question"},
	}
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, iss)

	f := newFakeForge(t)
	r := mdIssueReconciler(c, mdNewDriver(t, f, c))
	if _, err := r.Reconcile(context.Background(), mdReq(iss.Name)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(f.postedComments) != 1 {
		t.Fatalf("posted %d comments, want 1: the reconciler must drain pendingComments", len(f.postedComments))
	}
	if gi := mdGetIssue(t, c, iss.Name); len(gi.Status.PendingComments) != 0 {
		t.Fatalf("pendingComments not drained")
	}
}

// The MergeRequest reconciler drains pendingComments on an MR too.
func TestMergeRequestReconcilerDrainsPendingComments(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageImplementing)
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.PendingComments = []tatarav1alpha1.PendingComment{
		{RequestID: "req-2", Action: "comment", Body: "addressed in the last push"},
	}
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	r := mdMRReconciler(c, mdNewDriver(t, f, c))
	if _, err := r.Reconcile(context.Background(), mdReq(mr.Name)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(f.postedComments) != 1 {
		t.Fatalf("posted %d comments, want 1", len(f.postedComments))
	}
	if gm := mdGetMR(t, c, mr.Name); len(gm.Status.PendingComments) != 0 {
		t.Fatalf("pendingComments not drained")
	}
}

// The stage reconciler is what drives merging and deploying: nothing else in
// the operator owns those two pod-less stages.
func TestStageReconcilerMergesThenDelivers(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageMerging)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.ReviewedSHA = "sha-a"
	mr.Status.Status = "approved"
	iss := mdIssue(task, "tatara-operator", 41)
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), mdHelmfileRepo(), task, mr, iss)

	f := newFakeForge(t)
	f.head[7] = "sha-a"
	rd := mdNewReader(f)
	rd.tags["tatara-operator"] = "v1.4.0"
	rd.runs[helmfileRepoName] = mdSuccessfulApply("apply-sha")
	rd.pin["main"] = mdPin("tatara-operator", "1.3.0")
	rd.pin["apply-sha"] = mdPin("tatara-operator", "1.4.0")
	d := mdNewDriverWithReader(t, f, c, rd)
	r := &StageReconciler{Client: c, Driver: d}

	// PASS 1: merging.
	if _, err := r.Reconcile(context.Background(), mdReq("t1")); err != nil {
		t.Fatalf("Reconcile (merging): %v", err)
	}
	if f.mergeCalls != 1 {
		t.Fatalf("merge calls = %d, want 1", f.mergeCalls)
	}
	if got := mdGetTask(t, c, "t1"); got.Status.Stage != tatarav1alpha1.StageDeploying {
		t.Fatalf("stage = %q, want deploying", got.Status.Stage)
	}
	// The merge landed at the reviewed head, not at the mirror's stale one.
	if f.mergedHeads[0] != "sha-a" {
		t.Fatalf("merge pinned to %q, want sha-a", f.mergedHeads[0])
	}

	// The mirror learns the merge is applied.
	if err := mdSetMergedAt(c, mr.Name); err != nil {
		t.Fatalf("stamp mergedAt: %v", err)
	}

	// PASS 2: deploying -> delivered.
	if _, err := r.Reconcile(context.Background(), mdReq("t1")); err != nil {
		t.Fatalf("Reconcile (deploying): %v", err)
	}
	if len(f.closedIssues) != 1 {
		t.Fatalf("closed %d issues, want 1", len(f.closedIssues))
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.Stage != tatarav1alpha1.StageDelivered || got.Status.DeliveredAt == nil {
		t.Fatalf("stage = %q deliveredAt = %v, want delivered/set", got.Status.Stage, got.Status.DeliveredAt)
	}
}

func mdSetMergedAt(c client.Client, name string) error {
	var mr tatarav1alpha1.MergeRequest
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: mdNS, Name: name}, &mr); err != nil {
		return err
	}
	mr.Status.MergedAt = &mdMergedAt
	return c.Status().Update(context.Background(), &mr)
}

// A terminal Task is not driven at all.
func TestStageReconcilerIgnoresOtherStages(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageImplementing)
	mr := mdMR(task, "tatara-operator", 7)
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	f.mergePanics = true
	r := &StageReconciler{Client: c, Driver: mdNewDriver(t, f, c)}
	res, err := r.Reconcile(context.Background(), mdReq("t1"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("a stage the driver does not own must not be polled")
	}
}

// mergePRSquash IS THE SINGLE writer.Merge EGRESS (contract C.5.2). "Agents never
// merge" is structural only while that stays true, so it is asserted on the
// SOURCE, not hoped for in review: exactly one call to a Merge method exists in
// the package, and it is the one inside mergePRSquash.
func TestSingleMergeEgress(t *testing.T) {
	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob internal/controller: %v", err)
	}
	fset := token.NewFileSet()
	var sites []string
	for _, path := range sources {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Merge" {
				return true
			}
			// writer.Merge(ctx, repoURL, token, number, method, expectedHeadSHA)
			if len(call.Args) != 6 {
				return true
			}
			sites = append(sites, fset.Position(call.Pos()).String())
			return true
		})
	}
	if len(sites) != 1 {
		t.Fatalf("found %d SCMWriter.Merge call sites, want exactly 1 (mergePRSquash): %v", len(sites), sites)
	}
	if !strings.HasPrefix(sites[0], "deploy_supervision.go:") {
		t.Fatalf("the single Merge egress moved: %s (it must stay inside mergePRSquash)", sites[0])
	}
}
