package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/prompt"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// crashClient is the REAL interruption point. It wraps a client and makes the
// NEXT MergeRequest status write fail, exactly once - which lands the failure
// BETWEEN the forge post and the mirror append, the one window v5's design left
// open. Nothing about the fake forge changes: the review and its inline comments
// ARE on the forge when the process dies.
type crashClient struct {
	client.Client
	armed bool
}

func (c *crashClient) Status() client.SubResourceWriter {
	return &crashStatusWriter{SubResourceWriter: c.Client.Status(), c: c}
}

type crashStatusWriter struct {
	client.SubResourceWriter
	c *crashClient
}

func (w *crashStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if w.c.armed {
		if _, ok := obj.(*tatarav1alpha1.MergeRequest); ok {
			w.c.armed = false
			return fmt.Errorf("crash: the reconciler died between the forge post and the mirror append")
		}
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func reviewFindingLinePtr(v int) *int { return &v }

func pendingReviewFixture(verdict string, round int, sha string) *tatarav1alpha1.PendingReview {
	body := "## Review: changes requested"
	if verdict == "approve" {
		body = "## Review: approved"
	}
	return &tatarav1alpha1.PendingReview{
		Body:  body,
		SHA:   sha,
		Round: round,
		Findings: []tatarav1alpha1.ReviewFinding{
			{Path: "internal/controller/merge.go", Line: reviewFindingLinePtr(42), Body: "this merge is not pinned to the reviewed head", Severity: "critical"},
			{Path: "internal/scm/github.go", Line: reviewFindingLinePtr(610), Body: "APPROVE 422s on a self-authored PR", Severity: "high"},
		},
	}
}

// F6-1: a pending bot review whose owning Task has LEFT reviewing is STALE. A
// maintainer approval routed through the webhook enters merging directly, and a
// review pod that raced the F6-1 teardown can re-arm PendingReview afterwards -
// DrainPendingReview must NOT post it or overwrite the approved reviewedSHA. It is
// dropped "refused": PendingReview cleared, zero forge posts, reviewedSHA
// untouched.
func TestDrainPendingReview_OwningTaskLeftReviewing_DropsStale(t *testing.T) {
	task := mdTask("t-stale", "clarify", tatarav1alpha1.StageMerging)
	mr := mdMR(task, "tatara-operator", 8)
	mr.Status.PendingReview = pendingReviewFixture("approve", 1, "sha-rearmed")
	mr.Status.ReviewedSHA = "sha-approved"

	base := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)
	f := newFakeForge(t)
	d := mdNewDriver(t, f, base)

	if err := d.DrainPendingReview(context.Background(), mdGetMR(t, base, mr.Name)); err != nil {
		t.Fatalf("DrainPendingReview: %v", err)
	}
	if f.postReviewCalls != 0 {
		t.Fatalf("PostReview calls = %d, want 0: a review whose task left reviewing must not post", f.postReviewCalls)
	}
	got := mdGetMR(t, base, mr.Name)
	if got.Status.PendingReview != nil {
		t.Fatal("PendingReview must be cleared when a stale review is dropped")
	}
	if got.Status.ReviewedSHA != "sha-approved" {
		t.Fatalf("reviewedSHA = %q, want sha-approved unchanged: a stale drop must not overwrite the approval", got.Status.ReviewedSHA)
	}
}

// ==========================================================================
// REVIEW POST SURVIVES A CRASH (contract I, fixes V6-4, V7-5, V7-6).
//
// Kill the reconciler BETWEEN the forge post and the mirror append, then re-run
// it. The forge must receive ZERO additional reviews and ZERO additional inline
// comments (the BODY MARKER dedups it), and the mirror must converge to EXACTLY
// ONE copy of each comment (the externalId SET-UNION dedups it).
//
// The externalId set-union dedups the MIRROR. It can NEVER dedup the FORGE.
// ==========================================================================
func TestReviewPostSurvivesACrash(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageReviewing)
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.PendingReview = pendingReviewFixture("request_changes", 1, "sha-a")
	mr.Status.ReviewedSHA = "sha-a"
	mr.Status.Status = "needs-changes"
	mr.Status.ReviewRounds = 1

	base := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)
	cc := &crashClient{Client: base, armed: true}

	f := newFakeForge(t)
	f.head[7] = "sha-a"

	// PASS 1: the forge post lands; the mirror append dies.
	d := mdNewDriver(t, f, cc)
	err := d.DrainPendingReview(context.Background(), mdGetMR(t, base, mr.Name))
	if err == nil {
		t.Fatalf("expected the injected crash to surface; if it did not, the test has no interruption point")
	}
	if f.postReviewCalls != 1 {
		t.Fatalf("pass 1: PostReview calls = %d, want 1", f.postReviewCalls)
	}
	if len(f.reviews[7]) != 1 || len(f.comments["rev-1"]) != 2 {
		t.Fatalf("pass 1: the forge did not receive the review + 2 inline comments")
	}
	if got := mdGetMR(t, base, mr.Name); got.Status.PendingReview == nil {
		t.Fatalf("pass 1: pendingReview cleared despite the failed mirror append - it MUST be cleared LAST")
	}

	// PASS 2: the reconciler re-runs against a healthy client.
	d2 := mdNewDriver(t, f, base)
	if err := d2.DrainPendingReview(context.Background(), mdGetMR(t, base, mr.Name)); err != nil {
		t.Fatalf("pass 2: DrainPendingReview: %v", err)
	}

	// THE FORGE: zero additional reviews, zero additional inline comments.
	if f.postReviewCalls != 1 {
		t.Fatalf("the forge received %d reviews; the round marker must dedup the POST", f.postReviewCalls)
	}
	if len(f.reviews[7]) != 1 {
		t.Fatalf("the forge carries %d reviews, want exactly 1", len(f.reviews[7]))
	}
	total := 0
	for _, cs := range f.comments {
		total += len(cs)
	}
	if total != 2 {
		t.Fatalf("the forge carries %d inline comments, want exactly 2", total)
	}

	// THE MIRROR: exactly one copy of each comment. The skip path MUST still
	// reconcile the mirror (fix V7-6: jump to 4b, not to 5) - a skip that jumps
	// past the comment-id fetch mirrors ZERO comments and this assert catches it.
	got := mdGetMR(t, base, mr.Name)
	if len(got.Status.Comments) != 2 {
		t.Fatalf("mirror carries %d comments, want exactly 2 (one per finding)", len(got.Status.Comments))
	}
	seen := map[string]int{}
	for _, c := range got.Status.Comments {
		seen[c.ExternalID]++
		if c.ReviewRound != 1 {
			t.Fatalf("comment %s carries reviewRound %d, want 1", c.ExternalID, c.ReviewRound)
		}
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("comment %s appears %d times in the mirror", id, n)
		}
	}
	if got.Status.PendingReview != nil {
		t.Fatalf("pendingReview not cleared on the successful re-run")
	}
	if got.Status.CommentCount != 2 {
		t.Fatalf("commentCount = %d, want 2", got.Status.CommentCount)
	}
}

// The event enum is {COMMENT}. A request_changes outcome posts a COMMENT review
// and lands at stage=implementing - NOT parked(review-post-refused). The fake
// forge FAILS THE TEST if it is ever handed APPROVE or REQUEST_CHANGES.
func TestReviewPostRequestChangesGoesToImplementing(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageReviewing)
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.PendingReview = pendingReviewFixture("request_changes", 1, "sha-a")
	mr.Status.ReviewedSHA = "sha-a"
	mr.Status.Status = "needs-changes"
	mr.Status.ReviewRounds = 1
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	d := mdNewDriver(t, f, c)
	if err := d.DrainPendingReview(context.Background(), mdGetMR(t, c, mr.Name)); err != nil {
		t.Fatalf("DrainPendingReview: %v", err)
	}
	if f.postReviewCalls != 1 {
		t.Fatalf("PostReview calls = %d, want 1", f.postReviewCalls)
	}
	if !strings.Contains(f.reviews[7][0].Body, scm.ReviewMarker("1", "sha-a")) {
		t.Fatalf("the review body carries no round marker: %q", f.reviews[7][0].Body)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.Stage != tatarav1alpha1.StageImplementing {
		t.Fatalf("stage = %q/%q, want implementing (NOT parked)", got.Status.Stage, got.Status.StageReason)
	}
	if gm := mdGetMR(t, c, mr.Name); gm.Status.PendingReview != nil || gm.Status.Status != "needs-changes" {
		t.Fatalf("mr not settled: pendingReview=%v status=%q", gm.Status.PendingReview, gm.Status.Status)
	}
}

// An approve on a non-review Task advances to merging, and only AFTER
// pendingReview is nil (the F.3 gate).
func TestReviewPostApproveGoesToMerging(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageReviewing)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	mr := mdMR(task, "tatara-operator", 7)
	pr := pendingReviewFixture("approve", 1, "sha-a")
	pr.Findings = nil
	mr.Status.PendingReview = pr
	mr.Status.ReviewedSHA = "sha-a"
	mr.Status.Status = "approved"
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	d := mdNewDriver(t, f, c)
	if err := d.DrainPendingReview(context.Background(), mdGetMR(t, c, mr.Name)); err != nil {
		t.Fatalf("DrainPendingReview: %v", err)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.Stage != tatarav1alpha1.StageMerging {
		t.Fatalf("stage = %q, want merging", got.Status.Stage)
	}
	if gm := mdGetMR(t, c, mr.Name); gm.Status.PendingReview != nil {
		t.Fatalf("pendingReview must be cleared BEFORE the Task advances")
	}
}

// A structural 4xx (422 "Can not approve your own pull request", 401, 403) is
// TERMINAL: park at review-post-refused, log ERROR, and NEVER hot-requeue.
// writeback_review.go:158-196 treats ANY Approve error as firstErr and requeues
// FOREVER; that is the bug this closes.
func TestReviewPostRefusedParksAndDoesNotRequeue(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageReviewing)
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.PendingReview = pendingReviewFixture("request_changes", 1, "sha-a")
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	f.postReviewErr = fmt.Errorf("422 Can not approve your own pull request: %w", scm.ErrReviewRefused)
	d := mdNewDriver(t, f, c)

	// It must NOT return an error: a returned error is a controller-runtime
	// requeue, and a structural 4xx is not retryable.
	if err := d.DrainPendingReview(context.Background(), mdGetMR(t, c, mr.Name)); err != nil {
		t.Fatalf("a structural 4xx must NOT requeue, got err = %v", err)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.Stage != tatarav1alpha1.StageParked || got.Status.StageReason != stage.ReasonReviewPostRefused {
		t.Fatalf("stage = %q/%q, want parked/review-post-refused", got.Status.Stage, got.Status.StageReason)
	}
	if gm := mdGetMR(t, c, mr.Name); gm.Status.PendingReview != nil {
		t.Fatalf("pendingReview must be cleared on a terminal refusal, else the reconciler hot-loops")
	}
	// And a re-run posts NOTHING more.
	if err := d.DrainPendingReview(context.Background(), mdGetMR(t, c, mr.Name)); err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if f.postReviewCalls != 1 {
		t.Fatalf("PostReview calls = %d, want 1: the park must not re-drive the post", f.postReviewCalls)
	}
}

// A failing forge write in the review-post drain MUST increment
// operator_scm_writes_total{result="error"}: this is the metric the
// TataraSCMWriteErrors / TataraSCMWriteFailureRatioHigh alerts select on
// (#301 - the reap dropped every emitter without dropping the alerts).
func TestReviewPostFailureIncrementsSCMWritesTotal(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageReviewing)
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.PendingReview = pendingReviewFixture("request_changes", 1, "sha-a")
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	f.postReviewErr = fmt.Errorf("500 internal server error")
	d := mdNewDriver(t, f, c)
	reg := prometheus.NewRegistry()
	d.Metrics = obs.NewOperatorMetrics(reg)

	if err := d.DrainPendingReview(context.Background(), mdGetMR(t, c, mr.Name)); err == nil {
		t.Fatalf("expected the forge failure to surface as an error")
	}
	got := testutil.ToFloat64(d.Metrics.SCMWriteCounter("github", "post_review", "error"))
	if got != 1 {
		t.Fatalf("operator_scm_writes_total{provider=github,verb=post_review,result=error} = %v, want 1", got)
	}
}

// The comment ids come from a SECOND read. The create-review response carries NO
// comments array (which is what GitHub actually returns), so a reconciler that
// expects them back from PostReview mirrors ZERO findings.
func TestReviewPostCommentIDsComeFromASecondRead(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageReviewing)
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.PendingReview = pendingReviewFixture("request_changes", 2, "sha-b")
	mr.Status.ReviewRounds = 2
	mr.Status.Status = "needs-changes"
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	d := mdNewDriver(t, f, c)
	if err := d.DrainPendingReview(context.Background(), mdGetMR(t, c, mr.Name)); err != nil {
		t.Fatalf("DrainPendingReview: %v", err)
	}
	if f.listReviewCommentCall == 0 {
		t.Fatalf("ListReviewComments was never called: the ids can only come from a second read")
	}
	got := mdGetMR(t, c, mr.Name)
	if len(got.Status.Comments) != 2 {
		t.Fatalf("mirror carries %d comments, want 2", len(got.Status.Comments))
	}
	byPath := map[string]tatarav1alpha1.Comment{}
	for _, cm := range got.Status.Comments {
		if cm.ExternalID == "" {
			t.Fatalf("mirrored comment carries no externalId: the set-union has no key")
		}
		if cm.ReviewRound != 2 {
			t.Fatalf("comment reviewRound = %d, want 2", cm.ReviewRound)
		}
		byPath[cm.Path] = cm
	}
	if cm, ok := byPath["internal/controller/merge.go"]; !ok || cm.Line != 42 {
		t.Fatalf("finding path/line did not round-trip into the mirror: %+v", byPath)
	}
}

// FORK PR / review-kind: a kind=review Task given submit_outcome(approve) posts
// the review and lands in parked(awaiting-human). It NEVER reaches merging -
// UNCONDITIONALLY. The fake forge's Merge PANICS.
func TestReviewPostForkPRNeverMerges(t *testing.T) {
	task := mdTask("t1", "review", tatarav1alpha1.StageReviewing)
	mr := mdMR(task, "tatara-operator", 7)
	pr := pendingReviewFixture("approve", 1, "sha-a")
	pr.Findings = nil
	mr.Status.PendingReview = pr
	mr.Status.Status = "approved"
	mr.Status.ReviewedSHA = "sha-a"
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	f.mergePanics = true
	f.head[7] = "sha-a"
	d := mdNewDriver(t, f, c)

	if err := d.DrainPendingReview(context.Background(), mdGetMR(t, c, mr.Name)); err != nil {
		t.Fatalf("DrainPendingReview: %v", err)
	}
	if f.postReviewCalls != 1 {
		t.Fatalf("the review IS posted on a human's PR: PostReview calls = %d", f.postReviewCalls)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.Stage != tatarav1alpha1.StageParked || got.Status.StageReason != stage.ReasonAwaitingHuman {
		t.Fatalf("stage = %q/%q, want parked/awaiting-human", got.Status.Stage, got.Status.StageReason)
	}

	// And even if something drove merging anyway, the merge would panic. Prove
	// the stage machine refuses it.
	got.Spec.MergeOrder = []string{"tatara-operator"}
	got.Status.Stage = tatarav1alpha1.StageMerging
	if _, err := d.ReconcileMerging(context.Background(), mdProject(), got); err == nil {
		t.Fatalf("a kind=review Task must never be merged")
	}
}

// THE FINDINGS REACH THE NEXT POD, with the sweep NEVER RUN. Without both the
// mirror write-back AND the operator note, the agent has no idea what to fix,
// re-submits, hits maxReviewRounds, and dies at parked(review-loop-exhausted) -
// on EVERY changes-requested cycle, for the first hour after every review.
func TestReviewPostFindingsReachTheNextPod(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageReviewing)
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.PendingReview = pendingReviewFixture("request_changes", 1, "sha-a")
	mr.Status.ReviewedSHA = "sha-a"
	mr.Status.Status = "needs-changes"
	mr.Status.ReviewRounds = 1
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	d := mdNewDriver(t, f, c)
	if err := d.DrainPendingReview(context.Background(), mdGetMR(t, c, mr.Name)); err != nil {
		t.Fatalf("DrainPendingReview: %v", err)
	}

	// THE SWEEP IS NEVER RUN. Render the ACTUAL bundle the next implement pod
	// gets, from the CRs as they now stand, and assert on its bytes.
	nextTask := mdGetTask(t, c, "t1")
	nextMR := mdGetMR(t, c, mr.Name)
	bundle, err := prompt.Render(prompt.Input{
		Task:          nextTask,
		MergeRequests: []tatarav1alpha1.MergeRequest{*nextMR},
	})
	if err != nil {
		t.Fatalf("prompt.Render: %v", err)
	}
	// BELT 1: the mirror write-back, as <comment> elements.
	if !strings.Contains(bundle, "this merge is not pinned to the reviewed head") {
		t.Fatalf("the finding is not in the next pod's bundle as a <comment>:\n%s", bundle)
	}
	if !strings.Contains(bundle, `path="internal/controller/merge.go"`) {
		t.Fatalf("the finding lost its path anchor in the bundle:\n%s", bundle)
	}
	// BELT 2: the operator note, so the findings ride even if the mirror append
	// lost a race.
	if !strings.Contains(bundle, `agent="operator"`) {
		t.Fatalf("no operator note in the bundle:\n%s", bundle)
	}
	if !strings.Contains(bundle, "Review requested changes on tatara-operator!7 @ sha-a") {
		t.Fatalf("the operator note does not carry the findings:\n%s", bundle)
	}

	// The note is written ONCE, not once per re-run.
	if err := d.DrainPendingReview(context.Background(), mdGetMR(t, c, mr.Name)); err != nil {
		t.Fatalf("re-run: %v", err)
	}
	notes := 0
	for _, n := range mdGetTask(t, c, "t1").Status.Notes {
		if strings.Contains(n.Body, "Review requested changes on tatara-operator!7") {
			notes++
		}
	}
	if notes != 1 {
		t.Fatalf("the belt note was written %d times, want 1", notes)
	}
}

// reviewBeltNote renders each finding's location for the next pod's operator
// note. After WP2, ReviewFinding.Line is *int: a non-nil line renders "path:line",
// and a nil line (a file-level finding, #398) renders just the path - NEVER the
// pointer address a bare %d on a *int would print.
func TestReviewBeltNote_RendersLineAndFileLevel(t *testing.T) {
	pr := &tatarav1alpha1.PendingReview{
		SHA: "sha-a",
		Findings: []tatarav1alpha1.ReviewFinding{
			{Path: "internal/scm/github.go", Line: reviewFindingLinePtr(42), Body: "line-anchored", Severity: "high"},
			{Path: "docs/README.md", Line: nil, Body: "file-level", Severity: "low"},
		},
	}
	note := reviewBeltNote("tatara-operator", 7, pr)

	if !strings.Contains(note, "- internal/scm/github.go:42 [high] line-anchored") {
		t.Fatalf("non-nil line did not render as path:line:\n%s", note)
	}
	if !strings.Contains(note, "- docs/README.md [low] file-level") {
		t.Fatalf("nil line did not render as a bare path (file-level):\n%s", note)
	}
	if strings.Contains(note, "0xc0") || strings.Contains(note, "%!d") {
		t.Fatalf("a *int was formatted as a pointer/garbage:\n%s", note)
	}
}

// --- the pending-comment drain (C.5.3, same shape) ------------------------

// mr_write(comment) / issue_write(comment) post ONCE: the requestId marker is
// the forge-side dedup, and the mirror append is a set-union on externalId.
func TestDrainPendingCommentsIsIdempotent(t *testing.T) {
	task := mdTask("t1", "clarify", tatarav1alpha1.StageClarifying)
	iss := mdIssue(task, "tatara-operator", 41)
	iss.Status.PendingComments = []tatarav1alpha1.PendingComment{
		{RequestID: "req-1", Action: "comment", Body: "what do you mean by fast?"},
	}
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, iss)

	f := newFakeForge(t)
	d := mdNewDriver(t, f, c)
	if err := d.DrainPendingComments(context.Background(), mdGetIssue(t, c, iss.Name)); err != nil {
		t.Fatalf("DrainPendingComments: %v", err)
	}
	if len(f.postedComments) != 1 {
		t.Fatalf("posted %d comments, want 1", len(f.postedComments))
	}
	if !strings.Contains(f.postedComments[0], PendingCommentMarker("req-1")) {
		t.Fatalf("the posted comment carries no requestId marker: %q", f.postedComments[0])
	}
	got := mdGetIssue(t, c, iss.Name)
	if len(got.Status.PendingComments) != 0 {
		t.Fatalf("pendingComments not drained: %+v", got.Status.PendingComments)
	}
	if len(got.Status.Comments) != 1 || got.Status.Comments[0].ExternalID == "" {
		t.Fatalf("the comment did not reach the mirror with its externalId: %+v", got.Status.Comments)
	}

	// Re-queue the SAME requestId (a crash between the post and the drain): the
	// forge already carries the marker, so it is NOT posted twice.
	fresh := mdGetIssue(t, c, iss.Name)
	fresh.Status.PendingComments = []tatarav1alpha1.PendingComment{
		{RequestID: "req-1", Action: "comment", Body: "what do you mean by fast?"},
	}
	if err := c.Status().Update(context.Background(), fresh); err != nil {
		t.Fatalf("re-queue: %v", err)
	}
	if err := d.DrainPendingComments(context.Background(), mdGetIssue(t, c, iss.Name)); err != nil {
		t.Fatalf("DrainPendingComments (re-run): %v", err)
	}
	if len(f.postedComments) != 1 {
		t.Fatalf("the forge received %d comments; the requestId marker must dedup the POST", len(f.postedComments))
	}
	if got := mdGetIssue(t, c, iss.Name); len(got.Status.Comments) != 1 {
		t.Fatalf("the mirror carries %d copies of one comment", len(got.Status.Comments))
	}
}

// C.2.12 defers issue_write(edit|close) through PendingComment as a comment
// intent carrying a marker (Task 12's encoding). The drain must READ THE MARKER
// BACK and perform the edit/close, not post the marker as a comment body.
func TestDrainPendingCommentsEditAndClose(t *testing.T) {
	task := mdTask("t1", "clarify", tatarav1alpha1.StageClarifying)
	iss := mdIssue(task, "tatara-operator", 41)
	iss.Status.PendingComments = []tatarav1alpha1.PendingComment{
		{RequestID: "req-edit", Action: "comment", Body: "<!-- tatara-edit -->\ntitle: a better title\nthe new body"},
		{RequestID: "req-close", Action: "comment", Body: "<!-- tatara-close -->\nsuperseded by #99"},
	}
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, iss)

	f := newFakeForge(t)
	d := mdNewDriver(t, f, c)
	if err := d.DrainPendingComments(context.Background(), mdGetIssue(t, c, iss.Name)); err != nil {
		t.Fatalf("DrainPendingComments: %v", err)
	}
	if len(f.editedIssues) != 1 {
		t.Fatalf("edited %d issues, want 1 (the tatara-edit marker was posted as a comment body?)", len(f.editedIssues))
	}
	if f.editedIssues[0] != "szymonrychu/tatara-operator#41|a better title|the new body" {
		t.Fatalf("edit = %q", f.editedIssues[0])
	}
	if len(f.closedIssues) != 1 {
		t.Fatalf("closed %d issues, want 1", len(f.closedIssues))
	}
	if !strings.HasSuffix(f.closedIssues[0], "\nsuperseded by #99") {
		t.Fatalf("close comment = %q", f.closedIssues[0])
	}
	if !strings.Contains(f.closedIssues[0], PendingCommentMarker("req-close")) {
		t.Fatalf("the close comment carries no requestId marker: %q", f.closedIssues[0])
	}
	for _, pc := range f.postedComments {
		if strings.Contains(pc, "tatara-edit") || strings.Contains(pc, "tatara-close") {
			t.Fatalf("a marker was posted as a comment body: %q", pc)
		}
	}
	got := mdGetIssue(t, c, iss.Name)
	if got.Status.State != "closed" {
		t.Fatalf("issue state = %q, want closed", got.Status.State)
	}
	if len(got.Status.PendingComments) != 0 {
		t.Fatalf("pendingComments not drained")
	}
}

// #420: closing an issue posted the SAME close comment twice on a re-drain
// (a crash/requeue between CloseIssue and removePendingComments leaves the
// close intent in PendingComments, and the drain re-ran CloseIssue with no
// forge-side check). The close comment must carry the requestId marker, and
// a re-drain must find it on the thread and NOT post it again - same shape
// as postThreadComment, contract C.5.3.
func TestDrainPendingCommentsCloseIsIdempotent(t *testing.T) {
	task := mdTask("t1", "clarify", tatarav1alpha1.StageClarifying)
	iss := mdIssue(task, "tatara-operator", 41)
	iss.Status.PendingComments = []tatarav1alpha1.PendingComment{
		{RequestID: "req-close", Action: "comment", Body: "<!-- tatara-close -->\nsuperseded by #99"},
	}
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, iss)

	f := newFakeForge(t)
	d := mdNewDriver(t, f, c)
	if err := d.DrainPendingComments(context.Background(), mdGetIssue(t, c, iss.Name)); err != nil {
		t.Fatalf("DrainPendingComments: %v", err)
	}
	if len(f.closedIssues) != 1 {
		t.Fatalf("closed %d issues, want 1", len(f.closedIssues))
	}
	if !strings.Contains(f.closedIssues[0], PendingCommentMarker("req-close")) {
		t.Fatalf("the close comment carries no requestId marker: %q", f.closedIssues[0])
	}

	// Re-queue the SAME requestId (a crash between CloseIssue and the
	// PendingComments drain): the forge thread already carries the marker, so
	// the close comment must NOT be posted a second time.
	fresh := mdGetIssue(t, c, iss.Name)
	fresh.Status.PendingComments = []tatarav1alpha1.PendingComment{
		{RequestID: "req-close", Action: "comment", Body: "<!-- tatara-close -->\nsuperseded by #99"},
	}
	if err := c.Status().Update(context.Background(), fresh); err != nil {
		t.Fatalf("re-queue: %v", err)
	}
	if err := d.DrainPendingComments(context.Background(), mdGetIssue(t, c, iss.Name)); err != nil {
		t.Fatalf("DrainPendingComments (re-run): %v", err)
	}
	marked := 0
	for _, tc := range f.thread[41] {
		if strings.Contains(tc.Body, PendingCommentMarker("req-close")) {
			marked++
		}
	}
	if marked != 1 {
		t.Fatalf("the forge thread carries %d copies of the close comment; the requestId marker must dedup the POST", marked)
	}
}

// AUTO-MERGE IS NEVER ARMED. DisableAutoMerge is retained solely to disarm PRs
// opened BEFORE the cutover; no arm call follows it, anywhere.
func TestReviewPostAutoMergeNeverArmed(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageMerging)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.ReviewedSHA = "sha-a"
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t) // EnableAutoMerge t.Fatalf's if it is ever called
	f.head[7] = "sha-a"
	d := mdNewDriver(t, f, c)
	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	if f.mergeCalls != 1 {
		t.Fatalf("the operator merges directly; auto-merge is never armed")
	}
}

var _ = time.Now

// --- the reviewing exit vs the stale informer cache (2026-07-19) -----------

// staleListClient serves MergeRequest Lists with one MR replaced by a frozen
// snapshot - exactly what the informer cache does in the instant after a
// status write it has not observed yet - while Get and every write pass
// through live.
type staleListClient struct {
	client.Client
	stale *tatarav1alpha1.MergeRequest
}

func (s *staleListClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := s.Client.List(ctx, list, opts...); err != nil {
		return err
	}
	if ml, ok := list.(*tatarav1alpha1.MergeRequestList); ok {
		for i := range ml.Items {
			if ml.Items[i].Namespace == s.stale.Namespace && ml.Items[i].Name == s.stale.Name {
				ml.Items[i] = *s.stale.DeepCopy()
			}
		}
	}
	return nil
}

// PR 389's stall, reproduced: DrainPendingReview cleared pendingReview, then
// advanceAfterReview re-listed the owned MRs through the CACHED client, saw its
// OWN pre-write copy with pendingReview still set, and returned nil silently -
// leaving the primary edge-triggered advance to the reconciler's 30s
// level-triggered re-drive. The freshly-settled MR must overlay its stale
// listed copy so the advance lands first try.
func TestDrainPendingReview_StaleCachedSelfDoesNotBlockAdvance(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageReviewing)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	mr := mdMR(task, "tatara-operator", 389)
	pr := pendingReviewFixture("approve", 1, "sha-a")
	pr.Findings = nil
	mr.Status.PendingReview = pr
	base := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	d := mdNewDriver(t, f, &staleListClient{Client: base, stale: mr.DeepCopy()})

	if err := d.DrainPendingReview(context.Background(), mdGetMR(t, base, mr.Name)); err != nil {
		t.Fatalf("DrainPendingReview: %v", err)
	}
	if got := mdGetTask(t, base, "t1"); got.Status.Stage != tatarav1alpha1.StageMerging {
		t.Fatalf("stage = %q, want merging: a stale cached copy of the just-settled MR must not veto the advance", got.Status.Stage)
	}
}

// The same staleness on ANOTHER owned MR: A's drain settled on the server but
// not yet in the cache when B drains. The advance decision must list through
// the uncached APIReader when one is wired, so B's drain is not vetoed by A's
// ghost pendingReview.
func TestDrainPendingReview_AdvanceListsOwnedMRsUncached(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageReviewing)
	task.Spec.MergeOrder = []string{"tatara-operator", "tatara-cli"}
	mrA := mdMR(task, "tatara-operator", 7)
	mrA.Status.Status = "approved"
	mrA.Status.ReviewedSHA = "sha-a" // settled: its own drain already ran
	mrB := mdMR(task, "tatara-cli", 8)
	prB := pendingReviewFixture("approve", 1, "sha-b")
	prB.Findings = nil
	mrB.Status.PendingReview = prB
	base := newMirrorClient(t, mdProject(), mdSecret(),
		mdRepo("tatara-operator"), mdRepo("tatara-cli"), task, mrA, mrB)

	// The stale cache still shows A owing its review.
	staleA := mrA.DeepCopy()
	staleA.Status.Status = ""
	staleA.Status.ReviewedSHA = ""
	staleA.Status.PendingReview = pendingReviewFixture("approve", 1, "sha-a")

	f := newFakeForge(t)
	d := mdNewDriver(t, f, &staleListClient{Client: base, stale: staleA})
	d.APIReader = base

	if err := d.DrainPendingReview(context.Background(), mdGetMR(t, base, mrB.Name)); err != nil {
		t.Fatalf("DrainPendingReview: %v", err)
	}
	if got := mdGetTask(t, base, "t1"); got.Status.Stage != tatarav1alpha1.StageMerging {
		t.Fatalf("stage = %q, want merging: the uncached read must see A's settled review", got.Status.Stage)
	}
}

func TestTerminalMREdge(t *testing.T) {
	review := mdTask("t1", "review", tatarav1alpha1.StageReviewing)
	impl := mdTask("t2", "implement", tatarav1alpha1.StageReviewing)
	merged := tatarav1alpha1.MergeRequest{Status: tatarav1alpha1.MergeRequestStatus{State: "merged"}}
	closed := tatarav1alpha1.MergeRequest{Status: tatarav1alpha1.MergeRequestStatus{State: "closed"}}
	open := tatarav1alpha1.MergeRequest{Status: tatarav1alpha1.MergeRequestStatus{State: "open"}}

	// All merged -> delivered(mr-merged-externally).
	edge, ok := terminalMREdge(review, []tatarav1alpha1.MergeRequest{merged})
	require.True(t, ok)
	require.Equal(t, tatarav1alpha1.StageDelivered, edge.To)
	require.Equal(t, stage.ReasonMRMergedExternally, edge.Reason)

	// All terminal, one closed-unmerged -> rejected(mr-closed-externally).
	edge, ok = terminalMREdge(review, []tatarav1alpha1.MergeRequest{merged, closed})
	require.True(t, ok)
	require.Equal(t, tatarav1alpha1.StageRejected, edge.To)
	require.Equal(t, stage.ReasonMRClosedExternally, edge.Reason)

	// An open MR -> no finalize.
	_, ok = terminalMREdge(review, []tatarav1alpha1.MergeRequest{merged, open})
	require.False(t, ok)

	// Empty set -> no finalize.
	_, ok = terminalMREdge(review, nil)
	require.False(t, ok)

	// Non-review kind, all merged -> no finalize (implement keeps its lifecycle).
	_, ok = terminalMREdge(impl, []tatarav1alpha1.MergeRequest{merged})
	require.False(t, ok)
}

// A merged/closed MR carrying a STALE pendingReview must still finalize:
// terminalMREdge runs BEFORE the pendingReview-owed gate in reviewAdvanceEdge.
func TestReviewAdvanceEdge_TerminalMRBeatsStalePendingReview(t *testing.T) {
	review := mdTask("t1", "review", tatarav1alpha1.StageReviewing)
	merged := tatarav1alpha1.MergeRequest{Status: tatarav1alpha1.MergeRequestStatus{
		State:         "merged",
		PendingReview: &tatarav1alpha1.PendingReview{},
	}}
	edge, ok := reviewAdvanceEdge(review, []tatarav1alpha1.MergeRequest{merged}, 3)
	require.True(t, ok, "a merged MR must finalize even with a stale pendingReview")
	require.Equal(t, tatarav1alpha1.StageDelivered, edge.To)
	require.Equal(t, stage.ReasonMRMergedExternally, edge.Reason)
}
