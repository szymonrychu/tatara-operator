package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// fixedTime returns a deterministic, monotonically-increasing timestamp for
// test comment fixtures - offset seconds from a fixed epoch, so a slice of
// incoming comments sorts oldest-first exactly like a real forge listing.
func fixedTime(offsetSeconds int) time.Time {
	return time.Unix(1_700_000_000, 0).Add(time.Duration(offsetSeconds) * time.Second)
}

// mirrorHasComment reports whether mr's mirrored Status.Comments contains a
// comment with the given ExternalID.
func mirrorHasComment(mr *tatarav1alpha1.MergeRequest, externalID string) bool {
	for _, c := range mr.Status.Comments {
		if c.ExternalID == externalID {
			return true
		}
	}
	return false
}

func TestRedeliverMRComments_MirrorsAndAdvancesCursor(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedExternalMRWithReviewOwner(t, ctx, proj, repo, 30, "op12-review-task-30") // ownership external, cursor ""
	incoming := []scm.IssueComment{
		{ExternalID: "100", Author: "alice", Body: "please take over", CreatedAt: fixedTime(1)},
		{ExternalID: "101", Author: "alice", Body: "still stuck", CreatedAt: fixedTime(2)},
	}
	if err := d.redeliverMRComments(ctx, proj, repo, mr, incoming); err != nil {
		t.Fatal(err)
	}
	got := getMR(t, ctx, proj, repo, 30)
	if got.Status.LastMirroredCommentID != "101" {
		t.Fatalf("cursor = %q, want 101", got.Status.LastMirroredCommentID)
	}
	if !mirrorHasComment(got, "100") || !mirrorHasComment(got, "101") {
		t.Fatalf("comments not mirrored")
	}
	tk := getTask(t, "op12-review-task-30")
	if len(tk.Status.PendingEvents) < 2 { // mr_comment TaskEvents delivered to the owning task
		t.Fatalf("mr_comment events not delivered: %d", len(tk.Status.PendingEvents))
	}
	for _, ev := range tk.Status.PendingEvents {
		if ev.Kind != "mr_comment" {
			t.Fatalf("delivered event kind = %q, want mr_comment", ev.Kind)
		}
	}
}

// TestRedeliverMRComments_SkipsBotComment covers the "never redeliver the
// bot's own comment" branch: the cursor still advances past it (so the next
// sweep does not re-consider it), but it is neither mirrored nor delivered as
// a TaskEvent.
func TestRedeliverMRComments_SkipsBotComment(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedExternalMRWithReviewOwner(t, ctx, proj, repo, 31, "op12-review-task-31")
	incoming := []scm.IssueComment{
		{ExternalID: "200", Author: proj.Spec.Scm.BotLogin, Body: "## Review: needs-changes", CreatedAt: fixedTime(1)},
	}
	if err := d.redeliverMRComments(ctx, proj, repo, mr, incoming); err != nil {
		t.Fatal(err)
	}
	got := getMR(t, ctx, proj, repo, 31)
	if got.Status.LastMirroredCommentID != "200" {
		t.Fatalf("cursor = %q, want 200 (cursor still advances past a skipped bot comment)", got.Status.LastMirroredCommentID)
	}
	if mirrorHasComment(got, "200") {
		t.Fatalf("the bot's own comment must never be mirrored by redelivery")
	}
	tk := getTask(t, "op12-review-task-31")
	if len(tk.Status.PendingEvents) != 0 {
		t.Fatalf("the bot's own comment must never be delivered as a TaskEvent, got %d pending", len(tk.Status.PendingEvents))
	}
}

// TestRedeliverMRComments_SkipsCursorComment covers the idempotency guard: a
// re-run with incoming still containing the already-delivered cursor comment
// (as if listPRCommentsAfter's cursor-not-found fallback replayed it) must not
// re-deliver or re-advance past it a second time in a way that duplicates work
// beyond what a plain re-run already tolerates.
func TestRedeliverMRComments_SkipsCursorComment(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedExternalMRWithReviewOwner(t, ctx, proj, repo, 32, "op12-review-task-32")
	mr.Status.LastMirroredCommentID = "300"
	if err := k8sClient.Status().Update(ctx, mr); err != nil {
		t.Fatalf("seed cursor: %v", err)
	}
	incoming := []scm.IssueComment{
		{ExternalID: "300", Author: "alice", Body: "already delivered", CreatedAt: fixedTime(1)},
	}
	if err := d.redeliverMRComments(ctx, proj, repo, mr, incoming); err != nil {
		t.Fatal(err)
	}
	got := getMR(t, ctx, proj, repo, 32)
	if mirrorHasComment(got, "300") {
		t.Fatalf("the cursor's own comment must not be re-mirrored")
	}
	tk := getTask(t, "op12-review-task-32")
	if len(tk.Status.PendingEvents) != 0 {
		t.Fatalf("the cursor's own comment must not be re-delivered, got %d pending", len(tk.Status.PendingEvents))
	}
}

// TestRedeliverMRComments_MintsOwnerWhenMissing is the belt-and-suspenders
// coverage: an external MR with NO controller owner (a caller that reaches
// redeliverMRComments without going through sweepPRs' own PRReview mint) still
// gets a review Task minted via the SAME EnsureTaskForMRComment the webhook
// fast path uses, and the incoming comment is delivered to it.
func TestRedeliverMRComments_MintsOwnerWhenMissing(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedOpenMR(t, ctx, proj, repo, 33, "renovate/x", "octocat", "h33") // no owner
	incoming := []scm.IssueComment{
		{ExternalID: "400", Author: "octocat", Body: "please take a look", CreatedAt: fixedTime(1)},
	}
	if err := d.redeliverMRComments(ctx, proj, repo, mr, incoming); err != nil {
		t.Fatal(err)
	}
	got := getMR(t, ctx, proj, repo, 33)
	ownerName, ok := ownerControllerName(got)
	if !ok || ownerName == "" {
		t.Fatalf("redeliverMRComments must belt-and-suspenders mint a review task when the MR carries no owner")
	}
	if !mirrorHasComment(got, "400") {
		t.Fatalf("comment not mirrored")
	}
	tk := getTask(t, ownerName)
	if len(tk.Status.PendingEvents) != 1 {
		t.Fatalf("mr_comment event not delivered to the newly-minted owner: %d pending", len(tk.Status.PendingEvents))
	}
}

// TestRedeliverMRComments_OversizedBodyStillDelivers is the #495 regression.
// TaskEvent.Body carries a CRD MaxLength of 4096 bytes; a forge comment larger
// than that used to go into the event verbatim, so the API server 422-ed the
// whole Task status write. That is not log noise: the write it rejects is the
// mr_comment redelivery to a review Task parked awaiting-human, and because the
// error returns BEFORE the cursor advances, the oversized comment poisons the
// MR's redelivery cursor forever - every later comment on that MR is stuck
// behind it, on every subsequent sweep.
func TestRedeliverMRComments_OversizedBodyStillDelivers(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"exactly at the limit", strings.Repeat("a", tatarav1alpha1.TaskEventBodyMaxBytes)},
		{"one byte over", strings.Repeat("a", tatarav1alpha1.TaskEventBodyMaxBytes+1)},
		{"far over", strings.Repeat("a", 100_000)},
		// The cut lands mid-rune: byte 4096 is the SECOND byte of a 2-byte
		// rune, so a naive body[:4096] yields invalid UTF-8 that the API
		// server's JSON encoder rejects even after the length is legal.
		{"multi-byte rune split at the cut", strings.Repeat("a", tatarav1alpha1.TaskEventBodyMaxBytes-1) + strings.Repeat("é", 64)},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			d, proj, repo := newOwnershipDriver(t, ctx)
			number := 60 + i
			taskName := fmt.Sprintf("op12-oversize-task-%d", number)
			mr := seedExternalMRWithReviewOwner(t, ctx, proj, repo, number, taskName)
			incoming := []scm.IssueComment{
				{ExternalID: "900", Author: "alice", Body: tc.body, CreatedAt: fixedTime(1)},
			}
			if err := d.redeliverMRComments(ctx, proj, repo, mr, incoming); err != nil {
				t.Fatalf("redelivery of an oversized comment must not fail: %v", err)
			}
			got := getMR(t, ctx, proj, repo, number)
			if got.Status.LastMirroredCommentID != "900" {
				t.Fatalf("cursor = %q, want 900: an oversized comment must not freeze the redelivery cursor",
					got.Status.LastMirroredCommentID)
			}
			tk := getTask(t, taskName)
			if len(tk.Status.PendingEvents) != 1 {
				t.Fatalf("mr_comment event not delivered: %d pending", len(tk.Status.PendingEvents))
			}
			ev := tk.Status.PendingEvents[0]
			if len(ev.Body) > tatarav1alpha1.TaskEventBodyMaxBytes {
				t.Fatalf("delivered body = %d bytes, want <= %d", len(ev.Body), tatarav1alpha1.TaskEventBodyMaxBytes)
			}
			if !utf8.ValidString(ev.Body) {
				t.Fatalf("delivered body is not valid UTF-8: truncation split a rune")
			}
			if len(tc.body) > tatarav1alpha1.TaskEventBodyMaxBytes &&
				!strings.Contains(ev.Body, taskEventTruncatedMarker) {
				t.Fatalf("a truncated redelivery must say so; body tail = %q", tail(ev.Body, 80))
			}
		})
	}
}

// TestRedeliverMRComments_OversizedCommentDoesNotAbortBatch covers the batch
// half of #495: the append error returned mid-loop, so one oversized comment
// also swallowed every comment after it in the same pass.
func TestRedeliverMRComments_OversizedCommentDoesNotAbortBatch(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedExternalMRWithReviewOwner(t, ctx, proj, repo, 70, "op12-oversize-task-70")
	incoming := []scm.IssueComment{
		{ExternalID: "910", Author: "alice", Body: strings.Repeat("a", 20_000), CreatedAt: fixedTime(1)},
		{ExternalID: "911", Author: "alice", Body: "the short follow-up nobody ever saw", CreatedAt: fixedTime(2)},
	}
	if err := d.redeliverMRComments(ctx, proj, repo, mr, incoming); err != nil {
		t.Fatal(err)
	}
	got := getMR(t, ctx, proj, repo, 70)
	if got.Status.LastMirroredCommentID != "911" {
		t.Fatalf("cursor = %q, want 911", got.Status.LastMirroredCommentID)
	}
	tk := getTask(t, "op12-oversize-task-70")
	if len(tk.Status.PendingEvents) != 2 {
		t.Fatalf("both comments must be delivered, got %d pending", len(tk.Status.PendingEvents))
	}
	if tk.Status.PendingEvents[1].Body != "the short follow-up nobody ever saw" {
		t.Fatalf("the comment after the oversized one was dropped: %q", tk.Status.PendingEvents[1].Body)
	}
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
