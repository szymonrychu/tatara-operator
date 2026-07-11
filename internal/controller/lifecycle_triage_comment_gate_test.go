package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// triagePostCommentWriter is a minimal SCMWriter for triagePostComment's
// gate tests (FIX-6): Comment captures posts, GetIssueState reports the
// configured closed state.
type triagePostCommentWriter struct {
	labelWriter
	closed       bool
	postedBodies []string
}

func (w *triagePostCommentWriter) GetIssueState(_ context.Context, _, _ string, _ int) (scm.IssueState, error) {
	return scm.IssueState{Closed: w.closed}, nil
}

func (w *triagePostCommentWriter) Comment(_ context.Context, _, _, body string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.postedBodies = append(w.postedBodies, body)
	return nil
}

type triagePostCommentReader struct {
	fakeProposalReader
	comments []scm.IssueComment
}

func (r *triagePostCommentReader) ListIssueComments(_ context.Context, _, _ string, _ int) ([]scm.IssueComment, error) {
	return r.comments, nil
}

// TestTriagePostComment_ClosedIssue_Suppressed is FIX-6: triagePostComment
// bypassed the closed-state rule (its in-code justification only covered
// rule-1 last-word); it must now suppress a post to an already-closed issue.
func TestTriagePostComment_ClosedIssue_Suppressed(t *testing.T) {
	_, task, _ := seedLabelTask(t, "tpc-closed", nil)
	proj := projOf(t, task)
	w := &triagePostCommentWriter{closed: true}
	r := reconcilerFor(w, &triagePostCommentReader{})

	require.NoError(t, r.triagePostComment(context.Background(), proj, task, "still awaiting go-ahead"))

	w.mu.Lock()
	defer w.mu.Unlock()
	require.Empty(t, w.postedBodies, "must not post to an already-closed issue")
}

// TestTriagePostComment_DuplicateContent_Suppressed is FIX-6: a comment whose
// normalized body already exists on the thread (from the bot) must not repost
// - e.g. the auto-approve audit note or a discuss/close-withheld note
// retried on the same content.
func TestTriagePostComment_DuplicateContent_Suppressed(t *testing.T) {
	_, task, _ := seedLabelTask(t, "tpc-dup", nil)
	proj := projOf(t, task)
	w := &triagePostCommentWriter{}
	r := reconcilerFor(w, &triagePostCommentReader{comments: []scm.IssueComment{
		tcBody("tatara-bot", -60, "still awaiting go-ahead"),
		tcBody("human", -30, "hmm"), // breaks rule-1 last-word so dedup is reached
	}})

	require.NoError(t, r.triagePostComment(context.Background(), proj, task, "still awaiting go-ahead"))

	w.mu.Lock()
	defer w.mu.Unlock()
	require.Empty(t, w.postedBodies, "must not repost identical content already on the thread")
}

// TestTriagePostComment_OpenDistinct_Posts is the regression guard: an open,
// distinct-content post must still go through.
func TestTriagePostComment_OpenDistinct_Posts(t *testing.T) {
	_, task, _ := seedLabelTask(t, "tpc-open", nil)
	proj := projOf(t, task)
	w := &triagePostCommentWriter{}
	r := reconcilerFor(w, &triagePostCommentReader{})

	require.NoError(t, r.triagePostComment(context.Background(), proj, task, "hello"))

	w.mu.Lock()
	defer w.mu.Unlock()
	require.Equal(t, []string{"hello"}, w.postedBodies)
}

// tcBody builds an scm.IssueComment like tc() (comment_gate_test.go) but with
// an explicit body, for the dedup fixtures above.
func tcBody(author string, minsAgo int, body string) scm.IssueComment {
	c := tc(author, minsAgo)
	c.Body = body
	return c
}
