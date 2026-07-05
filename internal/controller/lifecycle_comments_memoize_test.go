package controller

// Regression test for S20: finishTriage's discuss-arm silence gate calls both
// hasHumanReply and botHasLastWord, each of which independently fetched
// Status.IssueComment via ListIssueComments -- a double SCM round-trip on the
// authored+human-replied success path. triageReader.listComments memoizes the
// comments slice ONLY on success (never caches an error), so a transient
// first-call failure still lets the second check retry live (fail-open,
// unchanged from before the fix).

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// commentCountingReader controls GetIssue (authorship) and counts
// ListIssueComments calls, optionally failing only the first call.
type commentCountingReader struct {
	fakeProposalReader
	issueBody string
	comments  []scm.IssueComment
	firstErr  error // when set, returned on the FIRST ListIssueComments call only
	callCount int
}

func (r *commentCountingReader) GetIssue(_ context.Context, _, _ string, _ int) (scm.IssueContent, error) {
	return scm.IssueContent{Body: r.issueBody}, nil
}

func (r *commentCountingReader) ListIssueComments(_ context.Context, _, _ string, _ int) ([]scm.IssueComment, error) {
	r.callCount++
	if r.callCount == 1 && r.firstErr != nil {
		return nil, r.firstErr
	}
	return r.comments, nil
}

// TestFinishTriage_Discuss_AuthoredWithHumanReply_MemoizesComments verifies the
// success path (tatara-authored issue, human already replied) fetches
// ListIssueComments exactly ONCE across hasHumanReply + botHasLastWord.
func TestFinishTriage_Discuss_AuthoredWithHumanReply_MemoizesComments(t *testing.T) {
	task, proj := seedDiscussSilenceTask(t, "memo-ok")

	rdr := &commentCountingReader{
		issueBody: "an idea\n\n" + tataraAuthoredMarker,
		comments:  []scm.IssueComment{{Author: "szymon", Body: "looks interesting, proceed"}},
	}
	w := &commentCapturingWriter{}
	r := reconcilerFor(w, rdr)

	_, err := r.finishTriage(context.Background(), proj, task)
	require.NoError(t, err)

	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.LifecycleState)
	require.Equal(t, 1, rdr.callCount,
		"authored+human-replied success path must fetch ListIssueComments exactly once (memoized)")
}

// TestFinishTriage_Discuss_FirstListCommentsErrorStillRetriesLive verifies the
// fail-open path: a transient error on the FIRST ListIssueComments call (from
// hasHumanReply) must NOT be cached, so botHasLastWord's call is a second, live
// attempt -- matching pre-fix behavior.
func TestFinishTriage_Discuss_FirstListCommentsErrorStillRetriesLive(t *testing.T) {
	task, proj := seedDiscussSilenceTask(t, "memo-err")

	rdr := &commentCountingReader{
		issueBody: "an idea\n\n" + tataraAuthoredMarker,
		comments:  []scm.IssueComment{{Author: "szymon", Body: "looks interesting, proceed"}},
		firstErr:  errors.New("scm down"),
	}
	w := &commentCapturingWriter{}
	r := reconcilerFor(w, rdr)

	_, err := r.finishTriage(context.Background(), proj, task)
	require.NoError(t, err)

	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.LifecycleState)
	require.Equal(t, 2, rdr.callCount,
		"a first-call ListIssueComments error must not be cached; botHasLastWord must retry live (call count 2)")
}
