package controller

// Tests for FIX: silence repeated "still awaiting go-ahead" discuss comments.
// A tatara-authored issue that gets action=discuss with NO human comment must
// NOT receive a comment (silent hold -> Conversation).
// It MUST receive a comment when a human has replied.
// Human-filed issues always receive the discuss comment (unchanged path).

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"k8s.io/apimachinery/pkg/types"
)

// discussSilenceReader controls GetIssue (authorship) and ListIssueComments
// (human-reply check) independently per test case.
type discussSilenceReader struct {
	fakeProposalReader
	issueBody string
	comments  []scm.IssueComment
	listErr   error // when set, ListIssueComments returns it (fail-open test)
}

func (r *discussSilenceReader) GetIssue(_ context.Context, _, _ string, _ int) (scm.IssueContent, error) {
	return scm.IssueContent{Body: r.issueBody}, nil
}

func (r *discussSilenceReader) ListIssueComments(_ context.Context, _, _ string, _ int) ([]scm.IssueComment, error) {
	return r.comments, r.listErr
}

// commentCapturingWriter extends labelWriter with a Comment capture.
type commentCapturingWriter struct {
	labelWriter
	commentBodies []string
}

func (w *commentCapturingWriter) Comment(_ context.Context, _, _, body string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.commentBodies = append(w.commentBodies, body)
	return nil
}

func (w *commentCapturingWriter) CloseIssue(_ context.Context, _, _ string, number int, _ string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = append(w.closed, number)
	return nil
}

// seedDiscussSilenceTask seeds a task with Phase=Succeeded and action=discuss
// for use by the discuss-silence tests.
func seedDiscussSilenceTask(t *testing.T, suffix string) (*tatarav1alpha1.Task, *tatarav1alpha1.Project) {
	t.Helper()
	_, task, _ := seedLabelTask(t, "ds-"+suffix, nil)
	ctx := context.Background()
	var fresh tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &fresh))
	fresh.Status.Phase = "Succeeded"
	fresh.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: "discuss", Comment: "still awaiting go-ahead"}
	require.NoError(t, k8sClient.Status().Update(ctx, &fresh))

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))

	return getTaskByName(t, task.Name), &proj
}

// TestFinishTriage_TataraAuthored_Discuss_NoHumanComment_SilentHold verifies
// that when the agent emits action=discuss on a tatara-authored issue and no
// human has replied, finishTriage enters Conversation WITHOUT posting a comment.
// This is the primary repro for tatara-operator#29 (3 identical bot comments).
func TestFinishTriage_TataraAuthored_Discuss_NoHumanComment_SilentHold(t *testing.T) {
	task, proj := seedDiscussSilenceTask(t, "nohu")

	rdr := &discussSilenceReader{
		// Issue body carries the marker -> tatara-authored.
		issueBody: "an idea\n\n" + tataraAuthoredMarker,
		// No comments at all -> no human reply.
		comments: nil,
	}
	w := &commentCapturingWriter{}
	r := reconcilerFor(w, rdr)

	_, err := r.finishTriage(context.Background(), proj, task)
	require.NoError(t, err)

	// Must enter Conversation (silent hold).
	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.DeployState,
		"tatara-authored issue with discuss and no human reply must enter Conversation silently")

	// Must NOT post any comment.
	w.mu.Lock()
	posted := len(w.commentBodies)
	w.mu.Unlock()
	require.Zero(t, posted,
		"tatara-authored issue with discuss and no human reply must NOT post a comment; got: %v", w.commentBodies)
}

// TestFinishTriage_TataraAuthored_Discuss_WithHumanComment_PostsComment verifies
// that when a human HAS replied, the discuss comment is posted (normal flow).
func TestFinishTriage_TataraAuthored_Discuss_WithHumanComment_PostsComment(t *testing.T) {
	task, proj := seedDiscussSilenceTask(t, "withhu")

	rdr := &discussSilenceReader{
		issueBody: "an idea\n\n" + tataraAuthoredMarker,
		comments:  []scm.IssueComment{{Author: "szymon", Body: "looks interesting, proceed"}},
	}
	w := &commentCapturingWriter{}
	r := reconcilerFor(w, rdr)

	_, err := r.finishTriage(context.Background(), proj, task)
	require.NoError(t, err)

	// Must enter Conversation (discuss flow).
	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.DeployState)

	// Must post the discuss comment.
	w.mu.Lock()
	posted := len(w.commentBodies)
	w.mu.Unlock()
	require.Equal(t, 1, posted,
		"tatara-authored issue with discuss and a human reply MUST post the discuss comment")
}

// TestFinishTriage_HumanFiled_Discuss_AlwaysPostsComment verifies that for a
// human-filed issue (no tataraAuthoredMarker), the discuss comment is always
// posted regardless of whether there are prior comments.
func TestFinishTriage_HumanFiled_Discuss_AlwaysPostsComment(t *testing.T) {
	task, proj := seedDiscussSilenceTask(t, "humfil")

	rdr := &discussSilenceReader{
		// No marker in body -> human-filed issue.
		issueBody: "I want a new feature",
		comments:  nil,
	}
	w := &commentCapturingWriter{}
	r := reconcilerFor(w, rdr)

	_, err := r.finishTriage(context.Background(), proj, task)
	require.NoError(t, err)

	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.DeployState)

	// Always post for human-filed issues.
	w.mu.Lock()
	posted := len(w.commentBodies)
	w.mu.Unlock()
	require.Equal(t, 1, posted,
		"human-filed issue with discuss must ALWAYS post the discuss comment")
}

// TestFinishTriage_HumanFiled_Discuss_BotHasLastWord_Suppresses reproduces the
// tatara-operator#74 loop: a HUMAN-filed issue where a human replied once long
// ago and the bot now has the last word must NOT receive yet another discuss
// comment.
func TestFinishTriage_HumanFiled_Discuss_BotHasLastWord_Suppresses(t *testing.T) {
	task, proj := seedDiscussSilenceTask(t, "lastword")

	old := time.Date(2026, 6, 16, 21, 30, 0, 0, time.UTC)
	newer := time.Date(2026, 6, 20, 5, 3, 0, 0, time.UTC)
	rdr := &discussSilenceReader{
		issueBody: "I want a new feature", // no marker -> human-filed
		comments: []scm.IssueComment{
			{Author: "szymonrychu", CreatedAt: old},  // stale human reply
			{Author: "tatara-bot", CreatedAt: newer}, // bot has the last word
		},
	}
	w := &commentCapturingWriter{}
	r := reconcilerFor(w, rdr)

	_, err := r.finishTriage(context.Background(), proj, task)
	require.NoError(t, err)

	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.DeployState)
	w.mu.Lock()
	posted := len(w.commentBodies)
	w.mu.Unlock()
	require.Zero(t, posted,
		"human-filed issue where the bot has the last word must NOT re-post a discuss comment; got: %v", w.commentBodies)
}

// TestFinishTriage_HumanFiled_Discuss_ListCommentsError_PostsComment verifies the
// botHasLastWord fail-open path: when ListIssueComments errors, the silence gate
// must POST the discuss comment rather than suppress it.
func TestFinishTriage_HumanFiled_Discuss_ListCommentsError_PostsComment(t *testing.T) {
	task, proj := seedDiscussSilenceTask(t, "listerr")

	rdr := &discussSilenceReader{
		issueBody: "I want a new feature", // no marker -> human-filed, authored clause skipped
		listErr:   errors.New("scm down"),
	}
	w := &commentCapturingWriter{}
	r := reconcilerFor(w, rdr)

	_, err := r.finishTriage(context.Background(), proj, task)
	require.NoError(t, err)

	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.DeployState)
	w.mu.Lock()
	posted := len(w.commentBodies)
	w.mu.Unlock()
	require.Equal(t, 1, posted,
		"ListIssueComments error must fail open and POST the discuss comment; got: %v", w.commentBodies)
}

func (w *commentCapturingWriter) EnsureLabel(_ context.Context, _, _, _, _ string) error { return nil }
