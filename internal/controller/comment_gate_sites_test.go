package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// botLastWordReader returns a fixed comment timeline for the gate to inspect.
// It implements only ListIssueComments (issue path); the isPR fallback in
// decideCommentGate also lands on ListIssueComments because it does not
// implement PRCommentLister.
type botLastWordReader struct {
	labelReader
	comments []scm.IssueComment
}

func (r *botLastWordReader) ListIssueComments(_ context.Context, _, _ string, _ int) ([]scm.IssueComment, error) {
	return r.comments, nil
}

// TestPostTerminalComment_BotLastWord_Suppressed is the #112/#126 repro: repeated
// terminal-diagnostics posts must stop once the bot already had the last word.
func TestPostTerminalComment_BotLastWord_Suppressed(t *testing.T) {
	_, task, _ := seedLabelTask(t, "term-supp", nil)
	rdr := &botLastWordReader{comments: []scm.IssueComment{
		{Author: "human", CreatedAt: time.Date(2026, 6, 28, 10, 0, 0, 0, time.UTC)},
		{Author: "tatara-bot", CreatedAt: time.Date(2026, 6, 28, 11, 0, 0, 0, time.UTC)},
	}}
	w := &commentCapturingWriter{}
	r := reconcilerFor(w, rdr)

	r.postTerminalComment(context.Background(), task, "Task run terminated (`Failed` / `PodLost`).")

	w.mu.Lock()
	defer w.mu.Unlock()
	require.Zero(t, len(w.commentBodies),
		"terminal diagnostics must be suppressed when the bot had the last word; got %v", w.commentBodies)
}

// TestPostTerminalComment_HumanReplied_Posts verifies the gate opens after a
// human (any non-bot, no approver list configured) replies.
func TestPostTerminalComment_HumanReplied_Posts(t *testing.T) {
	_, task, _ := seedLabelTask(t, "term-post", nil)
	rdr := &botLastWordReader{comments: []scm.IssueComment{
		{Author: "tatara-bot", CreatedAt: time.Date(2026, 6, 28, 10, 0, 0, 0, time.UTC)},
		{Author: "human", CreatedAt: time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)},
	}}
	w := &commentCapturingWriter{}
	r := reconcilerFor(w, rdr)

	r.postTerminalComment(context.Background(), task, "Task run terminated (`Failed` / `PodLost`).")

	w.mu.Lock()
	defer w.mu.Unlock()
	require.Equal(t, 1, len(w.commentBodies),
		"terminal diagnostics must post once when a human replied after the bot")
}

// reviewGateWriter records Approve calls and returns a controllable PR author.
type reviewGateWriter struct {
	labelWriter
	approveCount int
	prAuthor     string
}

func (w *reviewGateWriter) Approve(_ context.Context, _, _ string, _ int, _ string) error {
	w.approveCount++
	return nil
}

func (w *reviewGateWriter) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	return scm.PRState{Author: w.prAuthor}, nil
}

// TestWriteBackReview_BotLastWord_SuppressesApprove is the tatara-cli#77 repro:
// the bot must not re-post a review verdict on a human PR when it already had the
// last word. Approve must not fire and WritebackPending must be cleared so the
// task does not requeue and re-attempt.
func TestWriteBackReview_BotLastWord_SuppressesApprove(t *testing.T) {
	_, task, _ := seedLabelTask(t, "rev-supp", nil)
	ctx := context.Background()
	var fresh tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &fresh))
	fresh.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
	require.NoError(t, k8sClient.Status().Update(ctx, &fresh))
	task = getTaskByName(t, task.Name)

	rdr := &botLastWordReader{comments: []scm.IssueComment{
		{Author: "human", CreatedAt: time.Date(2026, 7, 7, 3, 0, 0, 0, time.UTC)},
		{Author: "tatara-bot", CreatedAt: time.Date(2026, 7, 7, 4, 42, 0, 0, time.UTC)},
	}}
	w := &reviewGateWriter{prAuthor: "human"}
	r := reconcilerFor(w, rdr)

	_, err := r.writeBackReview(ctx, task)
	require.NoError(t, err)
	require.Zero(t, w.approveCount,
		"review verdict must be suppressed when the bot had the last word on the PR")
}
