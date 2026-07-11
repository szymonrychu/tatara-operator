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

// TestParkWithComment_BotMR_Suppressed verifies rule 2: a park note on the bot's
// own MR is fully suppressed (label + Status only), while the task still parks.
func TestParkWithComment_BotMR_Suppressed(t *testing.T) {
	_, task, _ := seedLabelTask(t, "park-botmr", nil)
	ctx := context.Background()
	var fresh tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &fresh))
	fresh.Spec.Source.IsPR = true
	fresh.Spec.Source.AuthorLogin = "tatara-bot" // bot-authored PR -> rule 2
	require.NoError(t, k8sClient.Update(ctx, &fresh))
	task = getTaskByName(t, task.Name)

	w := &commentCapturingWriter{}
	r := reconcilerFor(w, &botLastWordReader{})

	require.NoError(t, r.parkWithComment(ctx, task, w, "tok", "deadline",
		"lifecycle: MRCI deadline reached for PR #7; parking."))

	w.mu.Lock()
	posted := len(w.commentBodies)
	w.mu.Unlock()
	require.Zero(t, posted,
		"a park note on the bot's own MR must be fully suppressed (rule 2); got %v", w.commentBodies)
	require.Equal(t, "Parked", getTaskByName(t, task.Name).Status.DeployState)
}
