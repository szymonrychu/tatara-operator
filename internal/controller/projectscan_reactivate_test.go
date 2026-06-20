package controller

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// reactReader is a minimal SCMReader stub returning canned comments (or an error)
// for the reactivation author check.
type reactReader struct {
	scm.SCMReader
	comments []scm.IssueComment
	err      error
}

func (r *reactReader) ListIssueComments(context.Context, string, string, int) ([]scm.IssueComment, error) {
	return r.comments, r.err
}

// convTask builds an in-memory Task with the source-repo/number dedup labels and
// a Conversation/Stopped lifecycle state. findConvTaskToReactivate is pure (reads
// the slice only), so no k8s client is needed.
func convTask(repoSlug string, num int, state string, lastAct time.Time) tatarav1alpha1.Task {
	la := metav1.NewTime(lastAct)
	return tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				labelSourceRepo:   sanitizeRepoLabel(repoSlug),
				labelSourceNumber: strconv.Itoa(num),
			},
		},
		Status: tatarav1alpha1.TaskStatus{
			LifecycleState: state,
			LastActivityAt: &la,
		},
	}
}

func TestFindConvTaskToReactivate_AuthorAware(t *testing.T) {
	base := time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC)
	bot := "szymonrychu-bot"
	cand := candidate{repo: "o/r", number: 7, updatedAt: base.Add(time.Hour)}

	cases := []struct {
		name     string
		state    string
		lastAct  time.Time
		comments []scm.IssueComment
		readErr  error
		want     bool // true => reactivated (non-nil)
	}{
		{
			name:    "bot-only comment after lastActivity -> NOT reactivated",
			state:   "Conversation",
			lastAct: base,
			comments: []scm.IssueComment{
				{Author: bot, CreatedAt: base.Add(30 * time.Minute)},
			},
			want: false,
		},
		{
			name:    "human comment after lastActivity -> reactivated",
			state:   "Conversation",
			lastAct: base,
			comments: []scm.IssueComment{
				{Author: bot, CreatedAt: base.Add(10 * time.Minute)},
				{Author: "szymon", CreatedAt: base.Add(40 * time.Minute)},
			},
			want: true,
		},
		{
			name:    "human comment but BEFORE lastActivity -> NOT reactivated",
			state:   "Conversation",
			lastAct: base.Add(50 * time.Minute),
			comments: []scm.IssueComment{
				{Author: "szymon", CreatedAt: base.Add(40 * time.Minute)},
			},
			want: false,
		},
		{
			name:    "ListIssueComments error -> reactivated (fail-open)",
			state:   "Conversation",
			lastAct: base,
			readErr: errors.New("boom"),
			want:    true,
		},
		{
			name:    "Stopped state, human comment after -> reactivated",
			state:   "Stopped",
			lastAct: base,
			comments: []scm.IssueComment{
				{Author: "szymon", CreatedAt: base.Add(40 * time.Minute)},
			},
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			existing := []tatarav1alpha1.Task{convTask("o/r", 7, tc.state, tc.lastAct)}
			rdr := &reactReader{comments: tc.comments, err: tc.readErr}
			got := findConvTaskToReactivate(context.Background(), cand, existing, rdr, bot)
			if (got != nil) != tc.want {
				t.Fatalf("reactivated=%v, want %v", got != nil, tc.want)
			}
		})
	}
}
