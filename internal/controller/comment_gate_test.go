package controller

import (
	"context"
	"testing"
	"time"

	"github.com/szymonrychu/tatara-operator/internal/scm"
)

func tc(author string, minsAgo int) scm.IssueComment {
	return scm.IssueComment{Author: author, CreatedAt: time.Unix(1_700_000_000, 0).Add(time.Duration(minsAgo) * time.Minute)}
}

func TestBotHasLastWordAmong(t *testing.T) {
	const bot = "tatara-bot"
	approvers := []string{"maintainer"}
	tests := []struct {
		name     string
		comments []scm.IssueComment
		breakers []string
		want     bool
	}{
		{"no comments", nil, approvers, false},
		{"bot never spoke", []scm.IssueComment{tc("maintainer", 1)}, approvers, false},
		{"bot last, silent", []scm.IssueComment{tc("maintainer", 1), tc(bot, 2)}, approvers, true},
		{"approver after bot, open", []scm.IssueComment{tc(bot, 1), tc("maintainer", 2)}, approvers, false},
		{"third party after bot ignored, silent", []scm.IssueComment{tc(bot, 1), tc("random", 2)}, approvers, true},
		{"third party after bot with empty breakers, open", []scm.IssueComment{tc(bot, 1), tc("random", 2)}, nil, false},
		{"unordered slice, bot newest", []scm.IssueComment{tc(bot, 5), tc("maintainer", 2), tc("random", 4)}, approvers, true},
		{"empty author skipped", []scm.IssueComment{tc(bot, 1), tc("", 2)}, approvers, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := botHasLastWordAmong(tt.comments, bot, tt.breakers); got != tt.want {
				t.Fatalf("botHasLastWordAmong = %v, want %v", got, tt.want)
			}
		})
	}
}

// gateFakeSCM implements the reader+writer methods decideCommentGate touches.
// The embedded nil interfaces satisfy the full SCMReader/SCMWriter signatures;
// only ListIssueComments and GetPRState are overridden (the only methods the
// gate calls). *gateFakeSCM does not implement PRCommentLister, so the isPR path
// falls back to ListIssueComments.
type gateFakeSCM struct {
	scm.SCMReader
	scm.SCMWriter
	comments []scm.IssueComment
	listErr  error
	prAuthor string
	prErr    error
}

func (f *gateFakeSCM) ListIssueComments(context.Context, string, string, int) ([]scm.IssueComment, error) {
	return f.comments, f.listErr
}

func (f *gateFakeSCM) GetPRState(context.Context, string, string, int) (scm.PRState, error) {
	return scm.PRState{Author: f.prAuthor}, f.prErr
}

func TestDecideCommentGate(t *testing.T) {
	const bot = "tatara-bot"
	ctx := context.Background()
	tests := []struct {
		name string
		fake *gateFakeSCM
		isPR bool
		hint string
		want gateReason
	}{
		{"bot mr via hint", &gateFakeSCM{}, true, bot, gateBotMR},
		{"bot mr via GetPRState", &gateFakeSCM{prAuthor: bot}, true, "", gateBotMR},
		{"human pr, bot last word", &gateFakeSCM{prAuthor: "human", comments: []scm.IssueComment{tc(bot, 2)}}, true, "human", gateLastWord},
		{"issue, bot last word", &gateFakeSCM{comments: []scm.IssueComment{tc(bot, 2)}}, false, "", gateLastWord},
		{"issue, human last word open", &gateFakeSCM{comments: []scm.IssueComment{tc(bot, 1), tc("human", 2)}}, false, "", gateOpen},
		{"list error fails open", &gateFakeSCM{listErr: context.DeadlineExceeded}, false, "", gateOpen},
		{"getprstate error falls to rule1", &gateFakeSCM{prErr: context.DeadlineExceeded, comments: []scm.IssueComment{tc(bot, 2)}}, true, "", gateLastWord},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decideCommentGate(ctx, tt.fake, tt.fake, "o", "n", "https://github.com/o/n", "tok", 5, tt.isPR, bot, tt.hint, nil)
			if got != tt.want {
				t.Fatalf("decideCommentGate = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDecideCommentGate_FailOpenNilReader(t *testing.T) {
	if got := decideCommentGate(context.Background(), nil, nil, "o", "n", "u", "t", 1, false, "bot", "", nil); got != gateOpen {
		t.Fatalf("nil reader must fail open, got %q", got)
	}
}
