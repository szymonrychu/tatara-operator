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
		{"same-timestamp breaker favours posting", []scm.IssueComment{{Author: bot, CreatedAt: time.Unix(1_700_000_000, 0)}, {Author: "maintainer", CreatedAt: time.Unix(1_700_000_000, 0)}}, approvers, false},
		{"zero timestamp breaker ignored, bot silent", []scm.IssueComment{tc(bot, 1), {Author: "maintainer"}}, approvers, true},
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
		name     string
		fake     *gateFakeSCM
		provider string
		isPR     bool
		hint     string
		want     gateReason
	}{
		{"bot mr via hint (github)", &gateFakeSCM{}, "github", true, bot, gateBotMR},
		{"bot mr via GetPRState", &gateFakeSCM{prAuthor: bot}, "github", true, "", gateBotMR},
		{"gitlab hint ignored, GetPRState wins (human)", &gateFakeSCM{prAuthor: "human", comments: []scm.IssueComment{tc("human", 2)}}, "gitlab", true, bot, gateOpen},
		{"gitlab hint ignored, GetPRState says bot", &gateFakeSCM{prAuthor: bot}, "gitlab", true, "human", gateBotMR},
		{"human pr, bot last word", &gateFakeSCM{prAuthor: "human", comments: []scm.IssueComment{tc(bot, 2)}}, "github", true, "human", gateLastWord},
		{"issue, bot last word", &gateFakeSCM{comments: []scm.IssueComment{tc(bot, 2)}}, "github", false, "", gateLastWord},
		{"issue, human last word open", &gateFakeSCM{comments: []scm.IssueComment{tc(bot, 1), tc("human", 2)}}, "github", false, "", gateOpen},
		{"list error fails open", &gateFakeSCM{listErr: context.DeadlineExceeded}, "github", false, "", gateOpen},
		{"getprstate error falls to rule1", &gateFakeSCM{prErr: context.DeadlineExceeded, comments: []scm.IssueComment{tc(bot, 2)}}, "github", true, "", gateLastWord},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decideCommentGate(ctx, tt.fake, tt.fake, "o", "n", "https://github.com/o/n", "tok", tt.provider, 5, tt.isPR, bot, tt.hint, nil)
			if got != tt.want {
				t.Fatalf("decideCommentGate = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDecideCommentGate_FailOpenNilReader(t *testing.T) {
	if got := decideCommentGate(context.Background(), nil, nil, "o", "n", "u", "t", "github", 1, false, "bot", "", nil); got != gateOpen {
		t.Fatalf("nil reader must fail open, got %q", got)
	}
}

func TestPermitComment(t *testing.T) {
	const bot = "tatara-bot"
	botLast := []scm.IssueComment{tc("human", 5), tc(bot, 10)}
	humanLast := []scm.IssueComment{tc(bot, 5), tc("human", 10)}
	tests := []struct {
		name       string
		kind       string
		comments   []scm.IssueComment
		botLogin   string
		wantPermit bool
		wantReason string
	}{
		{"non-refine refused under bot last word", "implement", botLast, bot, false, "bot_last_word"},
		{"clarify cannot answer its own comment", "clarify", botLast, bot, false, "bot_last_word"},
		{"review refused under bot last word", "review", botLast, bot, false, "bot_last_word"},
		{"refine may answer bot last word", "refine", botLast, bot, true, ""},
		{"non-refine permitted when human has last word", "implement", humanLast, bot, true, ""},
		{"refine permitted when human has last word", "refine", humanLast, bot, true, ""},
		{"fail open on empty bot login", "implement", botLast, "", true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			permit, reason := PermitComment(tt.kind, tt.comments, tt.botLogin, nil)
			if permit != tt.wantPermit || reason != tt.wantReason {
				t.Fatalf("PermitComment(%q)=(%v,%q), want (%v,%q)", tt.kind, permit, reason, tt.wantPermit, tt.wantReason)
			}
		})
	}
}
