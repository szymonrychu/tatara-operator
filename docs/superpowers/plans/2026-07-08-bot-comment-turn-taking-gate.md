# Bot comment turn-taking gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop tatara re-commenting on its own issues (turn-taking gate) and MRs (bot-MR silence) by routing every controller comment egress through one gate.

**Architecture:** A new `internal/controller/comment_gate.go` holds a pure turn-taking decision (`botHasLastWordAmong`), a bot-MR author resolver (`resolveBotMR`), and a `decideCommentGate` that combines them. Thin `TaskReconciler` methods (`commentGateReason`, `gatedComment`) wrap it. The ~13 direct `writer.Comment` / `writer.Approve` / `writer.RequestChanges` sites are rerouted through the wrappers. Fail-open on every read error so missed-webhook recovery is preserved.

**Tech Stack:** Go (stdlib `log/slog`, `slices`, `time`), controller-runtime, existing `internal/scm` interfaces.

## Global Constraints

- Newest stable Go; build/test via `mise exec -- go test ./...` and `mise exec -- golangci-lint run`.
- JSON logs via `log/slog`; log every suppression at INFO with `action`, `reason`, `resource_id`, `repo`, `number`.
- Metric on every suppression: `Metrics.SCMWrite(provider, "comment", "suppressed_last_word" | "suppressed_bot_mr")`.
- Fail-open: empty `botLogin`, nil reader, unsplittable repo, or any SCM read error -> post proceeds (`gateOpen`). Matches `botHadLastWord` / `humanCommentAfter`.
- Table-driven tests with `t.Run`. Wrap errors `fmt.Errorf("context: %w", err)`.
- KISS. No must-post allowlist. REST `commentOnIssue` (handlers.go:677) is unchanged.
- Silence-breaker set = `union(EffectiveReporterLogins(proj,repo), EffectiveMaintainerLogins(proj,repo))`; empty set => any non-bot breaks silence.

Reference signatures (already in the repo):
- `scmContext(ctx, task) (v1alpha1.Project, v1alpha1.Repository, scm.SCMWriter, token string, provider string, err error)` — `writeback.go:742`
- `r.ReaderFor(provider, token) (scm.SCMReader, error)`; `r.SCMFor(provider) (scm.SCMWriter, error)` — `task_controller.go:75,79`
- `scm.SCMWriter.GetPRState(ctx, repoURL, token, number) (scm.PRState, error)`; `scm.PRState.Author`
- `scm.SCMReader.ListIssueComments(ctx, owner, name, number) ([]scm.IssueComment, error)`; optional `scm.PRCommentLister.ListPRComments(ctx, owner, name, number)`
- `scm.IssueComment{ Author, Body string; CreatedAt time.Time }`
- `scm.OwnerRepo(repoURL) (owner, name string, err error)`
- `v1alpha1.EffectiveReporterLogins(proj, repo) []string`; `v1alpha1.EffectiveMaintainerLogins(proj, repo) []string`
- `lifecyclePR(task) (number int, prURL string)`; `task.Spec.Source.{IssueRef, Number, IsPR, AuthorLogin, URL}`

---

### Task 1: Core turn-taking primitives

**Files:**
- Create: `internal/controller/comment_gate.go`
- Test: `internal/controller/comment_gate_test.go`

**Interfaces:**
- Produces:
  - `type gateReason string` with `gateOpen gateReason = ""`, `gateBotMR gateReason = "bot_mr"`, `gateLastWord gateReason = "last_word"`
  - `func commentSilenceBreakers(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository) []string`
  - `func botHasLastWordAmong(comments []scm.IssueComment, botLogin string, breakers []string) bool`

- [ ] **Step 1: Write the failing test**

```go
package controller

import (
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && mise exec -- go test ./internal/controller/ -run TestBotHasLastWordAmong`
Expected: FAIL (`undefined: botHasLastWordAmong`).

- [ ] **Step 3: Write minimal implementation**

```go
package controller

import (
	"slices"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

type gateReason string

const (
	gateOpen     gateReason = ""
	gateBotMR    gateReason = "bot_mr"
	gateLastWord gateReason = "last_word"
)

// commentSilenceBreakers returns the deduped set of logins whose comment breaks
// the bot's silence: the reporter intake allowlist unioned with the
// maintainer/approver allowlist for this repo. An empty result means no lists are
// configured, in which case any non-bot author breaks silence.
func commentSilenceBreakers(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository) []string {
	seen := map[string]bool{}
	var out []string
	for _, l := range tatarav1alpha1.EffectiveReporterLogins(proj, repo) {
		if l != "" && !seen[l] {
			seen[l] = true
			out = append(out, l)
		}
	}
	for _, l := range tatarav1alpha1.EffectiveMaintainerLogins(proj, repo) {
		if l != "" && !seen[l] {
			seen[l] = true
			out = append(out, l)
		}
	}
	return out
}

// botHasLastWordAmong reports whether the bot must stay silent: it has a comment
// and no silence-breaker has commented since. A comment breaks silence iff its
// author is non-empty, not the bot, and (breakers empty OR author in breakers).
// Order is by CreatedAt, robust to SCM list ordering.
func botHasLastWordAmong(comments []scm.IssueComment, botLogin string, breakers []string) bool {
	var tBot, tBreak int64 = -1, -1
	for _, c := range comments {
		ts := c.CreatedAt.UnixNano()
		switch {
		case c.Author == botLogin:
			if ts > tBot {
				tBot = ts
			}
		case c.Author != "" && (len(breakers) == 0 || slices.Contains(breakers, c.Author)):
			if ts > tBreak {
				tBreak = ts
			}
		}
	}
	if tBot < 0 {
		return false
	}
	return tBreak <= tBot
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && mise exec -- go test ./internal/controller/ -run TestBotHasLastWordAmong`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/controller/comment_gate.go internal/controller/comment_gate_test.go
git commit -m "feat: turn-taking primitives for bot comment gate"
```

---

### Task 2: `decideCommentGate` + `resolveBotMR`

**Files:**
- Modify: `internal/controller/comment_gate.go`
- Test: `internal/controller/comment_gate_test.go`

**Interfaces:**
- Consumes: `gateReason`, `botHasLastWordAmong` (Task 1)
- Produces:
  - `func resolveBotMR(ctx context.Context, writer scm.SCMWriter, repoURL, token string, number int, botLogin, hint string) bool`
  - `func decideCommentGate(ctx context.Context, reader scm.SCMReader, writer scm.SCMWriter, owner, name, repoURL, token string, number int, isPR bool, botLogin, authorHint string, breakers []string) gateReason`

- [ ] **Step 1: Write the failing test**

Add to `comment_gate_test.go`:

```go
import "context"

// gateFakeSCM implements the reader+writer methods decideCommentGate touches.
type gateFakeSCM struct {
	scm.SCMReader
	scm.SCMWriter
	comments   []scm.IssueComment
	listErr    error
	prAuthor   string
	prErr      error
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
		name    string
		fake    *gateFakeSCM
		isPR    bool
		hint    string
		want    gateReason
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && mise exec -- go test ./internal/controller/ -run TestDecideCommentGate`
Expected: FAIL (`undefined: decideCommentGate`).

- [ ] **Step 3: Write minimal implementation**

Append to `comment_gate.go` (add `context` to imports):

```go
// resolveBotMR reports whether the PR/MR at number is authored by the bot.
// Prefers the pre-known hint (TaskSource.AuthorLogin); else reads GetPRState.
// A read error resolves to false (fall through to the rule-1 turn-taking gate).
func resolveBotMR(ctx context.Context, writer scm.SCMWriter, repoURL, token string, number int, botLogin, hint string) bool {
	if hint != "" {
		return hint == botLogin
	}
	if writer == nil {
		return false
	}
	st, err := writer.GetPRState(ctx, repoURL, token, number)
	if err != nil {
		return false
	}
	return st.Author == botLogin
}

// decideCommentGate reports whether a bot comment must be withheld and why.
// Rule 2 (bot MR) short-circuits before any comment listing. Rule 1 lists the
// conversation and applies botHasLastWordAmong. Fail-open (gateOpen) on missing
// inputs or read errors.
func decideCommentGate(ctx context.Context, reader scm.SCMReader, writer scm.SCMWriter, owner, name, repoURL, token string, number int, isPR bool, botLogin, authorHint string, breakers []string) gateReason {
	if botLogin == "" || reader == nil || owner == "" {
		return gateOpen
	}
	if isPR && resolveBotMR(ctx, writer, repoURL, token, number, botLogin, authorHint) {
		return gateBotMR
	}
	var (
		comments []scm.IssueComment
		err      error
	)
	if isPR {
		if pl, ok := reader.(scm.PRCommentLister); ok {
			comments, err = pl.ListPRComments(ctx, owner, name, number)
		} else {
			comments, err = reader.ListIssueComments(ctx, owner, name, number)
		}
	} else {
		comments, err = reader.ListIssueComments(ctx, owner, name, number)
	}
	if err != nil {
		return gateOpen
	}
	if botHasLastWordAmong(comments, botLogin, breakers) {
		return gateLastWord
	}
	return gateOpen
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && mise exec -- go test ./internal/controller/ -run TestDecideCommentGate`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/controller/comment_gate.go internal/controller/comment_gate_test.go
git commit -m "feat: decideCommentGate combining bot-MR + turn-taking rules"
```

---

### Task 3: Reconciler wrappers `commentGateReason` + `gatedComment`

**Files:**
- Modify: `internal/controller/comment_gate.go`
- Test: `internal/controller/comment_gate_wrapper_test.go`

**Interfaces:**
- Consumes: `decideCommentGate`, `commentSilenceBreakers`, `gateReason`
- Produces (methods on `*TaskReconciler`):
  - `func (r *TaskReconciler) commentGateReason(ctx, proj *v1alpha1.Project, repo *v1alpha1.Repository, writer scm.SCMWriter, token, provider string, number int, isPR bool, authorHint string) gateReason`
  - `func (r *TaskReconciler) gatedComment(ctx, proj *v1alpha1.Project, repo *v1alpha1.Repository, writer scm.SCMWriter, token, provider string, number int, isPR bool, authorHint, ref, body string) (posted bool, err error)`

- [ ] **Step 1: Write the failing test**

Create `internal/controller/comment_gate_wrapper_test.go`. Reuse the existing `gateFakeSCM` (same package). Provide `ReaderFor` returning it, a Comment recorder, and a metrics stub. Check the repo for the existing metrics fake name first: `rg -n "func.*SCMWrite" internal/controller -g '*_test.go'` and reuse it; if none, the test may pass `Metrics` from `obs.NewOperatorMetrics(prometheus.NewRegistry())`.

```go
package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/prometheus/client_golang/prometheus"
)

type recordingGateSCM struct {
	*gateFakeSCM
	posted []string
}

func (f *recordingGateSCM) Comment(_ context.Context, _, ref, body string) error {
	f.posted = append(f.posted, ref+"|"+body)
	return nil
}

func newGateTaskReconciler(fake *recordingGateSCM) *TaskReconciler {
	return &TaskReconciler{
		SCMFor:    func(string) (scm.SCMWriter, error) { return fake, nil },
		ReaderFor: func(string, string) (scm.SCMReader, error) { return fake, nil },
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}
}

func gateProjRepo(bot string) (*tatarav1alpha1.Project, *tatarav1alpha1.Repository) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{Provider: "github", BotLogin: bot}
	repo := &tatarav1alpha1.Repository{}
	repo.Spec.URL = "https://github.com/o/n"
	return proj, repo
}

func TestGatedComment_SuppressesLastWord(t *testing.T) {
	const bot = "tatara-bot"
	fake := &recordingGateSCM{gateFakeSCM: &gateFakeSCM{comments: []scm.IssueComment{tc(bot, 2)}}}
	proj, repo := gateProjRepo(bot)
	r := newGateTaskReconciler(fake)
	posted, err := r.gatedComment(context.Background(), proj, repo, fake, "tok", "github", 7, false, "", "o/n#7", "hello")
	if err != nil || posted {
		t.Fatalf("want (false,nil), got (%v,%v)", posted, err)
	}
	if len(fake.posted) != 0 {
		t.Fatalf("suppressed post must not call Comment, got %v", fake.posted)
	}
}

func TestGatedComment_PostsWhenOpen(t *testing.T) {
	const bot = "tatara-bot"
	fake := &recordingGateSCM{gateFakeSCM: &gateFakeSCM{comments: []scm.IssueComment{tc(bot, 1), tc("human", 2)}}}
	proj, repo := gateProjRepo(bot)
	proj.Spec.Scm.MaintainerLogins = []string{"human"}
	r := newGateTaskReconciler(fake)
	posted, err := r.gatedComment(context.Background(), proj, repo, fake, "tok", "github", 7, false, "", "o/n#7", "hello")
	if err != nil || !posted {
		t.Fatalf("want (true,nil), got (%v,%v)", posted, err)
	}
	if len(fake.posted) != 1 {
		t.Fatalf("open gate must post exactly once, got %v", fake.posted)
	}
}

func TestGatedComment_SuppressesBotMR(t *testing.T) {
	const bot = "tatara-bot"
	fake := &recordingGateSCM{gateFakeSCM: &gateFakeSCM{prAuthor: bot}}
	proj, repo := gateProjRepo(bot)
	r := newGateTaskReconciler(fake)
	posted, _ := r.gatedComment(context.Background(), proj, repo, fake, "tok", "github", 7, true, "", "o/n#7", "park note")
	if posted || len(fake.posted) != 0 {
		t.Fatalf("bot MR must be silent, got posted=%v calls=%v", posted, fake.posted)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && mise exec -- go test ./internal/controller/ -run TestGatedComment`
Expected: FAIL (`r.gatedComment undefined`). If it fails to compile on `obs.NewOperatorMetrics` or `ScmSpec` field names, fix the constructor/field names to the repo's actual symbols (grep them) before proceeding.

- [ ] **Step 3: Write minimal implementation**

Append to `comment_gate.go` (add `github.com/szymonrychu/tatara-operator/internal/scm` and `sigs.k8s.io/controller-runtime/pkg/log` if not present):

```go
// commentGateReason resolves the reader + silence-breakers for the task's
// project/repo and returns the gate decision for a bot comment on (number,isPR).
// Fail-open (gateOpen) when ReaderFor is nil, the reader cannot be built, or the
// repo URL is unsplittable.
func (r *TaskReconciler) commentGateReason(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, writer scm.SCMWriter, token, provider string, number int, isPR bool, authorHint string) gateReason {
	botLogin := ""
	if proj != nil && proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	if botLogin == "" || r.ReaderFor == nil || repo == nil {
		return gateOpen
	}
	owner, name, err := scm.OwnerRepo(repo.Spec.URL)
	if err != nil {
		return gateOpen
	}
	reader, err := r.ReaderFor(provider, token)
	if err != nil {
		return gateOpen
	}
	breakers := commentSilenceBreakers(proj, repo)
	return decideCommentGate(ctx, reader, writer, owner, name, repo.Spec.URL, token, number, isPR, botLogin, authorHint, breakers)
}

// gatedComment posts body to ref via writer.Comment unless commentGateReason
// withholds it. A withheld post is (false, nil) and records
// SCMWrite(provider,"comment","suppressed_<reason>"). ref is the SCM ref the
// caller already builds (provider sigils preserved).
func (r *TaskReconciler) gatedComment(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, writer scm.SCMWriter, token, provider string, number int, isPR bool, authorHint, ref, body string) (bool, error) {
	l := log.FromContext(ctx)
	reason := r.commentGateReason(ctx, proj, repo, writer, token, provider, number, isPR, authorHint)
	if reason != gateOpen {
		if r.Metrics != nil {
			r.Metrics.SCMWrite(provider, "comment", "suppressed_"+string(reason))
		}
		l.Info("scm comment suppressed",
			"action", "scm_comment_suppressed", "reason", string(reason), "ref", ref)
		return false, nil
	}
	err := writer.Comment(ctx, token, ref, body)
	r.recordSCM(provider, "comment", err)
	return err == nil, err
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && mise exec -- go test ./internal/controller/ -run TestGatedComment`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/controller/comment_gate.go internal/controller/comment_gate_wrapper_test.go
git commit -m "feat: TaskReconciler gatedComment + commentGateReason wrappers"
```

---

### Task 4: Route alert re-fire (#243) through the gate

**Files:**
- Modify: `internal/controller/writeback_proposal.go:135`
- Test: `internal/controller/writeback_proposal_refire_test.go`

**Interfaces:**
- Consumes: `decideCommentGate`, `commentSilenceBreakers` (target is an issue, no `TaskReconciler` receiver in scope here — this is a `ProjectReconciler` path; call the free `decideCommentGate` directly with the reader already built at `writeback_proposal.go:112`).

- [ ] **Step 1: Write the failing test**

Add a test that drives the incident-dedup branch with an existing tracked issue whose newest comment is the bot's, and asserts no re-fire comment is posted. Reuse the project's existing proposal test harness (`rg -n "recordExistingProposal\|findOpenIssueByLabel\|ProposedIssue" internal/controller -g '*_test.go'` to find the closest existing test and copy its Project/Task/fake wiring). The assertion:

```go
// bot authored the last comment on the tracked issue -> re-fire note suppressed.
if len(writer.commentBodies) != 0 {
	t.Fatalf("alert re-fire must be gated when bot had last word, got %v", writer.commentBodies)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && mise exec -- go test ./internal/controller/ -run Refire`
Expected: FAIL (re-fire comment posted).

- [ ] **Step 3: Write minimal implementation**

Replace the post at `writeback_proposal.go:135`. Current:

```go
issueRef := fmt.Sprintf("%s#%d", existing.Repo, existing.Number)
cerr := writer.Comment(ctx, token, issueRef, alertGroupRefireComment(task.Spec.ProposedIssue.AlertGroup))
r.recordSCM(proj.Spec.Scm.Provider, "comment", cerr)
if cerr != nil {
	l.Error(cerr, "proposal: alert-group re-fire comment (non-fatal)", "issue_ref", issueRef)
}
```

New (this file's receiver is `*ProjectReconciler`; `reader` from the enclosing dedup block, `botLogin`, `breakers` resolved here):

```go
issueRef := fmt.Sprintf("%s#%d", existing.Repo, existing.Number)
botLogin := proj.Spec.Scm.BotLogin
owner, name, _ := strings.Cut(existing.Repo, "/")
breakers := commentSilenceBreakers(proj, &repo)
if decideCommentGate(ctx, reader, nil, owner, name, repo.Spec.URL, token, existing.Number, false, botLogin, "", breakers) != gateOpen {
	r.Metrics.SCMWrite(proj.Spec.Scm.Provider, "comment", "suppressed_last_word")
	l.Info("proposal: alert re-fire note suppressed, bot had last word",
		"action", "scm_comment_suppressed", "issue_ref", issueRef)
} else {
	cerr := writer.Comment(ctx, token, issueRef, alertGroupRefireComment(task.Spec.ProposedIssue.AlertGroup))
	r.recordSCM(proj.Spec.Scm.Provider, "comment", cerr)
	if cerr != nil {
		l.Error(cerr, "proposal: alert-group re-fire comment (non-fatal)", "issue_ref", issueRef)
	}
}
```

Confirm `strings` is imported. `reader` is in scope (the `if r.ReaderFor != nil` block builds it as `reader` at `:112`); if it is a different local name, use that.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && mise exec -- go test ./internal/controller/ -run Refire`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/controller/writeback_proposal.go internal/controller/writeback_proposal_refire_test.go
git commit -m "fix: gate alert re-fire comment on bot-last-word (#243)"
```

---

### Task 5: Route terminal-diagnostics (#112/#126) through the gate

**Files:**
- Modify: `internal/controller/task_controller.go:558-577` (`postTerminalComment`)
- Test: `internal/controller/terminal_diag_gate_test.go`

**Interfaces:**
- Consumes: `TaskReconciler.gatedComment` (Task 3)

- [ ] **Step 1: Write the failing test**

Drive `postTerminalComment` (or its caller `commentTerminalDiagnostics`) with a fake where the linked issue's newest comment is the bot's, assert no post. Reuse fakes from `terminal_diag_test.go` (already imports the string). Grep it for the existing writer/reader wiring and extend with a bot-last-word comment list.

```go
if len(writer.commentBodies) != 0 {
	t.Fatalf("terminal diagnostics must be gated when bot had last word, got %v", writer.commentBodies)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && mise exec -- go test ./internal/controller/ -run TerminalDiag`
Expected: FAIL (comment posted).

- [ ] **Step 3: Write minimal implementation**

`postTerminalComment` currently (task_controller.go:558):

```go
func (r *TaskReconciler) postTerminalComment(ctx context.Context, task *tatarav1alpha1.Task, body string) {
	if r.SCMFor == nil || task.Spec.Source == nil || task.Spec.Source.IssueRef == "" {
		return
	}
	l := log.FromContext(ctx)
	_, _, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		l.Error(err, "terminal-diagnostics: scm context for comment (non-fatal)", "resource_id", task.Name)
		return
	}
	cerr := writer.Comment(ctx, token, task.Spec.Source.IssueRef, body)
	r.recordSCM(provider, "comment", cerr)
	if cerr != nil { ... }
	l.Info("terminal diagnostics commented on issue", ...)
}
```

Replace the `writer.Comment` block. `scmContext` returns `proj, repo` (currently discarded) — bind them:

```go
	proj, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		l.Error(err, "terminal-diagnostics: scm context for comment (non-fatal)", "resource_id", task.Name)
		return
	}
	posted, cerr := r.gatedComment(ctx, &proj, &repo, writer, token, provider,
		task.Spec.Source.Number, task.Spec.Source.IsPR, task.Spec.Source.AuthorLogin,
		task.Spec.Source.IssueRef, body)
	if cerr != nil {
		l.Error(cerr, "terminal-diagnostics: post comment (non-fatal)",
			"resource_id", task.Name, "issue_ref", task.Spec.Source.IssueRef)
		return
	}
	if posted {
		l.Info("terminal diagnostics commented on issue",
			"action", "task_terminal_commented", "resource_id", task.Name, "issue_ref", task.Spec.Source.IssueRef)
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && mise exec -- go test ./internal/controller/ -run TerminalDiag`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/controller/task_controller.go internal/controller/terminal_diag_gate_test.go
git commit -m "fix: gate terminal-diagnostics comment on bot-last-word (#112/#126)"
```

---

### Task 6: Route review writeback (#77 re-review) through the gate

**Files:**
- Modify: `internal/controller/writeback_review.go` (`writeBackReview`)
- Test: `internal/controller/writeback_review_gate_test.go`

**Interfaces:**
- Consumes: `TaskReconciler.commentGateReason` (Task 3). Review verbs (approve/request_changes/comment) all target the same PR; gate once up front and, when suppressed, clear WritebackPending so the task finishes instead of requeuing.

- [ ] **Step 1: Write the failing test**

Drive `writeBackReview` with a verdict (decision `approve`) on a PR whose newest comment is the bot's (bot had last word), assert `writer.Approve` is NOT called and WritebackPending is cleared. Reuse `writeback_review` / `task_writeback_test.go` fakes (which already fake `Approve` and `GetPRState`).

```go
if writer.approveCount != 0 {
	t.Fatalf("re-review must be suppressed when bot had last word, approves=%d", writer.approveCount)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && mise exec -- go test ./internal/controller/ -run ReviewGate`
Expected: FAIL (approve called).

- [ ] **Step 3: Write minimal implementation**

In `writeBackReview`, after resolving `scmContext` (`_, repo, writer, token, provider, err`) and `number`, insert the gate before the `switch v.Decision`:

```go
	number := task.Spec.Source.Number
	// Review PRs are human-authored (bot PRs route to issueLifecycle), so the
	// gate here enforces rule 1 turn-taking: do not re-post a verdict when the
	// bot already had the last word and no reporter/approver has replied.
	if r.commentGateReason(ctx, &project, &repo, writer, token, provider, number, true, task.Spec.Source.AuthorLogin) != gateOpen {
		if r.Metrics != nil {
			r.Metrics.SCMWrite(provider, "comment", "suppressed_last_word")
		}
		l.Info("review verdict suppressed: bot had last word on the PR",
			"action", "scm_comment_suppressed", "resource_id", task.Name, "decision", v.Decision)
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "Reviewed", "review suppressed: bot had last word")
	}
```

Note: `writeBackReview` currently discards the project from `scmContext` (`_, repo, ...`). Bind it as `project` for the gate call. Confirm the local names via the file head.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && mise exec -- go test ./internal/controller/ -run ReviewGate`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/controller/writeback_review.go internal/controller/writeback_review_gate_test.go
git commit -m "fix: gate review-verdict writeback on bot-last-word (cli#77 re-review)"
```

---

### Task 7: Route park comments (rule 2 bot-MR silence) through the gate

**Files:**
- Modify: `internal/controller/lifecycle.go:150-193` (`parkWithComment`)
- Test: `internal/controller/lifecycle_park_gate_test.go`

**Interfaces:**
- Consumes: `TaskReconciler.gatedComment` (Task 3). Bot-authored PR -> `gateBotMR` -> silent park (label + Status only). Issue / human PR -> rule 1.

- [ ] **Step 1: Write the failing test**

Drive `parkWithComment` with a bot-authored PR task (`Source.IsPR=true`, `Source.AuthorLogin=bot`), assert no comment posted but LifecycleState becomes `Parked`. Reuse `lifecycle_park_gitlab_test.go` / `lifecycle_mrci_test.go` wiring (they already exercise park + GetPRState).

```go
if len(writer.commentBodies) != 0 {
	t.Fatalf("bot MR park must be silent, got %v", writer.commentBodies)
}
// and assert task Parked via the fake client / returned state
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && mise exec -- go test ./internal/controller/ -run ParkGate`
Expected: FAIL (park comment posted).

- [ ] **Step 3: Write minimal implementation**

`parkWithComment` ends with (lifecycle.go:185):

```go
		if commentRef != "" {
			cerr := writer.Comment(ctx, token, commentRef, msg)
			r.recordSCM(provider, "comment", cerr)
			if cerr != nil {
				l.Error(cerr, "lifecycle: park comment (non-fatal)", "resource_id", task.Name)
			}
		}
	}
	return r.setLifecycleState(ctx, task, "Parked", reason)
```

`parkWithComment` needs `proj`+`repo` for the gate. It has `task` and builds `provider`/`token`. Resolve project+repo once via `scmContext` at the top of the comment block (or thread them in). Minimal change: fetch them for the gate:

```go
		if commentRef != "" {
			proj, repo, _, _, _, scErr := r.scmContext(ctx, task)
			if scErr != nil {
				// fail-open: post ungated rather than lose the note
				cerr := writer.Comment(ctx, token, commentRef, msg)
				r.recordSCM(provider, "comment", cerr)
				if cerr != nil {
					l.Error(cerr, "lifecycle: park comment (non-fatal)", "resource_id", task.Name)
				}
			} else {
				number, _ := lifecyclePR(task)
				if _, cerr := r.gatedComment(ctx, &proj, &repo, writer, token, provider,
					number, task.Spec.Source.IsPR, task.Spec.Source.AuthorLogin, commentRef, msg); cerr != nil {
					l.Error(cerr, "lifecycle: park comment (non-fatal)", "resource_id", task.Name)
				}
			}
		}
	}
	return r.setLifecycleState(ctx, task, "Parked", reason)
```

For non-PR park tasks `lifecyclePR` returns 0; `gatedComment` with isPR=false uses `task.Spec.Source.Number` semantics via the issue path — pass `task.Spec.Source.Number` when `!IsPR`. Adjust: `number := task.Spec.Source.Number; if task.Spec.Source.IsPR { number, _ = lifecyclePR(task) }`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && mise exec -- go test ./internal/controller/ -run ParkGate`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/controller/lifecycle.go internal/controller/lifecycle_park_gate_test.go
git commit -m "fix: silence park comments on bot-authored MRs (rule 2)"
```

---

### Task 8: Route remaining issue-comment sites through the gate

**Files:**
- Modify: `internal/controller/writeback.go:339,360,437`; `internal/controller/lifecycle.go:504`; `internal/controller/lifecycle.go:846` (`triagePostComment`); `internal/controller/lifecycle_implement.go:90,278`
- Test: `internal/controller/comment_gate_sites_test.go`

**Interfaces:**
- Consumes: `TaskReconciler.gatedComment` (Task 3). Each site targets `task.Spec.Source.IssueRef` (an issue): `number=task.Spec.Source.Number`, `isPR=task.Spec.Source.IsPR`, `authorHint=task.Spec.Source.AuthorLogin`.

For each site, replace `cerr := writer.Comment(ctx, token, task.Spec.Source.IssueRef, BODY)` + `r.recordSCM(provider,"comment",cerr)` with:

```go
_, cerr := r.gatedComment(ctx, &proj, &repo, writer, token, provider,
	task.Spec.Source.Number, task.Spec.Source.IsPR, task.Spec.Source.AuthorLogin,
	task.Spec.Source.IssueRef, BODY)
```

binding `proj, repo` from the `scmContext` call each function already makes (several currently discard them with `_`). `lifecycle_implement.go:278` uses `fresh.Spec.Source.*` — use `fresh` there. `triagePostComment` (lifecycle.go:827) already fetches writer/token; add a `scmContext` resolve for proj+repo or thread them from the caller. Where a site has no cheap proj/repo access, resolve via `r.scmContext(ctx, task)` (fail-open on error, posting ungated).

- [ ] **Step 1: Write the failing test**

One table-driven test invoking each modified function with a bot-last-word issue and asserting suppression, plus one open case asserting a single post. Reuse existing per-function test harnesses (`task_writeback_test.go`, `lifecycle_implement_test.go`, `lifecycle_test.go`).

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && mise exec -- go test ./internal/controller/ -run GateSites`
Expected: FAIL.

- [ ] **Step 3: Apply the replacements above at each of the 7 sites.**

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && mise exec -- go test ./internal/controller/ -run GateSites`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "fix: route remaining issue-comment egress through the bot-last-word gate"
```

---

### Task 9: Full verification + regression guard

**Files:**
- Test: whole `internal/controller` package

- [ ] **Step 1: Full package test + race**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && mise exec -- go test ./... 2>&1 | tail -30`
Expected: all PASS. Fix any test that asserted the OLD always-post behaviour (search: `rg -n "commentBodies|Comment called" internal/controller -g '*_test.go'` and update expectations where a bot-last-word fixture now correctly suppresses).

- [ ] **Step 2: Lint**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && mise exec -- golangci-lint run ./internal/controller/... 2>&1 | tail -20`
Expected: clean.

- [ ] **Step 3: Grep for un-gated egress (regression guard)**

Run: `cd /Users/szymonri/Documents/tatara-op-wt-bot-gate && rg -n "writer\.Comment\(" internal/controller --type go -g '!*_test.go'`
Expected: only `projectscan.go:2200` (commentSiblingMarker, intentionally idempotent) and the internals of `gatedComment` remain. Every other site goes through `gatedComment` / `decideCommentGate`.

- [ ] **Step 4: Commit any test fixups**

```bash
git add -A
git commit -m "test: update comment-egress expectations for the turn-taking gate"
```

## Self-Review notes

- Spec coverage: rule 1 (turn-taking) = Tasks 1-6,8; rule 2 (bot-MR silence) = Task 7 + `decideCommentGate` gateBotMR; reporter/approver breakers = `commentSilenceBreakers` (Task 1); metrics = Task 3; fail-open = Tasks 2-3; unchanged sibling-marker + REST = Task 9 guard.
- Type consistency: `gatedComment`/`commentGateReason`/`decideCommentGate`/`resolveBotMR`/`botHasLastWordAmong`/`commentSilenceBreakers` names used identically across tasks.
- The only cross-task assumption to verify at execution time: exact repo symbol names for the metrics constructor (`obs.NewOperatorMetrics`) and `ScmSpec` fields (`BotLogin`, `MaintainerLogins`, `ReporterLogins`) — grep and correct if they differ, do not invent.
