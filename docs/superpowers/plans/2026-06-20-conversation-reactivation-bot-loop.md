# Conversation reactivation bot-comment loop fix - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop lifecycle Tasks in `Conversation` from re-posting near-identical "Silent hold" comments every issueScan cycle.

**Architecture:** Two independent guards, both keyed on the same idea (does the bot already have the last word?). Layer A makes issueScan reactivation author-aware so the bot's own comment never retriggers a reactivation. Layer C adds a "bot has the last word" suppression clause to the discuss / close-withheld silence gates so a single stale human reply no longer unlocks perpetual re-posting.

**Tech Stack:** Go 1.x (per go.mod), controller-runtime, testify. Tests via `mise exec -- go test ./internal/controller/...`.

## Global Constraints

- Newest stable Go; build/test ONLY through mise: `mise exec -- go test ./...`, `mise exec -- golangci-lint run`. Never bare `go`.
- JSON logs via `log/slog`/controller-runtime logr; log every business action at INFO with structured fields.
- KISS. No new abstractions beyond the two small helpers below.
- gofmt + golangci-lint must pass. Wrap errors with `fmt.Errorf("context: %w", err)`.
- Table-driven tests with `t.Run`. Fail-open is intentional in both layers - test it explicitly.
- Work in worktree `.worktrees/fix-conv-reactivation-loop` (branch `fix/conv-reactivation-bot-loop`). Never build/deploy from the worktree.

---

### Task 1: Layer A - author-aware issueScan reactivation

**Files:**
- Modify: `internal/controller/projectscan.go` - add `humanCommentAfter` helper; change `findConvTaskToReactivate` signature + body (currently lines 117-140); update its one call site in `issueScan` (currently line 886).
- Test: `internal/controller/projectscan_reactivate_test.go` (create).

**Interfaces:**
- Consumes: `scm.SCMReader.ListIssueComments(ctx, owner, name, number) ([]scm.IssueComment, error)`; `scm.IssueComment{Author string; Body string; CreatedAt time.Time}`; `candidate{repo, number, updatedAt, isPR ...}` where `candidate.repo` is the `"owner/name"` slug.
- Produces:
  - `func humanCommentAfter(ctx context.Context, reader scm.SCMReader, owner, name string, number int, botLogin string, since time.Time) bool`
  - `func findConvTaskToReactivate(ctx context.Context, c candidate, existing []tatarav1alpha1.Task, reader scm.SCMReader, botLogin string) *tatarav1alpha1.Task`

- [ ] **Step 1: Write the failing tests**

Create `internal/controller/projectscan_reactivate_test.go`:

```go
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
			LifecycleState:  state,
			LastActivityAt:  &la,
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
			name:     "ListIssueComments error -> reactivated (fail-open)",
			state:    "Conversation",
			lastAct:  base,
			readErr:  errors.New("boom"),
			want:     true,
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
```

Note: replace the placeholder `convTask` body with whatever the existing tests use to build a Task carrying labels `labelSourceRepo`/`labelSourceNumber` and `Status.LifecycleState`/`Status.LastActivityAt`. Grep `sanitizeRepoLabel`, `labelSourceRepo`, `labelSourceNumber` in projectscan.go and mirror an existing constructor (e.g. in `projectscan_run_test.go`). The labels must satisfy: `t.Labels[labelSourceRepo] == sanitizeRepoLabel("o/r")` and `t.Labels[labelSourceNumber] == "7"`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mise exec -- go test ./internal/controller/ -run TestFindConvTaskToReactivate_AuthorAware -v`
Expected: COMPILE FAIL (`findConvTaskToReactivate` has the old 2-arg signature / unknown helper) - the signature mismatch is the failing state we want before implementing.

- [ ] **Step 3: Add the `humanCommentAfter` helper**

In `internal/controller/projectscan.go`, near `botCommentedOnIssue` (~line 1306), add:

```go
// humanCommentAfter reports whether the issue has a comment authored by a
// non-bot (a human) with CreatedAt strictly after `since`. On a read error it
// returns true (fail-open: the caller reactivates, preserving the missed-webhook
// recovery the reactivation gate exists for; the discuss/close silence gate makes
// an over-eager reactivation a silent no-op).
func humanCommentAfter(ctx context.Context, reader scm.SCMReader, owner, name string, number int, botLogin string, since time.Time) bool {
	comments, err := reader.ListIssueComments(ctx, owner, name, number)
	if err != nil {
		return true
	}
	for _, c := range comments {
		if c.Author != "" && c.Author != botLogin && c.CreatedAt.After(since) {
			return true
		}
	}
	return false
}
```

- [ ] **Step 4: Make `findConvTaskToReactivate` author-aware**

Replace the function (currently lines 117-140). New version threads `ctx`, `reader`, `botLogin` and only reactivates when a non-bot comment is newer than the task's LastActivityAt:

```go
func findConvTaskToReactivate(ctx context.Context, c candidate, existing []tatarav1alpha1.Task, reader scm.SCMReader, botLogin string) *tatarav1alpha1.Task {
	if c.isPR {
		return nil
	}
	repoLabel := sanitizeRepoLabel(c.repo)
	numLabel := strconv.Itoa(c.number)
	for i := range existing {
		t := &existing[i]
		if t.Labels[labelSourceRepo] != repoLabel || t.Labels[labelSourceNumber] != numLabel {
			continue
		}
		state := t.Status.LifecycleState
		if state != "Conversation" && state != "Stopped" {
			continue
		}
		if t.Status.LastActivityAt == nil {
			continue
		}
		if !c.updatedAt.After(t.Status.LastActivityAt.Time) {
			continue
		}
		// Author-aware gate: the bot's own queued comment lands after LastActivityAt
		// and would otherwise re-trigger reactivation every scan (the Conversation
		// re-comment loop). Only reactivate when a HUMAN comment is newer than our
		// last activity. Fail-open (reactivate) when we cannot read the author.
		owner, name, ok := strings.Cut(c.repo, "/")
		if !ok || reader == nil || botLogin == "" {
			return t
		}
		if humanCommentAfter(ctx, reader, owner, name, c.number, botLogin, t.Status.LastActivityAt.Time) {
			return t
		}
		return nil
	}
	return nil
}
```

Ensure `strings` is imported in projectscan.go (it already imports `strconv`; add `"strings"` if absent - check the import block).

- [ ] **Step 5: Update the call site in `issueScan`**

At the reactivation pass (currently line 885-886), compute `botLogin` once before the loop and pass the new args:

```go
botLogin := ""
if proj.Spec.Scm != nil {
	botLogin = proj.Spec.Scm.BotLogin
}
for _, c := range cands {
	task := findConvTaskToReactivate(ctx, c, existing, reader, botLogin)
	if task == nil {
		continue
	}
	// ... unchanged reactivation body ...
}
```

- [ ] **Step 6: Run tests + full package**

Run: `mise exec -- go test ./internal/controller/ -run TestFindConvTaskToReactivate_AuthorAware -v`
Expected: PASS (all 5 subtests).

Run: `mise exec -- go test ./internal/controller/...`
Expected: PASS (no regressions; the existing reactivation callers compile with the new signature).

- [ ] **Step 7: Commit**

```bash
cd .worktrees/fix-conv-reactivation-loop
git add internal/controller/projectscan.go internal/controller/projectscan_reactivate_test.go
git commit -m "fix: author-aware Conversation reactivation (Layer A)

findConvTaskToReactivate now requires a non-bot comment newer than the
task's LastActivityAt before reactivating, so the bot's own hold comment
no longer re-triggers the reactivate->re-comment loop. Fail-open on read
error.

Claude-Session: https://claude.ai/code/session_01CB4oDBNg6RTzX53HHKner6"
```

---

### Task 2: Layer C - "bot has the last word" silence clause

**Files:**
- Modify: `internal/controller/lifecycle.go` - add `triageReader.botHasLastWord` (near `hasHumanReply`, ~line 743); add the OR-clause to the discuss arm (~879-894) and the close-withheld arm (~822-837). Do NOT touch the implement self-approve guard (~921).
- Test: `internal/controller/lifecycle_discuss_silence_test.go` (extend with the #74 shape).

**Interfaces:**
- Consumes: `triageReader{reader scm.SCMReader; owner, repoName string; issueNum int; botLogin string; resolved bool}`; `scm.IssueComment.CreatedAt`.
- Produces: `func (tr triageReader) botHasLastWord(ctx context.Context) (bool, error)`.

- [ ] **Step 1: Write the failing test**

Append to `internal/controller/lifecycle_discuss_silence_test.go`:

```go
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
			{Author: "szymonrychu", CreatedAt: old},   // stale human reply
			{Author: testBotLogin, CreatedAt: newer},  // bot has the last word
		},
	}
	w := &commentCapturingWriter{}
	r := reconcilerFor(w, rdr)

	_, err := r.finishTriage(context.Background(), proj, task)
	require.NoError(t, err)

	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.LifecycleState)
	w.mu.Lock()
	posted := len(w.commentBodies)
	w.mu.Unlock()
	require.Zero(t, posted,
		"human-filed issue where the bot has the last word must NOT re-post a discuss comment; got: %v", w.commentBodies)
}
```

Resolve `testBotLogin`: grep how `seedDiscussSilenceTask`/`seedLabelTask` sets `project.Spec.Scm.BotLogin` (the silence tests already rely on a bot login for `hasHumanReply`). Use that exact value (replace `testBotLogin` with the literal, e.g. `"tatara-bot"`). Add `"time"` to the test file imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `mise exec -- go test ./internal/controller/ -run TestFinishTriage_HumanFiled_Discuss_BotHasLastWord_Suppresses -v`
Expected: FAIL - one comment posted (current code: authored=false skips the gate, so it posts).

- [ ] **Step 3: Add `botHasLastWord`**

In `internal/controller/lifecycle.go`, after `hasHumanReply` (~line 757):

```go
// botHasLastWord reports whether the newest comment on the issue is authored by
// the bot (the bot already had the last word). Newest is by CreatedAt, so it is
// robust to SCM list ordering. No comments -> false (the bot has not spoken).
// Used to suppress repeated hold comments once the bot has responded and no human
// has replied since.
func (tr triageReader) botHasLastWord(ctx context.Context) (bool, error) {
	if !tr.resolved {
		return false, nil
	}
	comments, err := tr.reader.ListIssueComments(ctx, tr.owner, tr.repoName, tr.issueNum)
	if err != nil {
		return false, err
	}
	newest := -1
	for i := range comments {
		if newest == -1 || comments[i].CreatedAt.After(comments[newest].CreatedAt) {
			newest = i
		}
	}
	return newest >= 0 && comments[newest].Author == tr.botLogin, nil
}
```

- [ ] **Step 4: Add the OR-clause to the discuss arm**

In the `case "discuss":` arm, after the existing `skipComment` block (currently ends ~line 894), before `if !skipComment {`, add:

```go
		if !skipComment {
			lastWord, lerr := tr.botHasLastWord(ctx)
			if lerr != nil {
				l.Info("triage discuss: last-word check failed; posting comment (fail open)",
					"action", "lifecycle_discuss_silence_check", "resource_id", task.Name, "err", lerr.Error())
			} else if lastWord {
				skipComment = true
				l.Info("triage discuss: bot already has the last word; suppressing comment",
					"action", "lifecycle_discuss_silent_hold", "resource_id", task.Name)
			}
		}
```

- [ ] **Step 5: Add the OR-clause to the close-withheld arm**

In the `case "close":` arm's `hasUnmergedChange` branch, after its existing `skipComment` block (currently ends ~line 837), before `if !skipComment {`, add the same block with the close-withheld action labels:

```go
		if !skipComment {
			lastWord, lerr := tr.botHasLastWord(ctx)
			if lerr != nil {
				l.Info("triage close-withheld: last-word check failed; posting note (fail open)",
					"action", "lifecycle_close_withheld_silence_check", "resource_id", task.Name, "err", lerr.Error())
			} else if lastWord {
				skipComment = true
				l.Info("triage close-withheld: bot already has the last word; suppressing note",
					"action", "lifecycle_close_withheld_silent_hold", "resource_id", task.Name)
			}
		}
```

- [ ] **Step 6: Run the new test + the existing silence tests**

Run: `mise exec -- go test ./internal/controller/ -run TestFinishTriage -v`
Expected: PASS - the new `BotHasLastWord_Suppresses` test plus all three pre-existing discuss-silence tests (`NoHumanComment_SilentHold`, `WithHumanComment_PostsComment`, `HumanFiled_AlwaysPostsComment`). The pre-existing tests use `comments == nil`, so `botHasLastWord` is false and the existing clause still decides them.

- [ ] **Step 7: Run the full package + lint**

Run: `mise exec -- go test ./internal/controller/...`
Expected: PASS.
Run: `mise exec -- golangci-lint run ./internal/controller/...`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
cd .worktrees/fix-conv-reactivation-loop
git add internal/controller/lifecycle.go internal/controller/lifecycle_discuss_silence_test.go
git commit -m "fix: suppress repeat hold comments once bot has last word (Layer C)

discuss + close-withheld silence gates now also suppress when the newest
issue comment is the bot's, decoupling repeat-suppression from the
tatara-authored marker. Covers human-filed issues (operator#74) where a
stale human reply had unlocked perpetual re-posting. Implement-arm
self-approve guard untouched.

Claude-Session: https://claude.ai/code/session_01CB4oDBNg6RTzX53HHKner6"
```

---

## Self-Review

**Spec coverage:**
- Layer A (author-aware reactivation) -> Task 1. ✓
- Layer C (silence-gate last-word clause) -> Task 2. ✓
- Shared primitive (bot-has-last-word / human-comment-after, by CreatedAt) -> `humanCommentAfter` (Task 1) + `botHasLastWord` (Task 2). ✓
- Fail-open on read error -> Task 1 Step 1 (error subtest) + Task 2 Step 4/5 (lerr branch). ✓
- Loop regression -> Task 1 "bot-only comment after lastActivity -> NOT reactivated" subtest. ✓
- Out of scope (stale comment cleanup, webhook, #76 egress, idle minutes, implement self-approve guard) -> untouched. ✓

**Placeholder scan:** `convTask`/`testBotLogin` are flagged in-task with exact grep instructions to resolve against existing helpers - not left vague. No TBD/TODO.

**Type consistency:** `findConvTaskToReactivate(ctx, c, existing, reader, botLogin)` used identically in Task 1 Step 4 (def) and Step 5 (call). `humanCommentAfter` and `botHasLastWord` signatures match their call sites. `scm.IssueComment.CreatedAt` is the real field (verified in scm.go:133-137).

## Deploy (after merge, not part of this branch)

Merge to operator `main` (CI builds + pushes image), then a tatara-helmfile MR bumping BOTH the chart version and the pinned `image.tag`. See [[tatara-operator-deploy-chart-version-and-image-tag]].
