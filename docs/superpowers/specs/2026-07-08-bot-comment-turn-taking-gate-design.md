# Bot comment turn-taking gate (rule 1) + bot-MR silence (rule 2)

Date: 2026-07-08
Repo: tatara-operator
Status: design approved, pre-implementation

## Problem

Tatara re-comments on its own issues and MRs. Observed live:

- **#243** "Alert re-fired ... already tracked by this issue" x9 (through
  2026-07-08). Source: `alertGroupRefireComment` posted on every incident
  re-fire dedup, ungated.
- **#112 / #126** "Task run terminated (`Failed` / ...)" / "Wrapper pod failed
  to boot" x15. Source: `postTerminalComment`, posted on every terminal run,
  ungated.
- **tatara-cli #77** bot posted two review comments ("Approving in spirit...",
  then "Approve-quality... already merged"). Source: `writeBackReview` re-review
  on a human PR, ungated.

A bot-last-word gate already exists (`botHadLastWord`, `botIsLastCommenter`,
triage `botHasLastWord`/`hasHumanReply`) but only guards **scan task-creation**
(`mrScan`, `issueScan`) and **triage**. The ~14 controller functions that post
comments directly via `writer.Comment` / `writer.Approve` /
`writer.RequestChanges` bypass it. That is the spam.

`mrScan` already routes bot-authored PRs to the `issueLifecycle`/MRCI drive path
(never `review`), so the bot does not formally self-review its own MRs. But the
lifecycle drive path still *comments* on bot MRs (park / deadline notes).

## Rules (user-specified)

1. **Turn-taking on issues.** If tatara authored the last comment, post nothing
   more until a **reporter or approver** comments again. A random third-party
   comment does NOT break the silence.
2. **Never comment on its own MR.** On a bot-authored PR/MR the bot drives it
   (merge / push / close), it does not comment. Park / deadline handoffs become
   silent state transitions (label + `Task.Status`), zero comments.

## Design

### Single choke point: `internal/controller/comment_gate.go`

A free function decides suppression; thin reconciler wrappers call it before any
egress. Fail-open on every read error / missing input (post proceeds) to
preserve missed-webhook recovery, matching `botHadLastWord` and
`humanCommentAfter`.

```go
type gateReason string

const (
    gateOpen     gateReason = ""
    gateBotMR    gateReason = "bot_mr"    // rule 2
    gateLastWord gateReason = "last_word" // rule 1
)

// decideCommentGate reports whether a bot comment to (owner,name,number) must be
// withheld and why. isPR selects the PR/MR comment timeline. botAuthorHint, when
// non-empty, is the pre-known PR author (TaskSource.AuthorLogin) and skips the
// GetPRState read. repoURL+token are needed only for the GetPRState fallback.
func decideCommentGate(
    ctx context.Context, reader scm.SCMReader,
    owner, name string, number int, isPR bool,
    repoURL, token, botLogin string, botAuthorHint string,
    breakers []string,
) gateReason
```

Logic:

1. Fail-open guard: `botLogin == "" || reader == nil || owner == ""` -> `gateOpen`.
2. **Rule 2** (`isPR` only): resolve author. Use `botAuthorHint` when set; else
   `reader.GetPRState(ctx, repoURL, token, number).Author` (best-effort; read
   error -> treat as not-bot, fall through to rule 1). If author == botLogin ->
   `gateBotMR`.
3. **Rule 1**: list the conversation - `PRCommentLister.ListPRComments` when
   `isPR` and the reader supports it, else `ListIssueComments` (same fallback as
   `botHadLastWord`). On read error -> `gateOpen`. If `botHasLastWordAmong(
   comments, botLogin, breakers)` -> `gateLastWord`.
4. Else `gateOpen`.

### Turn-taking semantic: `botHasLastWordAmong`

Stricter than the existing `botIsLastCommenter` (which only checks the single
newest author). This one honours the reporter/approver-only silence-breaker:

```go
// botHasLastWordAmong reports whether the bot must stay silent: the bot has a
// comment and no silence-breaker has commented since. A comment breaks silence
// iff Author != "" && Author != botLogin && (len(breakers) == 0 || Author in
// breakers). Timeline order is by CreatedAt (robust to SCM list ordering).
func botHasLastWordAmong(comments []scm.IssueComment, botLogin string, breakers []string) bool {
    var tBot, tBreak time.Time
    for _, c := range comments {
        switch {
        case c.Author == botLogin:
            if c.CreatedAt.After(tBot) { tBot = c.CreatedAt }
        case c.Author != "" && (len(breakers) == 0 || slices.Contains(breakers, c.Author)):
            if c.CreatedAt.After(tBreak) { tBreak = c.CreatedAt }
        }
    }
    if tBot.IsZero() { return false }        // bot never spoke -> may post first word
    return !tBreak.After(tBot)               // silent unless a breaker spoke after the bot
}
```

`breakers = union(EffectiveReporterLogins(proj, repo), EffectiveMaintainerLogins(
proj, repo))`. Empty union preserves historical open behaviour (any non-bot
breaks silence), matching triage `hasHumanReply`.

### Reconciler wrapper

`TaskReconciler` (and the one `ProjectReconciler` proposal site) gets a thin
method that resolves inputs and records the metric:

```go
// gatedComment posts body unless decideCommentGate withholds it. Returns
// (posted, err). A suppressed post is (false, nil) and records
// SCMWrite(provider,"comment","suppressed_<reason>").
func (r *TaskReconciler) gatedComment(
    ctx context.Context, proj *v1alpha1.Project, repo *v1alpha1.Repository,
    reader scm.SCMReader, writer scm.SCMWriter, token, provider string,
    owner, name string, number int, isPR bool, repoURL, botAuthorHint, ref, body string,
) (bool, error)
```

- `botLogin` from `proj.Spec.Scm.BotLogin`; `breakers` from the Effective*Logins
  helpers.
- On `gateOpen`: `writer.Comment(ctx, token, ref, body)`, `recordSCM(...)`,
  return `(err == nil, err)`.
- On suppression: log at INFO (`action=scm_comment_suppressed`, `reason`,
  `resource_id`, `repo`, `number`), `Metrics.SCMWrite(provider,"comment",
  "suppressed_"+reason)`, return `(false, nil)`.

A `reader` is required. Every egress site already has `scmContext` (which yields
writer+token+provider) or `ReaderFor`; the wrapper additionally resolves the
reader. If no reader is available it falls open (posts) rather than dropping.

### Site routing (14 egress points)

| # | Site | Target | Gate |
|---|------|--------|------|
| 1 | `writeback_review.go:45` Approve | human PR | rule 1 (isBotMR always false) |
| 2 | `writeback_review.go:52` RequestChanges | human PR | rule 1 |
| 3 | `writeback_review.go:79` Comment | human PR | rule 1 |
| 4 | `lifecycle.go:186` `parkWithComment` | issue or bot/human PR | rule 2 if bot PR, else rule 1 |
| 5 | `lifecycle.go:504` discuss comment | issue | rule 1 |
| 6 | `lifecycle.go:846` `triagePostComment` | issue | rule 1 |
| 7 | `writeback.go:339` warn | issue | rule 1 |
| 8 | `writeback.go:360` ResultSummary | issue | rule 1 |
| 9 | `writeback.go:437` comment | issue | rule 1 |
| 10 | `writeback_proposal.go:135` alert re-fire | issue | rule 1 (**#243**) |
| 11 | `lifecycle_implement.go:90` outcome | issue | rule 1 |
| 12 | `lifecycle_implement.go:278` outcome | issue | rule 1 |
| 13 | `task_controller.go:568` `postTerminalComment` | issue | rule 1 (**#112/#126**) |
| - | `projectscan.go:2200` `commentSiblingMarker` | issue | **unchanged** - already marker-idempotent |

`writer.Suggest` (writeback_review.go:56) is not gated on its own; it rides the
`request_changes` decision (site 2) and is skipped when that verb is suppressed.

Park sites (MRCI `lifecycle_mrci.go`, Merge `lifecycle_merge.go`, MainCI
`lifecycle_mainci.go`) all funnel through `parkWithComment` (site 4), so gating
there covers them. Author resolution uses `task.Spec.Source.AuthorLogin` when set,
else `GetPRState`. The rare "PR not bot-authored; parking" note (mrci.go:52) is a
human PR -> rule 1 (one-shot, lands).

### Out of scope

- REST `commentOnIssue` (handlers.go:677) keeps its existing stricter
  "bot ever commented -> 409" gate (the brainstorm cap-1 agent-tool rule). Not a
  spam source; no change.
- `mrScan` author-routing is already correct (rule 2 at task-creation). No change.

## Testing (TDD)

Unit, table-driven, `t.Run`:

- `botHasLastWordAmong`: bot-only (silent), breaker-after-bot (open),
  third-party-after-bot with non-empty breakers (silent), third-party with empty
  breakers (open), no comments (open), bot-never-spoke (open), unordered
  CreatedAt (robust).
- `decideCommentGate`: bot-MR via hint (bot_mr), bot-MR via GetPRState fallback
  (bot_mr), human PR last-word (last_word), issue last-word (last_word),
  reader-nil / read-error / empty-botLogin (open).
- Per-site behavioural tests (fake SCM reader+writer): a suppressed decision
  skips `writer.Comment` and records `suppressed_<reason>`; an open decision
  posts exactly once. Cover site 10 (#243 re-fire), site 13 (#112 terminal),
  sites 1-3 (#77 re-review), site 4 bot-MR suppression.

## Rollout

Operator-only change. Merge to `main` -> CI builds image/charts ->
`tatara-helmfile` MR bumps operator image tag + chart pins (BOTH), reviewed via
diff, applied by pipeline. `semver:patch` label (bugfix). Deployed via g-sha
recovery if the bot PAT is still expired (see the CD-token incident memory).
Verify live: `SCMWrite{result=~"suppressed_.*"}` climbs and #243/#112 stop
accreting comments.
