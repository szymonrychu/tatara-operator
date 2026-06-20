# Conversation reactivation bot-comment loop - design

Date: 2026-06-20
Branch: `fix/conv-reactivation-bot-loop`
Repo: tatara-operator

## Problem

Lifecycle Tasks in `Conversation` (awaiting-human) state re-comment on their
linked issue every issueScan cycle, posting near-identical "Silent hold"
comments indefinitely. Live evidence (operator image `e878161`, which already
contains the #76 brainstorm-dedup fix):

- `szymonrychu/tatara-operator#74`: 42 bot comments, latest 2026-06-20T05:03Z.
- `szymonrychu/tatara-operator#75`: 39 bot comments, latest 2026-06-20T03:03Z.
- Operator logs show `issueScan: reactivated conversation task` for both issues
  every hour on the hour (04:00:04, 05:00:04, 06:00:04 ...).

This is NOT the bug #76 fixed. #76 gated the brainstorm `commentOnIssue` egress
endpoint. This loop is in the issueScan Conversation-reactivation path, untouched
by #76.

## Root cause

Two compounding defects, both keyed on the wrong question. The correct primitive
for both is: **does the bot already have the last word on this issue?** (i.e. the
newest issue comment is authored by `botLogin`).

### Defect A - author-blind reactivation (root of the loop)

`findConvTaskToReactivate` (internal/controller/projectscan.go) reactivates a
`Conversation`/`Stopped` Task to `Triage` whenever
`candidate.updatedAt.After(task.Status.LastActivityAt)`. It does not check WHO
caused the update. The bot's own queued "Silent hold" comment (drained to the SCM
by the reconcile at lifecycle.go:410, after `enterConversation` stamps
`LastActivityAt`) lands on the issue slightly after `LastActivityAt`, so the next
scan reads it as "new activity we missed" and reactivates. The reactivated Task
re-runs the agent, which posts another hold comment, which bumps `updatedAt`
again. Infinite loop, paced by the scan interval.

### Defect C - silence gate keys on "ever replied" + a marker

The discuss / close-withheld arms in `runIssueOutcome` (lifecycle.go:870-906 and
818-847) already try to suppress repeated hold comments via a silence gate, but
its precondition is wrong:

- It only engages for `isTataraAuthored` issues (body contains
  `<!-- tatara-authored -->`). #74 is human-authored and lacks the marker, so the
  gate is skipped entirely.
- Even when engaged, it suppresses only if `hasHumanReply` is false, where
  `hasHumanReply` means "any non-bot comment exists, ever". On #74 the maintainer
  commented twice on 2026-06-16; that one old reply opens the gate permanently, so
  every later re-triage re-posts.

The right key is "has a human replied SINCE the bot's last comment", which is the
negation of the same "bot has the last word" primitive.

## Fix

Add one shared helper expressing the primitive, then wire it into both paths.

### Shared primitive

A function over an issue's comments that reports whether the newest comment is the
bot's (the bot has the last word). Newest is determined by `IssueComment.CreatedAt`
(max), not list order, so it is robust to SCM ordering. Reuses the existing
`scm.SCMReader.ListIssueComments`. The existing `botCommentedOnIssue`
(projectscan.go:1306) and `triageReader.hasHumanReply` (lifecycle.go:743) are the
two current call sites of this list; the new helper sits alongside them.

### Layer A - author-aware reactivation

`findConvTaskToReactivate` gains access to the scan `reader` and `botLogin` (both
already in `issueScan` scope; `ReaderFor` is wired on `ProjectReconciler`). When
the cheap `updatedAt.After(LastActivityAt)` trigger fires, it additionally requires
a non-bot comment with `CreatedAt` strictly after `LastActivityAt` before
reactivating. If the only new comment(s) since `LastActivityAt` are the bot's, do
not reactivate.

- Fail-open: on a `ListIssueComments` error, reactivate (preserve the
  missed-webhook recovery the gate exists for). Layer C makes an over-eager
  reactivation harmless (the agent holds silently), so fail-open is safe.
- Scope: the comments fetch happens only for candidates that already passed the
  `updatedAt`/`Conversation`-state filter - a handful of issues per scan, not every
  open issue.
- Known limitation: a human NON-comment action during Conversation (label edit,
  body edit) does not advance any comment, so this path will not reactivate on it.
  That is acceptable - such actions are webhook-driven, and the periodic scan is a
  backstop for missed comment webhooks specifically.

### Layer C - silence gate gains a "bot has the last word" clause

Keep the existing `isTataraAuthored && !hasHumanReply` suppression (it correctly
silences the bot on its OWN zero-engagement idea from the very first cycle - the
#29 fix) and ADD a second suppression clause: also suppress when the bot already
has the last word (the newest issue comment is the bot's). Final predicate in the
discuss and close-withheld arms:

```
suppress = (isTataraAuthored && !hasHumanReply) || botHasLastWord
```

This:

- adds marker-independent coverage for human-authored brainstorming issues like
  #74, where one stale human reply (06-16) had unlocked perpetual re-posting;
- still posts the FIRST genuine response to a human (at that moment the human's
  comment is newest, so `botHasLastWord` is false), then suppresses the repeats;
- preserves all three existing discuss-silence tests unchanged (each uses
  `comments == nil`, where `botHasLastWord` is false, so the existing clause alone
  decides them).

The implement-arm self-approve guard (lifecycle.go:921) is NOT touched -
`isTataraAuthored`/`hasHumanReply` keep their existing meaning there.

`botHasLastWord` determines "newest" by `IssueComment.CreatedAt` (max). No comments
-> false (the bot has not spoken). Fail-open on read error (post the comment),
matching the current arms' discipline.

## Components touched

- `internal/controller/projectscan.go` - `findConvTaskToReactivate` signature +
  body; new shared helper (or extend the existing comment-author helpers).
- `internal/controller/projectscan.go` - the single `findConvTaskToReactivate`
  call site in `issueScan` passes `reader` + `botLogin`.
- `internal/controller/lifecycle.go` - discuss + close-withheld silence gates use
  the new primitive; `hasHumanReply` either generalised to
  `hasHumanReplySince`/`botHasLastWord` or replaced.

## Testing (TDD)

Table-driven unit tests with a fake `SCMReader`:

- `findConvTaskToReactivate`:
  - newest comment is the bot's, `updatedAt > LastActivityAt` -> NOT reactivated.
  - a non-bot comment newer than `LastActivityAt` -> reactivated.
  - `ListIssueComments` error -> reactivated (fail-open).
  - non-Conversation/Stopped state, PR candidate, nil `LastActivityAt` -> unchanged
    existing behavior.
- silence gate:
  - bot has last word -> comment suppressed (both human-authored and tatara-authored
    issues; covers the #74 shape).
  - human comment newer than bot's last -> comment posted.
  - reader error -> comment posted (fail-open).
- A regression test reproducing the loop: a `Conversation` task whose only
  post-`LastActivityAt` activity is the bot's own comment must not reactivate.

## Out of scope

- The accumulated 40+ stale comments on #74/#75 are not deleted by this change;
  cleanup (if wanted) is a separate manual/runbook step.
- No change to webhook handling, the brainstorm `commentOnIssue` egress gate (#76),
  or `ConversationIdleMinutes`.

## Deploy

Per CLAUDE.md hard rules: merge to operator `main` (CI builds + pushes image), then
a tatara-helmfile MR bumping BOTH the chart version and the pinned `image.tag`.
Not deployed by this branch.
