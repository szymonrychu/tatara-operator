# Interaction-loop coverage: every human SCM action gets a sane reaction

Date: 2026-07-18 (rev 3, post delta-verify: shared sever operation)
Topic: Close the gaps where a natural human SCM action (close an issue, edit a
body, reply inline, retract an approval, push to a branch) produces NO operator
reaction. Owner intent: "interact through issues/MRs/comments only; the system
behaves like a well-managed development team." Design only; implementation
follows sign-off.

Scope is WS3 from `findings-webhook-primary-reactivity.md`: findings I1-I5,
M1/M2/M4/M5, and the also-ignored inventory. WS1-A (formal-review re-entry
respecting `ParkedFromStage`) and WS1-B (mint/routing race fixes) are separate
in-flight branches; this design composes with them and never re-opens their
surface. Rev 2 corrected two mechanically-inverted claims (I3 reopen, I4
single-reply resume) and folded in six reviewer items. Rev 3 fixes the shared
root cause behind both - a CR-side ownerRef change that left `Status.IssueRefs`
and reaper bookkeeping in split state (closed-CR leak + spurious terminal comment)
- with a single `SeverIssueFromTask` operation both paths use. All cited
file:line references are traced to HEAD dd0e32f.

## Executive summary

The task-centric redesign wired the FIRST-CONTACT events (issue open, comment,
MR open, review verdict) but left the rest of the human vocabulary silent. A
human can close an issue and the agent keeps burning a review on it; edit the
scope and the agent never sees it; reply to an inline review thread into the
void; reply to a parked task during a 7-day window that ends with their words
discarded. None of these are races or regressions - they are unbuilt reactions,
and every one violates the well-managed-team contract.

This design adds those reactions under three hard rules:

1. **No new gate bypass (#294).** No new edge lands in `clarifying`,
   `brainstorming`, `approved`, or `implementing` from an origin that skips the
   C.6 approval grammar. Re-engagement of a dead-end always flows through a
   fresh `clarify` Task (which re-runs the gate), never a smuggled re-entry.
2. **No new webhook-goroutine stage mutation (#353 asymmetry).** Webhook
   handlers run on EVERY replica; reconcilers are leader-only. Every WS3
   reaction is one of: (a) an idempotent MIRROR write, (b) an idempotent
   PENDING-EVENT append, or (c) a POKE the leader-only reconcile/reaper
   consumes. Every stage mutation WS3 introduces (issue-close stop, no-re-entry
   re-mint, deploy-timeout comment, edit-driven unpark) is performed leader-only.
   WS3 adds ZERO new mutation to the HTTP goroutine.
3. **Fold into existing machinery, KISS.** Reuse the mirror sync,
   `Issue.status.pendingComments`, the terminal reaper, `MintForItem`, and the
   pending-event queue. One new stage reason, at most a small edge set, and
   per-event SCM parse additions - nothing structurally new.

## Design principles (the spine)

### Concurrency classification

Every reaction is tagged by WHERE it may safely run:

| Class | Safe on any replica? | Mechanism | Used by |
|---|---|---|---|
| MIRROR write | yes (idempotent set-union / status upsert, `objbudget.Fit*`, RetryOnConflict) | on-demand mirror refresh | edited body, inline reply, synchronize, PR closed/merged |
| PENDING-EVENT append | yes (idempotent, drop-oldest, RetryOnConflict; existing `AppendTaskEvent`) | append only, NO unpark | inline reply, edited body, review comment, M4 fold |
| LEADER mutation | NO - leader-only reconcile/reaper | stage `Enter`, ownerRef drop, `MintForItem`, pod delete, operator comment | issue-close stop, no-re-entry re-mint, edit-driven unpark, deploy comment, M5 SHA |

The webhook's job for a LEADER-mutation reaction is only to make the mirror
reflect the forge truth PROMPTLY (a safe MIRROR write) plus, where relevant,
append a pending event (a safe APPEND) - never to `Enter` a stage, drop an
ownerRef, or drive an unpark. The leader-only reconcile that watches that CR
(the Task/Issue reconcile fires on the resourceVersion bump the webhook's write
causes) performs the mutation within one reconcile.

Note the deliberate contrast with the EXISTING code: `driveCommentUnpark`
(pending_events.go:108,119) and the `review_apply` appliers mutate stage from
the HTTP goroutine today - WS1-B's flagged F6-1 surface. WS3 does not remove it
and, critically, **adds nothing to it**. In particular the edit-driven unpark
(I2) is driven LEADER-SIDE, NOT through `driveCommentUnpark`, so it does not
widen the F6-1 surface (this resolves the reviewer's surface-widening objection
structurally, not by policy).

### Gate-safety invariant

The four gated stages are unreachable from any WS3 origin:

- issue-close -> `rejected` (terminal, no code ever runs off it).
- edited / inline-reply / synchronize / review-comment -> mirror + pending
  event only; NO stage change from the webhook.
- no-re-entry reply -> `SeverIssueFromTask(Orphan)` + a FRESH `clarify` mint via
  `MintForItem` (goes THROUGH C.6), never a re-entry edge.
- deploy-timeout -> operator comment only.

## Per-event design

Latency class: **W** = immediate on webhook delivery; **C** = leader
reconcile/reaper cycle (seconds-to-minutes); **W->C** = webhook makes the mirror
truthful + appends immediately, leader acts next reconcile.

| Event | Reaction | Component | Latency | Concurrency | GitHub | GitLab |
|---|---|---|---|---|---|---|
| I3 issue closed (owned, live-stage) | stop -> `rejected(issue-closed)`; SEVER the closed Issue (delete CR + clear IssueRefs); tear down pod; bot PR closed by reaper | mirror refresh (webhook) -> leader `ApplyIssueClosedStop` from Issue reconcile | W->C | LEADER | `issues` action=closed | Issue Hook action=close |
| I2 issue body/title edited (owned) | refresh mirror body/title; if body/title changed, append `issue_edited` pending event; unpark driven LEADER-side | mirror refresh + `AppendTaskEvent` (webhook) -> leader `driveUnparks`/Task reconcile | W->C | MIRROR + PENDING (unpark leader-only) | `issues` action=edited | Issue Hook action=synchronize (its map of `update`); ALSO run diff on labeled/unlabeled |
| I1 inline review-thread reply (owned MR) | parse as MR comment; mirror + pending event; NO verdict, NO transition | SCM parse (GitHub) + `deliverPendingEvent` | W | MIRROR + PENDING | `pull_request_review_comment` action=created | already covered by Note Hook |
| I4 human reply to parked(no-re-entry) | SEVER the open Issue from the old Task (orphan CR + clear IssueRefs + strip forge label) + `MintForItem` (mirror comments -> humanHasLastWord) -> fresh ACTIVE clarify | pending append (webhook) -> leader Task reconcile / reaper backstop | W->C | LEADER | any comment | any Note |
| I5 first parked(deploy-timeout) | one rate-limited operator comment (own marker) on the owned issue naming repo + retry n/max | leader deploy-stage/reaper -> `Issue.status.pendingComments` | C | LEADER | operator comment | operator note |
| M1 push to agent MR (synchronize) during reviewing | refresh mirror head/CI; NO review restart (merging head-pin catches a real move) | mirror refresh (webhook) | W | MIRROR | `pull_request` action=synchronize | MR Hook action=synchronize (its map of `update`), IsPR |
| M2 approval dismissed / MR unapproved | ignored (documented); use request_changes or close to stop | none | - | - | `pull_request_review` state=dismissed | MR Hook action=unapproved |
| M4 review on owned MR, Task Get NotFound | fold to `deliverPendingEvent` instead of silent drop | existing webhook path (one-line) | W | PENDING | both | both |
| M5 approval with empty commit SHA | short-circuit BEFORE any MR write: fall back to mirror `Status.HeadSHA`; if also empty, fold, do not clear PendingReview | leader `ApplyReviewApproval` | W(existing) | (existing review path) | numeric id carries commit_id | last_commit may be empty |
| labeled == configured trigger label (orphan issue) | mint via `MintForItem`; skip bot actor + the operator's own approved/declined projection labels | webhook `handleForgeItem` -> `Minter` | W | (idempotent mint, WS1-B) | `issues` action=labeled, `label.name` | changes.labels diff, `ChangedLabel` |
| PR closed/merged (owned MR, out-of-band) | refresh mirror state (merged/closed, MergedAt); existing merge/review reconcile converges | mirror refresh (webhook) | W->C | MIRROR | `pull_request` action=closed (+merged flag) | MR Hook action=merge/close |

## Detailed designs

### I3 - issue closed mid-flight (the stop edge)

A human closing the driving issue is the unambiguous "stop this" signal. A
well-managed team stops the work, closes the PR it opened with a note, and if
the human reopens, starts fresh.

**Target: `rejected` with a new reason `ReasonIssueClosed`.** `rejected` is
semantically "this item was declined/cancelled and will not proceed" - exactly
a human close, and the sibling of clarify `decision=close` -> `rejected(declined)`.
`rejected`'s terminal reaper (`reapOne` -> `releaseTerminal`) already closes the
bot PR via `closeOwnMRs` (all four clauses hold for a `task/<name>` bot branch)
with its existing "Closing: the tatara task that opened this PR ended in
`rejected`(`issue-closed`)" comment. `notifyTerminalIssue` skips the CLOSED issue
(reaper.go:541), so no bot comment and no `tatara-parked` label land on it - which
is correct (the human closed it deliberately) and load-bearing for the reopen
path below. A human-authored PR (kind=review Task) is never touched (`ourMR`
false).

**Which stages get the edge.** The stop fires only for LIVE, non-terminal,
non-`deploying` stages that can own an open issue:

```
triaging, brainstorming, clarifying, investigating, refining,
approved, implementing, reviewing, merging   ->  rejected(issue-closed)
```

`deploying` is EXCLUDED on purpose, not merely to dodge a race: once the code is
merged and deploying, closing the issue must not rewind shipped work - the same
"a merged MR is finished, no rewind" boundary the review path enforces. It also
removes the only window where the operator itself closes an issue while the Task
is still live (C.4 closes owned issues at `deploying`, before stamping
`deliveredAt` - deploy_stage.go:99), so no operator-close is ever mistaken for a
human-close, no marker needed. `clarify decision=close` enters
`rejected(declined)` FIRST and closes the issue SECOND, so that Task is already
terminal (excluded) when the close is mirrored.

**Reopen (BLOCKING-1 fix - the mechanism was inverted in rev 1).** The naive
"the reaper orphans the reopened issue in minutes" is FALSE: at reap time the
issue is still CLOSED, so `releaseOwnership` keeps the ref (`state != "closed"`
gate, reaper.go:754); `releaseTerminal` short-circuits on `AnnTerminalReleased`
and never re-runs (reaper.go:516-518); the reopened issue stays owned by the
terminal Task and `IsOrphanIssue` returns false (sweep.go:133-142) until the CR
cascades at `RejectedRetention` = 24h (constants.go:16). Real dead-air = 24h.

A bare ownerRef drop makes it WORSE (rev-2 defect 1): `ownedIssues` walks
`Task.Status.IssueRefs`, not ownerRefs (reaper.go:956-966); nothing removes the
IssueRefs entry; and the ONLY Issue-CR deletion anywhere is ownerRef cascade via
the owning Task. Drop the ref and the closed CR is now un-owned AND un-cascadable
- when `deleteReapedTask` deletes the Task at retention it does NOT cascade the
CR, which then leaks forever with the IssueReconciler mirror-syncing it every
cadence.

Fix: **`ApplyIssueClosedStop` runs the shared `SeverIssueFromTask` op in
DeleteCR mode** (see "The sever operation" below): it clears the issue from
`Task.Status.IssueRefs` and DELETES the closed Issue CR outright. No leak (the CR
is gone, not orphaned), no split state (both sides detached). The mirror is a
rebuildable projection, so deleting it costs nothing.

REOPEN after a stop-time CR delete: `handleIssueOpened` (`reopened`) builds the
ForgeItem from the LIVE event (`State:"open"`) and calls `MintForItem`. `issueCR`
returns nil (NotFound), and `IsOrphanIssue(cr=nil)` -> `IsAllowedReporter` = true
(orphan) - it never needed the CR. Because the deterministic Task name still
resolves to the old terminal Task, `createTaskRaceSafe` deletes that
stale-terminal twin (intake.go:245-251) on this delivery; the fresh ACTIVE
clarify Task mints on the next sweep tick against the freed name, and its
`MintIssueTask` -> `SyncIssue` re-creates the mirror CR. Bound drops from 24h to
a sweep cadence (minutes). The fresh Task is ACTIVE: no `tatara-parked` label was
ever stamped on the closed forge issue (the reaper skipped it), so `MintStage`
returns triaging. The old rejected Task, now owning nothing, survives its 24h
retention as a debugging artifact.

**Signal path (concurrency).**
1. Webhook `issues`/Issue-Hook action=closed, owned issue: on-demand mirror
   refresh of `Issue.Status.State` (safe MIRROR write; no Task mutation). Any
   replica.
2. Leader-only `IssueReconciler` (reconciles on the Issue status bump) observes
   owned + `Status.State=="closed"` + owning Task in a live non-`deploying`
   stage -> `ApplyIssueClosedStop(ctx, c, task, now)`.
3. `ApplyIssueClosedStop` (new `internal/controller/issue_apply.go`, mirroring
   `review_apply.go`): `RetryOnConflict` `Enter(task, mrs, StageRejected,
   ReasonIssueClosed, now)` + `Status().Update`; `SeverIssueFromTask(DeleteCR)`
   for the closed issue; tear the wrapper pod down inline (`agent.DeleteWrapper`,
   leader-safe, same idiom as the review appliers). Immediate but clean: the
   in-flight turn is abandoned, no half-finished branch is pushed.
4. The existing terminal reaper closes the bot PR.

**Accepted race (reviewer item 8).** If a pod outcome parks the Task in the
window between the human close and the leader observing it, the Task is no longer
in a live source stage, so `ApplyIssueClosedStop` refuses (its live-stage gate).
The closed issue then rides the EXISTING parked reaper: a `parked(backlog-sweep)`
anchor reaps when its issues close (reaper.go:445-450); any other park ages out
at `parkRetention` still owning the closed (harmless) issue. So the stop edge is
best-effort on live stages, NOT a guarantee that fires from every stage - stated,
not implied.

Alternative considered: `failed(issue-closed)` needs zero new edges (every live
stage already has a `failed` edge) but mislabels a human cancellation as a
platform failure and inflates `operator_task ... failed`. See owner decision 1.

### I2 - issue edited (Goal refresh), LEADER-side unpark

`TaskSpec.Goal` is a mint-time snapshot in the Task SPEC. Do NOT mutate it on a
live Task: spec mutation mid-implementation has no precedent and risks confusing
an in-flight agent. Treat an edit like a comment.

On an owned-issue update the webhook (a) refreshes the mirror
`Issue.Status.Body/Title` (safe MIRROR write - the agent's `scm_read(kind=issues)`
is served from the mirror, so a re-read sees the new scope), and (b) if the body
or title actually CHANGED (diff the mirror's prior `Status.Body/Title`, not the
action string), appends an `issue_edited` pending event via `AppendTaskEvent`.
The webhook does NOT call `driveCommentUnpark` for this path.

**Unpark is LEADER-side only (pinned).** For a `parked(awaiting-human)` /
`parked(backlog-sweep)` Task, the fresh `issue_edited` pending event is a non-bot
human signal that may re-engage the Task, but the actual `Unpark` is driven by
the leader-only `driveUnparks` project loop and the Task reconcile (which fires
on the pending-event append), NEVER from the HTTP goroutine. This keeps the edit
path off the F6-1 surface entirely. On `parked(identity-unverified)` a body edit
is not an approval phrase, so C.6 does not pass and the Task stays parked -
correct. See owner decision 4.

**GitLab naming (reviewer item 4).** `glActionAndLabel` maps an issue `update` to
`"synchronize"` (gitlab.go:203-208) and short-circuits a label diff to
`labeled`/`unlabeled` FIRST (gitlab.go:192-201). So the I2 handler runs on: GitHub
`issues` action=edited; GitLab Issue Hook action=`synchronize` AND action=
`labeled`/`unlabeled`. It is gated `!IsPR` to separate it from M1's MR
synchronize. The `labeled`/`unlabeled` path MUST also run the body/title diff,
because a combined body+label edit surfaces as `labeled` (the label diff wins the
switch). Keying the reaction on the actual mirror DIFF, not the action string, is
what gives GitHub/GitLab parity across their divergent action vocabularies.

### I1 - inline review-thread reply

The review pod posts inline findings; a human replies on that line. GitHub
delivers this as `pull_request_review_comment` (distinct from `issue_comment` and
`pull_request_review`), which the parser drops to `Kind:"other"` (github.go:113).
GitLab delivers inline diff replies as ordinary Note Hooks, which `glNoteEvent`
already maps to `mr_comment` (gitlab.go:147) - so GitLab is ALREADY covered and
only GitHub needs the parse.

Add a GitHub `pull_request_review_comment` case building an MR comment event
(`IsComment=true`, `IsPR=true`, `Number` = PR number, `CommentBody` =
`comment.body`, `CommentID` = `comment.id`), routed to `deliverPendingEvent`. It
is a plain comment, NOT a verdict: it mirrors + queues, never drives a stage
transition. `action=created` acts; edited/deleted ignored. The GitHub/GitLab
asymmetry (GitHub splits the event, GitLab funnels all notes) is noted, not
papered over.

### I4 - human reply to a parked(no-re-entry) task (BLOCKING-2 fix)

Parked stages with no F.6 re-entry (`stage-deadline`, `review-loop-exhausted`,
`review-post-refused`, `implement-declined`, `admission-starved`,
`fold-adoption-unverified`, `doc-timeout`, `handoff-stalled`,
`agent-contract-mismatch`, `object-too-large`): today a reply lands in
`pendingEvents`, `Unpark` returns the default no-re-entry, and the event is
discarded at `parkRetention` (up to 7d of silence ending in the human's words
dropped). (`declined`/`false-positive` are `rejected`, not parked - they take the
I3 reopen path.)

**Do NOT add a re-entry edge.** Re-entering `implementing` from
`parked(review-loop-exhausted)` escapes `maxReviewRounds` one reply at a time -
exactly the bypass WS1-A's F1 is closing. Instead a human reply triggers an
immediate, gate-respecting fresh start.

**Two rev-1 mechanisms were inverted.** (a) Routing through `releaseTerminal`
stamps `TataraParkedLabel` (`notifyTerminalIssue`, reaper.go:617), and
`MintStage` checks that label FIRST (sweep.go:189-191), so the fresh Task lands
`parked(backlog-sweep)` with zero pods - the human must reply TWICE. (b) A bare
ownerRef drop leaves the issue in `Task.Status.IssueRefs`, so when the old Task
ages out its `reapParked -> releaseTerminal -> ownedIssues` reads the STALE
IssueRefs, finds the now-OPEN issue owned by the FRESH clarify Task, and posts
"tatara has stopped working this issue... Comment to pick it back up" AND
re-stamps `TataraParkedLabel` on an actively-worked issue (reaper.go:592-627).

Fix: the shared `SeverIssueFromTask` op in ORPHAN mode, leader-side:
1. Webhook appends the reply as a pending event (existing path) AND mirrors it
   onto the issue thread (`AppendCommentToMirror`) - both safe on any replica.
2. The leader-only Task reconcile (fires on the pending-event append; the reaper
   is the backstop) detects a non-backlog `parked(no-re-entry)` Task carrying a
   fresh non-bot pending event and runs `SeverIssueFromTask(Orphan)`: clear the
   issue from the old Task's `IssueRefs` (this is what kills defect-(b)'s spurious
   comment + label re-stamp - `ownedIssues` no longer walks it), drop the
   still-OPEN CR's ownerRef, and REMOVE the `tatara-parked` forge label if the
   mirror shows it present (operator-on-promotion, sweep.go:189-191). NEVER stamp
   the label, NEVER call `notifyTerminalIssue`.
3. Immediately call the shared `MintForItem` funnel (leader-side, idempotent) for
   the now-orphan OPEN issue, **building the `ForgeItem.Issue` from the MIRROR CR
   so its `Comments` end with the human reply** - `MintStage` runs with
   `webhookOriginated=false`, so ACTIVE-vs-parked is decided by
   `humanHasLastWord(iss.Comments)` (sweep.go:309). This is a hard dependency:
   step 1's `AppendCommentToMirror` MUST have landed the reply as the mirror's
   last non-bot comment before this call (the reconcile reads the same client
   that append committed through; if cache-lagged, the leader re-reconciles and
   completes on the next pass). No `tatara-parked` + human-last-word -> triaging
   (ACTIVE). One reply -> one active `clarify` Task.

The triggering reply is preserved because it lives in the ISSUE MIRROR thread,
which the fresh clarify agent reads in full - it does NOT depend on the old
Task's `pendingEvents`. The old dead-end Task, now owning ZERO issues, ages out;
its bot PR (if any, e.g. review-loop-exhausted) is closed by its OWN terminal
reap's `closeOwnMRs` - which is why the sever touches only the issue side, never
`deleteReapedTask`s the old Task (that would cascade-delete the bot-MR mirror
without closing the forge PR). The transient window where the old exhausted PR
and the fresh Task's future PR coexist is accepted. Gate safety: fresh clarify ->
C.6, no re-entry edge.

This aligns with WS1-A's `ParkedFromStage` model: WS1-A decides WHICH parks
re-enter and where; WS3-I4 defines what a reply to a genuinely-no-re-entry park
does (fresh gated re-mint via `MintForItem`, never a smuggled re-entry). See
owner decision 2.

### I5 - deploy failure surfacing (with a distinct marker)

Today a stuck deploy is silent until the full `parked(deploy-timeout)` retry
cycle ends in the generic reaper `failed(deploy-blocked)` comment. A well-managed
team says "heads up, the deploy is stuck" at the FIRST timeout.

On the first entry into `parked(deploy-timeout)` (leader-only deploy stage /
reaper), enqueue ONE `Issue.status.pendingComments` entry on each owned open
issue: "Deployment of `<repo>` has not completed after `<budget>`; retry
`<deployReentries>`/`<maxDeployReentries>`." Repo attribution comes from
`mergeOrder` / the MR that did not reach `deployedAt`. Reuses the existing
`pendingComments` drain; no agent spawned; provider-agnostic.

**Distinct marker (reviewer item 5).** `enqueueRefireComment` is a webhook
`*Server` method keyed on a single dedicated `Issue.Status.LastRefireCommentAt`
field (server.go:789). The I5 producer is LEADER-side and MUST use its own
enqueue plus its OWN cooldown marker (a new field, e.g.
`LastDeployTimeoutCommentAt`, or a distinct annotation) - sharing
`LastRefireCommentAt` would clobber the incident-refire cooldown on an issue that
is both an incident tracker and deploy-blocked. Both write `pendingComments`;
they must not share the timestamp field. The terminal `failed(deploy-blocked)`
keeps its existing reaper comment.

### M1 - synchronize during reviewing

A human pushing to the agent's branch mid-review should not restart the review:
that needs a webhook-goroutine stage mutation, burns one review pod per push
(humans push repeatedly), and duplicates the head-move machinery that exists.
`merging` re-reads the LIVE head and bounces `merging->reviewing` on a real move
(`HeadMoved`, capped by `maxHeadMoveReentries`, stage.go:715), and the merge is
head-pinned (`expectedHeadSHA` -> `ErrHeadMoved`) so it cannot land against a
moved head. Correctness is guaranteed at merge time.

So on `synchronize`/MR-update for an owned MR (Kind=="mr" && IsPR), the webhook
only refreshes the mirror head/CI on demand (safe MIRROR write) so the reviewing
agent's next `scm_read(kind=mr)` sees the new head. No stage transition; review
restart explicitly declined. GitHub `pull_request` action=synchronize; GitLab MR
Hook action=synchronize (its map of `update`). Both refresh the mirror. This
shares the "strong vs weak signal" reasoning with M2.

### M2 - dismissed approval / unapproved MR

Ignored, documented. A dismissal is a weak, often-administrative signal; rewinding
`merging`->`reviewing`/`implementing` off it from the webhook goroutine is the
WS1-A F1/F2 rewind-shipped-work hazard. The strong stop signals are already
handled: `changes_requested` re-enters implementing on an UNMERGED MR, and closing
the issue (I3) or the PR terminates the Task. GitHub `pull_request_review`
state=dismissed and GitLab MR action=unapproved both fall through to ignored.

### M4 - review on an owned MR whose Task Get returns NotFound

`handleReview` currently accepts "ignored" without folding when the owning Task
name no longer resolves (reaped mid-flight, server.go:349-351) - the signal is
lost. Fix: call `deliverPendingEvent` on that branch before `accept`, matching
every other fold branch, so the review is mirrored + queued and the sweep
re-adopts. One line, within the existing any-replica-safe append path.

### M5 - approved review with empty commit SHA (short-circuit first)

`ApplyReviewApproval` stamps `ReviewedSHA` only when `reviewCommitSHA != ""`
(review_apply.go:104). GitHub's numeric review always carries `commit_id`;
GitLab's synthesized approval can carry an empty `last_commit.id`, leaving
`ReviewedSHA` stale and later firing a `merging->reviewing` head-moved bounce
toward `failed(head-moving)`.

Fix (reviewer item 6): resolve the effective SHA at the TOP of
`ApplyReviewApproval`, BEFORE the `FitMergeRequest` loop that clears
`PendingReview`. If `reviewCommitSHA == ""`, fall back to the owned MR's mirror
`Status.HeadSHA` (already synced - no extra forge call). If that is ALSO empty,
return `(false, nil)` and fold to the comment path BEFORE clearing any
`PendingReview` - otherwise the approval half-applies (PendingReview cleared, Task
not advanced), which is worse than folding.

## Also-ignored inventory (explicitly accepted)

| Event | Decision | Rationale |
|---|---|---|
| issues.unlabeled | accept-ignore | removing a label is not a stop signal; closing the issue is (I3) |
| issues.labeled (non-trigger, non-projection) | accept-ignore | no lifecycle meaning; mirror refreshes at cadence |
| issues.labeled == configured trigger label (orphan) | REACT (small, in-scope) | mint via `MintForItem` so a human adding the trigger label spawns work now; MUST verify `ChangedLabel` equals the project's configured trigger label, skip bot actors, and exclude the operator's own approved/declined projection labels (issue_controller.go:145-161), else a lifecycle-projection write self-triggers a mint |
| pull_request.edited (PR body/title) | accept-ignore | not a conversation signal; mirror converges at cadence |
| pull_request.ready_for_review (draft->ready) | accept-ignore | review Task is minted on MR-open regardless of draft; draft-skip is a separate policy (out of scope) |
| pull_request.closed (not merged, owned) | REACT via mirror refresh | leader merge/review reconcile finds the MR closed and converges; no new stage edge |
| pull_request merged out-of-band (owned) | REACT via mirror refresh | `merging` already treats `Status.State=="merged"` as done (merge.go:246); refresh lets it converge to deploying |
| push (non-default branch) | accept-ignore | only default-branch push triggers re-ingest (existing `handlePush`) |

## The sever operation (shared root-cause fix)

I3 and I4 both need to detach an Issue from a Task without leaving the split
state the reviewer found: the CR side (ownerRef) and the Task side
(`Status.IssueRefs`) and the reaper's bookkeeping must move together, or the
reaper walks stale refs (defect 2) and orphaned CRs leak (defect 1). Both use ONE
operation instead of two bespoke partial severances.

`SeverIssueFromTask(ctx, c, task, issueName, mode)`, leader-only, `mode ∈
{DeleteCR, Orphan}`:

1. **Task side FIRST.** Remove `issueName` from `task.Status.IssueRefs`
   (`RetryOnConflict` status update). Ordered first so the worst crash-state
   after it is "CR still owner-reffed but not listed by the Task": the CR keeps a
   valid controller owner (no B.2-rule-5 zero-owner violation), `ownedIssues`
   skips it (not in IssueRefs, so NO spurious terminal comment / label re-stamp),
   and the leader re-reconciles to finish step 2 idempotently.
2. **CR side:**
   - `DeleteCR` (I3): `Delete` the Issue CR. It is a rebuildable mirror; the
     reopen mint re-creates it via `SyncIssue`. This is the leak fix - the CR is
     gone, never an un-cascadable orphan. Crash between steps 1 and 2 is benign:
     the CR still carries the old Task's ownerRef (no leak either way), and the
     `IssueReconciler` re-sever completes the DeleteCR PROMPTLY - it detects a
     closed CR still owned by a `rejected(issue-closed)` Task and re-runs the
     DeleteCR on the next reconcile, so prompt reopen is restored on a controller
     cadence, NOT deferred to the Task's eventual `RejectedRetention` delete.
   - `Orphan` (I4): `Get` the CR, `dropOwnerRef(task)` (both plain+controller,
     which `RepairZeroController` tolerates as a no-op, own.go:165-167), and if
     `Status.Labels` shows `TataraParkedLabel`, `RemoveLabel` it on the forge
     (operator-on-promotion). The CR is now the orphan the fresh `MintForItem`
     re-adopts (`ownIssueForTask`). Crash between steps 1 and 2: the CR is still
     owned by the old Task, `IsOrphanIssue` stays false, the re-mint defers, and
     the leader re-reconciles to complete the drop - self-healing, no leak.
3. **No `AnnTerminalReleased` stamp, no `deleteReapedTask`.** The op detaches only
   this ONE issue; the old Task keeps its OTHER artifacts (MRs) and reaps them
   through its normal terminal path. Stamping `AnnTerminalReleased` (reaper.go:
   516-518) would wrongly short-circuit the old Task's bot-PR close too.

## Stage-machine changes

- New reason `ReasonIssueClosed` in the F.5 closed set (`stage.go` `Reasons` +
  `reasonSet`). `Unpark` needs no change - `issue-closed` is a `rejected` reason
  and `rejected` has no exits.
- New `Transitions` edges: `rejected(issue-closed)` from `triaging`,
  `brainstorming`, `clarifying`, `investigating`, `refining`, `approved`,
  `implementing`, `reviewing`, `merging` (9 rows, `deploying` excluded).
  Table-driven, a test asserts each is `Legal` and that `deploying`/terminals are
  NOT. (Zero edges if owner decision 1 picks `failed`.)
- No change to `LegalFor` guards, `reviewGateOpen`, or the kind=review guard.
- `SeverIssueFromTask` (shared) is called by `ApplyIssueClosedStop` (DeleteCR)
  and the I4 leader path (Orphan) - the single root-cause fix for the CR/IssueRefs
  split state.
- I4 sever(Orphan) + `MintForItem` (leader-side); I5 first-`deploy-timeout`
  `pendingComments` enqueue with a distinct marker.

## New / changed components

- `internal/scm/github.go`: parse `pull_request_review_comment` (I1); route
  `issues` action=edited (I2, reaction keys on mirror diff).
- `internal/scm/gitlab.go`: no new parse for I1/I2 (Note Hook + Issue-Hook
  `synchronize`/`labeled` already carry them); ensure the MR-update path exposes
  head/CI for M1.
- `internal/webhook/server.go`: route edited/inline-reply/synchronize/PR-closed
  to mirror refresh + `deliverPendingEvent`/`AppendTaskEvent`; M4 fold;
  trigger-label orphan mint (guarded); on-demand mirror refresh helper.
- `internal/webhook/pending_events.go`: `issue_edited` event kind; the edit path
  appends WITHOUT `driveCommentUnpark` (leader drives the unpark).
- `internal/controller/sever.go` (new): `SeverIssueFromTask(DeleteCR|Orphan)` -
  the shared CR-side + IssueRefs + label + ordering op used by I3 and I4.
- `internal/controller/issue_apply.go` (new): `ApplyIssueClosedStop` (I3, calls
  `SeverIssueFromTask(DeleteCR)`).
- `internal/controller/issue_controller.go`: detect owned + closed + live-stage
  -> `ApplyIssueClosedStop`; detect edited-body -> leader-side unpark drive.
- `internal/controller/reaper.go` / task reconcile: I4 sever(Orphan) +
  `MintForItem` built from the mirror CR (comments for `humanHasLastWord`).
- `internal/controller/deploy_stage.go` (+ Issue status field): I5 first-timeout
  comment with a distinct cooldown marker.
- `internal/controller/review_apply.go`: M5 short-circuit before the MR write.
- `api/v1alpha1`: `ReasonIssueClosed`; `Issue.Status.LastDeployTimeoutCommentAt`
  (I5 marker). `make generate manifests` (reason is a string const; the Issue
  status field is a CRD change).

## Testing

- Stage table: each new `rejected(issue-closed)` edge is `Legal` from its source
  and NOT from `deploying`/terminal; `ReasonIssueClosed` is a valid F.5 reason;
  `Unpark` returns no re-entry for it.
- I3: owned issue closed in each live stage -> `rejected(issue-closed)`, pod
  deleted, Issue CR DELETED + IssueRefs cleared (assert no leaked CR, no ongoing
  mirror sync), bot PR closed, human PR untouched; closed during `deploying` ->
  ignored (no rewind); operator's own C.4 close -> no false stop; Task already
  parked when close lands -> stop refused, parked reaper handles it; REOPEN ->
  `issueCR`=NotFound -> `IsOrphanIssue(nil)` true off the live event -> fresh
  ACTIVE mint after the stale-terminal delete + `SyncIssue` re-creates the mirror
  (assert not 24h).
- Sever op: `SeverIssueFromTask(DeleteCR)` leaves no CR and no IssueRefs entry;
  `Orphan` leaves an ownerless CR + no IssueRefs entry + no `tatara-parked`;
  crash-between-steps (IssueRefs cleared, CR step not run) leaves a validly-owned
  CR the reaper skips (no spurious comment) and a self-healing re-reconcile.
- I2: body edit -> mirror body updated + `issue_edited` event; label-only update
  -> mirror refresh, NO event (body-diff gate); combined body+label (GitLab
  `labeled`) -> event still fires; unpark asserted driven leader-side, NOT from
  the webhook goroutine.
- I1: GitHub `pull_request_review_comment` -> mirror + pending event, no
  transition; GitLab inline note already covered (regression guard).
- I4: reply to each no-re-entry park -> sever(Orphan) + `MintForItem` -> ONE
  active Task from ONE reply (assert no `tatara-parked` on the fresh mint, assert
  `humanHasLastWord` fired off the mirror comments, assert the reply survives in
  the mirror thread); assert the old Task's IssueRefs is cleared so its later
  terminal reap posts NO issue comment and does NOT re-stamp the label on the
  actively-worked issue; the old Task's bot PR still closes via its own reap;
  reply to a re-entry park (awaiting-human) still unparks (WS1-A boundary intact).
- I5: first `deploy-timeout` -> exactly one issue comment; subsequent retries ->
  no duplicate (own cooldown); incident-refire on the same issue -> its
  `LastRefireCommentAt` cooldown unaffected (no clobber); terminal
  `deploy-blocked` comment unchanged.
- M1: synchronize -> mirror head refreshed, stage unchanged; merge head-pin still
  bounces a real move.
- M2: dismissed/unapproved -> ignored (no transition), asserted.
- M4: owned MR, Task NotFound -> `deliverPendingEvent` called (not silent drop).
- M5: approval with empty SHA -> `ReviewedSHA` falls back to mirror head; empty
  mirror head -> no advance AND `PendingReview` NOT cleared (assert no
  half-apply).
- trigger-label: mint only when `ChangedLabel` == configured trigger label,
  non-bot actor, and NOT an approved/declined projection label.
- Concurrency: every new webhook reaction asserted to be mirror-write or
  pending-append only (no stage `Enter`, no ownerRef drop, no unpark from the HTTP
  goroutine); the leader mutations asserted reachable only through
  reconciler/reaper entry points.

Repo build gate: `mise exec -- make generate manifests test lint build`.

## Resolved decisions

1. Stop-edge target is `rejected(issue-closed)`, not a re-entry, not `parked`
   held-open (owner intent: cancel = stop + clean up the PR now).
2. `deploying` is excluded from the stop edge: merged/deploying work is not
   rewound by a late issue close (consistent with "a merged MR is finished").
3. No new webhook-goroutine stage mutation; leader-only for all WS3 mutations
   including the I2 edit-driven unpark (deliberate answer to #353 / F6-1).
4. No re-entry edge into any gated stage; re-engagement of a dead-end flows
   through a fresh `clarify` Task and the C.6 grammar (#294 preserved).
5. I3 reopen and I4 resume both work on the FIRST human action, via ONE shared
   `SeverIssueFromTask` op that moves the CR side, `Status.IssueRefs`, and labels
   together (no split state): I3 DeleteCRs the closed mirror (no leak; reopen
   mints off `IsOrphanIssue(nil)` + live `open`); I4 Orphans + clears IssueRefs
   (no spurious terminal comment/label re-stamp) so the fresh mint lands ACTIVE
   via `humanHasLastWord` on the mirror comments.

## Open decisions for owner sign-off

Listed with a recommendation and the one-line tradeoff each.

1. **Issue-close stop target.** Recommend `rejected(issue-closed)` (9 new table
   edges, semantically clean, immediate PR cleanup). Tradeoff: the zero-edge
   `failed(issue-closed)` needs only the new reason but labels a human
   cancellation as a platform failure and inflates the failed metric.
2. **No-re-entry reply behavior (I4).** Recommend the sever(Orphan) + fresh
   `clarify` re-mint (one reply lands one active Task, gate-safe). Tradeoff: it
   restarts from clarify and the old exhausted bot PR is closed by the old Task's
   reap; the lighter alternative is an ack-only comment that leaves re-engagement
   to the existing ~7d reap and needs a second human reply.
3. **Dismissed approval (M2).** Recommend ignore + document. Tradeoff: a
   maintainer who expects "dismiss = stop the merge" must instead use
   request_changes or close; rewinding on dismiss reopens the WS1-A
   rewind-shipped-work hazard.
4. **Edit-driven unpark (I2).** Recommend letting an `issue_edited` event drive
   `awaiting-human`/`backlog-sweep` unpark, LEADER-side. Tradeoff: a trivial typo
   edit could re-engage a parked Task; the alternative delivers the edit on next
   turn but does not itself unpark.
5. **Trigger-label immediacy.** Recommend minting on a webhook trigger-label add
   to an orphan issue (reactivity parity with issues.opened). Tradeoff: slightly
   widens the webhook mint surface (guarded against bot actors and projection
   labels); the alternative leaves label-driven intake to the sweep.
