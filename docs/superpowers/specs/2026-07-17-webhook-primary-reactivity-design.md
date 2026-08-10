# Restore webhook-primary reactivity in the operator

Date: 2026-07-17
Topic: Make SCM webhooks the primary, low-latency trigger for operator
reactions; demote the periodic sweep to an idempotent backstop. Wire the
human `pull_request_review` path. Remove dead cron plumbing.

## Executive summary

The `feat!: task-centric redesign (#318)` regressed the platform's reactive
contract: it made the periodic "B.4 sweep" the *sole* intake for first-contact
events. Webhook handlers now mint nothing for a new issue/MR - they only stamp
a liveness annotation or append a pending-event to an already-owned Task. As a
result, a brand-new issue (e.g. issue #353) does not spawn an agent until the
next sweep tick, breaking the invariant: *every external event ingested via
webhook must immediately result in the operator doing something.*

This change makes the webhook the primary minter and the sweep an idempotent
backstop, coordinated not by a lock but by natural-key idempotency (the
Flux/ArgoCD pattern, and the controller-runtime FAQ's prescribed answer). It
also wires the human `pull_request_review` path (dropped entirely today) and
removes two confirmed-dead cron mechanisms.

## Root cause

- `internal/webhook/server.go:249-269` - `handleForgeItem` doc: "It MINTS
  NOTHING. The B.4 SWEEP is the only intake... A webhook that minted its own
  Task would race the sweep for the same (repo, number) natural key and produce
  a second owner."
- `internal/webhook/server.go:297-334` - `handleIssueOpened` only stamps
  `tatara.dev/webhook-originated`; the actual Task mint waits for the sweep.
- `internal/controller/projectscan.go:1116-1122` - the sweep runs on the
  `issueScan` cron cadence and is the sole intake since the #318 cutover.

The #318 decision to make the sweep sole-intake was deliberate: it avoided a
dual-intake race where webhook and sweep both `Create` a Task for the same
natural key and produce two owners. The correct fix is not to delete the sweep
but to make the collision safe, so the webhook can mint immediately.

## Mechanism decision

**Chosen: idempotent natural-key dedup (no lock).** Both the webhook and the
sweep funnel into the same mint, keyed by a deterministic natural key
(project, repo, kind, number). Whichever fires first mints; the other's
`Create` collides on `AlreadyExists` at the apiserver and is swallowed with
`client.IgnoreAlreadyExists`. Race-free across leader failover, multiple
replicas during rollout, and informer cache-lag - the apiserver is the
arbiter, not a cached `List`.

**Rejected: in-process wait-lock** ("cron waits while a webhook mint is in
flight"). Only coordinates within one process; breaks under leader failover
and rolling deploys; adds a stuck-lock failure mode. Research (controller-
runtime FAQ, kubebuilder good-practices, Flux/ArgoCD source) is uniform that
idempotency, not locking, is the correct primitive here.

Supporting research: `docs/superpowers/research/` brief of 2026-07-17 (local,
gitignored). Key citations: controller-runtime FAQ (deterministic names +
`IgnoreAlreadyExists`), `k8s.io/client-go/util/workqueue` "Stingy" dedup,
Flux `notification-controller` annotate-to-poke, ArgoCD `AnnotationKeyRefresh`.

## Scope

In scope:
1. Unified reactive intake (issue open, issue comment, MR open, MR comment).
2. Race-safe natural-key dedup on the mint path.
3. Human `pull_request_review` handling (changes_requested, approved,
   commented).
4. Dead-cron cleanup (`cdScan`, `healthCheck`).

Out of scope (noted, not dropped silently):
- Documentation-on-merge reactivity. Owner kept documentation on the scheduled
  per-project batch; it is intentionally NOT treated as a violation here.
- Any change to the #294 issue approval grammar (`approval_grammar.go`).

## Components

### 1. Shared intake funnel

Extract the sweep's per-item "classify + mint" decision into one function used
by both the sweep loop and the webhook handlers. Today this logic lives in
`internal/controller/sweep.go`:
- `IsOrphanIssue` (sweep.go:136-146)
- `ClassifyPR` (sweep.go:405-427) -> `PRReview` / `PRAdopt`
- `MintStage` / `MintReviewStage` (sweep.go:192-203, 452-461)
- `SweepIssueKind = "clarify"` (sweep.go:100), `SweepReviewKind = "review"`
  (sweep.go:105)

New shared function (working name `intake.MintForItem`) takes
(ctx, project, repo, forgeItem) and returns the mint decision, then calls
`queue.EnqueueEvent`. The sweep loop calls it per item on its cadence; the
webhook handlers call it immediately per delivered event. Single source of
truth for "what Task does this event produce."

### 2. Race-safe dedup

Current dedup (`internal/queue/enqueue.go:134-163` `dedupExists`) reads the
informer cache (label-matched `List` over QueuedEvents/Tasks/Issues). Under
load the cache lags a just-created object, so a webhook mint followed by a
sweep tick can double-create (the failure class #348/#352 patched).

**Reconciliation with real code:** the sweep mints **Tasks directly**
(`mintTaskForIssue`/`mintReviewTaskForPR`), synchronously owning the Issue/MR
CR at mint time; it does NOT route first-contact intake through QueuedEvents.
QueuedEvents are ephemeral (GC-deleted by `reconcileDone`) and a
`parked(backlog-sweep)` Task must own its Issue CR at zero cost, so the durable
natural-key anchor is the **Task itself**, not a QueuedEvent. Idempotency is
therefore realized on the direct Task create, not by rerouting through the
queue (the incident/ticket QueuedEvent path is hardened too, but the intake
path is the Task).

Harden to natural-key idempotency on the Task create:
- Give the Task a **deterministic name** `IntakeTaskName(project, repo, kind,
  number)` (DNS-safe, length-bounded), so a concurrent second `Create` for the
  same natural key collides on `AlreadyExists`.
- On the mint path, do the existence check with a **live read**
  (`mgr.GetAPIReader()`, non-cached) rather than the cached `List`, shrinking
  the window before the deterministic-name collision even applies.
- Swallow the collision with `client.IgnoreAlreadyExists`; treat "already
  exists" as the normal, expected backstop outcome, not an error. Mirror the
  dispatcher's stale-terminal handling (delete a terminal same-name Task so a
  legitimately new event after the prior Task terminated is not blocked by a
  dead name).

Contract: concurrent mints (webhook + sweep) for the same live natural key
collapse to exactly one Task.

### 3. Webhook handlers become primary minters

`internal/webhook/server.go`:
- `handleIssueOpened` (297-334): after the existing `isBotActor` /
  `IsAllowedReporter` gates, call the shared funnel to mint immediately (in
  addition to keeping the `webhook-originated` liveness stamp).
- `handleIssueComment` (345-368): for a comment on an issue that has **no
  owning Task yet** (orphan), call the shared funnel to mint. For a comment on
  an already-owned issue, keep the existing pending-event path
  (`pending_events.go` - already reactive).
- MR opened: mint the review Task for a non-bot human PR immediately via the
  shared funnel (today sweep-only). Agent-authored MRs continue to adopt into
  their existing owning Task by owner-ref.
- MR comment on an orphan MR: mint via the shared funnel; on an owned MR, keep
  the existing pending-event path.

Bot/reporter gating unchanged (`isBotActor` server.go:376-382,
`IsAllowedReporter` logins.go:39-51). The sweep code stays and becomes the
backstop; with race-safe dedup it finds "already minted" on nearly every tick.

### 4. Human `pull_request_review` path

Today `internal/scm/github.go:99-100` parses a `pull_request_review` delivery
as a plain `mr` event; the `review.state` is never read (no `Review` field in
`ghPayload` github.go:41-70), and the event falls through to
`accept("ignored")` in `handleForgeItem`.

Add:
- Parse `review.state` (`approved` | `changes_requested` | `commented`) and
  `review.id` from the GitHub payload; add the equivalent GitLab approval-event
  mapping in `internal/scm/gitlab.go`.
- Resolve the owning Task: deterministic `MergeRequestName(repo, number)` Get
  (`mergerequest_types.go:11`, used at `pending_events.go:159`), then the
  controller owner-ref -> Task (`own.ControllerOwner`, `pending_events.go:78`).
- Gate the actor on `IsMaintainer` (`api/v1alpha1/logins.go:73-83`,
  closed-by-default verified-maintainer list).
- Route by state:
  - `changes_requested` on a **Tatara-owned** MR (owning Task
    `Spec.Kind != "review"`): re-enter `StageImplementing` when the owned MR is
    **not yet merged**, via one new stage edge `merging -> implementing` (plus
    the existing `reviewing -> implementing`). Only the stages that can
    actually hold an owned unmerged MR (`reviewing`, `merging`, and `parked`
    from those) re-enter; earlier stages (`clarifying`, `brainstorming`) do NOT
    get a re-entry edge, because that would bypass the #294 C.6 approval gate (a
    security regression) - those fold to the pending-event/comment path.
    An **already-merged MR is finished** - a
    changes-requested review on it does NOT rewind the Task; it folds to the
    pending-event/comment path (not lost) and logs. The boundary is the
    MergeRequest CR's merged state (not the Task stage): merged -> finished,
    no rewind; not merged -> re-enter implementing. Terminal Tasks
    (`rejected`, `failed`, terminal `parked`) are likewise not resurrected.
  - `approved` on a Tatara-owned MR: apply the same edge the review pod's
    `approve` verdict uses - `StageReviewing -> StageMerging` (stage.go:261),
    gated on `reviewGateOpen` (all owned MRs `PendingReview == nil`). The
    actual merge still waits on CI-green + mergeability (merge.go:288-293).
    A maintainer approval is authoritative and may short-circuit a pending bot
    review (clears/substitutes for `PendingReview`) - confirmed by owner.
  - `commented`: fold into the existing pending-event/comment path (the review
    agent sees it).
  - `dismissed` / `edited` actions: ignored.
- Idempotency: dedup on `review.id + state` so redelivered or edited review
  events do not re-fire the transition.

Adopted human PRs (owning Task `Spec.Kind == "review"`, orphan-MR origin per
`ClassifyPR`) are not driven by this path - the operator only reviews them.

### 5. Dead-cron cleanup

Remove confirmed-dead plumbing (no dispatch call site; MEMORY 2026-07-13
confirms dead):
- `cdScan` / CD supervision: `CDScanActivity`, `LastCDScan`
  (`api/v1alpha1/project_types.go:351-367, 845-847`), stamp plumbing
  (`projectscan.go:45-46, 464-466`).
- `healthCheck`: `HealthCheckActivity`
  (`api/v1alpha1/project_types.go:322-341, 390-401`).

Regenerate CRDs (`make generate manifests`). Confirm no chart/values or
sample CR references remain.

## Data flow (after)

New issue (non-bot, allowed reporter):
```
webhook delivery -> handleIssueOpened -> gates pass
  -> intake.MintForItem -> EnqueueEvent(dedupKey, deterministic QE name)
     -> live-read existence check -> Create QueuedEvent
        apiserver OK -> Dispatcher admits -> Task minted -> clarify agent pod
sweep tick (later) -> intake.MintForItem (same key)
  -> Create QueuedEvent (same name) -> AlreadyExists -> IgnoreAlreadyExists -> no-op
```

Human requests changes on a Tatara-owned MR:
```
pull_request_review delivery -> parse review.state=changes_requested, review.id
  -> IsMaintainer gate -> resolve MR CR (MergeRequestName) -> owner Task
  -> Task non-terminal && Kind != review
     -> re-enter StageImplementing -> implement agent pod
  -> dedup on (review.id, state)
```

## Edge cases

- Concurrent webhook + sweep mint for the same key -> exactly one Task
  (deterministic-name collision).
- Cache-lag: sweep's cached view misses the webhook's fresh mint -> live-read
  check + deterministic name still collapse to one Task.
- Redelivered webhook (GitHub at-most-once, manual redelivery) -> dedup no-op.
- Redelivered / edited / dismissed `pull_request_review` -> `(review.id,state)`
  dedup prevents re-firing.
- Bot-authored issue/MR/comment -> `isBotActor` gate, ignored (self-loop
  guard preserved).
- Non-maintainer PR review -> `IsMaintainer` gate, ignored.
- `changes_requested` on an adopted human PR (owning Task `Kind == review`) ->
  not driven to implementing (operator only reviews it).
- Terminal owning Task + late review -> not resurrected.
- `changes_requested` on an already-merged (finished) MR -> no rewind; folds to
  pending-event/comment.

## Testing

- Table-driven unit tests on `intake.MintForItem`: each event type (issue
  open, orphan-issue comment, human MR open, orphan-MR comment) -> expected
  Task kind + mint stage.
- Concurrent-double-mint test: two goroutines mint the same natural key ->
  exactly one Task; the loser gets `AlreadyExists`.
- Sweep-after-webhook test: webhook mints, then a sweep pass over the same
  item no-ops (asserts backstop idempotency).
- `pull_request_review` routing tests: matrix of
  {changes_requested, approved, commented, dismissed} x
  {owned, adopted} x {non-terminal, terminal} -> expected transition or no-op.
- Review-event dedup test: same `(review.id, state)` delivered twice -> one
  transition.
- Dead-cron removal: `make generate manifests test lint` green; no dangling
  references to `cdScan` / `healthCheck`.

Repo hard-rule build gate: `make generate manifests test lint build`.

## Resolved decisions

1. `changes_requested` re-enters implementing only while the MR is not yet
   merged; an already-merged MR is finished and is not rewound (owner:
   "approved MR is finished MR").
2. `approved` maintainer review is authoritative and may short-circuit a
   pending bot review (owner confirmed).
