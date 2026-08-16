# Queued, event-driven adoption of dependency upgrade merge requests

Date: 2026-08-16
Status: approved

## Problem

An adoptable dependency merge request (a Renovate MR matching
`Spec.UpgradePolicy.adoptBranchPrefix` and an adoptable author) is adopted ONLY
inside a sweep pass. The webhook that learns about it first does not mint and
does not enqueue: it stamps `tatara.dev/sweep-requested` on the Repository
(`internal/webhook/server.go:802`, `requestRepoSweep`) to pull the next sweep
slot forward, and then discards the signal.

That annotation is ONE SHOT AND SELF-CONSUMING. The pass it pulls forward
computes `adoptHeadroom = maxOpenUpgrades - openUpgradeLaneCount` once for the
whole pass (`internal/controller/sweep.go:1376-1399`), adopts up to that many
merge requests oldest-first, and skips the remainder with
`SweepSkipUpgradeHeadroom`. The skipped merge request's own annotation was
cleared by that same pass. Nothing re-drives it. It waits for its repository's
next `issueScan` slot, which on the `infrastructure` project is
`0 */4 * * *` - up to four hours.

### Measured

One Renovate run on `szymonrychu/charts`, 2026-08-16:

```
06:35:19  mr_sweep_requested      charts!1024
06:35:21  mr_sweep_requested      charts!1025
06:35:48.064  mr_sweep_requested  charts!1026
06:35:48.430  sweep_skip_pr       charts!1026  upgrade_headroom_bound
06:35:48/49   sweep_adopt_upgrade_mr  charts!1024, charts!1025
06:41:09      mt-u-charts-1024 state=done       (6 minutes end to end)
07:00         charts!1026 still unadopted, 0 live upgrade lanes,
              1024 and 1025 already MERGED
```

The same shape hit `szymonrychu/containers` !1280, !1281 and !1283 at 05:07-05:08
the same morning.

So the lanes sat empty for hours while queued-in-spirit work existed, and the
third merge request of a three-MR Renovate run was still untouched after its two
siblings had been reviewed, approved and merged. The cap behaved as a rate
limiter (2 per 4 hours per repository) rather than as a concurrency bound.

### Root cause

Three independent facts compose into the defect:

1. `internal/controller/intake.go:192-199` - `MintForItem` explicitly declines
   `PRAdoptUpgrade` on the webhook path, because a webhook delivery holds no
   headroom budget.
2. `internal/controller/sweep.go:1376` - headroom is computed once per sweep
   pass, and a pass is gated on the `issueScan` cron cadence.
3. Nothing observes an upgrade lane freeing. `projectControllerBuilder`
   (`internal/controller/project_controller.go:996-1027`) has no `Task` edge,
   and the sweep's next-due time is never shortened by a Task reaching a
   terminal state.

The signal is not queued anywhere. It is spent and dropped.

## Goal

Webhook deliveries for adoptable dependency merge requests become durable queue
entries. When more are open than the project can run at once, the surplus waits
in the queue and is admitted the moment a slot frees - not at the next sweep
tick. The sweep becomes a pure backstop for deliveries missed while the operator
was offline.

## Decisions

Four decisions were taken during design. Each is recorded with its consequence
because each changes observable platform behaviour.

### D1 - Adopted merge requests are bounded by the general pool, not by a per-kind cap

`maxOpenUpgrades` no longer bounds adoption. Adopted upgrade Tasks become
ordinary queue citizens bounded by `Project.QueueCapacity()`
(`MaxConcurrentAgents`) and the `MaxLivePods` mint ceiling, exactly like every
other kind.

Consequence, stated plainly: on the `infrastructure` project
(`maxConcurrentAgents: 7`, `maxLivePods: 6`) a Renovate batch may occupy up to
six concurrent agent pods, where today it occupies two. Adoption now competes
with issue and review work for the same pool instead of holding a private
two-lane allowance.

Rejected: teaching the dispatcher a per-kind upgrade gate. It preserves the
tuned cap but introduces a per-kind admission concept the queue does not have,
for a cap the user does not want.

### D2 - The upgrade cron keeps `maxOpenUpgrades`, counting only its own work

`maxOpenUpgrades` survives as the knob governing the upgrade CRON - the agent
that proactively hunts for dependency bumps (`internal/controller/projectscan.go:1710-1740`).
`openUpgradeLaneCount` is split so it counts only cron-minted upgrade work.

Without this split the cron would read a draining Renovate backlog as "lanes
full" and fall silent for as long as the backlog lasted, which is a behaviour
change nobody asked for and one that would be invisible until someone noticed
the cron had stopped proposing bumps.

Discriminator: a `tatara.dev/upgrade-origin: adopted` label stamped at mint on
adopted Tasks, and mirrored on the QueuedEvent so the not-yet-minted half of the
count excludes them too.

### D3 - Queued adoptions sit at priority 2

Priority 2 is the cron/sweep tier. Non-incident admission tickets - the events
that give an ALREADY RUNNING Task its next pod - are also priority 2
(`internal/controller/task_stage.go:2036-2040`; only `incident` overrides to 0).
Webhook-originated mints are priority 1.

Enqueueing adoptions at priority 1 would let a twelve-MR Renovate run leapfrog
the next stage of every NON-INCIDENT task already underway. An incident Task's
downstream tickets already rank ahead of priority 1: `task_stage.go:2032`
applies `WithPriority(0)` to EVERY stage ticket of a Task whose `Spec.Kind` is
`"incident"`, not only its investigating stage - what keys on the stage itself
(`agentKind == stage.AgentIncident`) is the queue CLASS (alert vs normal), a
separate axis from priority. `admitPool` sorts by `(EffectivePriority, Seq)`, so
those twelve would still drain before any half-done implement or review task on
a non-incident Task got its next pod, and the one-hour starvation guard would
reserve exactly one slot after an hour of that.

**Correction (whole-branch review): priority 2 does NOT decline to jump ahead of
work already started.** An earlier draft of this section said it did, and that
claim was false. `ensureTicket` allocates a ticket's `Spec.Seq` at TRANSITION
time, not at Task-creation time, and `queueOrderBefore` sorts `(priority, seq)`
ascending. Twelve adoptions enqueued at 06:35 therefore all carry a LOWER seq
than a review ticket cut at 06:36 for a Task that started hours earlier, and each
adoption that finds mint room takes a normal-pool slot ahead of it. What priority
2 actually declines is jumping ahead of priority 0 - incidents AND an incident
Task's downstream tickets alike, both covered by the same `WithPriority(0)`
keyed on `task.Spec.Kind == "incident"`. Priority 1 is a documented tier -
reserved for a human-originated webhook mint - but `grep -rn "WithPriority"`
finds exactly three call sites (adoption's two, both priority 2, and the
incident override at priority 0) and no producer sets it, so nothing today
actually competes at priority 1. That is still the right tier for adoptions and
the priority does not change: were priority 1 ever to gain a producer, it would
add the overtake of every non-incident Task's next-stage ticket REGARDLESS OF
SEQ, on top of the seq-bounded overtake priority 2 already has, and no human
waits on a Renovate bump.

The overtake is BOUNDED, which is why it stays acceptable: `liveMintBudget` caps
concurrent mints at `MaxLivePods`, so a starved ticket waits a few adopted-task
lifetimes, not indefinitely.

**Consequence of D1 nobody had written down.** `hasStarvingPriority2` already
covers every non-incident stage ticket - all priority 2, same as adoptions - so
the guard is not a NEW trigger; removing the two-lane adoption cap adds volume,
not the mechanism. What changes is that a Renovate backlog can now hold
priority-2 events in the normal pool for hours. A queued adoption older than
`starvationBudget` (1h) trips the guard, which permanently reserves ONE of the
normal pool's `QueueCapacity` slots (`internal/controller/queue_controller.go:714`;
the alert pool is separate, capped by `AlertCapacity` and drained at `:711`) for
priority 2 for as long as the backlog persists - capacity taken away from
priority-0 work by a mechanism that exists to protect the nightly doc batch, now
driven more often by dependency bumps. Accepted: it is one slot, and the guard
fires only after an hour of genuine starvation.

### D4 - The webhook keeps queued events fresh

A queued adoption may wait, and Renovate rebases merge requests constantly, so
the captured head SHA can go stale and the merge request can be merged or closed
while its event is still queued.

The webhook already receives `synchronize` and `closed`/`merged` deliveries for
these merge requests and currently uses them only to refresh mirrors. It will
also:

- on `synchronize`, refresh `headSHA`, title and body on a still-`Queued`
  adoption event;
- on `closed`/`merged`, delete a still-`Queued` adoption event.

Rejected: re-reading the merge request from the forge at admit time. It is the
most authoritative option but the dispatcher makes zero SCM calls today, and
adding a token/secret dependency plus a network failure mode to the hot
admission path is a poor trade for a freshness guarantee the webhook can give
for free.

Rejected: trusting the snapshot outright. It burns an agent pod on merge
requests that merged while queued.

A mirror CR is not an option as the fresh source: mirrors are created by
`bindMRToTask` DURING adoption, so an unadopted merge request has none.
Verified on the live cluster - `mr-charts-1026` did not exist while
`mr-charts-1024` and `mr-charts-1025` did.

## Design

### Data flow

```
Renovate opens MR
  -> webhook handleMROpened, AdoptionCandidate == true
  -> EnqueueEvent(dedupKey "adopt-upgrade|<repo>|<number>",
                  class normal, priority 2,
                  payload.AdoptedUpgrade = PRRef snapshot)
  -> event sits Queued while the pool is full
  -> dispatcher admit pass
  -> MintAdoptedUpgradeTask(...)
  -> Task terminal -> pool inflight drops -> next queued adoption admits
```

The immediacy requirement needs no new trigger. `DispatcherReconciler` already
watches `Task` (`internal/controller/queue_controller.go:1069`, `mapTaskToQE`)
and every `doReconcile` runs a full admit pass across the whole queue, so a
Task's terminal write already re-evaluates admission. What was missing was never
the trigger - it was the work being in the queue at all.

### Components

1. **`internal/webhook/server.go`** - the `AdoptionCandidate` branch
   (`server.go:802`) enqueues instead of calling `requestRepoSweep`.
   `handleMRSynchronize` refreshes a still-queued adoption event;
   `handleMRClosed` deletes one. `requestRepoSweep` itself stays for its other
   callers if any; the adoption call site is the only one replaced.

2. **`api/v1alpha1/queuedevent_types.go`** - new optional typed payload field
   `AdoptedUpgrade *AdoptedUpgradeRef` carrying the PRRef snapshot: `Number`,
   `Title`, `Author`, `HeadSHA`, `HeadBranch`, `Body`, `Labels`, `Repo`,
   `HeadRepo`. Required because `QueuedTaskBlueprint` carries no `Source`,
   `InitialState` or PR fields, and `MintAdoptedUpgradeTask` needs all of them.
   `ValidateQueuedEventSpec` gains the rule that `AdoptedUpgrade` is mutually
   exclusive with the admission-ticket shape (`AgentKind`+`TaskRef`).

3. **`internal/controller/queue_controller.go`** - the mint branch routes
   `payload.AdoptedUpgrade != nil` to `MintAdoptedUpgradeTask` rather than the
   generic `BuildTaskFromQueuedEvent`. This keeps mirror binding,
   `AnnTakeoverHeadBranch`, `MergeOrder` and the adopted significance floor in
   the single mint funnel that already owns them. The minted Task is stamped
   with `LabelQueuedEvent` and `LabelMintedBy` so `mapTaskToQE` observes its
   terminal write and `reconcileDone` can garbage-collect the spent event -
   without these stamps the event would be admitted and never reaped.

4. **`internal/controller/sweep.go`** - `adoptHeadroom` and the
   `SweepSkipUpgradeHeadroom` skip are removed. The `PRAdoptUpgrade` arm
   enqueues under the same dedup key as the webhook, so a duplicate collides on
   `AlreadyExists` and burns no sequence number.

5. **`internal/controller/projectscan.go`** - `openUpgradeLaneCount` excludes
   adopted work on both halves (Tasks and not-yet-minted QueuedEvents), keyed on
   the `tatara.dev/upgrade-origin` label. The cron path is otherwise untouched.

6. **`internal/controller/upgrade_adopt.go`** - `MintAdoptedUpgradeTask` stamps
   `tatara.dev/upgrade-origin: adopted`.

### Error handling

- Enqueue failure on the webhook returns 500 so the forge redelivers, matching
  the existing `MintTombstoneDeleted` handling at `server.go:835-842`.
- A merge request merged while the operator was offline is caught twice: the
  sweep no longer lists it among open merge requests, and the `AdoptUpgradeMR`
  predicates still apply at mint time.
- Enqueue is never capacity-gated. The queue IS the buffer; gating the producer
  is precisely the defect being removed.
- A queued event whose repository or project is deleted is garbage-collected via
  the existing `Project` owner reference.

### Observability

- `MintOutcomeTotal{kind="upgrade"}` continues to count the mint outcome,
  unchanged, now incremented from the dispatcher path.
- `SweepSkippedTotal{reason="upgrade_headroom_bound"}` loses its producer. The
  reason is removed from the `sweepSkipReasons` seeded set in
  `internal/obs/sweep_metrics.go` so a dead series is not scraped forever.
  Any alert or dashboard referencing it in `tatara-observability` must be
  checked in the same change window.
- A counter for adoption events enqueued and for queued events dropped on
  merge/close, following the existing label conventions
  (`activity="upgrade"`, `kind="upgrade"`).

## Testing

- Table tests for the enqueue predicate on the webhook path, and for the dedup
  collision between a webhook enqueue and a sweep enqueue of the same merge
  request.
- Envtest for the sequence that actually broke: three adoptions enqueued against
  a saturated pool, asserting the third is admitted on the terminal write of the
  first rather than on a sweep tick.
- Unit tests for synchronize-refresh and closed-delete against a still-queued
  event, including the case where the event has already been admitted (must be
  left alone).
- Regression test that `openUpgradeLaneCount` ignores adopted Tasks and adopted
  QueuedEvents, so the cron is not suppressed by a Renovate backlog.
- Regression test that a merge request already owned by a live Task does not
  produce a second queued event.

## Out of scope

- The `AdoptUpgradeMR` / `ClassifyPR` predicates deciding WHAT is adoptable.
- Agent behaviour once a merge request is adopted.
- The upgrade cron's schedule and its `maxOpenUpgrades` value.
- Any change to `tatara-observability` alert rules beyond checking for a
  reference to the retired skip reason.
