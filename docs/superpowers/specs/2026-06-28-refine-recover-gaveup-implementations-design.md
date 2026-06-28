# Refine-driven recovery of gave-up implementations

Date: 2026-06-28
Status: approved (design), pending implementation

## Problem

When an issueLifecycle implementation gives up, the Task parks
(`LifecycleState=Parked`) and the issue is left open with the
`tatara-implementation` label and a `ParkReason`. Three gaps follow:

1. `recoverOrphans` re-rolls such issues every period, but **unbounded and
   blind** - it creates a fresh Task each time (counters reset), so a
   genuinely-hard issue re-rolls forever (token burn, the historical
   reroll-loop failure mode).
2. The Task CR does **not** record that implementation gave up, nor how many
   times - there is no cross-Task attempt memory.
3. The `refine` flow (agent-driven, prompted by `internal/refine/goal.go`)
   frames in-flight implementations as hands-off and is told "Do not create
   tasks", so it never acts on a stranded implementation. Stuck issues are
   neither driven to delivery nor to closure.

## Goal

Drive gave-up implementations to a terminal outcome - delivered, closed, or
escalated to a human - with `refine` as the judgment layer and a **bounded**
auto-reroll backstop, and with the Task CR tracking the give-up.

## Scope

In scope - implementation **failure** give-ups (recoverable, may reroll):
`implement-failed` (agent-run Failed: turn error, BootCrashLoop,
AgentUnreachable, PodLost), `maxIterations`, `refused-no-explanation`,
`deadline`.

Out of scope - deliberate, intentional terminals (NOT a stuck give-up):
`refused-declined` / `already_done` (these set `tatara-declined` and are an
explicit agent decision, handled by the existing decline path). `refine` may
still close an out-of-scope issue if it is genuinely already-done or a
duplicate, but the reroll/escalation machinery here does not target them.

## Decisions (locked)

- Reroll path: **comment + bounded auto-reroll**. `refine` comments refined
  scope or closes; the operator's `recoverOrphans` performs the actual reroll,
  bounded. No new MCP tool.
- Attempt bound: **`maxImplGiveUps = 3`**. Reroll while `ImplementGiveUps < 3`;
  at `>= 3`, stop rerolling and escalate (human).
- Immediate close (regardless of count): already-delivered / duplicate /
  obsolete, decided by the `refine` agent.
- **No new label.** The counter on the Task CR drives gating; the refine
  escalation comment notifies humans; a metric surfaces blocked issues on the
  dashboard.

## Components

### 1. Give-up tracking on the Task CR

`api/v1alpha1` Task `Status` gains:

- `ImplementGiveUps int` - count of implementation attempts that have given up
  for this issue's durable lifecycle Task. Increments **once per give-up** (the
  transition into `Parked` from `Implement` with an in-scope reason),
  transition-guarded so a steady-state reconcile of an already-parked Task does
  not double-count. Semantics: the first failed attempt sets it to 1; reroll is
  allowed while `< maxImplGiveUps`, so exactly 3 attempts run, then escalate.

Const `maxImplGiveUps = 3` (operator). The counter persists across rerolls
because recovery **adopts the same durable per-issue lifecycle Task in place**
(re-enters `Implement`) rather than creating a fresh Task. The give-up reason is
already recorded in `Status.ParkReason`; the in-scope reason set above
distinguishes a recoverable give-up from a deliberate decline.

### 2. Bounded auto-reroll (`recoverOrphans`, `internal/controller/projectscan.go`)

For an open `tatara-implementation` issue whose matching lifecycle Task is
terminal (Parked) and not live (no live pod):

- `ImplementGiveUps < maxImplGiveUps` -> adopt-in-place, re-enter `Implement`,
  `ImplementGiveUps++` (one reroll). Replaces today's fresh-Task creation so the
  counter survives.
- `ImplementGiveUps >= maxImplGiveUps` -> **do not reroll**; leave the Task
  parked. The issue stays open + `tatara-implementation` for `refine`/human.

The existing in-flight guards are respected: a still-working issue (non-terminal
Task or live pod) is never rerolled.

### 3. Surface give-up to the refine agent (`task_list` MCP)

`task_list` (tatara-cli, served by the operator's MCP surface) must expose, per
listed task: `LifecycleState`, `ParkReason`, and `ImplementGiveUps`, so the
refine agent can identify gave-up issues and their attempt count. If any of
these are already returned, extend only what is missing.

### 4. Refine prompt: gave-up issues are top priority (`internal/refine/goal.go`)

Add a first-priority category to `GoalProject`. For each open issue whose
implementation has **given up** (its lifecycle Task is terminal/Parked with an
in-scope give-up `ParkReason`) - never one that is actively implementing
(non-terminal Task / live pod):

- already delivered / duplicate / obsolete -> **close now** (regardless of
  count), via the existing close action.
- still wanted and `ImplementGiveUps < 3` -> **comment refined, actionable
  scope** so the next auto-reroll has a better chance. Do not close.
- `ImplementGiveUps >= 3` (at cap) -> **comment a failure summary** that
  escalates to a human (what was tried, why it failed, what input is needed).
  Do not close, do not attempt to reroll.

The refine barrier runs before `recoverOrphans` each cycle, so a refined comment
or a close lands before the reroll consumes an attempt. Refine still creates no
Tasks; it only comments and closes. The prompt must make the live-vs-gaveup
distinction explicit (act only on terminal/Parked; never touch a live
implementation).

### 5. Blocked-issue visibility (metric, replaces the label)

Extend the existing `tatara_issue_state` gauge
(`internal/obs/operator_metrics.go`, set from the Task list each reconcile) with
a `blocked` state, derived from `parked-in-Implement && ImplementGiveUps >= 3`.
The task-delivery Grafana dashboard then shows blocked-needs-human issues with
no GitHub-side label.

### 6. Counter durability (reaper invariant)

The per-issue lifecycle Task holds the counter, so the reaper
(`internal/controller/reaper.go`) must **not** GC a parked-in-Implement Task
while its issue is still open. (This matches the "one durable Task per issue"
model.) If a Task is nonetheless absent for an open `tatara-implementation`
issue, recovery treats it as a fresh start (`ImplementGiveUps = 0`) - acceptable
degradation, not a loop, because the reaper invariant makes it rare.

## Data flow

```
implement gives up -> Task Parked (ParkReason in-scope), issue open+impl label
   |
   v  (each refine cycle, barrier-first)
refine agent reads task_list (LifecycleState, ParkReason, ImplementGiveUps)
   |- already-done/dup/obsolete  -> close issue
   |- wanted, count<3            -> comment refined scope
   |- count>=3                   -> comment failure summary (escalate)
   |
   v  (recoverOrphans, after refine barrier, periodDue)
open tatara-implementation issue, matching parked task, not live
   |- count<3 -> adopt-in-place, re-enter Implement (reroll); next give-up ++
   |- count>=3 -> skip (blocked); tatara_issue_state=blocked     (escalated)
```

## Error handling

- Counter increments are transition-guarded (consumed on reroll, not per
  reconcile) - mirror the boot-crash per-UID idempotency guard so a re-reconcile
  cannot double-count.
- Label/comment/close actions remain idempotent (refine already tolerates
  repeat comments via the bot-engaged / 409 egress gates).
- A reaped Task -> recovery restarts from 0 (bounded, not a loop).

## Testing (TDD)

- `ImplementGiveUps` increments exactly once per reroll; not on steady-state
  reconcile of a parked Task.
- `recoverOrphans`: rerolls when `count < 3`; skips (blocked) when `count >= 3`;
  never rerolls a live/non-terminal task.
- Adopt-in-place preserves `ImplementGiveUps` (fresh-Task path would reset it).
- Reaper spares a parked-in-Implement Task whose issue is open.
- `task_list` output includes `LifecycleState`, `ParkReason`,
  `ImplementGiveUps`.
- `tatara_issue_state` emits `blocked` for parked-in-Implement && count>=3.
- `refine` prompt golden test: gave-up priority category present; close vs
  comment vs escalate wording correct; live-vs-gaveup distinction explicit.
- Give-up reason classification: in-scope reasons recover; `refused-declined` /
  `already_done` do not enter the reroll path.

## Deploy

Single operator change (api types + projectscan + lifecycle + reaper + refine
goal + obs metric + cli task_list fields). Ships via the operator image + chart
bump in tatara-helmfile. No new MCP tool, no new label, no skill file.

## Out of scope / non-goals

- Re-triaging from scratch (we resume at Implement, preserving context).
- Recovering deliberate declines.
- A human-facing GitHub label (replaced by metric + escalation comment).
