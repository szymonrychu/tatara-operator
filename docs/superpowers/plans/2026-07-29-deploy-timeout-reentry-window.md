# Plan: give a merge/deploy-timeout un-park re-entry a usable window (#513)

## Problem

`1a9f676` ("preserve merge/deploy-timeout work clock across un-park re-entry",
#480/#493, shipped v1.39.4) made the podless stage work clock cumulative across
a timeout park/un-park round trip: `stage.Enter` folds this attempt's elapsed
time into `Status.StageElapsedCarrySeconds` on a
`parked(merge-timeout|deploy-timeout)` edge and preserves it on the matching
re-entry, and `ArmedClock` pulls `since` back by the carry.

A timeout park by definition happens only once the budget is already exhausted,
so every re-entry arrives already over budget and re-parks on the same reconcile
pass. Live evidence (#513, Task `mt-c-tatara-operator-506-c5d5724ce8829203`):
re-entry lifetimes of 7.2 ms, 8.8 ms, 11.3 ms, `elapsed` flat at ~2h6m59s across
all four parks, and terminal `failed` 107 s after the first timeout.
`MaxDeployReentries = 3` bought three re-entries and zero deploy time.

## Design

Split the two things `1a9f676` conflated:

- **Reported residency stays cumulative.** `StageAgeSeconds` keeps adding the
  carry, so `operator_task_stage_age_seconds` still reports true whole-round-trip
  residency. That was the real defect #480 measured.
- **The deadline gets a fresh bounded window per re-entry.** A timeout re-entry
  arms the work clock from `StageEnteredAt` with NO carry subtraction, against a
  short `TimeoutReentryBudget` instead of the full stage budget.

Total wall-clock stays bounded and far below the ~16 h that #480 killed:

| stage     | base budget | + 3 re-entries x 30m | total |
|-----------|-------------|----------------------|-------|
| deploying | 2h          | 1h30m                | 3h30m |
| merging   | 4h          | 1h30m                | 5h30m |

`carry > 0` is the discriminator, not `DeployReentries > 0`: `Enter` zeroes the
carry on every edge that is not a timeout round-trip (including a HEAD-MOVED
merging exit and `enterFreshImplementing`), so a genuinely fresh entry into
merging/deploying never inherits the short window, whereas the re-entry counters
can be stale from an earlier cycle.

## Changes

### 1. `api/v1alpha1/constants.go`

Add next to `MaxMergeReentries`/`MaxDeployReentries`:

```go
// TimeoutReentryBudget is the work-clock budget ONE merge-timeout/
// deploy-timeout un-park re-entry gets, replacing the full stage budget for
// that attempt (issue #513). The carry from stage.Enter makes the reported
// residency cumulative; without a fresh window here the re-entry arrives
// already over budget and re-parks on the same reconcile pass, spending all
// MaxMergeReentries/MaxDeployReentries laps in milliseconds.
TimeoutReentryBudget = 30 * time.Minute
```

### 2. `internal/stage/stage.go` - `ArmedClock`, podless branch

Replace the unconditional carry subtraction with:

```go
// A merge-timeout/deploy-timeout re-entry (carry > 0, set by Enter only on
// that round trip) gets its OWN bounded window measured from THIS entry
// (issue #513) - subtracting the carry here would put it over budget on
// arrival, since the park only happens once the budget is spent. Reported
// residency stays cumulative via StageAgeSeconds.
if t.Status.StageElapsedCarrySeconds > 0 {
    return ClockWork, t.Status.StageEnteredAt.Time, v1alpha1.TimeoutReentryBudget, elapse
}
return ClockWork, t.Status.StageEnteredAt.Time, budget, elapse
```

Update the `// CLOCK 3 ONLY` comment block and `ArmedClock`'s doc comment to
describe the split. `Enter` is NOT changed - carry accumulation and preservation
stay exactly as `1a9f676` left them.

### 3. `internal/controller/deploy_stage.go` - `ReconcileDeploying`

`merging` logs `merge_waiting` on every poll pass; `deploying` logs nothing, so
2h07m of a wedged deploy produced zero telemetry. Add a `deploy_waiting` INFO
log on both `RequeueAfter` returns, carrying the pending MR names and the count,
mirroring `internal/controller/merge.go`'s `merge_waiting` field style.

### 4. Tests

- `internal/stage`: park `deploying` on `deploy-timeout`, un-park, assert
  `ArmedClock` returns `TimeoutReentryBudget` measured from the re-entry stamp,
  and that `Elapsed` is false immediately after re-entry and true only after
  `TimeoutReentryBudget`. Same for `merging`/`merge-timeout`.
- Assert a genuinely fresh entry into `deploying` (carry zeroed) still gets the
  full 2h budget.
- Assert `StageAgeSeconds` still reports cumulative time across the round trip.
- A regression test walking the full loop: 3 re-entries each survive more than
  one reconcile pass and the Task is not terminal within seconds of the first
  park.
- `internal/controller`: `deploy_waiting` is logged while an owned MR lacks
  `deployedAt`.

## Out of scope (tracked separately)

- The alert wording/aggregation defect (`tatara-observability` rule
  `ffr5wlsj7yxa8a` says "N Task(s)" while counting park events) - separate repo.
- The ARC runner `no space left on device` failures that stopped v1.40.0 from
  publishing, which is why this Task's deploy never landed at all.
