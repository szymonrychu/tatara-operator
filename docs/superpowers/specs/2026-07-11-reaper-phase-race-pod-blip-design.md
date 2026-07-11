# Reaper phase-race pod blip + non-leader-gated maintenance ticker

Date: 2026-07-11

## Problem

Wrapper pod `tatara-tatara-gh-tatara-operator-issue-203` was observed being
deleted and recreated repeatedly (6 distinct pod UIDs in 24h,
`kube_pod_container_status_restarts_total == 0` on every incarnation - so
delete/recreate churn, not a container crash/OOM). Grafana investigation
(`.claude/skills/grafana-debugging-start/docs/grafana-investigations/2026-07-11-operator-issue-203-pod-blip.md`)
traced it to the orphan reaper.

### Defect 1 - phase-terminal reap races `resetAgentRun` continuation

`internal/controller/reaper.go` `orphanReason` reaps a wrapper pod when
`isTerminal(task.Status.Phase)` (phase `Succeeded`/`Failed`). But
`phase=Succeeded` is set on turn-batch completion (`task_terminate`,
`reason=NoPendingSubtasks`) - the task terminates, then is frequently revived
by `resetAgentRun` to continue in the SAME pod (front-half implement handoff:
`triageImplementAction` sets `terminal=false, same Task continues`;
conversation reactivation on scan). In the window between `phase=Succeeded` and
the continuation, the reaper deletes the still-needed warm pod. Observed for
task `scan-qe-84k9j` (issue #203): `task_terminate phase=Succeeded` at
12:03:57Z, `reaped orphan wrapper pod reason="task phase Succeeded"` at
12:04:01Z, `lifecycle_transition Triage->Conversation` at 12:04:02Z - the pod
killed while the task's lifecycle was still live.

The idle backstop (`IdlePodReapAfter`, default 30m / min 5m, issue #237)
already reaps genuinely-idle pods with a `!taskHasInflightTurn` guard. The
standalone phase-terminal branch is the eager, race-prone duplicate.

### Defect 2 - maintenance ticker runs in every replica

`CallbackServer.Start` (`internal/controller/turncallback.go`) runs a
`pollRequeue` ticker goroutine calling `PollOnce` + `ReapOrphans`. It is
registered via `callbackRunnable` whose `NeedLeaderElection()` returns `false`
(correct for the HTTP handler, which must serve on every replica), so the
ticker also runs in every replica. Confirmed: two operator replicas
(`-nhzc7`, `-xm6lh`, same ReplicaSet) logged the identical reap 0.5s apart.
Deletes are idempotent so it is not corrupting, but every replica does
full-namespace pod+task Lists every 30s and the same latent exposure applies
to `PollOnce` turn-driving.

## Fix

### Fix 1 - lifecycle-gated phase reap

In `orphanReason`, gate the phase-terminal reap on an empty `DeployState`:

```go
// A phase-terminal task is reaped promptly only when it has NO lifecycle to
// continue: an empty DeployState marks a one-shot (non-conversational) task
// with nothing left to run. A lifecycle task that shows phase Succeeded (a
// turn batch drained: NoPendingSubtasks) may be about to be revived by
// resetAgentRun to continue in the SAME warm pod (front-half implement
// handoff, conversation reactivation); phase-reaping it there kills the pod
// mid-continuation (the blip). Its DeployState is non-empty, so this branch
// skips it - the lifecycle-terminal branch reaps it once genuinely finished,
// and the idle backstop reaps it if it goes idle.
if task.Status.DeployState == "" && isTerminal(task.Status.Phase) {
    return fmt.Sprintf("task phase %s", task.Status.Phase), true
}
if isLifecycleTerminal(task.Status.DeployState) {
    return fmt.Sprintf("task lifecycle %s", task.Status.DeployState), true
}
```

Rationale: `phase=Succeeded` means the task terminated. The only way a
terminated task continues is `resetAgentRun`, and every such path carries a
non-empty `DeployState` (implement handoff -> `Implement`; reactivation ->
`Triage`). One-shot kinds that hit `phase=Succeeded` are genuinely done
(empty `DeployState`) and reaped immediately as before. Cost: a one-shot pod
that finishes with empty `DeployState` is unaffected (still instant); a
lifecycle pod parked at `phase=Succeeded` with a non-terminal `DeployState`
(e.g. await-approval `Conversation`) is now held warm until it either
continues or the idle backstop reaps it after `IdlePodReapAfter` - bounded,
not a leak, and strictly better than the current instant-reap-then-cold-respawn.

### Fix 2 - leader-gate the maintenance ticker

Split the ticker out of `CallbackServer.Start` into a new method
`RunMaintenance(ctx context.Context) error` that owns the `pollRequeue` ticker
loop (`PollOnce` when `Session != nil`, then `ReapOrphans`), blocking until
`ctx.Done()`. `Start` keeps only the HTTP server + graceful-shutdown goroutine.

In `cmd/manager/wire.go` add a second runnable:

```go
type maintenanceRunnable struct{ srv *controller.CallbackServer }
func (m maintenanceRunnable) Start(ctx context.Context) error { return m.srv.RunMaintenance(ctx) }
func (m maintenanceRunnable) NeedLeaderElection() bool { return true }
```

registered with `mgr.Add(maintenanceRunnable{srv: cbServer})` alongside the
existing `callbackRunnable`. controller-runtime runs leader-election runnables
only on the leader (and runs them normally when leader-election is disabled, so
single-replica/dev is unaffected). The HTTP handler stays in every replica; the
poll+reap loop runs only on the leader.

## Testing

`internal/controller/reaper_test.go`:
- phase `Succeeded` + non-empty non-terminal `DeployState` (e.g. `Conversation`,
  `Implement`) -> NOT an orphan.
- phase `Succeeded` + empty `DeployState` -> orphan (`task phase Succeeded`).
- lifecycle-terminal `DeployState` (`Done`/`Stopped`/`Parked`) -> orphan
  (unchanged).
- idle backstop unchanged (non-terminal task, no inflight turn, aged past
  `IdlePodReapAfter` -> orphan).
- stale task-uid / task-absent branches unchanged.

Runnable wiring:
- `maintenanceRunnable.NeedLeaderElection() == true`,
  `callbackRunnable.NeedLeaderElection() == false`.
- `RunMaintenance` returns nil on `ctx` cancel (no ticker leak).

Gate: `mise run` generate/manifests (no CRD field change expected),
`test` (`-race` in CI), `lint`, `build`.

## Out of scope (noted, not silently dropped)

- Issue-derived wrapper pod naming (`podNameSuffix` -> `issue-<N>`) is
  deterministic by design; the `tatara.dev/task-uid` label already
  disambiguates incarnations, so it is not the kill trigger.
- A broader `PollOnce` concurrency/idempotency audit beyond removing the
  N-replica execution via leader-gating.
