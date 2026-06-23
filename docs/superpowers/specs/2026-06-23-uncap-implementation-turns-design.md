# Uncap turns for implementation tasks

Date: 2026-06-23

## Problem

`turnCap` (internal/controller/task_controller.go) bounds every Task to a
maximum number of agent turns: `task.Spec.MaxTurns` if set, else
`project.Spec.Agent.MaxTurnsPerTask` (live: 50), else the hardcoded 50. When a
Task hits the cap it terminates `Succeeded`/`MaxTurnsReached`, even mid-work.

For implementation work this cuts the agent off before the job is done: a
non-trivial PR can legitimately need many turns (plan, multiple subtasks,
review fixes). The cap exists as a runaway guard, but per-turn timeout and
maxPodRecreations already bound runaway independently. The count cap mostly
just kills long-but-healthy implementation runs.

## Decision

Remove the turn-count cap for the two kinds that actually write code:
`implement` and `issueLifecycle`. All other kinds keep the cap.

Scope decisions (confirmed with user):
- Kinds uncapped: `implement`, `issueLifecycle`. Nothing else.
- `selfImprove` (also a coding kind) stays capped, by explicit choice.
- Truly uncapped, not raised-to-high. No new backstop; per-turn timeout
  (`TurnTimeoutSeconds`, default 1800s) and `maxPodRecreations` remain the
  runaway bounds.
- An explicit `task.Spec.MaxTurns > 0` STILL caps, even for implement kinds.
  It is a deliberate per-task override (tests and manual control rely on it).

## Design

New `turnCap` precedence:

1. `task.Spec.MaxTurns > 0` -> that value, capped. (explicit override wins)
2. `task.Spec.Kind` in `{"implement","issueLifecycle"}` -> uncapped.
3. `project.Spec.Agent.MaxTurnsPerTask > 0` -> that value, capped.
4. default `50`, capped.

Signature changes to surface the uncapped state explicitly instead of a
sentinel number:

```go
func turnCap(project *Project, task *Task) (cap int, capped bool)
```

Call site (task_controller.go ~817):

```go
if cap, capped := turnCap(project, task); capped && task.Status.TurnsCompleted >= cap {
    return r.terminate(ctx, task, "Succeeded", "MaxTurnsReached",
        fmt.Sprintf("reached turn cap %d", cap))
}
```

When `capped` is false the cap branch is skipped entirely; no fake number is
ever printed in the terminate message.

## Tests (TDD, table-driven)

In `task_controller_test.go` (or a focused new test file):

- implement Kind, no explicit MaxTurns, TurnsCompleted huge -> NOT terminated
  by the cap (turn submitted / proceeds).
- issueLifecycle Kind likewise uncapped.
- review Kind still capped at project value, terminates at the cap.
- explicit `task.Spec.MaxTurns` still caps an implement Task (override wins).

Keep the existing `TestTaskReconcile_MaxTurnsCap` passing (it sets
`Spec.MaxTurns`, which still caps under rule 1).

## Helmfile / deploy

No `tatara-helmfile` change. `project.MaxTurnsPerTask: 50` stays as the cap for
the still-capped kinds. Ships via operator main -> CI image -> helmfile
operator chart + image.tag bump (standard deploy, gated).

## Out of scope

- selfImprove uncapping (explicitly left capped).
- Any change to per-turn timeout or maxPodRecreations.
