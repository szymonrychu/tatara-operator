# Refine agent rework (2026-06-28)

## Problem

The project refiner runs as a pre-scan barrier (opt-in `closedLookbackDays>0`).
As shipped it (a) names its pod off-convention (`tatara-<proj>-<gh|gl>-scan`),
(b) files NEW issues (followups + splits) which then cascade into triage
agents, and (c) runs with the full tool set (no per-kind profile). The intent
is narrower: refine is a peer of brainstorm/incident with a different input -
the existing backlog - that grooms it and feeds the same human go/nogo
approval queue, without spawning reacting agents.

## Decisions (brainstormed + approved)

- Refine is complementary to brainstorm/incident; it does NOT pause the project.
- Input: the project's existing open issues + recent closed issues/commits.
- Actions: close duplicates (cite canonical), close already-implemented (cite
  commit/PR/closed sibling), and edit/tighten the survivors. NO issue creation
  (drop followups AND splits).
- Refined actionable issues are LEFT in their existing proposal/brainstorming
  (approval-pending) state. They advance only via the user's go/nogo comment -
  the same gate brainstorm/incident proposals use. The refiner never applies the
  trigger label and never escalates to implementation.
- The refiner's own actions must not trigger any issue-reacting agent.

## Changes

1. **Naming.** `podNameSuffix` (internal/agent/pod.go) returns `refine` for
   `kind=="refine"` -> pod `tatara-<project>-<gh|gl>-refine`, matching
   `...-brainstorm`. Verify the refine Task gets `StampPodName` at creation.

2. **Goal.** Rewrite `refine.GoalProject` (internal/refine/goal.go): drop the
   create_issue tool and the Split/Followup actions; keep close-duplicate,
   close-already-implemented, edit-scope. Add explicit rules: SKIP issues that
   already carry the trigger/approved/implementation label (in-flight - editing
   one would spawn a lifecycle agent), never apply the trigger label, never open
   PRs/implement, leave actionable issues in their current state for human
   go/nogo. Update `goal_test.go`.

3. **Reaction suppression - no code change, codified by test.** Verified the
   existing gates already suppress reaction to the refiner's bot actions:
   - webhook `issue_comment` from the bot is ignored (webhook/server.go:513);
   - webhook `issues` events are ignored unless the issue carries the trigger
     label (backlog proposals do not; the refiner does not add it);
   - issueScan dedups brainstorming-labelled issues with a terminal task, and
     does not scan closed issues;
   - `findConvTaskToReactivate` reactivates only on a newer HUMAN comment, so a
     bot edit bumping `updatedAt` cannot reactivate.
   Add a regression test asserting a bot-authored `issues edited` event without
   the trigger label spawns/reactivates nothing.

## Out of scope (follow-up)

Hard tool-layer enforcement: define a `refine` tool+skill profile in tatara-cli
(close/dedup/edit read-only set, no create_issue / label / implement tools) and
map `kind=="refine"` to it in `toolProfileForKind`/`skillProfileForKind`. This
is a cross-repo change (cli profile + coordinated cli-pin -> wrapper -> operator
deploy). Until then the goal prompt + the reaction gates above are the
enforcement; the refiner is fail-open on tools.
