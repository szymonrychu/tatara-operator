# Systemic-group implementation dedup (lead-per-repo, combined PR)

Date: 2026-06-23

## Problem

A single brainstorm can produce multiple connected issues sharing a
`systemicId`. Today each issue independently spawns its own
`issueLifecycle` Task (dedup keyed on `(repo#number)`), so one brainstorm
output of N connected issues spawns N implementation agents with no
awareness of each other. They can duplicate or clobber cross-cutting work.

Partial mechanism already exists:
- `ProposedIssueSpec.SystemicID` (`api/v1alpha1/task_types.go:21`).
- Brainstorm stamps `tatara/systemic-<id>` label + footer per issue
  (`internal/controller/writeback.go:552-554`).
- `proposalBacklogCount` groups by `tatara/systemic-<id>`, counting a
  systemic group as one against the backlog cap
  (`internal/controller/projectscan.go:1456-1481`).

Gap: nothing collapses implementation agents within a systemic group.

## Goal

At most ONE implementation agent per `(systemicId, repo)`. That agent (the
lead) implements all its same-repo siblings in one combined PR and is aware
of the cross-repo group. Non-lead siblings spawn no agent and are marked.

## Decisions (locked)

1. **Lead agent + siblings**: one lead per repo owns same-repo siblings;
   siblings spawn no agent.
2. **Lead per repo**: an N-repo group gets N agents (one per repo), each
   aware of the whole group.
3. **One combined PR**: the lead implements all same-repo siblings and the
   PR closes all of them (`Closes #A, Closes #C`).
4. **Comment-only marking**: collapsed siblings get an idempotent comment
   linking the lead; no new label.

## Design

### Lead election (stateless, deterministic)

Within a `(systemicId, repo)`, lead = lowest open issue number carrying
`tatara/systemic-<id>`. Re-derived every scan from currently-open issues;
no stored election state. If the lead closes before the group is drained,
the next-lowest open sibling is auto-elected on the next scan.

### Dedup gate in `issueScan`

For a candidate issue carrying a systemic label:
- **candidate == lead**: create the `issueLifecycle` Task. The Task goal
  enumerates all same-repo siblings (implement them, PR closes all) plus
  cross-repo siblings as references/context only.
- **candidate != lead**: skip Task creation (treated as deduped) and post
  an idempotent comment: `Tracked by #<lead> (systemic group). No separate
  agent.` No new label.

### Cross-repo group derivation

Reuse the project-level systemic grouping already built for
`proposalBacklogCount` (projectscan spans all project repos). Same-repo
siblings -> implement; other-repo siblings -> reference only in the lead's
goal/context.

### SystemicID on Task

Stamp `systemicId` onto the Task (currently lost after issue creation) for
observability and reconcile grouping. The lead Task records the group it
owns. Add a field to `TaskSpec` (and surface in `TaskStatus` if needed for
the lead's owned-siblings set), regenerate CRDs and the hand-maintained
RBAC if required (per `operator-chart-rbac-hand-maintained-2026-06-20`).

### Metrics + logs (platform rules 12-13)

- Counter: collapsed siblings (labels: project, systemicId).
- Counter or gauge: systemic groups led.
- INFO log per dedup decision with structured fields: systemicId, repo,
  lead issue number, collapsed set.

### Idempotent comment

Reuse the egress-gated `commentOnIssue` path with a marker-presence check
(per `brainstorm-no-repeat-comment-2026-06-19`) so reconcile loops never
re-comment. The marker is the `Tracked by #<lead>` line.

## Testing (TDD)

Unit tests, following `systemic_proposal_test.go`:
- Lead election: lowest open issue number wins; re-election after lead
  closes.
- Non-lead skip: no Task created for non-lead siblings; idempotent comment
  posted once.
- Lead Task goal enumeration: same-repo siblings appear as `Closes #N`;
  cross-repo siblings appear as references, not as `Closes`.
- SystemicID propagation onto the Task.

## Out of scope

- Sequential re-election (combined-PR chosen instead).
- Reviving `CronActivity.MaxPerRepo` lane occupancy
  (`operator-laneoccupancy-starves-recovery-2026-06-15`).
- DAG / declared ordering among issues.

## Open implementation detail (resolve in planning)

Whether `issueScan` enumerates issues per-repo or project-wide determines
where lead election and cross-repo derivation hook in. Does not change the
design, only placement.
