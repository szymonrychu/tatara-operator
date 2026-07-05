# Post-merge documentation agent - design

Date: 2026-07-05
Primary repo: tatara-operator (also touches tatara-cli, tatara-agent-skills,
tatara-helmfile)
Status: approved (brainstorming), pending spec review
Depends on: `2026-07-05-operator-simplification-design.md` Batch 3 (pod.go
per-kind table) should land first so the new kind is one table row.

## Goal

A new, optional, default-OFF platform feature: when a merge lands on a
component repo's default branch, an autonomous agent updates the project's
dedicated central documentation repo if (and only if) the merged change warrants
it. Fully autonomous - it opens an MR against the docs repo that auto-merges on
green CI, or cleanly no-ops. Enable for tatara immediately after it ships.

## User decisions (locked)

- **Write path**: agent opens an MR against the docs repo; auto-merge on green
  (mkdocs build). Bot-authored, no human gate.
- **Trigger**: every merge to a default branch spawns one documentation Task;
  the agent self-decides update-vs-no-op.
- **Doc scope**: the central docs repo only. Agent reads the merged diff +
  docs repo, writes only the docs repo.
- **Review gate**: the docs MR is EXEMPT from the platform's review/mrScan
  agents (low-risk prose; avoids the known re-review token-burn loop). mkdocs CI
  still gates.
- **Model**: `claude-sonnet-5` for the doc pod.

## Architecture

New agent kind `documentation`, **repo-scoped to the DOCS repo**. This is the
key structural choice: the doc Task clones and edits the documentation repo (its
working repo); the triggering component and its SHA range ride as annotations
(input context). This inverts the usual "work on the repo that fired the event"
but lets the completion reuse `writeback.go:writeBackOpenChange` unchanged (it
opens a PR in the Task's repo = the docs repo).

### Data flow

```
merge to component/main
  -> SCM webhook: push event (carries before/after SHA)
  -> webhook/server.go handlePush
       gate: proj.Spec.Documentation != nil && .Enabled
       gate: pushed repo != docs repo         (no self-trigger loop)
       gate: proj has an enrolled docs Repository CR
  -> queue/enqueue.go EnqueueEvent -> documentation QueuedEvent
       RepositoryRef = docs repo
       annotations: sourceRepo, sourceBaseSHA (before), sourceHeadSHA (after)
  -> queue_controller -> documentation Task
  -> agent/pod.go: model=claude-sonnet-5,
       TATARA_TOOL_PROFILE=documentation, TATARA_SKILL_PROFILE=documentation,
       clone = docs repo
  -> wrapper runs the tatara-documentation-workflow skill:
       1. shallow-clone the (public) source repo, `git diff base..head`
       2. read the docs repo, judge whether docs are affected
       3a. affected -> edit docs on a branch, `change_summary`(significance),
           writeback opens MR into docs repo w/ semver auto-merge label
       3b. not affected -> `decline_implementation("no doc-relevant change")`
  -> writeback.go writeBackOpenChange: PR in docs repo, semver auto-merge label
  -> mkdocs CI green -> auto-merge
```

### Diff acquisition (design decision)

The doc pod clones the docs repo, not the source. To see the merged change it
needs the source diff. **Chosen (MVP): annotations + skill-driven shallow clone.**
The operator passes `sourceRepo` + `sourceBaseSHA` + `sourceHeadSHA` as Task
annotations; the documentation skill instructs the agent to
`git clone --depth` the source repo (tatara repos are public) and
`git diff base..head`. No new MCP tool, no auth (public repos), git+network
already present in the pod. Egress policy already permits GitHub clones.

Alternative (deferred): an operator MCP tool `get_change_diff(repo, base, head)`
reusing `scm.Client`, exposed only to the documentation profile - cleaner and
auth-scoped, but adds tatara-cli tool surface. Adopt if shallow-clone proves
heavy or if private-repo projects need it.

## Components and touchpoints

### 1. CRD / API (tatara-operator/api/v1alpha1) - `make manifests` after

- `task_types.go:258` - add `documentation` to the `TaskSpec.Kind`
  `+kubebuilder:validation:Enum`.
- `task_types.go:158-164` - add `"documentation": true` to `repoScopedKinds`
  (needs a non-empty `RepositoryRef` = the docs repo).
- `project_types.go` - new `DocumentationSpec` on `ProjectSpec` (see below).
- `project_types.go` `AgentSpec.ModelByKind` (:163-165) - add `documentation`
  to the `k in [...]` list in the XValidation rule (:164) AND to its
  human-readable `message=` enumeration; bump `MaxProperties` 9->10 (:163). The
  second value-format CEL (:165, "must start with claude-") is unaffected.
- `project_types.go` `AgentSpec.EffortByKind` (:171-173) - same (rule + message
  at :172); bump `MaxProperties` (:171). Value CEL (:173) unaffected.
- `project_types.go` `UsageBudgetSpec.SpawnCeilingByKind` (:500-502) - add
  `documentation` to the rule + message (:502); bump `MaxProperties` (:500).
  Only needed if a per-kind spawn ceiling is wanted; otherwise it falls through
  to pool-class gating (budget.go:196-199).
- `queuedevent_types.go` `ValidateQueuedEventSpec` - no direct edit (covered by
  the shared `repoScopedKinds` map). NB: currently dead code, do not rely on it.
- `charts/tatara-operator/crd-bases/tatara.dev_{tasks,projects}.yaml` -
  generated; run `make manifests`, never hand-edit.

`DocumentationSpec` (exact `GrafanaSpec` pattern - inert unless `Enabled`):

```go
// DocumentationSpec configures the optional post-merge documentation agent:
// a merge to any enrolled component's default branch spawns a documentation
// Task that updates the central docs repo if the change warrants it. Inert
// unless Enabled.
type DocumentationSpec struct {
    Enabled bool `json:"enabled"` // no kubebuilder:default -> false; do NOT
                                  // gate behavior on == default (MEMORY trap)
    // Repo is the central documentation repo the agent maintains (git URL).
    // It must also be enrolled as a Repository CR under this Project so the
    // bot has push access and mkdocs CI runs.
    // +optional
    Repo string `json:"repo,omitempty"`
}
```

Gate (mirrors `server.go:852`): `if proj.Spec.Documentation == nil ||
!proj.Spec.Documentation.Enabled { skip }`.

### 2. Trigger (tatara-operator/internal/webhook/server.go)

- `handlePush` (:156) already fires on push to a repo's default branch (today it
  only stamps `ReingestRequestedAnnotation` for memory re-ingest). Add: after
  the reingest stamp, when the Documentation gate passes and the pushed repo is
  not the docs repo, `EnqueueEvent` a documentation QueuedEvent
  (RepositoryRef = docs repo; annotations = sourceRepo/base/head SHA). Reuses
  the existing dedup/seq machinery.
- Dedup key must include the source head SHA so re-delivered webhooks don't
  double-spawn, and two different merges do spawn two Tasks.

### 3. Pod (tatara-operator/internal/agent/pod.go)

Via the Batch-3 per-kind table, add the `documentation` row:
- tool profile `documentation`, skill profile `documentation`
- model `claude-sonnet-5`, effort default
- branch prefix `docs`, PR-title prefix `docs`
- `podNameSuffix`: falls through to the generic `issue-%d`/`mr-%d`; since the
  doc Task is SHA-keyed not number-keyed, give it a source-SHA-short suffix so
  concurrent doc Tasks don't collide on the pod-name slot.
- annotations `sourceRepo`/`sourceBaseSHA`/`sourceHeadSHA` -> pod env.

### 4. Turn-0 prompt / skills (tatara-operator/internal/controller)

- `turnloop.go:requiredSkillsForKind` (~:201-219) - add
  `case "documentation": return []string{"tatara-documentation-workflow"}`.
  Omitting this silently injects no skill directive.
- `task_controller.go` prompt selection (~:308-316) - falls to the generic
  `planTurnText` path automatically. Add a bespoke turn-0 prompt only if needed;
  MVP relies on the skill for the procedure.
- `turnCap` (~:1147,1160-1169) - documentation is NOT in the uncapped
  {implement, issueLifecycle} set, so it gets the default 50-turn cap. Correct
  for a short doc task; no edit.

### 5. Writeback (tatara-operator/internal/controller/writeback.go)

- `doWriteBack` (~:54-88) - no case needed; documentation pushes a branch and
  opens a PR, so it falls to `writeBackOpenChange` (the default arm). Confirmed:
  `writeBackOpenChange` (:110) resolves the Task's `RepositoryRef` into
  `primaryRepo` (:140) and calls `writer.OpenChange(repo.Spec.URL, ...,
  repo.Spec.DefaultBranch, ...)` (:236) - so with RepositoryRef = docs repo the
  MR lands in the docs repo against its default branch. Keep the doc Task's
  `ReposInScope` empty (or the docs repo only) so it does not fan out to other
  repos. Requires the agent to have called `change_summary` (significance) to get
  the auto-merge label.
- `derivePRTitle` (~:702-707) - add `case "documentation": kind = "docs"` (or
  via the Batch-3 table) so docs PRs title as `docs(...)` not `feat(...)`.

### 6. Conversation GC (tatara-operator/internal/controller/reaper.go)

- `gcConversations` (~:267-269) - add `"documentation"` to the batched-Kind
  case so S3-persisted transcripts are garbage-collected. S3 persistence is
  generic (pod.go:498-524), so without this the transcripts leak. Confirm the
  keying: documentation Tasks are SHA-keyed (may have no `Source.Number`) -
  ensure the GC path handles that (key on Task name/UID if Source.Number absent).

### 7. Metrics (cosmetic, optional)

- `task_controller.go:updateInflightGauge` (~:1378) and
  `operator_metrics.go` tasksGC pre-seed (~:546) - add `documentation` for a
  zero-baseline; the dynamic `else` branch already emits it otherwise.
- `project_controller.go:issueStateFor` (~:405-425) - optional
  `case "documentation":` for dashboard visibility.

### 8. Tool profile (tatara-cli/internal/mcp/profiles.go) - SEPARATE REPO

- Add a `"documentation"` entry to the `profiles` map (~:121-193). LOAD-BEARING:
  `resolveProfile` fails CLOSED to 4 tools if the string is unknown. Grant:
  `task_update`, `subtask_*`, `change_summary`, `decline_implementation`,
  `already_done`, plus whatever read tools the agent needs. No chat; handoff on.
- `toolProfileForKind` (~:8-29) - add the matching case for hygiene/tests (NOT
  runtime-load-bearing; `server.go` reads the env string directly).

### 9. Skill (tatara-agent-skills) - SEPARATE REPO

- New skill `tatara-documentation-workflow` with `profiles: ["documentation"]`
  frontmatter. LOAD-BEARING: the wrapper's `installSkills` only installs a skill
  whose `profiles:` contains the exact `TATARA_SKILL_PROFILE` string; a mismatch
  silently installs zero skills. The skill documents the procedure: clone source
  repo, diff base..head, judge doc impact, edit docs repo (mkdocs structure),
  call `change_summary`, or `decline_implementation` on no-op.

### 10. Wrapper (tatara-claude-code-wrapper) - NO CHANGE

Confirmed pure passthrough: reads `TATARA_SKILL_PROFILE` straight into
`bootstrap.Params.SkillProfile`. No per-kind logic.

## Enable for tatara (item 3)

After the feature ships (all repos merged, operator image built + deployed via
push-CD):

1. Enroll `tatara-documentation` as a `Repository` CR under the tatara Project
   (in `tatara-helmfile` values) - gives the bot push access; ensure the docs
   repo has an mkdocs-build CI check + auto-merge enabled.
2. `tatara-helmfile` MR setting
   `project.spec.documentation.{enabled: true, repo: <tatara-documentation git URL>}`.
3. Optional: `spec.agent.spawnCeilingByKind.documentation` if a per-kind ceiling
   is wanted; otherwise pool-class gating applies.

Ships INERT (default-off): the operator code deploys with the feature present
but no Project enables it until the helmfile MR above.

## Testing (TDD)

- API: `ValidateTaskSpec` accepts `documentation` with a RepositoryRef and
  rejects it without one (repoScopedKinds). CEL/`MaxProperties` round-trip via a
  `make manifests` + CRD-apply test or envtest.
- Webhook: `handlePush` enqueues a documentation QueuedEvent when the gate
  passes; does NOT when Documentation is nil/disabled; does NOT for a push to
  the docs repo itself; dedups on source head SHA.
- Pod: `documentation` resolves model=claude-sonnet-5, both profiles=
  `documentation`, branch/title prefix `docs`, source-SHA pod suffix, source
  annotations -> env.
- Writeback: a documentation Task with a ChangeSummary falls to
  `writeBackOpenChange` and stamps the auto-merge label; `derivePRTitle` returns
  `docs`.
- Reaper: a documentation Task's S3 transcript is GC-eligible.
- tatara-cli: `resolveProfile("documentation")` returns the intended tool set
  (not the fail-closed 4).
- tatara-agent-skills: the new skill's frontmatter `profiles` includes
  `documentation` (lint/test in that repo).

## Landmines (from the wiring audit)

1. **Two closed-enum gates** for the same string: the kubebuilder `Enum` marker
   AND the CEL rules on ModelByKind/EffortByKind/SpawnCeilingByKind. Miss either
   and the CRD rejects what the Go code accepts.
2. **Generated CRD trap**: `make manifests` regenerates the CRD YAML; hand-edits
   are overwritten and the deployed enum drifts.
3. **Fail-closed tool profile**: unknown `TATARA_TOOL_PROFILE` -> 4 tools only,
   silently. Different repo (tatara-cli), no compile-time link.
4. **Silent-skill failure**: unknown `TATARA_SKILL_PROFILE` -> zero skills,
   silently. Different repo (tatara-agent-skills), pure string match.
5. **Conversation-GC leak**: reaper only batches specific kinds; omit
   `documentation` and S3 transcripts never delete.
6. **branchKind vs derivePRTitle** are two independent switches; update both (or
   drive both from the Batch-3 table).
7. **Self-trigger loop**: the docs repo's own merges must NOT spawn a
   documentation Task, or it loops.
8. **Repo-scoping inversion**: the doc Task's RepositoryRef is the DOCS repo, not
   the merged repo. The merged repo/SHA are annotations. Get this backwards and
   writeback opens the MR in the wrong repo.

## Rollout order

1. Operator simplification Batch 3 (pod.go table) lands first.
2. tatara-cli: add `documentation` profile. Merge (image builds).
3. tatara-agent-skills: add the skill. Merge.
4. tatara-operator: kind + spec + trigger + pod row + writeback + reaper +
   `make manifests` + tests. Merge (image builds, push-CD deploys inert).
5. tatara-helmfile: enroll docs repo + enable for tatara.
