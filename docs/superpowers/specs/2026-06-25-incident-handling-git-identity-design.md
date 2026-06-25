# Incident-handling + git-identity improvements - Design

Date: 2026-06-25
Repo: tatara-operator (+ tatara-helmfile for deploy config)
Status: approved

## Goal

Three independent improvements to incident handling and agent git identity:

1. Mark incident-originated tracker issues with a dedicated `tatara-incident`
   label, on top of `tatara-brainstorming`.
2. Surface the alert-rule identity as a typed field on the incident Task, and
   codify the already-live "dedup while non-terminal, re-investigate once
   finished" behavior with an explicit test.
3. Make agent commits attributed to the bot identity, not the generic
   `tatara-agent` default.

These ship as one operator change set + one tatara-helmfile deploy.

## Background (verified live, 2026-06-25)

- Incident Tasks are created webhook-side in `createIncidentTask`
  (`internal/webhook/server.go:885`): `Kind="incident"`, project-scoped, labelled
  `tatara.dev/alert-group=<groupHash>` and enqueued with `groupHash` as the
  dedup key.
- Dedup is already state-aware: `EnqueueEvent` -> `dedupExists`
  (`internal/queue/enqueue.go`) lists both QueuedEvents (non-Done) AND Tasks
  (`!TaskTerminal`) carrying the dedup label; `BuildTaskFromQueuedEvent`
  propagates `LabelDedupKey` onto the Task. So a re-fired alert is skipped while
  a non-terminal incident Task exists, and a fresh one is created once the prior
  Task reaches Succeeded/Failed. This is the desired behavior; it just lacks a
  typed field and a test.
- Brainstorm/incident proposals are filed operator-side: the agent calls the
  `propose_issue` MCP route (`internal/restapi/handlers.go:407`), which creates an
  `implement` Task carrying `ProposedIssueSpec`; `createProposal`
  (`internal/controller/writeback.go:581`) calls `CreateIssue` with labels
  `[brainstorming]` (+ optional systemic). This is the single chokepoint where an
  incident label can be added.
- Agent pods get `GIT_TOKEN` = bot token (`internal/agent/pod.go`), so push AUTH
  is already the bot. The wrapper sets `git config user.name/user.email` from
  `GIT_USER_NAME`/`GIT_USER_EMAIL` (`cmd/wrapper/config.go:120-121`,
  `internal/bootstrap/repo.go:14`), defaulting to `tatara-agent` /
  `tatara-agent@szymonrichert.pl`. The operator never sets those env vars, so
  every commit author is the generic default, not the bot.

## Component A: incident marker label

### API
- Add `ScmSpec.IncidentLabel string` (`json:"incidentLabel,omitempty"`), mirroring
  `BrainstormingLabel`. Default resolves to `tatara-incident`.
- Add `ProposedIssueSpec.Incident bool` (`json:"incident,omitempty"`): set when
  the proposal was filed by an incident-investigation agent.

### Label resolution
- Add `incidentLabel(s *ScmSpec) string` helper in `internal/controller/labels.go`
  returning `s.IncidentLabel` or `"tatara-incident"` when unset/nil. Keep
  `lifecycleLabels` unchanged (the incident label is additive, not one of the
  mutually-exclusive phase labels, so it must NOT be swept by `setLifecycleLabel`).

### Origin detection (`proposeIssue`, handlers.go:407)
- Add `inflightIncidentTask(ctx, project) bool`: lists Tasks, returns true when a
  `Kind=="incident"` Task for the project is non-terminal (Phase not
  Succeeded/Failed). Mirrors `inflightBrainstormConversationKey`'s project-level
  in-flight inference (the agent identity is shared OIDC, so the caller's kind is
  inferred from the project's in-flight work, consistent with existing code).
- When true, set `ProposedIssueSpec.Incident = true` on the created Task.

### Label application (`createProposal`, writeback.go:626)
- When `task.Spec.ProposedIssue.Incident`, append `incidentLabel(proj.Spec.Scm)`
  to the `labels` slice passed to `CreateIssue`. Idempotent paths
  (`recordExistingProposal`, source-already-set) are unaffected: a re-filed issue
  keeps whatever labels it already has; we only add the label on first create.

### Tests
- `proposeIssue` with an in-flight incident Task -> created Task has
  `ProposedIssue.Incident == true`.
- `proposeIssue` with only an in-flight brainstorm -> `Incident == false`.
- `createProposal` with `Incident == true` -> `CreateIssue` labels contain both
  `tatara-brainstorming` and `tatara-incident`.
- `createProposal` with `Incident == false` -> labels contain only
  `tatara-brainstorming` (no incident label).
- `incidentLabel` returns default when unset and the override when set.

## Component B: alert-rule typed field + dedup test

### API
- Add `TaskSpec.AlertRule string` (`json:"alertRule,omitempty"`).
- Add `QueuedEventPayload.AlertRule string` (`json:"alertRule,omitempty"`).

### Plumbing
- `createIncidentTask` (server.go:885): compute `alertRuleName(alert)` =
  `alert.CommonLabels["alertname"]`, falling back to `alert.GroupKey` when absent;
  set it on the payload.
- `BuildTaskFromQueuedEvent` (enqueue.go): copy `p.AlertRule` to
  `task.Spec.AlertRule`.
- Dedup key stays `groupHash` (unchanged). The new field is descriptive, not the
  dedup key.

### Prompt surfacing
- The incident goal already embeds the full alert context (`renderAlertContext`),
  so the rule name is already visible to the agent; no prompt change required.
  The typed field exists for operator-side queryability and future correlation.

### Tests
- `createIncidentTask` sets `payload.AlertRule` from `commonLabels.alertname`
  (and falls back to groupKey when alertname absent).
- `BuildTaskFromQueuedEvent` copies `AlertRule` onto the Task spec.
- Dedup state-gating (the codifying test): given a non-terminal incident Task
  with dedup label L, a second firing with the same groupKey does NOT create a new
  Task (`created == false`); after the Task is marked terminal (Succeeded), a
  third firing DOES create a new one (`created == true`).

## Component C: clone-as-bot commit identity

### API
- Add `ScmSpec.BotEmail string` (`json:"botEmail,omitempty"`).

### Operator (`internal/agent/pod.go`)
- Add two pod env vars on the agent container: `GIT_USER_NAME = Spec.Scm.BotLogin`
  and `GIT_USER_EMAIL = Spec.Scm.BotEmail`. Only set each when the source value is
  non-empty (a Project without `BotEmail` keeps the wrapper default rather than
  exporting an empty override).

### Wrapper
- No change. `cmd/wrapper/config.go` already reads `GIT_USER_NAME`/`GIT_USER_EMAIL`
  with the generic default, and `bootstrap/repo.go` applies them via
  `git config --global`. The operator simply now provides bot values.

### Tests
- Pod env includes `GIT_USER_NAME == BotLogin` and `GIT_USER_EMAIL == BotEmail`
  when both set on the ScmSpec.
- When `BotEmail` is empty, no `GIT_USER_EMAIL` env var is emitted (wrapper default
  stands).

## CRD + deploy

- New API fields (`IncidentLabel`, `ProposedIssue.Incident`, `AlertRule`,
  `BotEmail`) require `make manifests`; CRDs are templated (`crd-bases/` +
  `templates/crds.yaml`) and applied by `helm upgrade`. No RBAC change (no new
  resources/verbs).
- Deploy (per HARD contract): merge to operator `main` -> CI builds image `<sha>` +
  chart `0.0.0-g<sha>` -> tatara-helmfile MR bumping BOTH the 3 chart pins
  (tatara-operator + project-tatara + project-infrastructure) AND
  `values/tatara-operator/common.yaml` image `tag`, plus set `botEmail` per Project:
  - `tatara` (GitHub): `143486966+szymonrychu-bot@users.noreply.github.com`
  - `infrastructure` (GitLab): the bot's GitLab commit email.
  `incidentLabel` keeps its `tatara-incident` default; no helmfile change.

## Out of scope

- #4 internal-issue Grafana alert rule (terraform, separate repo + deploy) - stays
  in ROADMAP.

## Constraints

- Newest stable Go; KISS; no tech-debt; JSON slog; metrics unaffected (no new
  failure surface). Charts cluster-agnostic (BotEmail is per-Project config in the
  helmfile, not baked in the chart). TDD: failing test first for each unit.
