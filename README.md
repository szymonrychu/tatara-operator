# tatara-operator

A Kubernetes operator that orchestrates the tatara platform's unattended
agentic-development loop. It owns four CRDs in the `tatara.dev/v1alpha1`
API group - `Project`, `Repository`, `Task`, `Subtask` - reconciled by a
controller-runtime manager. It ingests repositories into `tatara-memory`,
receives GitHub/GitLab webhooks to keep memory fresh and to start work
from issues, and spawns `tatara-claude-code-wrapper` pods (with
`tatara-cli` as their MCP server) to do the work, landing results back in
the SCM.

It subsumes the previously-scoped `tatara-tasks` (REST task store; the
CRDs are the store now), `tatara-gitlab-bridge` (webhook bridge; built
in), and the orchestration role of `tatara-argo-workflows` (replaced by
operator-native Pod/Job spawning).

## Status

Shipping 0.4.3. All milestones (M0-M6) and the per-project-memory line
(N1-N4) are complete. The manager reconciles Project/Repository/Task/
Subtask, provisions a per-project `tatara-memory` stack (CNPG Postgres,
Neo4j, LightRAG, memory service), ingests repositories and tracks the
last-ingested commit, serves an OIDC-gated REST API and HMAC-verified
GitHub/GitLab webhooks on a shared listener, turns labelled issues into
Tasks, runs the agent turn loop in `tatara-claude-code-wrapper` Pods, and
writes results back to the SCM as one PR per changed repo plus an issue
comment. Remaining work is the gated deploy steps tracked in `ROADMAP.md`.

## Conversation persistence (issue #114)

When an S3 bucket is configured (`s3*` Helm values -> operator ConfigMap +
`s3SecretName` creds), an issue's Claude conversation is persisted to S3 and
resumed across pods so a follow-up message continues the prior conversation
instead of starting from an empty context. The wrapper does the object I/O; the
operator records the pointer and drives the policy:

- Each `Task` is the durable, per-issue home for the conversation pointer
  (`Status.ConversationObjectKey` + `Status.SessionID`), recorded from the
  turn-complete callback and replayed to the next pod as `CONVERSATION_*` env in
  `BuildPod`.
- Resume vs compaction: under `handoverThresholdPercent` (default 25%) of the
  context window the next pod replays the full transcript; at/over it the pod
  starts fresh and gets the compacted text handover instead (the two are
  mutually exclusive, so the window never overflows).
- Forked conversations: each issue proposed by a brainstorm forks (S3
  copy-object) the brainstorm conversation onto its own key so siblings diverge.
- MR review: every MR, including user-authored, spins up a review/test agent
  with its own conversation, the PR head checked out read-only (it never pushes).
- GC: the reaper deletes a batch's S3 objects once all its sibling issues are
  closed (grace `s3ConversationRetentionHours`, default 72h).

The whole feature is **off and fully backwards-compatible until `s3Bucket` is
set** (no S3 env is injected, so pods behave exactly as before).

## Token budget (issue #189)

The operator can pause agent work when token usage approaches a limit within a
reset window, so a runaway spend or an exhausted subscription window stops
proactive work (and, at a higher emergency threshold, incident work) until the
window rolls. The two thresholds map onto the admission queue's two pools: the
normal pool (brainstorm, implement, review, ...) is held at the **proactive
percent** (default 50%); the alert pool (incidents) only at the **emergency
percent** (default 80%), so incidents keep flowing while proactive work is
paused.

Two modes:

- **customWindow** (API per-token billing) - fully operator-side. The operator
  meters its own per-turn token accounting (the same accounting behind
  `operator_task_tokens_total`) against an absolute `tokenLimit` within a
  cron-anchored reset window: `resetSchedule` (a 5-field cron) marks each window
  reset, `windowDuration` is the declared window length (it bounds the
  reset-boundary search). The accumulator lives on `Project.Status.TokenBudget`
  and rolls to zero at each boundary.
- **claudeSubscription** (Claude-code 5h + weekly limits) - report-through-agents.
  The wrapper reports the latest Claude usage snapshot (5h / weekly percent +
  reset) in the turn-complete callback; the operator gates on the larger of the
  two still-active windows. Inert until the wrapper reports a snapshot with a
  future-dated reset (so it never blocks until that wrapper support lands).

Config layers a per-Project `spec.tokenBudget` block over operator-wide env
defaults (`tokenBudget*` chart values -> `TOKEN_BUDGET_*`): a project with no
block inherits the fleet defaults, a block overrides per project and its
`enabled` is authoritative. Observability: `operator_token_budget_used_ratio`
{project, scope=used|proactive|emergency} and
`operator_admission_blocked_total{project,class,reason="token_budget"}`, both on
the Tatara Loop dashboard, with a `TataraTokenBudgetBlocked` alert.

The whole feature is **off and fully backwards-compatible until enabled** (the
zero config is disabled, so the admit path is byte-for-byte unchanged).

## Layout

```
cmd/manager/                       # controller-runtime manager entrypoint + wiring
api/v1alpha1/                      # Project/Repository/Task/Subtask types + deepcopy
internal/controller/               # Project/Repository/Task reconcilers, turn loop, write-back
internal/agent/                    # agent wrapper Pod/Service + turn session/callback
internal/ingest/                   # repo-ingest Job builder
internal/memory/                   # per-project memory stack builders (CNPG/Neo4j/LightRAG/memory)
internal/scm/                      # GitHub/GitLab clients + repo scan + provider registry
internal/restapi/                  # OIDC-gated CRUD REST API
internal/webhook/                  # HMAC-verified push + work-item webhook server
internal/auth/                     # OIDC verifier + client-credentials token source
internal/config/                   # env-scalar config
internal/obs/                      # JSON slog + Prometheus metrics
internal/version/                  # build-stamped version info
charts/tatara-operator/            # cluster-agnostic Helm chart + CRDs
```

## Agent pod customization

`Project.spec.agent` carries optional knobs that shape the wrapper Pod and hook
into the agent session lifecycle. All are optional; omitting them leaves the
default Pod unchanged.

- `agent.hooks` - shell commands the wrapper runs (via `sh -c`) at fixed
  lifecycle points: `preClone`, `postClone`, `conversationStart`,
  `conversationRestart`, `agentTurnFinished`, `conversationFinished`. The
  operator passes each non-empty command to the wrapper as a `HOOK_*` env var.
  Hooks are best-effort: a non-zero exit is logged and counted
  (`ccw_lifecycle_hook_total`) but never aborts the agent run. `preClone`
  receives the repo URL and `postClone` the clone directory (as `$1` and via
  `TATARA_HOOK_REPO_URL` / `TATARA_HOOK_CLONE_DEST`); the conversation/turn
  hooks see `TATARA_TASK` / `TATARA_PROJECT` (and `agentTurnFinished` also
  `TATARA_TURN_ID`).
- `agent.extraEnvs` / `agent.extraEnvsFrom` - extra env vars / `envFrom`
  sources on the wrapper container. `extraEnvs` are appended AFTER the
  operator's own variables, so a stray extra cannot shadow a required one.
- `agent.extraVolumes` / `agent.extraVolumeMounts` - extra Pod volumes and
  wrapper-container mounts.
- `agent.extraInitContainers` / `agent.extraSidecarContainers` - extra init
  containers (run before the wrapper) and sidecars (run alongside it).

See `deploy-samples/tatara-project.yaml` for a worked example.

## Observability

Every long-running surface exposes Prometheus metrics on `/metrics`, and the
chart ships the consumer side so loop failures alert and graph instead of
sitting silent.

Chart objects, each gated like the existing `serviceMonitor` and shipped on by
default:

- `serviceMonitor.enabled` - scrape target for the operator `/metrics`.
- `prometheusRule.enabled` - a `tatara-loop` group of loop-failure alerts over
  the operator's own `operator_*` / `tatara_*` series. Two classes: deadman /
  liveness (operator down, no reconciles, no scan activity, a memory stack
  Failed, Tasks pinned at the concurrency cap) and active failures (reconcile,
  task-terminal, turn-timeout, boot-crash, agent-unreachable, ingest, lifecycle
  giveup, SCM-write, push-rejected, reaper-delete). `severityLabel` and
  `tasksInflightThreshold` are the only tunables; `additionalLabels` is the
  cluster-specific knob the infra helmfile sets so the cluster Prometheus
  `ruleSelector` matches (the chart bakes no selector label).
- `dashboard.enabled` - the "Tatara Loop" Grafana dashboard as a ConfigMap
  labelled `grafana_dashboard: "1"` for sidecar discovery, with `$project` /
  `$repo` template variables. Panels cover the loop golden signals, task
  outcomes, token usage (global / project / repo / issue), and the memory
  corpus. It hardcodes no datasource UID - a `datasource` template variable
  selects the cluster's Prometheus. `additionalLabels` and `folder` tune sidecar
  discovery and placement.

What stays cluster-side (never baked into this chart, per the cluster-agnostic
rule): Alertmanager receivers / routing, the Grafana datasource, and the
`ruleSelector` / sidecar label values.

Metrics powering the above (registered in `internal/obs`):

- `operator_task_tokens_total{project,repo,kind,issue,type}` - agent token spend
  (input / output), the global / project / repo / issue denominator.
- `operator_task_terminal_total{kind,phase,reason}` - every Task terminal
  transition, metered once at the `terminate()` chokepoint; the uniform loop
  success / failure denominator (terminal-failure reconciles return
  `(Result{}, nil)`, so `operator_reconcile_total` cannot stand in).
- `operator_lightrag_documents{project,status}` - per-project lightrag corpus
  size, read best-effort from lightrag's `/documents/status_counts` during the
  gauge recompute.

Alerts deliberately avoid the push-receiver / wrapper (`ccw_*`) series the
operator re-exposes: those TTL-evict and reset their run id per run, so
`rate()` / `increase()` / `absent()` over them are unreliable. Alerts key only
on the operator's own continuously-present series.

## Development

```bash
make generate   # controller-gen deepcopy
make manifests  # controller-gen CRD manifests into the chart
make test       # unit + envtest
make lint       # golangci-lint
make build      # static binary into bin/
make image      # container image
```

## License

AGPLv3. See `LICENSE`.
