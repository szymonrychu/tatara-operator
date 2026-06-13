# MEMORY - tatara-operator

Past decisions and their context. One line per entry, dated. Append-only
in spirit; prune only when a decision is reversed.

- 2026-06-13 (issue-lifecycle, on main 6976ae7, NOT yet deployed) New `issueLifecycle` Task kind: one persistent Task per issue carries an S1..S7 state machine in `Status.LifecycleState` (Triage->Conversation->Implement->MRCI->Merge->MainCI->Done/Stopped/Parked), reusing the existing agent-run + writeback primitives. Binders (issueScan/mrScan/webhook) now create `issueLifecycle` (Triage for issues, MRCI-entry for bot PRs) instead of `triageIssue`/`selfImprove`; legacy arms retained reachable for in-flight migration. KILLS the live `selfImprove ... merge -> 405 merge conflicts` controller-runtime backoff loop: the Merge state classifies a 405 as a conflict and spawns a resolve-conflict re-implement turn returning nil error (regression test `TestLifecycleMerge_405ConflictSpawnsResolveAttempt_ErrNil`); the legacy `writeBackSelfImprove` 405 path was ALSO guarded (absorb->clearWritebackPending) so the loop dies for any in-flight selfImprove Task without a manual deploy step. Spec/plans `docs/superpowers/{specs/2026-06-12-issue-lifecycle-agent-design.md,plans/2026-06-12-issue-lifecycle-m*}` (parent tatara repo).
- 2026-06-13 (issue-lifecycle) M3 context-guard ADAPTATION: the literal spec "context >50% after S7" does NOT map, because each implement run spawns a FRESH wrapper pod (resetAgentRun tears it down at run-end) - no long-lived session accumulates context across S4-S7 iterations. Realized as operator-computed-at-transitions: at each failure transition into Implement (MRCI/Merge-405/MainCI fail) the operator computes `LastTurnInputTokens*100/contextWindowTokens`; >= handoverThresholdPercent (default 50) marks `tatara.dev/pending-handover-resume` + populates `Status.Handover` (agent's `submit_handover` doc, else operator-built from ResultSummary+ImplementContext+branch); the next fresh Implement run injects it. Wrapper needs NO change (its `turn.Record.usage` was already posted to the callback; operator just deserializes it). Still honors the "operator-computed from usage" decision.
- 2026-06-13 (issue-lifecycle) BUGFIX: the `POST /tasks/{t}/issue-outcome` REST handler (`internal/restapi/handlers.go`) was left gated to the legacy `triageIssue` kind, so every `issueLifecycle` Triage run got 409 and could never record its outcome - `finishTriage` then read a nil `Status.IssueOutcome` and silently defaulted to `implement` (close/discuss decisions were impossible). It also rejected the `discuss` action even though the CRD enum (`implement;close;discuss`), `lifecycleTriageText`, and `finishTriage` all support it. Fix: accept `issueLifecycle` alongside `triageIssue`, accept `discuss`, and require a comment for `discuss` (matching `close`). Surfaced while triaging tatara-chat#1 as an issueLifecycle Triage agent.
- 2026-06-13 TOOLING GAP: `golangci-lint run` here does NOT gofmt-check (no gofmt/gofumpt linter enabled), so unformatted files pass lint + pre-commit and only `gofmt -l .` catches them. Always run `gofmt -l .` separately before merge (two M3/M4 files slipped through committed-dirty).
- 2026-06-12 (0.4.2) Ingest Job hardening: TTL lowered 3600->600s (self-GC 10m after finish); operator exponential backoff (30s base, 2x per failure, 30m cap) prevents re-creation flood - previously a failing Job was re-created on every reconcile with no gate, causing a 449-pod/144-job incident; IngestFailureCount + LastIngestFailureTime added to RepositoryStatus for state tracking.
- 2026-06-09 (0.2.12) B2 fix: `MemoryNotReady` was only cleared on the ingest path (after the `if !want` early-return), so already-ingested repos kept the stale condition. Reordered the memory gate + clear before `ingestDecision`, persisting the flip immediately. Also: the agent's headless CLI auth fails `unauthorized_client` because `tatara-cli-oidc/client-secret` (sops `cliOidcClientSecret`) is STALE vs the live tatara-operator Keycloak client; the operator's own `OPERATOR_OIDC_CLIENT_SECRET` works - user must re-sync the sops value (NOT a code bug).
- 2026-06-09 (0.2.11) Agent-native tatara tools (operator half): inject
  `TATARA_OPERATOR_URL` (config `OPERATOR_URL`, default
  `http://tatara-operator.tatara.svc:8080`) onto the agent pod so the CLI's
  `task_*`/`subtask_*` MCP tools have an endpoint - previously only
  `TATARA_MEMORY_URL` was set. Also clear the stale `MemoryNotReady` condition on
  Repositories once `project.status.memory.phase == Ready` (it lingered from
  provisioning and misled - the agent self-audit flagged it). Pairs with
  tatara-cli headless client-credentials auth + wrapper `tatara mcp-config` at
  boot. Note: the operator subagent died mid-O1 (socket error); O1 was finished +
  O2 done by the integrator.
- 2026-06-09 (0.2.10) Report/question tasks were invisible: a task with no code
  change pushes no branch, so write-back opened no PR and returned WITHOUT
  commenting - the agent's answer (`status.ResultSummary`) never reached the
  issue. Now when zero PRs are opened, `doWriteBack` posts the ResultSummary (or
  Goal) as an issue comment, so "check X / report Y" issues surface their result.
  Found via a user create-then-label issue ("check all MCP tools, report a table")
  that ran but produced nothing visible.
- 2026-06-09 (0.2.9) Cross-repo agent tasks: BuildPod gains a repos param and sets TATARA_REPOS (JSON array, primary first) so the wrapper clones all Project repos into /workspace/<name>; planTurnText gains the multi-repo workspace layout instruction; doWriteBack loops over all Project repos and attempts OpenChange per repo (4xx/no-branch repos are skipped), collects all PR URLs, and comments the issue with all links; primary PR URL remains in task.Status.PrURL. fakeWriterPerRepo added to writeback tests. Boy-scout: gofmt trailing blank line in operator_metrics.go.
- 2026-06-09 (0.2.8) Dedupe Task creation by issue ref: creating an issue WITH
  the label fires both `issues.opened` (label present) and `issues.labeled`, so
  the webhook made TWO Tasks (two agents, two PRs) per issue. `handleWorkItem`
  now skips creation if a non-terminal Task already exists for the issue ref;
  re-labeling after completion still re-triggers. Fixes the dogfood "create an
  issue with the label -> 2 agents" gotcha.
- 2026-06-08 (0.2.7) Agent created ZERO subtasks so nothing got implemented: the
  plan-turn prompt (`turnloop.planTurnText`) said "Do not start implementation in
  this turn", only `subtask_create`. Agents didn't decompose, so the loop ended
  with no work and write-back had no diff. Relaxed the prompt: implement small
  tasks directly in the plan turn (the wrapper auto-commits/pushes each turn),
  decompose into Subtasks only for multi-step work. Pairs with wrapper 0.1.3
  (excludes wrapper session config from the commit; bakes superpowers skills).
- 2026-06-08 (0.2.6) Agent never produced a PR: the agent edits files but does
  not reliably branch/commit/push, so write-back hit `422 head invalid` (branch
  `tatara/task-*` never existed). Fix split across repos: wrapper now enforces
  the git workflow (checkout -b + commit/push per turn, 0.1.2); operator exports
  `agent.TaskBranch` (single source of the `tatara/task-<name>` convention) and
  passes it as `TASK_BRANCH` env to the agent pod. `controller.taskBranch` now
  delegates to `agent.TaskBranch` so write-back, turn prompts, and the wrapper
  agree on the exact branch. Also noted this run: the operator created TWO Tasks
  for one issue (issue.opened-with-label + issue.labeled both fire) - dedupe by
  issue ref is a TODO (see ROADMAP).
- 2026-06-08 (0.2.5) lightrag `OPENAI_API_KEY` bug: `lightragEnv`
  (`internal/memory/lightrag.go`) set `LLM_BINDING_API_KEY` but not
  `OPENAI_API_KEY`. LightRAG's openai LLM/embedding paths fall back to the raw
  `OPENAI_API_KEY` env, so its async processing pipeline failed
  `KeyError 'OPENAI_API_KEY'` and every queued doc went `doc_status=failed`
  (963 on the cluster) - chunks/entities/relations never materialized even though
  `/documents/text` returned 200. Fix: source `OPENAI_API_KEY` from the same
  openai Secret (key `LLM_BINDING_API_KEY`). Surfaced only once the upstream
  chunk path drained end-to-end (memory 0.2.2/0.2.3 + ingester 0.2.2 kubectl
  fixes) - the dogfood was the first real exercise of LightRAG doc processing;
  earlier "index built" was schema/HNSW init, not document processing.
- 2026-06-08 (0.2.4) imagePullSecrets bug: operator-spawned **ingest Jobs**
  (`internal/ingest/job.go`) and **agent wrapper Pods** (`internal/agent/pod.go`)
  set their container image from a private Harbor registry but had no
  `imagePullSecrets`, so both failed `ErrImagePull` ("no basic auth credentials").
  The 0.2.1 fix only covered the per-Project memory pods (`internal/memory`).
  Fixed by mirroring the memory `imagePullSecrets(cfg)` helper in both packages,
  fed from `IMAGE_PULL_SECRET` (regcred) via `wire.go`. Surfaced only on the first
  real dogfood ingest (envtest never pulls images). Rule: every operator-spawned
  workload that pulls a private image needs imagePullSecrets wired from config.
- 2026-06-08 (0.2.3) All four cluster-managed secrets (`tatara-anthropic`,
  `tatara-cli-oidc`, `lightrag-openai`, `tatara-scm`) are now chart-rendered from
  sops values (`templates/managed-secrets.yaml`), replacing manual `kubectl create
  secret`. Each gated on its value(s); paired creds guarded with `and` so a
  half-set pair never renders an empty credential. Names from existing
  `*SecretName` values (+ new `scmSecretName`); data keys fixed by consumer code.
  Chart-only change (no Go): chart `version` 0.2.3, `appVersion` stays 0.2.2, image
  not rebuilt. Migration was delete-then-apply (manual secrets were not helm-owned,
  so adoption would conflict). Deferred: multi-project SCM secrets - would need a
  projects map (a list in `values.yaml`), which hard rule 6 forbids; one
  `tatara-scm` is rendered for the single Project.
- 2026-06-08 (0.2.2) Spawned agent auth switched from console API key to a
  long-lived Claude Code OAuth token: `pod.go` BuildPod now injects
  `CLAUDE_CODE_OAUTH_TOKEN` from Secret `<anthropicSecretName>` data key
  `oauth-token` (was `ANTHROPIC_API_KEY`/`api-key`). Pure replace, not additive:
  claude auth precedence puts `ANTHROPIC_API_KEY` above the OAuth token, so
  injecting both would leave the OAuth token inert. Wrapper needs no change
  (it passes os.Environ() straight to the claude child; OAuth login does not
  trigger the API-key dialog claudejson.go seeds). The `tatara-anthropic`
  Secret must carry `oauth-token` (from `claude setup-token`); the old `api-key`
  key is no longer read. Drives subscription billing instead of console API.
- 2026-06-06 Repo created at milestone M0 (scaffold). API group `tatara.dev`,
  version `v1alpha1`, kinds Project/Repository/Task/Subtask, all namespaced
  to `tatara`. Built on kubebuilder/controller-runtime (rejected plain
  client-go: boilerplate, no envtest, non-idiomatic; rejected Argo-backed
  reconcilers: argo retired for tatara).
- 2026-06-06 Shared contracts pinned in
  `~/Documents/tatara/docs/superpowers/plans/_tatara-operator-shared-contracts.md`.
  All milestones use those exact names/paths/signatures.
- 2026-06-06 obs/auth mirror tatara-chat `internal/{obs,auth}`;
  client-credentials TokenSource mirrors
  tatara-memory-repo-ingester `internal/push/auth.go` (Keycloak `audience`
  form param).
- 2026-06-06 M0 scaffold complete: api/v1alpha1 (4 kinds + deepcopy),
  internal/{config,obs,auth}, no-reconciler manager, Dockerfile, Makefile
  (generate/manifests/test/lint/build/image), chart skeleton lint-clean with
  CRDs + RBAC. `make generate && make manifests && make test` green;
  `helm lint charts/tatara-operator` clean.
- 2026-06-06 scheme.Builder (sigs.k8s.io/controller-runtime/pkg/scheme) avoided;
  using runtime.NewSchemeBuilder + explicit AddKnownTypes per apimachinery
  pattern (SA1019 staticcheck). This is a deviation from the plan which used
  scheme.Builder, but required to keep lint clean.
- 2026-06-06 Dockerfile GO_VERSION set to 1.26 (not 1.25 as in plan template)
  because go.mod requires go 1.26.0 - local toolchain is 1.26.0 per rule 1.
- 2026-06-06 Makefile HELM_BIN grep uses 'mise/installs/helm/' (trailing slash)
  to avoid matching 'helmfile' in the same mise PATH entries.
- 2026-06-06 (M1) Ingest result SHA flows via a per-Repository ConfigMap
  `<repo>-ingest-result` (key `sha`). The ingest Job patches it via the
  in-cluster API after `git rev-parse HEAD`; the reconciler pre-creates it
  (owner-ref Repository) and reads it on Job success. Chosen over pod-log
  parsing (brittle) and Job annotations (Job cannot patch itself cleanly).
  REQUIRES M6 chart to create ServiceAccount `tatara-ingest` + a Role granting
  get/create/update/patch on ConfigMaps in ns `tatara`. Ingest container also
  needs `kubectl` on PATH (the ingester image carries the Go toolchain; verify
  kubectl presence in M6, else switch the patch step to a tiny `curl` against
  the API or a dedicated sidecar).
- 2026-06-06 (M1) Re-ingest trigger: full when status.lastIngestedCommit=="",
  incremental (--since lastIngestedCommit) when annotation
  tatara.dev/reingest-requested (RFC3339) is newer than status.lastIngestTime.
  status.jobName is the single-flight guard. Conditions: Project `Ready`,
  Repository `Ingested`.
- 2026-06-06 (M1) Clone uses x-access-token:${SCM_TOKEN}@<host/path> in the
  init container; works for both GitHub and GitLab HTTPS with the Secret key
  `token`.
- 2026-06-06 (M1) OperatorMetrics registered against ctrlmetrics.Registry
  (controller-runtime's own prometheus registry) so they are served on the
  existing /metrics endpoint without a second registry or server.
- 2026-06-06 (M1) config.Config gained Namespace field (NAMESPACE env var,
  default tatara); needed by ingestConfig to namespace the Job and result CM.
- 2026-06-06 (M1) On ingest Job failure, RepositoryReconciler sets phase=Failed and clears jobName but does NOT bump lastIngestTime; an incremental failure (annotation still newer than lastIngestTime) therefore relaunches on the next reconcile. In-Job retries are bounded by the Job's backoffLimit=2; there is no separate reconciler-level backoff. This is intended.
- 2026-06-06 obs.Metrics (metrics.go) was dead code: created its own registry, never called in production. Consolidated all 5 platform metrics into OperatorMetrics (operator_metrics.go) on the injected registerer; deleted metrics.go and metrics_test.go.
- 2026-06-06 (M2) ReingestRequestedAnnotation moved to api/v1alpha1/annotations.go (canonical); internal/controller aliases it. This is the single source of truth per the plan's boy-scout correction requirement.
- 2026-06-06 (M2) SameRemote normalizes by stripping trailing .git and trailing /, then lowercasing the host for a case-insensitive compare. Chosen to handle the common GitHub/GitLab variant where clone URLs end in .git in the CRD but not in webhook payloads (or vice versa).
- 2026-06-06 (M2) Webhook server uses obs.OperatorMetrics.WebhookEvent(provider,kind,result) method (all-private fields) not a public CounterVec; plan's Task 5 code referenced a non-existent .WebhookEvents public field - corrected at implementation time.
- 2026-06-06 (M2) Work-item path (issue/mr with triggerLabel) is a metered 202 stub in M2; Task creation is deliberately deferred to M5 alongside scm.OpenChange/Comment write-back.
- 2026-06-06 (M3 Tasks 1-4) internal/restapi scaffold complete. API-drift corrections vs plan: (1) TaskStatus field is PrURL (not PRURL); (2) RepositoryStatus.LastIngestTime is *metav1.Time (pointer), nil-checked before formatting; (3) no auth.Middleware function exists - Mount takes func(http.Handler) http.Handler directly, which is the correct pattern. In-memory projectRef filtering for repositories/tasks (no fake-client field index). Branch feat/m3-restapi.
- 2026-06-06 (M3 Tasks 5-8) REST API complete. PATCH /tasks/{t} records resultSummary + dedupes AgentNote condition via apimeta.SetStatusCondition (bounded list, repeated notes update the same condition). POST /tasks/{t}/subtasks uses fmt.Sprintf("%s-st-%d", task, UnixNano) for deterministic names (fake client ignores generateName). PATCH /subtasks/{s} sets phase/result/turnId via status subresource. Shared HTTP_ADDR listener: webhook.Server gained Mount(chi.Router); webhook.HandlerRunnable added to serve arbitrary http.Handler; auth.Middleware added (mirrors tatara-chat); addWebhookServer in wire.go now takes ctx for OIDC discovery and builds one chi.Mux with both route groups. M3 Part A merged to main.
- 2026-06-06 (M4 Tasks 6-8) Turn-loop annotation contract: in-flight turnId stored as annotation `tatara.dev/current-turn` on Task; executing Subtask name as `tatara.dev/current-subtask`; callback requeues by bumping `tatara.dev/turn-complete` (RFC3339); pod recreation count in `tatara.dev/pod-recreations`. Annotations used (not new status fields) to avoid reopening the M0 CRD schema.
- 2026-06-06 (M4 Tasks 6-8) M5 write-back hook: terminated Succeeded Tasks get condition `type=WritebackPending, status=True, reason=AwaitingM5`. M5 SCM path queries this condition and clears it once the PR/MR + comment is landed.
- 2026-06-06 (M4 Tasks 6-8) API-drift: plan used `meta.SetStatusCondition` from `k8s.io/apimachinery/pkg/api/meta` but the actual import path is `k8s.io/apimachinery/pkg/api/meta` as `apimeta`; resolved at implementation. `mkProject`/`mkRepository` helpers in task_controller_test.go renamed to `mkTaskProject`/`mkTaskRepository` to avoid collision with pre-existing helpers in repository_controller_test.go (different signature). `contains`/`indexOf` helpers already declared in repository_controller_test.go; removed from task test file.
- 2026-06-06 (M4 Tasks 6-8) TurnsCompleted increment: bumped in `recordTurn` when the incoming task has a non-empty `annTurnComplete`, i.e. one turn per delivered callback. Plan turn (turn 0) completes without incrementing until the next recordTurn sees the callback; subtask turns each increment once on close.
- 2026-06-06 (M4 Task 9) Turn<->Task correlation via annotations (tatara.dev/current-turn, current-subtask, turn-complete, pod-recreations), not new CRD status fields, to avoid re-opening the M0 schema. M5 write-back keys off the WritebackPending condition set on Succeeded. CallbackServer.Session is optional (nil-guarded in poll backstop tick) so unit tests that only test the HTTP handler need not wire a Session.
- 2026-06-06 (M5) Agent branch naming convention: `tatara/task-<task-name>` (deterministic, no status annotation needed). TaskReconciler.SCMFor factory (injected, faked in tests) resolves provider from Task.Spec.Source.Provider or derived from Repository remote URL host (gitlab.* -> gitlab, else github). Task.Spec.Source is *TaskSource (pointer); nil-checked throughout. Task.Status.PrURL is the Go field (json: prURL) - plan used PRURL incorrectly; corrected at implementation. OpenChange permanent 4xx clears WritebackPending without requeue; transient errors propagate. Comment failure is non-fatal (log+continue). Webhook work-item -> Task path now complete: Task created with goal=ev.Body, source{provider,issueRef,url}, projectRef/repositoryRef, owner-ref Project. M2 stub test TestIssueWithTriggerLabelStubbed removed (replaced by workitem_test.go). GitLab Comment targets /issues/{iid}/notes not /merge_requests - appropriate for issue-origin work items; MR-origin comment targeting is follow-up if needed.
- 2026-06-06 (M5 code-review fixes) Operator<->agent branch contract: `tatara/task-<task-name>` is injected via turn prompts (planTurnText/turnText in turnloop.go); the operator also opens the PR/MR targeting that same branch. If the wrapper ever enforces branch pushing itself it MUST use this same value. The branch is NOT communicated via REPO_BRANCH env. providerForRemote now correctly handles github-substring hosts and logs a warning for unknown hosts defaulting to github. Permanent 4xx from OpenChange uses neutral reason WritebackSkipped. Issue comment body no longer duplicates the Source footer from writeBackBody.
- 2026-06-06 (M4 code-review fixes) Four distinct ports: HEALTH_ADDR (:8081) = manager health probe bind; INTERNAL_ADDR (:8082) = callback server bind (not via ingress); METRICS_ADDR (:9090); HTTP_ADDR (:8080). CALLBACK_URL (full routable in-cluster base URL) is separate from INTERNAL_ADDR (bind addr). M6 chart MUST: (1) set CALLBACK_URL to the operator callback Service DNS (e.g. http://tatara-operator-internal.tatara.svc:8082); (2) create a Service exposing INTERNAL_ADDR (:8082) for the callback path; (3) HEALTH_ADDR/INTERNAL_ADDR/METRICS_ADDR/HTTP_ADDR are four distinct ports in the Deployment. Added per-turn deadline: tatara.dev/turn-started-at stamped on each turn submit; turns exceeding turnTimeoutSeconds+60s grace -> Task phase=Failed/TurnTimeout in both the reconciler and the poll backstop. RetryOnConflict wraps Task annotation + Subtask status writes in turncallback.go. Empty/missing turnId -> HTTP 400 before any Task lookup. resolveTaskByTurn skips Tasks with empty annCurrentTurn. markSubtaskDone now uses task.Namespace (not r.PodConfig.Namespace). atoiSafe/itoa replaced by strconv.Atoi/Itoa.
- 2026-06-06 (M6) Chart hardened. Cluster-agnostic (rule 14): no baked regcred/affinity/ingress-host/storage-class; all from infra helmfile tatara bucket. Config: 15 keys via envConfig helper -> ConfigMap envFrom (rule 6); OPERATOR_OIDC_CLIENT_SECRET only secret env, via templated Secret (existingSecret override supported). Four distinct ports in Deployment: http :8080, health :8081, internal :8082, metrics :9090. Liveness/readiness probes on HEALTH_ADDR (:8081). Two Services: main (http:8080 + metrics:9090) and tatara-operator-internal (internal:8082). CALLBACK_URL in ConfigMap must be set to http://tatara-operator-internal.<ns>.svc:8082 by infra values.
- 2026-06-06 (M6) Manager RBAC: namespaced Role+RoleBinding (tatara.dev CRDs+status, batch/jobs, pods/services/configmaps/secrets-read/networkpolicies, events) + cluster-scoped ClusterRole+ClusterRoleBinding for CRD-reader (apiextensions.k8s.io/customresourcedefinitions). Single-cluster single-release assumed; ClusterRole name carries release fullname.
- 2026-06-06 (M6) tatara-ingest SA + ConfigMap-patch Role (get/create/update/patch) + RoleBinding added (M1 follow-up). SA name is the fixed string "tatara-ingest" (M1 Job hardcodes it); do not release-prefix without changing internal/ingest/job.go.
- 2026-06-06 (M6) Managed-pod NetworkPolicy selects tatara.dev/managed-by=tatara-operator. M1 ingest Job pod template (internal/ingest/job.go) and M4 agent Pod (internal/agent/pod.go) MUST set this label or the NetworkPolicy will not apply. Egress: DNS, tatara-memory:8080, tatara-chat:8080, operator:8080+8082, HTTPS-to-any (443). CIDR tightening of 443 deferred to wrapper hardening ROADMAP.
- 2026-06-06 (M6) Ingress gated by ingress.enabled (default false, rule 14). Host/className/path from infra values. Internal service (tatara-operator-internal) is NOT exposed via ingress. ServiceMonitor scrapes metrics port.
- 2026-06-07 (M6 Task 10) Chart lint + render verification passed: 0 chart(s) failed, 15 rendered objects (plan said 14; +1 from the pre-existing internal Service tatara-operator-internal). 4 container ports confirmed. NetworkPolicy podSelector, dual Service, envFrom x2, 12 ConfigMap keys, RBAC x4 kinds, ingest verbs all spot-checked. 4 CRDs all Namespaced. kubeconform not installed (helm lint + object count is sufficient gate per plan).
- 2026-06-07 (M6 Task 12) tatara-operator confidential Keycloak client + tatara-operator audience mapper added to infra/terraform/keycloak/tatara_clients.tf. terraform fmt clean; validate: Success. NOT applied (gated). The audience mapper means one tatara-cli token carries aud=tatara-memory AND aud=tatara-chat AND aud=tatara-operator (resolves M3 multi-audience need, no token exchange required).
- 2026-06-07 (M6 Task 13) tatara-operator release added to infra helmfile tatara bucket (oci://harbor.szymonrichert.pl/charts/tatara-operator version 0.1.0). values/tatara-operator/common.yaml (image tag pin), default.yaml (ingress host, externalWebhookBase, endpoints), default.secrets.yaml (sops-encrypted placeholder; real secret requires terraform apply + sops-secret-helper populate). helmfile diff/apply are gated (chart not yet published to OCI registry). callbackUrl wired to http://tatara-operator-internal.tatara.svc:8082 via default.yaml (CALLBACK_URL env var consumed by manager).
- 2026-06-07 (N1) cnpg api module pinned to v1.29.1 (matches live operator image ghcr.io/cloudnative-pg/cloudnative-pg:1.29.1 in all namespaces); part of main cloudnative-pg module, heavy transitive deps accepted to use upstream Cluster type (hand-rolling Cluster struct would violate rule 4). go directive bumped 1.26.0->1.26.3 (cnpg requires >=1.26.3; still 1.26 minor series).
- 2026-06-07 (N1) MEMORY_BASE_URL removed from config.Config and operator config; per-Project endpoint (status.memory.endpoint) replaces it. ingest.Config.MemoryBaseURL removed entirely in N3; ingest base URL now comes from project.Status.Memory.Endpoint passed directly to BuildJob. wire.go ingestConfigFromConfig no longer propagates the global URL; wire_test.go updated to expect empty MemoryBaseURL from the mapping.
- 2026-06-07 (N1) Project CRD gained spec.memory (*MemorySpec) and status.memory (*MemoryStatus); deepcopy and CRD manifest regenerated. Defaults (pgInstances 1, pgStorage 10Gi, neo4jStorage 10Gi) applied in builders not kubebuilder so absent spec.memory still provisions.
- 2026-06-07 (N1) per-Project memory builders landed in internal/memory (pure, unit-tested): NamesFor, Endpoint, PGCluster, Neo4jPasswordSecret, Neo4jStatefulSet, Neo4jService, LightragDeployment, LightragService, LightragPVC, MemoryDeployment, MemoryService, MemoryConfigMap, MemorySecret. All owner-ref'd to Project; make test + lint green.
- 2026-06-07 (N1) pin set Names(project) -> NamesFor(project): func and returned struct cannot share the name Names in Go.
- 2026-06-07 (N1) cnpg v1.29.1 exports SchemeGroupVersion (not GroupVersion); PGCluster TypeMeta.APIVersion uses cnpgv1.SchemeGroupVersion.String(). All struct field names (Instances, StorageConfiguration.Size, Bootstrap.InitDB.{Database,Owner,PostInitApplicationSQL}) match v1.27.x plan exactly.
- 2026-06-07 (N2) ProjectReconciler.Reconcile now provisions the full per-project memory stack via SSA (client.Apply/Patch), computes status.memory.{phase,endpoint}, and sets MemoryReady condition; single status update persists both Ready+MemoryReady atomically. fakeStackHealthy must set .Status.Replicas=1 in addition to ReadyReplicas/AvailableReplicas or envtest API server rejects the update (readyReplicas<=replicas validation).
- 2026-06-07 (N2) client.Apply (Patch variant) marked //nolint:staticcheck; deprecated in controller-runtime v0.24.1 with no stated removal version. Migration to typed r.Apply(ctx, applyconfig) requires generated applyconfiguration types for all 10 stack objects (incl. cnpg Cluster) - backlog item (NOT done in N4; the nolint is an accepted rule-4 deferral with this rationale until typed-apply is feasible).
- 2026-06-07 (N2 code-review fixes) operator_memory_stacks gauge: SetMemoryStacks(phase,1) replaced by SetMemoryStackCounts(prov,ready,failed int) that LISTs all Projects and sets all three phase gauges atomically after each reconcile; projects with nil status.memory are not counted. Duration histogram: fires once only on the Provisioning->Ready transition, measured from p.CreationTimestamp.Time (wall-clock to ready, not reconcile duration). applyMemoryStack neo4jPassword param removed (was unused; password Secret is applied separately). failMemory redundant ensureMemoryStatus call removed (p.Status.Memory is always non-nil at that point).
- 2026-06-07 (N3) ingest Job + wrapper pod target per-project `Project.status.memory.endpoint`; both reconcilers requeue (15s) until `status.memory.phase == "Ready"` (Repository sets `MemoryNotReady` condition). `ingest.BuildJob` takes base URL as a param; `agent.BuildPod` emits `TATARA_MEMORY_URL` (agent tatara-cli memory MCP reads it). Operator `Config.MemoryBaseURL` / `MEMORY_BASE_URL` removed (stale comments in wire.go/test also removed).
- 2026-06-07 (N4) Static tatara-memory retired; per-Project memory is operator-provisioned. Chart 0.2.0: Role adds postgresql.cnpg.io clusters(+/status), apps/deployments (WAS MISSING - N2/N3 Owns(&appsv1.Deployment{}) required it), apps/statefulsets, core/persistentvolumeclaims CRUD; secrets verbs widened to create/update/patch/delete (was read-only) for generated neo4j password + memory config Secrets. ConfigMap drops MEMORY_BASE_URL (N3), adds MEMORY_IMAGE/LIGHTRAG_IMAGE/NEO4J_IMAGE/OPENAI_SECRET_NAME. cnpg Cluster CRD in charts/tatara-operator/crds: provenance header added (v1.29.1 matching go.mod); N2 had vendored it without the header. Image pins: tatara-memory:0.2.0, lightrag v1.4.16 (tag, not digest; subchart pin), neo4j:5-community (native single-node STS, not upstream neo4j chart). Shared OpenAI secret name: lightrag-openai (ns tatara, key LLM_BINDING_API_KEY).
- 2026-06-07 (N5/fix) Deploy bug: neo4j ImagePullBackOff. Two root causes: (1) all operator-spawned workloads (neo4j StatefulSet, lightrag Deployment, tatara-memory Deployment, cnpg Cluster) lacked imagePullSecrets - harbor proxy-dockerhub requires auth. Fixed via new IMAGE_PULL_SECRET config scalar (env var -> ConfigMap -> manager -> memory.Config.ImagePullSecret -> injected into all four workload pod templates). (2) neo4j:5-community is not a valid tag; replaced with neo4j:2026.04.0 (proven pullable). Chart 0.2.1. cnpg ClusterSpec.ImagePullSecrets field is []cnpgv1.LocalObjectReference which is a type alias for github.com/cloudnative-pg/machinery/pkg/api.LocalObjectReference (NOT corev1.LocalObjectReference) - required a separate pgImagePullSecrets helper in pg.go; the corev1-typed imagePullSecrets helper in memory.go cannot be used directly.

- 2026-06-09 scm-projects: SCMWriter is the single egress interface (12 methods); GitHub board ops are net-new GraphQL (Projects v2), GitLab boards are scoped-label driven; approval label is source of truth, mirrored as ApprovalApproved condition; merge gated by MergePolicy + GetPRState CI.

2026-06-09 - **Agent-native tools were dead until cli 0.4.3.** Every wrapper agent run Succeeded with 0 subtasks / empty issue comments because tatara-cli's `tatara mcp` returned an error on tools/list (NewTool+WithRawInputSchema set both InputSchema and RawInputSchema; mcp-go refuses to marshal). claude loaded ZERO tatara tools while `claude mcp list` still said "Connected" (initialize handshake only). Root cause + fix in tatara-cli MEMORY (0.4.3, NewToolWithRawSchema). Rebuilt wrapper 0.1.8 (bundles cli 0.4.3); Project tatara agent.image -> 0.1.8 (kubectl patch; Project is kubectl-managed, not helm). Validated: bare Task -> agent called subtask_create via MCP -> Subtask created. Auth (operator/memory OIDC) was never the blocker; the curl probes bypassed claude.

2026-06-11 - **autonomous-cron ACTIVATED on Project tatara via personal PAT (no bot account, no Project board).** Cron scans iterate the Project's Repository CRs (projectReposForScan: RepositoryList filtered by Spec.ProjectRef); the GitHub Projects-v2 board is OPTIONAL (issueScan adds board items only when Spec.Scm.Board != nil). Activation gate is purely `Scm != nil && Cron != nil` - no botLogin/board/org dependency. botLogin == the PAT owner (szymonrychu), so mrScan routes the owner's own PRs to selfImprove and others' to review (author/actor egress gate via GetPRState). prReactionScope does NOT gate cron mrScan (it scans all open PRs); it only gates the reactive webhook path. First fire verified: listed 5 PRs/7 issues, created selfImprove+triageIssue+brainstorm Tasks; agent pods run wrapper 0.1.12.
2026-06-11 - **Wrapper image version numbering diverged from the repo.** Deployed agent image was 0.1.11, but wrapper repo appVersion only ever went 0.1.4->0.1.5->0.1.7->0.1.8 - the 0.1.9/0.1.10/0.1.11 tags were ad-hoc `make image VERSION=...` overrides during the graphify deploys, never committed to Chart.yaml. Resolution: re-cut wrapper 0.1.12 (forward of the deployed 0.1.11). Content is determined by the BAKED CLI, not the wrapper number: wrapper 0.1.12 bakes cli 0.6.0, a linear superset (17 graphify code_* tools + SCM propose/review/pr_outcome + issue_outcome), so it strictly supersedes 0.1.11 (older graphify-era cli) despite numbers. RULE: always cut wrapper forward of the deployed tag and verify content via the baked cli version.
2026-06-11 - **stampScan lacks RetryOnConflict (minor, 0.4.1 backlog).** projectscan.go stampScan does Get + Status().Update without retry, so a scan racing the main reconcile logs a benign "object has been modified" ERROR; the stamp succeeds on the next reconcile (timestamps ARE recorded; hourly schedule means no hot loop, dedup prevents duplicate Tasks). Wrap in retry.RetryOnConflict to kill the ERROR noise.
2026-06-11 - **Egress: applied the additive tatara-egress-internet allow ONLY (ipBlock 0.0.0.0/0:443 + DNS, keyed on tatara.io/egress=internet), NOT the runbook's namespace-wide default-deny-egress.** ns tatara today: only tatara-operator-managed-pods selects pods (managed-by=tatara-operator); chat/memory/cnpg/neo4j are selected by NO egress policy -> open egress. A blanket default-deny would newly restrict and likely break them. Manager pod is NOT managed-by -> open egress -> cron SCM calls (443) unrestricted. Hardening the namespace needs a per-pod egress audit first.
2026-06-12 (issue #3) memoryStackHealth tolerates NotFound: a NotFound read on any of the 4 owned objects (cnpg Cluster, neo4j STS, lightrag/memory Deployments) is treated as not-yet-ready (count stays 0 -> memoryPhase=Provisioning) instead of erroring. reconcileMemory no longer calls failMemory("HealthError") on a health read error: a non-NotFound (transient API/cache) error leaves phase+MemoryReady untouched and requeues with backoff. Failed is now reserved for password/apply errors only. Key insight: genuine provisioning stalls (objects exist but never become ready) already surface as a stuck Provisioning phase via memoryPhase, never Failed - so health-read-error->Failed only ever caused the spurious flap the issue describes; removing it needs no grace-period state.
2026-06-12 - **Per-repo fan-out (improvement #1): scan creation is intentionally O(#repos), not the old global maxPerCycle.** mrScan/issueScan now top up each repo's lane to MaxPerRepo (CronActivity field, was MaxPerCycle; default 1) via selectPerRepo + laneOccupancy. laneOccupancy counts a repo's Tasks in {Pending,Planning,Running} (EXCLUDES AwaitingApproval/Succeeded/Failed) so an awaiting-approval proposal does NOT hold the lane. Consequence: a single scan can create up to #repos Tasks (vs <=maxPerCycle before). This is BOUNDED, not unbounded: occupancy counts Pending, so each lane caps at MaxPerRepo and total in-flight Task CRs <= 2*#repos*MaxPerRepo (12 for tatara's 6 repos). Execution is throttled by spec.maxConcurrentTasks (atConcurrencyCap, EXECUTION-time gate on isActive only - it never throttles CREATION). A 60s backlogRequeue refills freed lanes. Rationale: a creation-side cap would defeat the fan-out; for tatara (6 repos) the bound is trivial. If a Project ever has very many repos, add a per-scan creation cap. brainstorm (BrainstormActivity.MaxPerCycle) is untouched (genuinely per-cycle). Branch feat/per-repo-fanout; awaiting deploy.
