# ROADMAP - tatara-operator

Planned work not yet started. One line per item; link to plans for detail.

- [x] **THE TASK-CENTRIC REDESIGN. Shipped 2026-07-13, chart 1.0.0 (MAJOR, BREAKING).** All 22 tasks
  of `docs/superpowers/plans/2026-07-12-task-centric-operator.md` (parent repo) against
  `2026-07-12-task-centric-CROSS-REPO-CONTRACT.md` v7. The phase/lifecycle/WorkItems machine is
  GONE (-27,000 LoC) and a 15-stage machine replaces it, with `Issue`/`MergeRequest` mirror CRDs,
  a single transition choke point, F.4 deadline clocks on every stage, a sequential operator-owned
  merge, and the B.4 sweep as the SOLE intake (the webhook mints nothing). `Subtask` and `WorkItems`
  are deleted. **CUTOVER: `kubectl delete crd subtasks.tatara.dev` is a MANDATORY explicit step** -
  `helm.sh/resource-policy: keep` means `helm upgrade` leaves the CRD behind. Decisions, traps and
  errata are in `MEMORY.md` (2026-07-13 entries).

Open, out of scope, deliberately not done:

- [ ] **Remove the superseded `documentationScan`** (+ `createDocumentationTask`, `documentationInFlightProject`, `oldestCommitSHA`/`latestCommitSHA`, `documentation_guard_test.go`). The 2026-07-13 wiring pass swapped the documentation cron to `MintDocBatch` (F2 nightly batch); `documentationScan` (per-changed-repo diff model) is now dead in production but still test-referenced (lint-safe). Delete once its QueuedEvent doc-kind path is confirmed unused.
- [ ] **#365: memory-stack anti-affinity/topologySpreadConstraints.** Follow-up from issue #355
  (memory-provisioning stream D) and #327: `internal/memory/memory_builders.go` sets no pod
  anti-affinity/topologySpreadConstraints on the cnpg PGCluster/Neo4jStatefulSet/LightragDeployment/
  MemoryDeployment, so a single node can (and did, in #327) host every backend for multiple
  projects. Chart-configurable per rule 6/14, same pattern as the existing `agentScheduling` value.
- [ ] **I1 metrics have no consuming alert yet.** The four K.1 metrics (`operator_task_stage`, `operator_task_stage_age_seconds`, `operator_task_parked_total`, `operator_queue_age_seconds`) are now emitted, but the deployed tatara-observability alerts still key on the OLD vocabulary. Port the K.2 contract alerts (stage-stall, incident-starvation, merge/deploy-blocked) onto these metrics in tatara-observability.
- [ ] **Per-activity scan-cron heartbeat alert** on `obs.SweepLastSuccessTimestamp{activity=brainstorm|documentation}` (metric-wiring audit, issue #370). `TataraLoopStalled` deliberately does NOT cover a stalled brainstorm/documentation cron specifically (both are opt-in with slow, project-configured cadences; a staleness check needs `absent_over_time`-aware gating so a project that never enabled the activity doesn't false-fire) - it only watches the always-on B.4 sweep via `operator_tasks_minted_per_sweep_count`. A narrower, lower-severity alert for "this project enabled brainstorm/documentation and it silently stopped running" is real alerting design, not a mechanical repoint, and was left undone here.
- [ ] **Companion tatara-observability allowlist entries** for `operator_review_post_total` and `operator_stage_drift_total` (metric-wiring audit, issue #370) - both are correctly wired and pre-warmed operator-side but absent from `scripts/metrics_allowlist.txt` and unconsumed by any alert/dashboard. Land after streams B (#59) / E (#60)'s concurrent allowlist rewrite there merges or rebase over it.
- [ ] tatara-observability allowlist entry for `operator_mr_ownership_flip_total` (direction, reason). Wired and pre-seeded operator-side; land with the streams B/E allowlist rewrite. Out of scope for the MR-ownership PR.
- [ ] tatara-observability allowlist entry for `operator_admission_wake_total` (class, agent_kind). Wired operator-side by the 2026-07-28 admission-wake watch; a rate that falls to zero while `operator_queue_admitted_total` keeps climbing is the signal the wake edge was lost and admission is back on the 5m `admissionRequeue` backstop. Land with the streams B/E allowlist rewrite. Out of scope for the admission-wake PR (single-repo change).


- [ ] RESIDUE 1: `refine`'s `mr_write(comment)` restriction (a refine agent may comment on an MR but
  may not open/close one) is enforced by a cli-side AND an operator-side check, **not by the schema**.
  It is the one non-uniform cell in the MCP profile table: every other capability is a whole tool the
  profile does or does not expose, this one is an action within a tool. A schema-level fix means
  splitting `mr_write` per action.
- [ ] RESIDUE 2: a persistently-rejected documentation PR **disappears quietly**. The nightly doc
  batch abandons it, logs it, and increments `operator_doc_task_abandoned_total{reason}` - but nothing
  escalates it to a human, so a docs PR that the reviewer rejects every night is a silent no-op
  forever. Wants an escalation path (an issue, or a park with a comment), not just a counter.
- [ ] RESIDUE 5: there is deliberately **NO `refresh=true` escape hatch on `scm_read`**. Do not add
  one. `scm_read` serves the mirror, and the mirror is the whole reason the platform's forge-request
  rate is bounded by the sweep cadence instead of by agent behaviour. A refresh flag hands every agent
  a forge-fanout button and undoes it; an agent that thinks the mirror is stale is describing a sweep
  bug, which is a thing to fix in the sweep.
- [ ] The `tatara-memory` spill endpoint. The A.7 byte guard spills over-budget comment batches
  through LightRAG's **text-ingest** path, so they are semantically indexed as if they were source
  material. It works and it is not a data leak, but it pollutes the recall index with raw comment
  dumps. Wants a dedicated blob/archive endpoint on tatara-memory that stores without indexing.
- [ ] Prune the metric families the reap left DECLARED with no emitter: `tatara_cd_cascade_failed`,
  `tatara_cd_cascade_stalled`, `tatara_cd_resolved_total` (died with `cdScan`) and
  `operator_agent_boot_crash_total` (died with `bootcrash.go`). They emit no series (labelled vecs are
  only materialized on first write) so they are inert, but "named and never emitted" is the exact
  defect contract K.1 calls out. No chart alert or dashboard panel references them any more.
- [ ] Sibling MergeRequest CR deleted mid-handoff: `advanceAfterReview` becomes unreachable; the Task
  now parks `handoff-stalled` at 5m instead of hanging, but that's a bound, not a recovery.
- [ ] Leader-election changeover mid-handoff: in-memory workqueue/rate-limiter state is not CR state;
  the 5m deadline bounds the damage. A full fix needs the drain re-derived from CR state on every
  reconcile. Partially addressed for QueuedEvent admission by the 2026-07-19 issue-batch's WP9
  (#395, `DispatcherReconciler` leadership-acquired backstop closing the rollout/leader-handoff
  alert-admission gap) - the review-handoff drain half of this residue already had its level-triggered
  re-drive (2026-07-19 incident #379 entry above); the general in-memory-workqueue case remains open.
- [x] 2026-07-19 issue-batch (issues #367/#369/#386/#392/#393/#394/#395/#397/#398) shipped via branch
  `fix-issue-batch`: see the dated 2026-07-19 `MEMORY.md` entries for the per-issue detail.
- [ ] `handoff-stalled` false positive on a slow-but-working drain: a >5m forge degradation parks a
  Task whose review already posted; `advanceAfterReview` then no-ops on its `Status.Stage != reviewing`
  guard and silently abandons the advance it still owns, recovered only by a backlog-sweep re-mint.
  F.6 re-entry is the WRONG remedy (every F.6 rule is comment-driven, nobody will comment on this);
  the right fix is drain-owned - teach the late drain that `parked[handoff-stalled]` is its own park
  reason and let it take the F.3 edge from parked. Needs Task-2 edge semantics + a drain-contract change.
- [ ] Single `OutcomeAccepted` condition slot: a second `/outcome` POST with a DIFFERENT payload
  clobbers a committed condition with a bare claim, switching the B2 guards off. Pre-existing,
  unreachable in the intended flow once respawn is suppressed (no second pod); surviving an
  adversarial second POST needs a second condition type.
- [ ] Partial-progress replay: re-claim of a genuinely orphaned stub re-files already-filed issues
  (forge writes are not idempotent). Inherent to the lease; pre-dates this change.
- [x] Post-merge documentation agent, operator half (new `documentation` Task kind, repo-scoped to a project's docs repo): CRD (Kind enum, repoScopedKinds, ProjectSpec.Documentation, ModelByKind/EffortByKind/SpawnCeilingByKind CEL+MaxProperties 9->10), webhook handlePush spawns a documentation QueuedEvent on a merge to a non-docs component repo (self-trigger guard + docs-repo-enrolled gate + head-SHA dedup; scm.WebhookEvent gained BaseSHA/HeadSHA for push events), pod.go kindProfiles row (tool/skill profile "documentation", model locked to claude-sonnet-5 via modelForKind, branch/PR-title prefix "docs", SHA-derived pod-name/branch suffix since the Task carries no Source.Number), turnloop required skill, writeback derivePRTitle, reaper gcConversations GC. Ships inert (no Project sets Documentation). Awaiting: tatara-cli `documentation` tool profile, tatara-agent-skills `tatara-documentation-workflow` skill, then a tatara-helmfile MR to enroll the docs repo + enable for tatara. Design: docs/superpowers/specs/2026-07-05-documentation-agent-design.md (parent tatara repo).
- [x] Token budget admission gate (issue #189): pause proactive work (normal pool) at a proactive percent and incident work (alert pool) at an emergency percent of token usage within a reset window, resuming when it rolls. Two modes - customWindow (operator-meters its own per-turn tokens against a tokenLimit in a cron-anchored window, accumulator on Project.Status.TokenBudget) and claudeSubscription (gates on wrapper-reported Claude 5h/weekly percent, inert until the wrapper PR lands). internal/budget engine + ProjectSpec/Status TokenBudget + operator-wide TOKEN_BUDGET_* config + dispatcher gate + per-turn accumulation + 2 metrics + dashboard panels + TataraTokenBudgetBlocked alert. Defaults 50%/80%, off until enabled. Branch tatara/fix-189-tatara-operator-aware-of-token-budget; awaiting deploy (image+chart bump, then enable tokenBudget on the tatara + infrastructure Projects in the infra helmfile) + a sister tatara-claude-code-wrapper PR (anthropic OAuth 5h/weekly header passthrough) to activate claudeSubscription mode.
- [x] feat/deep-architectural-research P1: brainstorm goal variant (tatara-deep-architectural-research skill named, skip_research token, ADR/RFC intent), skip_brainstorm->skip_research rename, brainstorm-outcome contract lock tests, Serena MCP env-gate (TATARA_SERENA_URL), archfitness SCM-isolation fitness function. Branch committed; await wrapper + cli sister PRs before deploy.
- [ ] GC the zero-owner mirror CRs. Two known sources, same class (B.1 never GCs a zero-owner object by design): an orphaned BOT PR on another Task's branch keeps an ownerless MergeRequest mirror that `ClassifyPR` never re-adopts; an issue OPENED then closed before the next sweep keeps the ownerless Issue mirror `MarkWebhookOriginated` created. Both are bounded and benign (small CRs, no pods, no forge calls) - a slow leak, not a bug. Wants a reaper pass over zero-owner Issue/MergeRequest CRs whose SCM artifact is closed.
- [ ] Phase 2: deploy Serena MCP server + wire TATARA_SERENA_URL value (helmfile); external-research MCP servers (arXiv/OpenAlex) + architectural-research egress.
- [ ] Phase 3: SCMProvider port extraction (strangler) once the archfitness fitness function trips on a real introduced coupling.

- [x] S3 conversation persistence (issue #114): cross-pod conversation resume via S3 (Task.Status.ConversationObjectKey/SessionID + BuildPod CONVERSATION_* env + turn-complete recording), 25% resume-vs-compaction hybrid (HandoverThresholdPercent default 50->25, mutually exclusive with full replay), brainstorm conversation forked per issue (copy-object), review/test agent for ALL MRs incl. user-authored (reviewText + PR-head read-only checkout), and reaper S3 GC when a batch fully closes (internal/objstore + operator_conversation_gc_total). Off until s3Bucket set. Branch tatara/fix-114-...; awaiting deploy (helmfile wiring + pin bump).
- [x] Project-level brainstorm (one Task per project per cycle, not per-repo): summed backlog, project-scoped in-flight guard, deterministic primary repo, updated goal prompt listing all repos. CRD MaxOpenProposals default 3->5 (project-wide). PR feat/project-level-brainstorm; awaiting deploy.
- [x] Brainstorm target-backlog (O1-O11): replaces the project-level cap above with a maintained
  `targetOpenProposals` backlog the controller refills on a maintainer verdict, not a cron cap -
  `deficit = max(0, target - pending - inflight)`, pending counted from Issue CRs
  (`Spec.ProposalKind`), never a live SCM label; new `ProjectReconciler.Watches(&Issue{})` event
  trigger + 15m cron backstop with a consecutive-skip circuit breaker (HISTORY: the breaker and
  brainstorm's own cron trigger were both retired by the `demand-driven-brainstorm` branch -
  demand-driven refill plus a durable cooldown gate replaced them, see `MEMORY.md`); a refill Task carries a
  quota annotation `submit_outcome` truncates to (clamped `[1,5]`); brainstorm pod turn-0 bundle
  gains a `<proposal_history>` block rendered from the Issue CR mirror; a declined proposal's
  mirror is retained (not deleted) under a dedicated 14-day `DeclinedProposalRetention`, distinct
  from `RejectedRetention` (24h) and `ParkRetention` (7d); 5 new brainstorm metrics. Branch
  `brainstorm-target-backlog`, plan+task briefs/reports/progress in
  `.superpowers/sdd/2026-07-26-brainstorm-target-backlog-plan/` (parent tatara-new repo; no
  separate design doc - the plan document there, `global-constraints.md`, carries the full
  architecture and the approved-spec-vs-code conflict table). Decisions, corrections and residuals
  are in the dated 2026-07-26/2026-07-27 `MEMORY.md` entries above. Companion repo changes:
  tatara-agent-skills (guardrails + council-brainstorm skills), tatara-helmfile
  (`targetOpenProposals` on the enrolled Projects), tatara-documentation (workflow/reference/tuning
  pages). semver:minor; awaiting deploy (merge order: this repo released and applied before the
  tatara-helmfile values change).
- [x] Systemic-group implementation dedup (lead-per-repo): one brainstorm of N connected issues (shared systemicId) spawns one agent per (systemicId, repo), resolving same-repo siblings in one combined PR + marking non-leads with an idempotent comment. SystemicGroup on Task/payload, electSystemicLeads, issueScan collapse gate, 2 metrics. PR #124; awaiting deploy (tatara-helmfile dual pin). Plan docs/superpowers/plans/2026-06-23-systemic-impl-dedup.md.
- [x] MR-ownership (branch feat/mr-ownership, OP1-OP13): MergeRequest gains status.ownership (tatara|external) + lastBotHeadSHA; merge gated on per-MR ownership instead of Kind=="review"; ReconcileOwnership backfill/drift-flip driven by both the leader reconciler and the sweep; gated /scm/mr-takeover endpoint is the only external->tatara path; new `takeover` Task kind minting into approved via AnnTakeoverHeadBranch; idempotent DrainOwnershipAnnouncement; general comment->task invariant closed for MRs via EnsureTaskForMRComment. See dated 2026-07-19 `MEMORY.md` entries. semver:minor; awaiting deploy.
- [x] Reactive agent triggering (branch reactive-agent-triggering, 14 tasks): fixes the identity-unverified un-park one-shot that stalled a Task for a day with the approval label already on the forge (gitlab helmfile#26) - `Task.status.approvalVerdict` makes the C.6 grammar pass durable, `driveUnparks` stops excluding identity-unverified and re-derives the rest from live state, `loadOwnedIssues` reads through the uncached reader, and every F.6 decline is now named and counted via `UnparkDecline`/`UnparkDetailed` instead of a silent `if !ok`. Adds a new `conversing` stage: pod-bearing, non-terminal, holds a concurrency slot, no carve-out in the reaper/accountant/transition table; runs the existing clarify agent kind; entered from clarifying/reviewing on a qualifying comment or from parked(awaiting-human)/parked(identity-unverified) via un-park; idle-TTL clock on its own `ConversationLastEventAt` (never `StageWorkStartedAt`, which pod rotation re-stamps); per-stage `PodTTLSeconds` makes a TTL expiry a pod rotation, not a conversation end; per-project ceiling (default 2, strictly below `MaxConcurrentAgents`' own default of 3 so the ceiling can actually bind) that DECLINES a new conversation on reaching it (the event stays queued and rides the Task's next turn) and only EVICTS the longest-idle conversation, via a separate backstop, once live conversations EXCEED the ceiling; cross-kind trigger rule resolved from the operator's own `Comment.AgentKind` write ledger, failing closed; consecutive bot-round counter observed but never acted on (no ping-pong cap, by maintainer decision). A 2026-07-28 security review during this branch found and fixed three further defects in the same `UnparkInput`-builder-lockstep class (`GrammarPassed`, `ConversingHasRoom`, then a pre-existing `MaxTurnsPerTask` bug that had been deleting every `parked(no-outcome)` Task after 7 days regardless of real turn count) plus an `MRs`-omission that left the kind=review already-merged guard inert on one path. A 2026-07-28 FINAL whole-branch review then found and fixed: a same-kind agent comment reaching its own live pod as a follow-up turn and disabling the idle backstop (CRITICAL); an unbounded per-pass conversing-ceiling overshoot plus a serial eviction path that could wedge the single-threaded ProjectReconciler for hours (CRITICAL, now a spent-per-admission room budget and a capped-per-pass eviction with a fast requeue for the remainder); the conversing-ceiling default lowered below `MaxConcurrentAgents` so it can actually bind; a `conversationIdleMinutes` floor so a small value cannot TTL-stop a conversing pod before it becomes Ready; `operator_bot_rounds` rebuilt as a periodic per-project-maximum recompute instead of a latching last-writer-wins gauge; this doc's own "evicts on reaching the ceiling" claim corrected to match the decline-then-backstop-evict behaviour; `conversing` added to the `operator_stage_drift_total` zero-baseline pre-seed; and a nil-`Tasks` guard on the eviction path. Decisions, corrections and the field-by-field `UnparkInput` audit are in the dated 2026-07-27/2026-07-28 `MEMORY.md` entries above. Plan+task briefs/reports in `.superpowers/sdd/2026-07-27-reactive-agent-triggering-plan/` (parent tatara-new repo). semver:minor; awaiting deploy.

## Agent-loop follow-ups (found during 2026-06-08 dogfood)

- [x] Issue/MR comments interrupt the agent nursing them (tatara-operator#25):
  in-flight turn -> comment queued on `Status.PendingInterjections`, drained by
  the reconciler to the wrapper `POST /v1/interject` (live PTY input); idle task
  -> re-triage; no live agent -> reactivate Parked or create a Triage task, now
  for MRs as well as issues.

- [x] Phase-label dedup + orphan-recovery (presence+state, Option A) - four phase
  labels (brainstorming/approved/implementation/declined) as state-of-truth;
  dedup+backstop key on label presence + task state (not label-added-time);
  webhook reactivates Parked tasks. Merged+deployed 306f596 (set-image). Plan:
  `../tatara/docs/superpowers/plans/2026-06-13-tatara-phase-label-dedup.md`.
  Optional cleanup: proposalBacklog doc comment still says "idea label".
- [x] 3-label issue lifecycle (tatara-idea/approved/rejected), conversation-driven
  approval, retire label-toggle approval (branch feat/label-lifecycle; awaiting
  deploy + one-time label migration). Plan:
  `../tatara/docs/superpowers/plans/2026-06-13-tatara-label-lifecycle.md`.
- [x] Dedupe Task creation by issue ref (shipped 0.2.8): `handleWorkItem` skips
  creation when a non-terminal Task already exists for the issue ref; re-labeling
  after completion still re-triggers.
- [x] Cross-repo agent tasks (shipped 0.2.9, O1-O3): BuildPod sets TATARA_REPOS
  (all Project repos, primary first); planTurnText tells the agent about
  /workspace/<name> layout; doWriteBack loops all Project repos and opens one PR
  per changed repo; issue comment carries all PR links.
  Plan: `docs/superpowers/plans/2026-06-09-cross-repo-agent-tasks.md`.
- [ ] Reconcile the staleness in `writeback.go` taskBranch comment: the branch is
  now also communicated to the wrapper via `TASK_BRANCH` env (not only the turn
  prompts), and the wrapper enforces the push.

- [x] M0 scaffold - kubebuilder project, four CRD types + deepcopy, go.mod,
  internal/{obs,auth,config}, no-reconciler manager, Dockerfile, Makefile,
  chart skeleton with CRDs. Plan:
  `docs/superpowers/plans/2026-06-06-tatara-operator-m0-scaffold.md`.
- [x] M1 Project + Repository + ingest - ProjectReconciler,
  RepositoryReconciler, ingest Job spawning, last-ingested-commit tracking.
  Plan: `docs/superpowers/plans/2026-06-06-tatara-operator-m1-project-repository-ingest.md`.
- [x] M2 webhook server (push) - HMAC verify, provider detection,
  push -> main-filtered incremental re-ingest. Work-item path is M5 stub.
  Plan: `docs/superpowers/plans/2026-06-06-tatara-operator-m2-webhook-push.md`.
- [x] M3 REST API (operator, Part A) - OIDC-gated CRUD over Project/Repository/Task/Subtask, shared HTTP_ADDR listener with webhook server. Plan: `docs/superpowers/plans/2026-06-06-tatara-operator-m3-restapi-cli-tools.md`. Part B (tatara-cli MCP tools, Tasks 10-13) is a separate repo/release.
- [x] M4 - Task reconciler + turn loop (wrapper Pod/Service, plan turn,
      subtask iteration, concurrency gate, callback server + poll backstop,
      bounded pod-loss retry). SCM write-back deferred to M5 via the
      WritebackPending condition hook.
- [x] M5 SCM write-back + work-item -> Task - GitHub+GitLab OpenChange/Comment
  via REST (httptest-faked); TaskReconciler write-back on Succeeded (envtest);
  webhook work-item -> Task (httptest, signed payload, fake client);
  scm.ByProvider wired into main. All tests green, lint clean.
- [x] M6 chart + deploy wiring - chart hardened: 4-port Deployment, dual-Service (main + internal callback), ConfigMap/Secret envFrom, RBAC (namespaced Role + CRD-reader ClusterRole), tatara-ingest SA+Role (M1 follow-up), managed-pod NetworkPolicy, ServiceMonitor, Ingress (cluster-agnostic). helm lint clean, 15 objects (14 plan + internal Service). Keycloak confidential client + audience mapper added to infra/terraform/keycloak/tatara_clients.tf. tatara-operator release added to infra helmfile tatara bucket (OCI chart 0.1.0) with common+default+sops values. All gated deploy steps listed below. Plan: docs/superpowers/plans/2026-06-06-tatara-operator-m6-chart-deploy.md.

## Per-project memory (N1-N4)

- [x] N1 complete - cnpg api dep + scheme, Project CRD memory fields, config image/secret fields, remove MEMORY_BASE_URL, internal/memory builder package (NamesFor, Endpoint, PGCluster, Neo4jPasswordSecret, Neo4jStatefulSet+Service, LightragDeployment+Service+PVC, MemoryDeployment+Service+ConfigMap+Secret). Plan: `docs/superpowers/plans/2026-06-07-per-project-memory-n1-builders.md`.
- [x] N2 provisioning reconcile - ProjectReconciler SSAs full per-project stack (PGCluster, neo4j StatefulSet, lightrag, tatara-memory), status.memory.phase/endpoint, MemoryReady condition, metrics, Owns() all stack kinds, memoryConfigFromConfig in wire.go. Plan: `docs/superpowers/plans/2026-06-07-per-project-memory-n2-provisioning.md`.
- [x] N3 ready-gating wiring - RepositoryReconciler + TaskReconciler gate on status.memory.Phase==Ready; ingest Job --base-url + BASE_URL env from status.memory.endpoint; agent wrapper pod TATARA_MEMORY_URL from same; ingest.BuildJob baseURL param replaces removed MemoryBaseURL config field. Plan: `docs/superpowers/plans/2026-06-07-per-project-memory-n3-wiring.md`.
- [x] N4 retire static tatara-memory + chart RBAC/values + image bump + deploy -
  operator chart Role gains postgresql.cnpg.io clusters(+/status), apps/deployments,
  apps/statefulsets, core/persistentvolumeclaims CRUD; secrets verbs widened to
  create/update/patch/delete (was read-only) for generated neo4j password + memory
  config Secrets; ConfigMap drops MEMORY_BASE_URL, adds MEMORY_IMAGE/LIGHTRAG_IMAGE/
  NEO4J_IMAGE/OPENAI_SECRET_NAME; Chart+appVersion 0.2.0; cnpg Cluster CRD provenance
  header added. Infra removes the static tatara-memory release + values dir, bumps
  operator image tag to 0.2.0, adds the image/secret values. Deploy + static-stack
  uninstall are gated. apps/deployments was MISSING from prior RBAC; added here.
  Plan: docs/superpowers/plans/2026-06-07-per-project-memory-n4-retire-deploy.md.

## Deploy follow-ons (gated - require human action in this order)

1. [ ] Add tatara.dev/managed-by=tatara-operator label to M1 ingest Job pod template (internal/ingest/job.go) and M4 agent Pod (internal/agent/pod.go) or the NetworkPolicy will not select them.
2. [ ] Build + push harbor.szymonrichert.pl/containers/tatara-operator:0.1.0 (operator image) to harbor.
3. [ ] Build + push tatara-claude-code-wrapper image to harbor; operator cannot run agents until published.
4. [ ] Build + push tatara-memory-repo-ingester image to harbor; operator cannot run ingest until published.
5. [ ] `terraform -chdir=infra/terraform/keycloak apply` - creates the tatara-operator confidential client + audience mapper (3 resources). Gate: review `terraform plan` output first.
6. [ ] Capture `terraform -chdir=infra/terraform/keycloak output -raw tatara_operator_client_secret` and populate the real value into `infra/helmfile/helmfiles/tatara/values/tatara-operator/default.secrets.yaml` via `sops-secret-helper` skill (placeholder currently reads REPLACE_WITH_KEYCLOAK_OUTPUT).
7. [ ] Publish OCI chart: `helm package charts/tatara-operator -d /tmp && helm push /tmp/tatara-operator-0.1.0.tgz oci://harbor.szymonrichert.pl/charts` (from tatara-operator main, not a worktree).
8. [x] tatara-anthropic (data key `oauth-token`) - chart-rendered from sops (0.2.3).
9. [x] tatara-cli-oidc (keys: client-id, client-secret) - chart-rendered from sops (0.2.3).
10. [x] tatara-scm (keys: token, webhookSecret) - chart-rendered from sops (0.2.3); single Project (multi-project deferred, rule 6).
11. [ ] `helmfile -e default -f infra/helmfile/helmfiles/tatara/helmfile.yaml.gotmpl -l application=tatara-operator diff` - review diff (should show 15 objects + 4 CRDs as net-new). Gate: present to human before apply.
12. [ ] `helmfile -e default -f infra/helmfile/helmfiles/tatara/helmfile.yaml.gotmpl -l application=tatara-operator apply` - ONLY after all above preconditions are satisfied and the human has reviewed the diff.

## N4 deploy follow-ons (gated - require human action in this order)

1. [ ] Build + push harbor.szymonrichert.pl/containers/tatara-operator:0.2.0.
2. [ ] Create shared Secret lightrag-openai (ns tatara, key LLM_BINDING_API_KEY)
   via sops-secret-helper, reusing the key from the retiring tatara-memory sops
   (recover from default.secrets.yaml BEFORE git rm, or from live cluster Secret).
3. [ ] helm package + push tatara-operator-0.2.0.tgz to oci://harbor.szymonrichert.pl/charts.
4. [ ] Confirm cnpg operator + lightrag-openai present in-cluster:
   `kubectl get crd clusters.postgresql.cnpg.io && kubectl -n tatara get secret lightrag-openai`
5. [ ] helmfile -e default -l application=tatara-operator diff (review; gate on human approval).
6. [ ] helmfile -e default -l application=tatara-operator apply.
7. [ ] helm uninstall tatara-memory -n tatara (empty static stack; no data migration needed).
8. [ ] Verify a Project provisions mem-<proj>-* and reaches status.memory.phase=Ready.

## SCM projects + PR/MR reactions (operator core)

- [x] Tasks 1-13 complete (0.3.0): SCMWriter interface (12 methods), GitHub REST v3 + Projects v2 GraphQL board ops, GitLab REST + label-driven board, DetectAndVerify extended fields, CRD (ScmSpec/BoardSpec, Task Kind/ApprovalRequired/ProposedIssue, ReviewVerdict/PROutcome, AwaitingApproval phase), webhook dispatch + Kind selection + prReactionScope gating + approval-label flip, approval gate + proposal-creation SCM egress, write-back branches on Kind (review/selfImprove/implement), REST POST /projects/{p}/issues + /tasks/{t}/review + /tasks/{t}/pr-outcome, metrics (action label + operator_scm_writes_total + operator_approval_gate_seconds). Plan: docs/superpowers/plans/2026-06-09-scm-projects-operator.md.
- [ ] tatara-cli 3 new MCP tools (propose_issue/review_verdict/pr_outcome) - target tatara-cli repo.
- [ ] Deploy 0.3.0: build + push operator image, helm package + push chart, helmfile apply.

## N5 deploy follow-ons - imagePullSecrets + neo4j tag fix (gated)

1. [ ] Build + push harbor.szymonrichert.pl/containers/tatara-operator:0.2.1.
2. [ ] helm package + push tatara-operator-0.2.1.tgz to oci://harbor.szymonrichert.pl/charts.
3. [ ] helmfile -e default -l application=tatara-operator diff (should show ConfigMap + Deployment image tag change).
4. [ ] helmfile -e default -l application=tatara-operator apply.
5. [ ] Verify neo4j pod reaches Running: `kubectl -n tatara get pod -l app.kubernetes.io/component=neo4j`.

## Scan stale-event cutoff (issue #285) - ABANDONED, mrScan deleted 2026-07-13

Pre-cutover items below all targeted mrScan, which the 2026-07-13 task-centric redesign deleted
(B.4 sweep is the sole intake now). See MEMORY.md 2026-07-17 (Task 9) for the dead-field removal
and the ScanMarks finding.

- [ ] **NEW, replaces the items below:** `ProjectStatus.ScanMarks`/`ScanMark` have ZERO readers or
  writers anywhere in the codebase (only the type declarations and generated deepcopy reference
  them) - the whole per-item high-water-mark mechanism this section was built around appears to
  have been dropped in the same redesign that deleted mrScan, without the field itself being
  removed. Confirm and either wire a consumer or delete the dead status field + CRD surface.
