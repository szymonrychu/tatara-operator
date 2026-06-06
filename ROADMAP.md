# ROADMAP - tatara-operator

Planned work not yet started. One line per item; link to plans for detail.

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
- [ ] M5 SCM write-back + work-item -> Task - scm interface (github+gitlab),
  branch/PR/MR/comment, work-item webhook -> Task.
- [ ] M6 chart + deploy wiring - NetworkPolicy, metrics, Keycloak client,
  infra helmfile `tatara` release. Must create `tatara-ingest` ServiceAccount
  + ConfigMap-patch Role (see MEMORY.md 2026-06-06 M1 entry).
