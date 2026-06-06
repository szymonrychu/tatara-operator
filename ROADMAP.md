# ROADMAP - tatara-operator

Planned work not yet started. One line per item; link to plans for detail.

- [x] M0 scaffold - kubebuilder project, four CRD types + deepcopy, go.mod,
  internal/{obs,auth,config}, no-reconciler manager, Dockerfile, Makefile,
  chart skeleton with CRDs. Plan:
  `docs/superpowers/plans/2026-06-06-tatara-operator-m0-scaffold.md`.
- [x] M1 Project + Repository + ingest - ProjectReconciler,
  RepositoryReconciler, ingest Job spawning, last-ingested-commit tracking.
  Plan: `docs/superpowers/plans/2026-06-06-tatara-operator-m1-project-repository-ingest.md`.
- [ ] M2 webhook server (push) - HMAC verify, provider detection,
  push -> main-filtered incremental re-ingest.
- [ ] M3 REST API + tatara-cli MCP tools - OIDC-gated CRUD + tool group.
- [ ] M4 Task reconciler + turn loop - wrapper Pod+Service, turn callbacks,
  subtask iteration, concurrency gating.
- [ ] M5 SCM write-back + work-item -> Task - scm interface (github+gitlab),
  branch/PR/MR/comment, work-item webhook -> Task.
- [ ] M6 chart + deploy wiring - NetworkPolicy, metrics, Keycloak client,
  infra helmfile `tatara` release. Must create `tatara-ingest` ServiceAccount
  + ConfigMap-patch Role (see MEMORY.md 2026-06-06 M1 entry).
