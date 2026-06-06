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
- [x] M5 SCM write-back + work-item -> Task - GitHub+GitLab OpenChange/Comment
  via REST (httptest-faked); TaskReconciler write-back on Succeeded (envtest);
  webhook work-item -> Task (httptest, signed payload, fake client);
  scm.ByProvider wired into main. All tests green, lint clean.
- [x] M6 chart + deploy wiring (Tasks 1-9 done) - chart hardened: 4-port Deployment, dual-Service (main + internal callback), ConfigMap/Secret envFrom, RBAC (namespaced Role + CRD-reader ClusterRole), tatara-ingest SA+Role (M1 follow-up), managed-pod NetworkPolicy, ServiceMonitor, Ingress (cluster-agnostic). helm lint clean, 15 objects. Tasks 10-13 (chart publish, Keycloak client, infra helmfile release) are out-of-band. Plan: docs/superpowers/plans/2026-06-06-tatara-operator-m6-chart-deploy.md.

## Deploy follow-ons (out-of-band, before first real run)

- [ ] Set callbackUrl in infra values to http://tatara-operator-internal.tatara.svc:8082 (the internal Service DNS).
- [ ] Add tatara.dev/managed-by=tatara-operator label to M1 ingest Job pod template (internal/ingest/job.go) and M4 agent Pod (internal/agent/pod.go) or the NetworkPolicy will not select them.
- [ ] Build + push harbor.szymonrichert.pl/containers/tatara-operator:0.1.0 before helmfile pins chart 0.1.0.
- [ ] Build + push tatara-claude-code-wrapper and tatara-memory-repo-ingester images; operator cannot run agents or ingest until both are published.
- [ ] Replicate ANTHROPIC_API_KEY into tatara namespace as Secret tatara-anthropic (not chart-rendered).
- [ ] Create tatara-cli-oidc Secret in tatara namespace (wrapper's tatara-cli mints operator/memory/chat tokens).
- [ ] Create per-Project SCM Secrets (keys: token, webhookSecret) in tatara namespace, one per Project (not chart-rendered).
- [ ] Task 12: Add tatara-operator confidential Keycloak client + audience mapper in infra/terraform/keycloak/tatara_clients.tf.
- [ ] Task 13: Add tatara-operator release to infra helmfile tatara bucket; run helmfile diff before apply.
