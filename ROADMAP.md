# ROADMAP - tatara-operator

Planned work not yet started. One line per item; link to plans for detail.

## Agent-loop follow-ups (found during 2026-06-08 dogfood)

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

## N5 deploy follow-ons - imagePullSecrets + neo4j tag fix (gated)

1. [ ] Build + push harbor.szymonrichert.pl/containers/tatara-operator:0.2.1.
2. [ ] helm package + push tatara-operator-0.2.1.tgz to oci://harbor.szymonrichert.pl/charts.
3. [ ] helmfile -e default -l application=tatara-operator diff (should show ConfigMap + Deployment image tag change).
4. [ ] helmfile -e default -l application=tatara-operator apply.
5. [ ] Verify neo4j pod reaches Running: `kubectl -n tatara get pod -l app.kubernetes.io/component=neo4j`.
