# MEMORY - tatara-operator

Past decisions and their context. One line per entry, dated. Append-only
in spirit; prune only when a decision is reversed.

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
