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
