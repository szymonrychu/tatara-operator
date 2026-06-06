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
