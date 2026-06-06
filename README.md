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

Milestone M0 (scaffold). API types, shared `internal/{obs,auth,config}`
packages, a no-reconciler manager, Dockerfile, Makefile, and a chart
skeleton carrying the CRDs. Reconciler, webhook, REST, and agent logic
land in M1-M6.

## Layout

```
cmd/manager/main.go               # controller-runtime manager entrypoint
api/v1alpha1/                      # Project/Repository/Task/Subtask types
internal/controller/              # reconcilers (M1+)
internal/obs/                     # JSON slog + Prometheus registry
internal/auth/                    # OIDC verifier + client-credentials token source
internal/config/                  # env-scalar config
charts/tatara-operator/           # cluster-agnostic Helm chart + CRDs
```

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
