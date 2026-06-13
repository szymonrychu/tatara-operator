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

Shipping 0.4.2. All milestones (M0-M6) and the per-project-memory line
(N1-N4) are complete. The manager reconciles Project/Repository/Task/
Subtask, provisions a per-project `tatara-memory` stack (CNPG Postgres,
Neo4j, LightRAG, memory service), ingests repositories and tracks the
last-ingested commit, serves an OIDC-gated REST API and HMAC-verified
GitHub/GitLab webhooks on a shared listener, turns labelled issues into
Tasks, runs the agent turn loop in `tatara-claude-code-wrapper` Pods, and
writes results back to the SCM as one PR per changed repo plus an issue
comment. Remaining work is the gated deploy steps tracked in `ROADMAP.md`.

## Layout

```
cmd/manager/                       # controller-runtime manager entrypoint + wiring
api/v1alpha1/                      # Project/Repository/Task/Subtask types + deepcopy
internal/controller/               # Project/Repository/Task reconcilers, turn loop, write-back
internal/agent/                    # agent wrapper Pod/Service + turn session/callback
internal/ingest/                   # repo-ingest Job builder
internal/memory/                   # per-project memory stack builders (CNPG/Neo4j/LightRAG/memory)
internal/scm/                      # GitHub/GitLab clients + repo scan + provider registry
internal/restapi/                  # OIDC-gated CRUD REST API
internal/webhook/                  # HMAC-verified push + work-item webhook server
internal/auth/                     # OIDC verifier + client-credentials token source
internal/config/                   # env-scalar config
internal/obs/                      # JSON slog + Prometheus metrics
internal/version/                  # build-stamped version info
charts/tatara-operator/            # cluster-agnostic Helm chart + CRDs
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
