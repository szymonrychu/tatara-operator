# tatara-project

Declarative tatara `Project` + `Repository` custom resources for one project,
rendered from Helm values.

This is the "cluster" half of a rook-ceph-style two-chart split:

| chart | role |
|---|---|
| `tatara-operator` | installs the operator Deployment + the `tatara.dev` CRDs (like `rook-ceph`) |
| `tatara-project` (this chart) | codifies one `Project` and its enrolled `Repository` CRs the operator reconciles (like `rook-ceph-cluster`) |

Install the operator once, then install one release of this chart per project
so a helmfile can declare whole projects declaratively (replacing
hand-applied raw manifests).

## Prerequisites

- The `tatara-operator` is running in the target namespace (its CRDs are
  installed).
- The Secret named in `project.spec.scmSecretRef` already exists in that
  namespace. This chart does **not** create it: per the cluster-agnostic rule,
  charts carry no secret material; the helmfile supplies it (sops).

## Usage

```sh
helm install my-project charts/tatara-project -n tatara -f my-values.yaml
```

See `deploy-samples/tatara-project-values.yaml` for a full worked example.

## Values

| key | description |
|---|---|
| `namespace` | Namespace for the CRs. Empty -> release namespace. Must match the operator's namespace. |
| `nameOverride` | Overrides the chart label only (not the Project name). |
| `project.name` | **Required.** `Project` metadata.name. |
| `project.annotations` | Optional annotations on the `Project`. |
| `project.spec` | **Required.** Rendered verbatim into `Project.spec` (`scmSecretRef` is required). See the `tatara.dev` Project CRD for every field. |
| `repositories[]` | List of `Repository` CRs. Each has `name`, optional `annotations`, and `spec`. |

### Notes on the CRD-chart model

`project.spec` and each `repositories[].spec` are emitted with `toYaml`, so
every current and future CRD field is settable from values without a chart
change. The "no lists in values.yaml" rule targets workload ENV ConfigMaps,
not CRD spec data, so list-shaped fields (repos, `agent.extraEnvs`,
`agent.extraVolumes`) live directly in values.

`repositories[].spec.projectRef` is auto-bound to `project.name`, so repos
never repeat it (an explicit `projectRef` still wins if set).

### Agent customization

The Project `agent` block accepts whatever the `tatara.dev` Project CRD
exposes; on current `main` that is `model`/`modelByKind`, `effort`, `permissionMode`,
`hooks`, `extraEnvs`/`extraEnvsFrom`, `extraVolumes`/`extraVolumeMounts`,
`extraSidecarContainers`/`extraInitContainers`, `mcpServers` (extra MCP
servers merged into `.mcp.json`), `skillSources` (extra skill repos the
wrapper clones and installs alongside the baked `tatara-agent-skills`), and
`promptAppendByKind` (project-specific text appended after the built-in
per-kind assignment prompt, keyed by agent kind plus a `"*"` wildcard). This
chart renders the spec verbatim, so it gains any future CRD field
automatically.
