# CLAUDE.md - tatara-operator

## What this repo is

The tatara control plane: a controller-runtime operator owning the
`tatara.dev/v1alpha1` CRDs (`Project`, `Repository`, `Task`, `Issue`,
`MergeRequest`, `QueuedEvent`). It ingests repos into `tatara-memory`, takes
GitHub/GitLab webhooks, spawns `tatara-claude-code-wrapper` pods with
`tatara-cli` as their MCP server, and lands the result back in the forge - including the merge, which nothing else in
the platform is allowed to do.

It subsumes the retired `tatara-tasks` (the CRDs are the task store),
`tatara-gitlab-bridge` (webhooks are built in) and the orchestration role of
`tatara-argo-workflows`.

## What this repo is NOT

- Not an agent. It spawns them and reads their outcomes; it never writes code.
- Not the deploy. Its chart is published here and applied from
  `tatara-helmfile`.

<!-- BEGIN tatara-shared-contract (generated from tatara-agent-skills/template/CLAUDE-shared.md - do not edit below this line) -->
## The tatara platform

`tatara` is not a repo. It is nine independent GitHub repositories under
`szymonrychu/`, enrolled as `Repository` CRs by the `tatara` Project
(`tatara-helmfile/values/project-tatara/common.yaml`):

| repo | owns |
|---|---|
| `tatara-operator` | the control plane: Project/Repository/Task CRs, the agent lifecycle, forge write-back, merge |
| `tatara-memory` | the memory and code-graph service |
| `tatara-memory-repo-ingester` | the ingester that feeds `tatara-memory` from git |
| `tatara-cli` | the `tatara` CLI and the MCP server every agent pod talks to |
| `tatara-claude-code-wrapper` | the agent pod image and its bootstrap |
| `tatara-agent-skills` | the skills plugin every agent pod loads, and this contract |
| `tatara-helmfile` | the helm releases and the platform enrollment CRs |
| `tatara-observability` | Grafana alert rules as terraform |
| `tatara-documentation` | the docs site and the design-doc archive |

There is no umbrella repo and no monorepo: each repo carries its own CI,
`Dockerfile` and chart where it ships one, `MEMORY.md` and `ROADMAP.md`, and
each deploys itself. Read the repo's own `README.md` for what it does; there is
no cross-repo architecture file.

## On-disk layout in an agent pod

Every repo a Task needs is cloned to `/workspace/<owner>/<repo>` on the task
branch before the agent's first turn. They are siblings: there is no parent
directory containing the others, and no gitignored nesting.

## Hard rules

1. **Newest stable Go** for any Go service. Pin the Go directive to the
   exact minor in `go.mod`.
2. **KISS, always.** Prefer simplicity over cleverness. Three similar
   lines is better than a premature abstraction.
3. **Boy-scout rule on adjacent issues.** If you see something easy to
   fix alongside current work, fix it. Do not ask.
4. **NEVER introduce tech-debt.** If a thing is complex, call it out in
   `MEMORY.md` with the rationale. Never defer cleanup to "later".
5. **Charts created via `helm create <name>`** then edited. Never
   hand-rolled.
6. **No plain ENVs in values.yaml. No lists in values.yaml.** All inputs
   map: camelCase scalar in `values.yaml` -> kebab-case key in
   ConfigMap/Secret -> workload consumes via `envFrom`. Genuinely
   list-shaped data is rendered into a templated ConfigMap and read at
   runtime.
7. **semver push-CD.** Every change declares `change_significance`
   (major/minor/patch) on `submit_outcome`, or a human sets a
   `semver:<level>` PR label. **The IMPLEMENTER owns the level; a
   reviewer may raise it, never lower it.** **Merge is an OPERATOR
   action, triggered by a review agent's approval. Auto-merge is never
   armed. Agents never call merge directly** - no MCP tool exposes it -
   **and agents never post a review either**: the operator writes the
   SCM review from the accepted verdict. The operator merges each repo
   in `Task.spec.mergeOrder` sequentially, on green CI, against the
   exact reviewed head SHA. It applies the `semver:<level>` label itself,
   as a one-way projection of `MergeRequest.status.significance`, BEFORE
   the merge - CI cuts the tag from the label at the merge commit, so a
   merge that lands before the label is a release that never gets tagged.
   Never hand-edit a deploy pin; never re-run a green release job (tag
   mode is not idempotent).

   **In-cluster carve-out (L.10):** **in-cluster agent pods** may not
   use `gh`/`glab` and may not merge. This is enforced structurally, not
   by instruction: the pod holds no forge token and the MCP profile
   exposes no merge action. **Workstation skills** run by a human at a
   terminal with their own `gh` auth KEEP `gh` and KEEP human-driven
   merge.
8. **EVERYTHING through superpowers.** brainstorming, writing-plans,
   test-driven-development, systematic-debugging,
   requesting-code-review, verification-before-completion,
   subagent-driven-development, using-git-worktrees,
   finishing-a-development-branch are mandatory. If a skill might
   apply, invoke it.
9. **Subagent-driven, parallel development** where tasks are
   independent. Dispatch in a single message for true parallelism.
10. **Branch flow:** worktree off `main` -> develop in worktree -> merge
    back to source repo `main` -> cleanup worktree -> build/deploy from
    `main` only. NEVER build or deploy from a worktree. Cleanup
    worktrees regularly.
11. **JSON logs only.** Stdlib `log/slog` in Go. Same logger structure
    everywhere.
12. **Log every business action at INFO** with structured fields
    (request_id, user, action, resource_id, duration_ms where
    relevant). WARN and ERROR used appropriately.
13. **Metrics for everything that counts, times out, or can fail.**
    Counters for events, histograms for durations, gauges for
    in-flight. Expose `/metrics` Prometheus endpoint on every service.
14. **Charts are cluster-agnostic.** A component's helm chart MUST assume
    nothing about the cluster it runs on: no baked `imagePullSecrets`,
    node affinity, ingress host/class, storage class, or replicated-
    secret names in `values.yaml`. All cluster-specific customization
    comes from the `tatara-helmfile` repo (per-bucket `values/common.yaml`
    + per-release `values/<name>/{common,<env>}.yaml` + sops
    `<env>.secrets.yaml`).
15. **Sonnet for implementation. Opus for merges.** Implementation
    subagents are sonnet (`claude-sonnet-4-6` or current stable). The
    merge subagent that integrates parallel work is opus. Plan and
    review work runs in opus.
16. **Never `kubectl set image`, `kubectl edit`, or `kubectl patch`
    spec fields on a helm-managed resource.** Bump chart appVersion
    and `helm upgrade` instead. Direct kubectl mutations leave orphan
    field-managers (kubectl-edit, kubectl-set, before-first-apply)
    that block helm 4 server-side apply on the next sync. Reason:
    burned us in the v0.1.1 -> v0.1.2 tatara-memory upgrade.

### Citing a rule

Rules 1-6 and 8-13 have never moved and are safe to cite by number. Four
numbers were reconciled when this block became one artifact, because the
per-repo copies had drifted into two incompatible numberings:

| rule | meaning | was |
|---|---|---|
| 7 | semver push-CD | 7 in the operator; unnumbered `## CD` prose in the other six |
| 14 | charts are cluster-agnostic | 14 in operator/helmfile/observability; absent from cli, memory, ingester, wrapper |
| 15 | sonnet implements, opus merges | 7 in the six non-operator copies, 15 in the operator |
| 16 | no kubectl mutation of helm-managed resources | 14 in cli and memory; absent elsewhere |

Anything written before this block existed may cite the old number. Design
docs under `tatara-documentation/docs/appendix/design-docs/` are an archive and
are NOT swept: read a rule citation there as the rule's text at the time, not
as a pointer into this list.

## Writing rules

- No em dashes. No smart quotes. No arrows. No decorative Unicode.
  Plain hyphens and straight quotes.
- No preamble. No recap unless asked. One line at most: what changed,
  any non-obvious choice.
- Show diffs, not whole files, for anything > 30 lines that already
  exists.
- No docstrings, type annotations, or comments on code not being
  changed.
- No error handling for scenarios that cannot happen.

## What I want from a Claude session here

- Read `MEMORY.md` and `ROADMAP.md` before non-trivial work.
- Update `MEMORY.md` when you make a non-obvious decision or hit a
  dead-end. One line per entry, dated.
- Update `ROADMAP.md` when you complete or re-scope a phase.
- Use `/handoff` if you are approaching context limits; do not soldier
  on.

## Toolchain (mise)

Every tatara repo pins its build tools in a root `.mise.toml`. mise is already
installed in the agent container and on PATH.

- In a freshly cloned repo, run `mise install` once before building. This
  installs the exact Go, golangci-lint, helm, etc. the repo pins.
- Invoke pinned tools through mise: `mise exec -- go build ./...`,
  `mise exec -- golangci-lint run`, or the repo task `mise run lint` /
  `mise run test` / `mise run build`. Do NOT call a bare `go`/`helm` for a
  build - it may be the wrong version. `mise exec` / `mise run` work in any
  shell; bare tools only resolve via the shim PATH.
- If you change a tool dependency, edit that repo's `.mise.toml` (pin an exact
  version), never install ad-hoc.
- `.mise.toml` under /workspace is pre-trusted; no `mise trust` needed.

## CD (semver push-CD)

See **hard rule 7**. It is the single source of truth and this section carries
no separate copy on purpose: the copy that used to live here said "the pipeline
merges bot-authored PRs on green required checks", which directly contradicts
rule 7's "merge is an OPERATOR action, auto-merge is never armed". A
contradiction left alive in a second section is the exact failure mode the
redesign's contract review kept finding: an implementer reads ONE section.

## This block is generated

Everything between the BEGIN and END markers is owned by
`tatara-agent-skills/template/CLAUDE-shared.md` and is byte-identical in every
tatara repo. Editing it in place is pointless - the next skills release
overwrites it. Change it with a PR against that file; the skills release
workflow fans it out. Content ABOVE the BEGIN marker and BELOW the END marker
is local to this repo and is never touched by the sync, which is where a repo
records how these rules apply to it.
<!-- END tatara-shared-contract -->


## Local notes

- The operator is the only component that merges. Rule 7 is not advice here, it
  is this repo's behavior: `Task.spec.mergeOrder`, sequential, green CI, the
  exact reviewed head SHA, `semver:<level>` label applied BEFORE the merge.
- Agent pods get no forge token and a merge-free MCP profile
  (`internal/agent/pod.go`). Keep it structural. Do not "helpfully" widen a
  profile to unblock an agent.
- CRD changes are API changes: `change_significance: major` unless the field is
  additive and optional.
