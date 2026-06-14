# CLAUDE.md - tatara

This file briefs any Claude session working on the `tatara` repo or any
of its component child repos (`tatara-memory`, `tatara-cli`, etc.). Every
child repo carries a copy of this file at its own root. Treat it as the
canonical contract.

## What this repo is

`tatara` is the docs and architecture index for the tatara platform. The
platform is split into eight independent GitHub repositories under
`szymonrychu/`. See `README.md` for the full list and `ARCHITECTURE.md`
for how they fit together. The previous monolithic implementation lives
at `~/Documents/tatara-old` as a reference.

## What this repo is NOT

- Not a monorepo. Each component is its own git repo with its own CI,
  helm chart, Dockerfile, MEMORY.md and ROADMAP.md.
- Not an umbrella helmfile. There is no top-level `helmfile.yaml.gotmpl`
  composing the platform; each component deploys itself.
- Not a place for code. Code belongs in the component repo it serves.

## On-disk layout

```
~/Documents/tatara/                   # this repo
├── tatara-memory/                    # child repo (gitignored)
├── tatara-cli/                       # child repo (gitignored)
├── tatara-memory-repo-ingester/      # child repo (gitignored)
├── tatara-claude-code-wrapper/       # child repo (gitignored)
├── tatara-argo-workflows/            # child repo (gitignored)
├── tatara-tasks/                     # child repo (gitignored)
└── tatara-gitlab-bridge/             # child repo (gitignored)
```

Each child clones from `github.com/szymonrychu/<name>` into the matching
subdirectory. The parent `.gitignore` keeps them out of this repo.

## Hard rules (copied to every component repo's CLAUDE.md)

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
7. **Sonnet for implementation. Opus for merges.** Implementation
   subagents are sonnet (`claude-sonnet-4-6` or current stable). The
   merge subagent that integrates parallel work is opus. Plan and
   review work runs in opus.
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
    comes from the `~/Documents/infra/helmfile` project (per-bucket
    `values/common.yaml` + per-release `values/<name>/{common,<env>}.yaml`
    + sops `<env>.secrets.yaml`). Tatara releases live in that repo's
    `helmfiles/tatara/` bucket.

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
