# Cron-driven `refine` agent - Design

Date: 2026-06-25
Repos: tatara-operator (controller + restapi + SCM), tatara-cli (MCP tools),
tatara-claude-code-wrapper (cli-pin bump), tatara-helmfile (deploy).
Status: approved

## Goal

A project-scoped `refine` agent that runs as the FIRST step of the
reconciliation cron cycle (before brainstorm / issueScan / mrScan). Each run it
loads all open + recently-closed issues across ALL repos in the project,
detects duplicates, overlaps, and already-implemented work, and acts
autonomously: close already-implemented or duplicate issues, edit scope on
existing ones, split overly-broad ones into child issues. The operator's
work-item-ledger reconcile propagates the resulting issue-state changes to the
Task CRs in the cluster. The point: brainstorm and implementation each cycle
operate on a deduped, scoped, current backlog instead of a noisy one.

## Decisions (locked)

- **Gating: piggyback the scan cycle as a hard pre-step.** No separate schedule.
  When any of {mrScan, issueScan, brainstorm} is due, the operator first ensures
  a `refine` Task has completed for that cycle; while a refine is non-terminal,
  that cycle's scans/brainstorm are deferred. Refine runs once per project per
  cycle.
- **Authority: fully autonomous.** The refiner closes / edits / splits any
  project issue (bot- or human-authored) directly when it judges it
  duplicate / implemented / overlapping. No human approval gate. (Guardrails:
  one refine per project per cycle, every action logged + metered, idempotent.)
- **Task propagation: operator-propagated.** The refiner acts only on the
  tracker (issues). The operator's work-item-ledger reconcile detects the
  issue-state changes and updates/closes/creates the corresponding Task CRs. The
  agent never writes Task CRs directly (single writer = operator).
- **Scope: open + recent-closed issues + recent commit history.** All open
  issues across the project's repos, plus issues closed within the lookback
  window (default 30 days, configurable), PLUS recent commit history on each
  repo's default branch within the same window. The commit history is a SECOND
  evidence source for "already implemented": an issue is implemented when a
  merged PR / closed sibling OR a commit message/diff shows its scope was already
  delivered (catches work landed by direct commit, by another agent on a shared
  branch, or by a squash that did not close the issue). PRs excluded (issues
  only); commits are read-only context.
- **Splits AND followups: direct create.** The refiner creates issues directly
  (autonomous), not through the `propose_issue` approval path. Two creation
  cases: (a) SPLIT - decompose a too-broad parent into child issues; (b) FOLLOWUP
  - file a NEW issue for work the refiner surfaces while reconciling (a gap, a
  missing piece, a half-done implementation spotted in the commit history, a
  discovered regression or tech-debt). Both use the same `create_issue` tool; the
  body links the originating issue/commit and states why it was filed.

## Components

### 1. Operator: new `refine` kind + cron barrier

- Add `refine` to the Task `Kind` enum (`api/v1alpha1/task_types.go`), to the
  project-scoped kinds set (`IsProjectScopedKind`), and to `toolProfileForKind`
  (`tatara-cli/internal/mcp/profiles.go`) -> `"refine"` profile.
- `Status.LastRefine *metav1.Time` on the Project (`project_types.go`), mirroring
  `LastBrainstorm`/`LastIssueScan`.
- Optional `Cron.Refine` config (`ScmCron`): `ClosedLookbackDays int`
  (default 30). No schedule field - refine fires off the existing cadence.
- Barrier in the scan reconcile (`internal/controller/projectscan.go`): compute
  whether any of {mrScan, issueScan, brainstorm} is due (existing `activityDue`).
  If at least one is due AND no refine has completed for this cycle
  (`LastRefine` older than the earliest due-activity base, OR a refine Task is
  non-terminal), then:
  - if no refine Task is in flight, dispatch one (dedup key `refine-<project>`,
    QueueClass alert/scan, one per project per cycle) and requeue;
  - while a refine Task for the project is non-terminal, SKIP dispatching the
    due scans/brainstorm this reconcile (defer);
  - when the refine Task reaches terminal, stamp `LastRefine` and let the due
    scans/brainstorm proceed (this or the next reconcile).
- The barrier MUST NOT deadlock: a refine Task that reaches terminal (Succeeded
  / Failed / Parked / Done) always releases the gate; the stamp is written on
  terminal regardless of success so a failed refine does not wedge the cycle.

### 2. Operator: refine goal builder + dispatch

- `internal/refine/goal.go` (new), mirroring `internal/incident/goal.go`:
  `GoalProject(repoSlugs []string, lookbackDays int) string` producing the agent
  goal: load the project issue inventory via `list_issues` AND the recent commit
  history via `list_commits`; for each cluster of related issues across repos,
  decide and act -
  - already-implemented (a `task_list` work-item shows a merged PR closing it, a
    closed sibling already did it, OR a commit in `list_commits` delivered its
    scope) -> `close_issue` with a comment citing the implementing PR / issue /
    commit SHA;
  - duplicate -> `close_issue` the dup with a comment "duplicate of <repo>#N",
    keep the canonical;
  - overlap / scope drift -> `edit_issue` to tighten title/body/labels;
  - too broad -> `create_issue` child (split) issues (linking the parent in the
    body) + `edit_issue` the parent to the residual scope (or close it if fully
    split);
  - newly-surfaced work -> `create_issue` a FOLLOWUP issue (a gap, a half-done
    implementation seen in the commit history, a discovered regression / tech-
    debt), body linking the originating issue/commit and stating why.
  The goal instructs: judge implemented-ness from `task_list` + closed siblings +
  commit history, never touch PRs, log reasoning in each comment, be idempotent
  (skip issues already resolved/linked; do not re-file a followup that already
  exists - check the loaded inventory first).
- Dispatch via the same QueuedEvent path as brainstorm/incident
  (`createScanTask`/`EnqueueEvent`), `Kind: "refine"`, project-scoped
  (`RepositoryRef: ""`), goal from `refine.GoalProject`, dedup `refine-<project>`.

### 3. Operator restapi + SCM: new capabilities

- SCM writer (`internal/scm/scm.go` interface + github.go/gitlab.go):
  - `CloseIssue(ctx, token, repo, number, comment)` - ALREADY EXISTS, reuse.
  - `EditIssue(ctx, token, repo, number, EditIssueReq{Title,Body,Labels *})` -
    NEW. Pointer fields = only set what's provided (PATCH semantics). GitHub
    `PATCH /issues/:n`, GitLab `PUT /issues/:iid`.
  - `CreateIssue` - ALREADY EXISTS, reuse for splits.
- SCM reader: a cross-repo issue list - `ListOpenIssues` exists; add
  `ListClosedIssues(ctx, owner, repo, since)`. Plus a commit-history reader:
  `ListCommits(ctx, owner, repo, since time.Time) ([]CommitRef, error)` (NEW) on
  the default branch, returning `CommitRef{SHA, Message, Author string; Date
  time.Time}`. GitHub `GET /repos/:o/:r/commits?since=<RFC3339>`, GitLab `GET
  /projects/:enc/repository/commits?since=<RFC3339>`. The restapi aggregates both
  issues and commits across the project's repos.
- restapi (`internal/restapi/handlers.go`) - new endpoints, all gated by the
  existing project/bot-authorship/egress checks:
  - `GET /projects/{p}/issues?state=open|all&closedSinceDays=N` -> aggregate
    open + recently-closed issues across the project's repos (repo, number,
    title, body, state, author, labels, closedAt, linked-PR if known).
  - `GET /projects/{p}/commits?sinceDays=N` -> aggregate recent default-branch
    commits across the project's repos ({repo, sha, message, author, date}).
  - `POST /projects/{p}/issues/{repo}/{number}/close` {comment}.
  - `PATCH /projects/{p}/issues/{repo}/{number}` {title?, body?, labels?}.
  - `POST /projects/{p}/issues/{repo}` {title, body, labels} (direct create for
    splits).
  Each validates the repo belongs to the project (mirrors `proposeIssue`).

### 4. cli: `refine` tool profile + new MCP tools

- `internal/mcp/profiles.go`: add a `refine` profile granting:
  `list_issues`, `list_commits`, `close_issue`, `edit_issue`, `create_issue`,
  `comment_on_issue`, `task_list`, `task_get`, `repo_list`,
  `report_internal_issue`, plus the memory + code-graph read groups (so it can
  judge implemented-ness against the graph). NOT granted: `propose_issue`
  (splits/followups use direct `create_issue`), `task_update`/`subtask_*`
  (operator-propagated; agent never writes Tasks).
- `internal/mcp/tools.go`: new tools wrapping the restapi endpoints:
  - `list_issues` {state?, closedSinceDays?} -> the aggregated issue inventory.
  - `list_commits` {sinceDays?} -> aggregated recent commit history across repos.
  - `close_issue` {repo, number, comment} (comment REQUIRED - every close
    explains itself).
  - `edit_issue` {repo, number, title?, body?, labels?}.
  - `create_issue` {repo, title, body, labels?} -> created issue ref (used for
    both splits and followups).

### 5. Operator: ledger propagation

- The work-item-ledger reconcile already keys Tasks to issues by repo+number and
  reconciles Task state from issue state. Extend so:
  - a refiner-closed issue (state closed) drives its bound Task terminal (close
    the work-item / mark the Task Done) on the next reconcile - verify the
    existing close-detection covers refiner closes (it closes via the same SCM
    `CloseIssue`, so the issue simply reads closed; the ledger's issue-closed
    path should already handle it - add a test).
  - split child issues are picked up as new candidates by the now-unblocked
    issueScan -> new Tasks (no special wiring; they are ordinary new issues).
  - scope edits change the issue body, which the ledger-sourced prompt re-reads
    on the next implementation (no special wiring).

## Error handling

- A refine Task that fails or parks still releases the cron barrier (stamp
  `LastRefine` on terminal regardless of outcome) so a bad refine never wedges
  brainstorm/scans.
- `list_issues` / close / edit / create restapi calls return structured errors;
  the agent tolerates a single-issue action failure and continues the batch
  (one un-closable issue does not abort the run).
- SCM `EditIssue` 404 (issue gone) is benign - log + skip (idempotency class,
  cf. the GitLab idempotency memory).

## Metrics + logging

- Counters (operator + cli): `refine_actions_total{action="close|edit|create",
  result}`; reuse `operator_scm_writes_total` for the SCM calls.
- INFO business log per action: `action=refine_close|refine_edit|refine_split`,
  `resource_id=<repo>#<n>`, decision rationale, `request_id`.

## Testing

- Operator: barrier defers scans while a refine Task is non-terminal + releases
  on terminal (incl. failed); `LastRefine` stamped on terminal; refine dispatch
  is one-per-project-per-cycle (dedup). `EditIssue` github/gitlab (PATCH
  semantics, only-set-provided, 404 benign). restapi handlers (repo-belongs-to-
  project, aggregate list across repos, close/edit/create). Ledger: a
  refiner-closed issue drives its Task terminal.
- cli: `refine` profile grants exactly the intended tools (and excludes
  propose_issue/task_update); each new tool round-trips its restapi endpoint.

## Out of scope

- No new schedule knob beyond `ClosedLookbackDays`; refine fires off the
  existing cadence.
- No human approval gate (fully autonomous, per decision).
- PRs are not refined (issues only).

## Constraints

Newest stable Go; KISS; no tech-debt; JSON slog; metrics on every action;
charts cluster-agnostic; TDD failing-test-first per unit; deploy only via
tatara-helmfile GitOps (operator + cli merges -> wrapper cli-pin bump ->
helmfile chart pins + image tags). The cli `tatara mcp` tools/list must serve
without a token (wrapper build-guard).
