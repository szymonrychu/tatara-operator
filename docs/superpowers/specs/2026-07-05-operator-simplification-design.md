# Operator simplification - design

Date: 2026-07-05
Repo: tatara-operator
Status: approved (brainstorming), pending spec review

## Goal

Reduce accidental complexity in the operator without changing behavior. Every
change is either dead-code removal, behavior-preserving dedup, or a pure
file/relocation split. No feature changes, no gate-logic changes.

Source: a 41-agent audit (`find` + adversarial `verify` against git-blame,
MEMORY.md, and pinned tests). 30 candidates were verified: **16 SAFE-SIMPLIFY**
(behavior-preserving, low-risk), **13 RISKY-BUT-VALID** (real wins touching
load-bearing code, each carrying a reduced-risk "safer scope"), **1 REJECTED**
(evidence was factually wrong - see Appendix).

User decision: execute the full set (16 SAFE + 13 RISKY including the four large
file-splits).

## Hard constraints (apply to every item)

1. **Behavior-preserving only.** If a change alters a gate, a metric label, an
   error string, a log field, or a status write's semantics, it is out of scope
   here - split it out and flag it.
2. **Pinned tests are the acceptance bar.** Each RISKY item names the existing
   tests that must pass byte-for-byte, plus any new regression test to add
   FIRST. `go build ./... && go vet ./... && golangci-lint run && go test
   ./internal/... -race` green before every merge.
3. **File-splits are `git mv` + move, never rewrite.** Same package, same
   receiver, no logic edits in the same commit.
4. **Small, reviewable commits.** One item (or one tightly-coupled group) per
   commit; conventional-commit `refactor:` type.
5. **Merge promptly** to keep conflict surface small against bot-authored
   branches (bots push to this repo continuously).

## Execution batches

Batches 1-3 are independent (different files) and parallelizable. Batch 4
(file-splits) each need their own short-lived worktree and prompt merge. Batch 5
(RISKY dedup) is per-item gated on its own tests.

### Batch 1 - Dead-code deletion (SAFE, trivial, independent)

- **S1** `queue_controller.go:66-74,507-515` - delete `blockKindFunc` and
  `queuedAutonomousCount`; both are orphaned (only their own unit tests call
  them). Delete or fold the dedicated tests.
- **S7** `ledger.go:212-251` - delete `ProjectReconciler.markWorkItemsClosed`
  (dead; restapi `markWorkItemsClosedViaClient` is the sole live impl) and the
  whole `ledger_refine_test.go`. Strip the now-unused `client` and `retry`
  imports from `ledger.go`. Do NOT port RetryOnConflict into the restapi copy
  (contradicts its documented best-effort rationale).
- **S14** `pushmetrics/receiver.go:318-326` - delete deprecated `Handler()`
  shim; update the 4 `receiver_test.go` sites to `r.PushHandler().ServeHTTP(...)`.

### Batch 2 - Byte-identical dedup collapses (SAFE, independent per file)

- **S2** `repository_controller.go:437-444,565-589,624-645` - rewrite 3 inline
  status writes to call the existing `patchStatus` with a mutate closure.
- **S4** `deploy_supervision.go:425-441,694-709` - extract
  `clearCascadeStatusFields(*TaskStatus)`; both `clearDeployState` and
  `setTaskImplementContext` call it before their own extra field.
- **S5** `gitlab.go:685-716` - extract `glParseRef(ref, sep, kind)`; keep
  `glHashRef`/`glBangRef` as thin wrappers (identical error text).
- **S6** `github.go:517-535` + `gitlab.go:418-437` - extract
  `createOrUpdateOnConflict(create, conflictStatus, update)` into `scm.go`.
  Keep ALL path/color/JSON body construction in the per-provider closures (that
  is where the historical `new_color` vs `color` bug lived). `ensure_label_test.go`
  must pass unmodified.
- **S8** `handlers.go:78-104,1289-1299` - collapse the three byte-identical
  `authorizeFor{Task,Project,Subtask}` into one `authorizeCaller(w, r) bool`;
  drop the dead CR argument at ~16 call sites. Keep the shared-OIDC-identity
  explainer comment.
- **S9** `webhook/server.go` (43 sites) - add `(s *Server) accept(...)` and
  `(s *Server) reject(w, status, msg, provider, kind, action, result)` helpers
  doing count+response; replace the repeated 2-3 line blocks. Identical labels,
  codes, bodies, ordering.
- **S13** `operator_metrics.go:988-1021` - add
  `addPositive(vec, delta, labels...)`; each `AddTaskTokens`/`AddTerminalTokens`
  site becomes one line.
- **S15** `turnloop.go:20-38` + `incident/goal.go` + `refine/goal.go` - extract
  the byte-duplicated turn-0 guidance literals into a new dependency-free leaf
  package `internal/promptguidance` (stdlib only, preserves the no-import-cycle
  constraint). All three import it.
- **S16** `pod.go:248-266` + `deploy_ledger.go:78-96` + `queue/seq.go:47-65` -
  factor the triplicated DNS1123 loop into one exported
  `SanitizeDNS1123(s, maxLen)` in a small leaf package; three sites pass their
  own cap. (Coordinate with S10 - same function.)

### Batch 3 - pod.go per-kind table consolidation (SAFE dedup + BRIDGE)

This batch is the bridge into the documentation-agent spec: collapsing the
parallel per-kind switches into one table means the new `documentation` kind
becomes one table row instead of edits to five functions. **Do this batch before
the doc-agent work.**

- **S10** `pod.go:248-266,319-337` - extract `sanitizeSlug(s, maxLen)`;
  `sanitizeDNS1123`(cap 63) and `slugifyTitle`(cap 40) become wrappers. Fold
  with S16's leaf package.
- **S11** `pod.go:843-893` - `toolProfileForKind` and `skillProfileForKind` are
  byte-identical; back both with one `var kindProfiles map[string]string` +
  `profileForKind(kind)`. Preserve fail-open default and every mapped value.
- **S12** `pod.go:902-932` - factor
  `resolveByKind(byKind, kind, activity, fallback)`; `modelForKind`/`effortForKind`
  call it. Same precedence order (including the healthCheck pseudo-key).

Net: one cohesive per-kind resolution table (`profile`, `model`, `effort`,
`branchKind`, PR-title prefix). Adding a kind = one row.

### Batch 4 - File-splits (RISKY-but-mechanical; `git mv` + move only)

Each in its own worktree off fresh main, split the companion `_test.go` along
the same boundaries (or note in MEMORY.md why left monolithic), verify with the
full package test suite, and merge promptly.

- **S3** `writeback.go` (1575L) -> `writeback.go` (dispatcher +
  `writeBackOpenChange` + shared `scmContext`/`recordSCM`), `writeback_proposal.go`,
  `writeback_review.go`, `writeback_selfimprove.go`, `writeback_issue.go`,
  `writeback_util.go`. Pure move.
- **S18** `task_controller.go:562-841` -> `bootcrash.go` (same package). Move
  only the boot-crash-specific block + its narrow consts
  (`bootCrashDetailCap`, `annBootCrash{Attempts,Diagnostics,LastPodUID}`).
  EXCLUDE `postTerminalComment`/`commentTerminalDiagnostics` (leave in place or
  extract to `terminal_diag.go` to match the existing `terminal_diag_test.go`).
- **S19** `lifecycle.go` (2598L) -> `lifecycle.go` (dispatcher +
  deadline/park/setLifecycleState/resetAgentRun + the genuinely-shared helpers:
  `enterConversation`, `triageCloseIssue`, `triagePostComment`, the `clear*`
  helpers, `setImplementContext`, `clearMergedChangeState`,
  `maybeMarkHandoverResume`), `lifecycle_triage.go`, `lifecycle_conversation.go`,
  `lifecycle_implement.go`, `lifecycle_mrci.go`, `lifecycle_merge.go`,
  `lifecycle_mainci.go`. Split `lifecycle_test.go` on the same boundaries.
  Update the two deep-audit-report docs' line citations (or add a note that they
  predate the split).
- **S29** `operator_metrics.go` (1230L, 79 fields) - move cohesive field/ctor/
  method groups into sibling files (`scm_metrics.go`, `queue_metrics.go`,
  `task_metrics.go`, `accountusage_metrics.go`, `cd_metrics.go`), following the
  existing `LifecycleMetrics` precedent, in separate commits with tests after
  each. **CD-cascade caveat**: leave `cdCascade*`/`cdResolved*` + their 3 methods
  in the residual struct, OR add a nil-receiver-no-panic regression test before
  moving. Manually diff every moved Help/Buckets/label-list (NamesStable test
  covers only 14 of ~67 names). First pass, not a full resolution.

### Batch 5 - RISKY dedup (per-item test-gated, use the safer scope)

Each item uses its **safer scope** verbatim (not the naive proposal) and adds
the named regression test FIRST.

- **S17** `task_controller.go` 15 Get+RetryOnConflict+Update sites - two tracks.
  Track A: add `patchTaskAnnotations`/`patchTaskStatus` mirroring
  `repository_controller.patchStatus` EXACTLY, incl. the unconditional
  `*task = *fresh` copy-back on both skip and write paths (preserves site-153
  resourceVersion adoption / the #175 409-storm fix); convert the ~11 Task-only
  sites behind `task_controller_audit_test.go` Findings 1/3/5 + `bootcrash_test.go`
  + a new site-153 "already seeded by another replica" test. Track B: keep
  `markSubtaskDone`'s NotFound-swallow and the 1108 Running-flip's non-tolerant
  Get as two distinct wrappers (or a `tolerateNotFound` flag) - do NOT collapse
  to one closure. `-race` + lint before merge; diff each bailout condition.
- **S20** `lifecycle.go:900-1097` `finishTriage` double comment-fetch - memoize
  ONLY on success (never cache an error), pointer receivers on `triageReader`,
  `listComments(ctx)` helper. Test: exactly one `ListIssueComments` on the
  success path, but two live attempts on the first-call-error path (preserves
  current fail-open retry).
- **S21** `lifecycle.go:2384-2450` + `writeback.go:465-520` semver stamping -
  split into TWO helpers (`ensureSemverLabelColor`, `addSemverLabelToPR`), each
  called at today's exact gated location (do not move relative to provider
  checks). Add a test asserting EnsureLabel still fires for `provider=="gitlab"`
  in `applySemverAutoMerge`. Do not unify log field names as a side effect.
- **S22** `projectscan.go:1475-1703` `brainstorm()`/`healthCheck()` 90% dup -
  extract the proven-identical per-repo loop + gather/context/goal/task/metrics
  tail into `runProjectScopedProposalCycle` parameterized by activity + 4 funcs.
  Keep the two post-loop early-return guards (no-valid-repos vs at-cap) in each
  thin wrapper preserving each function's current order (do NOT let the helper
  pick one order - that touches the 2026-06-13 flooding-incident path).
- **S23** `projectscan.go:1319-1464` issueScan 145-line loop body -> extract
  `r.issueScanPickOne(...)` (14-param). Convert every `continue` to an explicit
  `return false, nil`/`return false, err` at the SAME point (do not reorder the
  9 exits). Pass the precomputed `systemicLeads` map in (preserves the 2026-06-23
  pre-dedup scoping). Run the ~15 projectscan_*_test.go files unchanged/green.
- **S24** `github.go:583-605,776-789` + `gitlab.go:785-802` CI-status fold -
  keep per-item mappers provider-specific (extract GitHub's inline run-status to
  `ghRunCIStatus`), introduce ONE `foldCIStatuses(statuses ...string)` reducer
  with a truth table identical to today's 2-arg version; add isolated unit tests
  (empty->"", lone ""->"", blank-mixed) BEFORE wiring; delete the len==0 guards
  only after a test proves zero-arg returns "". `commit_ci_status_test.go` +
  `gitlab_audit_test.go` pass unchanged.
- **S25** `github.go:282-307` + `gitlab.go:319-344` paged-GET boilerplate -
  extract `doPagedGET` but parameterize BOTH headers (auth + accept), take a
  pre-joined absolute URL (don't touch gitlab's query-preserving rebuild),
  decide+document `HTTPError.Path` semantics, leave the pagination loop guards
  untouched. Add a direct helper unit test; rerun scm_audit_r2/r3 pagination
  tests.
- **S26** `handlers.go` mutation handlers - extract `s.mutateTaskStatus(...)`
  for ONLY the 5 handlers that share the exact skeleton (reviewVerdict,
  prOutcome, implementOutcome, brainstormOutcome, issueOutcome) with an optional
  `onSuccess` hook so `issueOutcome`'s `IssueOutcome(req.Action)` metric
  survives. Do NOT fold in patchTask/changeSummary/handover/postComment/
  patchSubtask (different contracts). Table-driven test pinning each migrated
  handler's metric name/error text/log fields/success metric.
- **S27** `handlers.go:1384-1441` - extract
  `resolveProjectSCMProviderToken(w, r, proj)` (provider + secret + raw token,
  NO emptiness check); leave each nil-guard, final factory call, and Writer's
  `token==""` 500-check in the thin wrappers. Do NOT add the empty-token check
  to Reader as a byproduct.
- **S28** `operator_metrics.go:518-611` - add `seedLabels(set, dims...)`
  cartesian helper; apply ONLY to the pure multi-dim blocks (reconcileTotal,
  webhookEvents, scanItemsTotal, ingestJobTotal, webhookDuration,
  memoryRetrievalProbe). Leave single-dim loops as-is. Do NOT touch
  `writebackSkip4xxTotal` (curated pairs, issue #166) - add a
  `TestWritebackSkip4xx_PreSeeded` guard. Leave `toolSurfaceProbe`/
  `restapiRequestsTotal` (reserved vantage label, per MEMORY).

## Testing strategy

- SAFE items (Batches 1-3): existing package tests pass unmodified; the whole
  point is byte-identical behavior. Add a test only where deleting code removes
  the last coverage of a still-wanted behavior.
- File-splits (Batch 4): `go build`/`vet`/`golangci-lint`/`go test` on the
  touched package; test files split alongside.
- RISKY dedup (Batch 5): the named regression test lands FIRST (red where it
  should be), then the refactor makes it green, then the pre-existing pinned
  tests confirm no drift. `-race` on the controller package for S17.

## Out of scope

- The one REJECTED candidate (see Appendix).
- Any behavior change, gate change, or metric/label change (split out + flag).
- Cross-repo changes (this spec is operator-only).

## Appendix - rejected candidate

`activityScheduleAndLast` / `stampScan` "same activity switch" (projectscan) -
**REJECT-WRONG**. The two functions do switch on the same 5 activity strings,
but unifying them is not behavior-preserving (per the 2026-06-15 healthCheck
MEMORY entry, the switches diverge by design when a new activity is added).
Leave as-is.
