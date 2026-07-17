# Task 3 report - webhook handlers become primary minters

Commit: `d1288a9` "feat: webhook mints issue/MR Tasks immediately via the shared intake funnel"

## Files changed

- `internal/webhook/server.go`:
  - Added import `internal/own`.
  - Added `func (s *Server) minter() *controller.Minter` (Metrics nil, per brief).
  - Added package-local `func repoSlug(repo *tatarav1.Repository) string` (webhook's own
    twin of `internal/controller`'s unexported helper of the same name - that one is
    unexported in a different package so the webhook package needs its own).
  - `handleForgeItem`: added the MR-opened route. Deviation from the brief's exact
    snippet: dropped `!ev.IsComment` and `!ev.IsReview` from the guard condition -
    `ev.IsComment` is already false at that point (handled and returned above in the
    same function) and `ev.IsReview` does not exist yet on `scm.WebhookEvent` (Task 4
    adds it), confirmed by reading `internal/scm/scm.go`'s actual field list. Gated on
    `ev.Kind == "mr" && ev.IsPR && (ev.Action == "opened" || ev.Action == "reopened")`.
  - `handleIssueOpened`: after the existing stamp + log, builds `controller.ForgeItem`
    from `ev` and calls `s.minter().MintForItem(ctx, &proj, repo, item, true, s.cfg.SpillerFor(&proj))`.
    Error -> 500 + reject (matching the stamp-failure precedent). `created` -> INFO log
    `action=issue_webhook_mint`. Always ends in `s.accept(...)`.
  - Added `handleMROpened`: bot gate -> matchRepo -> reporter gate -> build
    `controller.ForgeItem{IsPR: true, PR: scm.PRRef{...}}` -> `s.minter().MintForItem(...,
    false, ...)` -> INFO log `action=mr_webhook_mint` on create -> accept.
  - `handleIssueComment`: inserted the orphan mint before `deliverPendingEvent`, using
    the new `commentIsOrphan` helper; mint failures are logged only (never surfaced as
    a rejected delivery), per the brief.
  - Added `commentIsOrphan`: uncached read via `s.cfg.APIReader` (falls back to
    `s.cfg.Client`), `apierrors.IsNotFound` -> orphan; other read errors -> not orphan
    (fail-closed on minting); `own.ControllerOwner` -> orphan iff unowned.

- `internal/webhook/issue_opened_test.go`: updated
  `TestIssueOpened_StampsTheWebhookMarker` - the webhook now mints, so it asserts
  `iss.OwnerReferences` is non-empty and `allTasks(...)` has length 1, replacing the old
  `require.Empty(allTasks/allQEs)` assertions. Marker-stamp assertions unchanged. The
  other tests in that file (bot-authored, non-reporter, owned-issue-never-remarked,
  unknown-repo, closed-never-marks) needed no changes - none of their fixtures reach the
  mint path with anything to create.

- `internal/webhook/primary_mint_test.go` (new): the 5 tests from the brief verbatim,
  plus `postPROpened`/`prOpenedBy` helpers (`X-GitHub-Event: pull_request`, GitHub
  `ghWorkItem`-shaped JSON body confirmed against `internal/scm/github.go`'s actual
  parser).

- `internal/webhook/server_test.go`: **bug fix required to make the funnel usable from
  this package's fake client at all** - `seedClient`'s
  `WithStatusSubresource(...)` list was missing `&tatarav1.Issue{}` and
  `&tatarav1.MergeRequest{}`. Without them, controller-runtime's fake client's
  `versionedTracker.updateObject` (v0.24.1,
  `pkg/client/fake/versioned_tracker.go:246`) returns a bare `NotFound` for ANY
  `.Status().Update()` call on a GVK not registered as having a status subresource -
  which is exactly what `objbudget.FitIssue`/`FitMergeRequest` do inside
  `controller.SyncIssue`/`SyncMergeRequest`. This was latent because no webhook-package
  test previously drove `SyncIssue`/`SyncMergeRequest` through this shared client (only
  `internal/controller`'s own tests did, and those already register both types - see
  `internal/controller/mirror_test.go:42`). Diagnosed empirically: a throwaway debug
  test showed a plain `c.Get` on the pre-seeded Issue succeeding twice, while
  `objbudget.FitIssue`'s internal `RetryOnConflict` closure's `Get` on the identical key
  failed `NotFound` - traced to the fake client source, confirmed, fixed, then deleted
  the debug test.

## Commands run (actual output)

```
mise exec -- go build ./...                                        # clean, no output
mise exec -- go vet ./...                                           # clean, no output
export KUBEBUILDER_ASSETS="$(mise exec -- go run \
  sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.21 use 1.33.0 -p path)"
mise exec -- go test ./internal/webhook/ -run \
  'TestIssueOpened_MintsClarify|TestPROpened|TestSweepAfterWebhook' -v
  # PASS x3 (TestIssueOpened_MintsClarifyTaskImmediately,
  #          TestSweepAfterWebhook_NoDoubleMint, TestPROpened_MintsReviewTaskImmediately)
mise exec -- go test ./internal/webhook/... -race
  # ok  github.com/szymonrychu/tatara-operator/internal/webhook  8.781s
mise exec -- go test ./...
  # ok across all 25 test-bearing packages (api/v1alpha1, controller 31.992s, webhook
  # 8.319s, etc.) - full repo green
mise exec -- golangci-lint run ./internal/webhook/...
  # 0 issues
```

`KUBEBUILDER_ASSETS` note: `mise exec -- setup-envtest` failed ("No version is set for
shim") since it isn't a mise-pinned tool here; used the Makefile's own recipe
(`go run sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.21 use 1.33.0 -p
path`, matching `ENVTEST_VERSION`/`ENVTEST_K8S_VERSION` in the repo `Makefile`) instead -
environment setup, no code change.

## Deviations from the brief (and why)

1. `handleForgeItem`'s MR-opened guard drops `!ev.IsComment` (dead check - already
   returned true-branch above) and `!ev.IsReview` (field doesn't exist on
   `scm.WebhookEvent` yet; Task 4 adds it). Brief explicitly anticipated this.
2. `repoSlug` is package-local to `webhook` (brief called it "a tiny local helper");
   `internal/controller` already has an identically-named but unexported helper - no
   collision since different packages, no export needed.
3. Fixed `seedClient`'s missing `WithStatusSubresource` entries for Issue/MergeRequest
   (not mentioned in the brief, but required for `MintForItem`'s `SyncIssue`/
   `SyncMergeRequest` calls to work against this package's shared fake client at all;
   without it every mint fails with a spurious `NotFound` on the status write).

## Concerns

- None outstanding. Full repo test suite and lint are green. The `seedClient` fix is a
  test-only change (adds 2 types to an existing status-subresource list) and cannot
  affect production behavior.
- Left `docs/superpowers/plans/2026-07-17-webhook-primary-reactivity.md` and
  `docs/superpowers/specs/2026-07-17-webhook-primary-reactivity-design.md` (untracked,
  pre-existing in the worktree) out of this commit - they belong to the overall

## Fix note (review findings, commit `24db6e2`)

Two Important review findings fixed:

- **`internal/webhook/issue_opened_test.go`** (lines ~3-16): the file-level
  doc comment still said "THE WEBHOOK MINTS NOTHING itself (the B.4 sweep is
  the sole intake...)" - the exact opposite of what Task 3 shipped, and
  read as a canonical contract by convention in this codebase. Rewrote it to
  state the webhook is now the PRIMARY minter (mints the clarify Task
  in-request via the shared `controller.Minter`, owns the mirror immediately),
  the B.4 sweep is now a backstop whose re-pass over the same natural key
  (`IntakeTaskName`) no-ops, and the `tatara.dev/webhook-originated` marker
  still matters for the sweep's own cold-start path. No test logic touched,
  prose only.
- **`internal/webhook/primary_mint_test.go`**: added HTTP-driven coverage for
  `handleIssueComment`'s orphan-mint branch (server.go ~451-465), which had
  zero tests. Added a `commentBodyOn`/`postIssueComment` pair (parameterized
  variant of `reporter_gate_test.go`'s `issueCommentBy` + inline post pattern)
  and three tests:
  - `TestOrphanComment_NoMirror_MintsTask` - comment on an issue with no
    mirror CR yet; asserts a clarify Task is minted (`allTasks` len 1).
  - `TestOrphanComment_UnownedMirror_MintsTask` - comment on an issue whose
    mirror CR exists with no controller owner; asserts a Task is minted.
  - `TestOwnedMirrorComment_NoMint_PendingPathRuns` - comment on an issue
    owned by a pre-seeded Task (via `own.AddPlainOwner` +
    `own.HandOverController`, same helpers `pending_events_test.go` uses);
    asserts the Task count is unchanged AND the owning Task's
    `Status.PendingEvents` gets the comment queued (pending path still runs).

  Sanity-checked (not part of the commit): forcing `commentIsOrphan` to
  always return `false` flips the first two tests to FAIL, confirming they
  exercise the real mint path and aren't vacuously green.

Test command and actual output:

```
$ export KUBEBUILDER_ASSETS="/Users/szymonri/Library/Application Support/io.kubebuilder.envtest/k8s/1.33.0-darwin-arm64"
$ mise exec -- go test ./internal/webhook/... -race
ok  	github.com/szymonrychu/tatara-operator/internal/webhook	8.717s
```

62 tests total in the package, all passing (verbose run confirmed each
`--- PASS`, including the 3 new ones and every pre-existing test).
`KUBEBUILDER_ASSETS` was missing by default (`fork/exec
/usr/local/kubebuilder/bin/etcd: no such file or directory`); resolved per
Makefile's `ENVTEST`/`ENVTEST_K8S_VERSION` pins via
`go run sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.21 use 1.33.0 -p path`
- environment setup only, no code change.
  plan/design, not this task's diff.

## Post-review fix (final whole-branch review, blocker)

`internal/webhook/server.go`'s orphan-comment mint (`handleIssueComment`,
~line 610) called `MintForItem` with `webhookOriginated=false`. Confirmed the
real `MintStage` ordering (`internal/controller/sweep.go:188-198`):
`TataraParkedLabel` is checked FIRST (always wins), then `webhookOriginated`
(true -> `StageTriaging`), then `humanHasLastWord`, else `StageParked`/
`ReasonBacklogSweep`. With `false` and no prior comments/label, a fresh
orphan comment minted PARKED. The same-request
`deliverPendingEvent -> driveCommentUnpark` promotion path that was meant to
un-park it reads the informer cache (`resolveMirrorTarget` /
`own.ControllerOwner` in `pending_events.go`), which routinely still lags
the mint's just-written mirror/owner, so the promotion silently misses, no
pending event queues, and the Task is stranded parked with no sweep recovery
(the issue is now owned, so `IsOrphanIssue` skips it) - the HMAC-verified
human comment is silently dropped.

Fix: pass `webhookOriginated=true` (matches `issues.opened` at server.go:517
- a live HMAC-verified comment is the same liveness signal). The parked-label
outermost gate is untouched, so a deliberately backlog-parked issue still
mints PARKED.

Coverage: strengthened `TestOrphanComment_NoMirror_MintsTask` and
`TestOrphanComment_UnownedMirror_MintsTask` to also assert
`tasks[0].Spec.InitialStage == tatarav1.StageTriaging`. Confirmed TDD
ordering - pre-fix run (webhookOriginated still `false`):

```
--- FAIL: TestOrphanComment_NoMirror_MintsTask
    Error: Not equal: expected: "triaging" actual: "parked"
--- FAIL: TestOrphanComment_UnownedMirror_MintsTask
    Error: Not equal: expected: "triaging" actual: "parked"
```

Post-fix:

```
$ export KUBEBUILDER_ASSETS="$(mise exec -- go run sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.21 use 1.33.0 -p path)"
$ mise exec -- go test ./internal/webhook/... ./internal/controller/... -race
ok  	github.com/szymonrychu/tatara-operator/internal/webhook	9.029s
ok  	github.com/szymonrychu/tatara-operator/internal/controller	35.829s
```

540 subtests pass, 0 fail (verbose run, `--- PASS` count).
