# Final-review fix wave (fix/review-external-terminal-mr)

## Fix 1 - comment accuracy (two sites, comment-only)

Both comments in `internal/controller/task_stage.go` claimed `terminalMREdge`
was shared with "the endpoint no-op" (the REST `submit_outcome` handler in
`internal/restapi/outcome.go`). That's false: the endpoint's `terminalNoop`
path uses its own `mrTerminalStates`/`openMRs` helpers, a deliberately
separate, complementary state model - it never calls `terminalMREdge`.

- `reconcileClocks` finalize block (~line 104-111): comment now says
  `terminalMREdge` is reused so "this path, the pre-dispatch guard and
  reviewAdvanceEdge can never disagree with each other" - dropped the
  endpoint clause.
- `ensureStagePod` pre-dispatch guard (~line 1257-1262): comment now says
  "Reuses terminalMREdge so this can never disagree with reconcileClocks or
  reviewAdvanceEdge" - dropped the endpoint clause.

Verified all three real callers of `terminalMREdge` (defined in
`internal/controller/reviewpost.go:354`): `reconcileClocks`
(task_stage.go:117), the pre-dispatch guard (task_stage.go:1268), and
`reviewAdvanceEdge` (reviewpost.go:369). No code change, comments only.

## Fix 2 - missing metric on the MR-terminal no-op response class

`terminalNoop` in `internal/restapi/outcome.go` returns 200 with
`{"noop":true,"reason":"mr-terminal"}` but incremented no Prometheus counter.
Before this branch, the same call shape hit `o.bad(msg, "no-open-mr")`,
which counted via `obs.RestOutcomeRejectedTotal{kind,"no-open-mr"}`
(outcome.go:496). After the branch this 2xx no-op class was invisible in
restapi outcome metrics - a hard-rule violation (metrics mandatory).

Inspected the existing convention first:
- `obs.RestOutcomeAcceptedTotal` (`internal/obs/restapi_metrics_v2.go:14`):
  labels `{kind, outcome}`. Incremented by `(o *outcomeCtx) ok(action string, ...)`
  via `obs.RestOutcomeAcceptedTotal.WithLabelValues(o.kind, action).Inc()`
  (outcome.go:674) for every other 200-class outcome (`"declined"`,
  `"submitted"`, `"discuss"`, `"implement-unverified"`, etc).
- `obs.RestOutcomeRejectedTotal`: labels `{kind, reason}`, used by
  `bad`/`conflict` for 4xx/5xx paths - wrong shape for a 2xx no-op.

Chosen fix: `terminalNoop` now calls
`obs.RestOutcomeAcceptedTotal.WithLabelValues(o.kind, "mr-terminal-noop").Inc()`
directly (matches the `ok()` label signature `{kind, outcome}` exactly;
`"mr-terminal-noop"` follows the existing hyphenated action-name convention,
e.g. `"implement-unverified"`). No new metric was needed - the existing
Accepted counter fits the 2xx-response-class semantics precisely.

### Test extended

`TestOutcome_Review_MergedMR_NoOpNot400` (`internal/restapi/outcome_test.go`)
now snapshots `obs.RestOutcomeAcceptedTotal.WithLabelValues("review",
"mr-terminal-noop")` before the call and asserts it incremented by exactly 1
after, matching the before/after delta pattern already used in this repo for
global package-level counters (see `internal/restapi/takeover_test.go`'s
`RestTakeoverErrorTotal`/`OwnershipFlipCounter` assertions - these vars are
process-global, not per-test-registry-scoped, so absolute-value assertions
would be order-dependent).

Covering test run:
```
$ mise exec -- go test ./internal/restapi/... -run \
  'TestOutcome_Review_MergedMR_NoOpNot400|TestOutcome_Review_ClosedMR_NoOpNot400|TestOutcome_Review_OpenMR_NotNoOp' -v
=== RUN   TestOutcome_Review_MergedMR_NoOpNot400
--- PASS: TestOutcome_Review_MergedMR_NoOpNot400 (0.04s)
=== RUN   TestOutcome_Review_ClosedMR_NoOpNot400
--- PASS: TestOutcome_Review_ClosedMR_NoOpNot400 (0.00s)
=== RUN   TestOutcome_Review_OpenMR_NotNoOp
--- PASS: TestOutcome_Review_OpenMR_NotNoOp (0.00s)
PASS
ok  	github.com/szymonrychu/tatara-operator/internal/restapi	0.742s
```

## Full verification

```
$ mise run build   -> builds clean
$ mise run test    -> make test (go test ./... -race -count=1, envtest-backed)
  all packages ok, including internal/controller (41.0s) and internal/restapi (6.965s)
$ mise run lint    -> golangci-lint run ./... -> 0 issues.
```

## Commit

Single commit, both fixes:
`fix(review-terminal): count mr-terminal outcome no-op + correct finalize comments`

Files touched: `internal/controller/task_stage.go` (comments only),
`internal/restapi/outcome.go` (+1 metric increment line),
`internal/restapi/outcome_test.go` (extended one covering test).
