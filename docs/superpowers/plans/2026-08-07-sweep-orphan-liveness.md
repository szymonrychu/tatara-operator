# Sweep Orphan Liveness Implementation Plan (issue #521, MR1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the sweep silently skipping open issues whose owning Task was reaped, and make
every skip it does take individually legible in logs, counters and a deadman gauge.

**Architecture:** `IsOrphanIssue` clause (b) stops reading owner refs directly and instead takes a
resolved LIVE owner name; a new `Minter.resolveLiveOwner` Gets the named Task, and on NotFound
DROPS the tombstone ref (a write) so the same pass mints. The predicate returns a named reason
instead of a bare bool, the reason joins the existing closed `SweepSkip*` vocabulary, and every
skip is logged at INFO and counted. Separately, the three mint entry points swap a bare `created
bool` for a closed `MintOutcome` enum so a caller can never again log a re-mint that did not
happen.

**Tech Stack:** Go 1.26.3 (pinned in `.mise.toml`), controller-runtime, prometheus/client_golang,
`log/slog` via `sigs.k8s.io/controller-runtime/pkg/log` (logr), envtest 1.33.0.

## Global Constraints

- **Repo:** `/Users/szymonri/Documents/tatara-new/code/tatara-operator`. Work in a worktree off
  `main` (`superpowers:using-git-worktrees`). Never build or deploy from a worktree.
- **Toolchain:** every tool call goes through mise. `mise install` once, then
  `mise exec -- go ...`, `mise run test`, `mise run lint`. Never a bare `go`.
- **KISS (hard rule 2):** three similar lines beat a premature abstraction. Do not invent a
  generic "skip framework"; the two helpers named in this plan (`skipIssue`, `strandOrphan`) are
  the entire abstraction budget.
- **Never introduce tech debt (hard rule 4):** no "phase 2", no TODOs. If something must be
  complex, it gets a dated line in `MEMORY.md` (Task 8).
- **Observability is mandatory (hard rule 12/13):** every business action logs at INFO with
  structured fields (`action`, `resource_id`, plus context); anything that counts, times out or
  can fail gets a Prometheus series.
- **Writing rules:** no em dashes, no smart quotes, no arrows, no decorative Unicode. Plain
  hyphens and straight quotes, in code comments as well as prose.
- **No `api/` changes.** See "Regenerated files" below.
- **Semver:** `semver:minor`. See "Semver" below.
- **Scope fence:** this MR does NOT touch the state machine. See "Collides with MR6" below.

---

## Verified starting state (checked 2026-08-07 against `main` @ `ad420b5`)

The line numbers in the design doc were re-verified before planning. Corrections, so nobody
chases a stale pointer:

| Design doc says | Actually on `main` |
|---|---|
| `IsOrphanIssue` at `sweep.go:139-149` | correct, `sweep.go:139-149` |
| `SweepSkipMRClaimed` at `sweep.go:95-98` | correct, `sweep.go:95-98` |
| `obs.SweepSkippedTotal` incremented at `sweep.go:855` | actually `sweep.go:957` (in `sweepPRs`, `PRClaimed` arm) |
| pass-complete log at `sweep.go:790-794` | correct, `sweep.go:790-794` |
| `Unpark`/`UnparkDetailed` precedent at `stage.go:1265-1275` | correct: `Unpark` at `:1265`, `UnparkDetailed` at `:1275`, decline vocabulary at `:1224-1235` |
| `resume.go:164` logs a re-mint that never happened | correct, `resume.go:160-166` |
| `ownerTaskRequests` at `stage_controller.go:85-93` | actually `stage_controller.go:87-96` |

Additional facts this plan depends on, all verified:

- `Task` carries **no finalizers** anywhere in the repo, so a deleted tombstone Task frees its
  name immediately. Its owned Issue/MergeRequest mirrors cascade in the background.
- `internal/controller` boots a **single envtest control plane in `TestMain`**
  (`suite_test.go:44`), so *every* test in that package pays the control-plane boot whether or not
  it touches `k8sClient`. "envtest" below therefore means "this test drives the real API server
  via `k8sClient`"; "fake client" means `fake.NewClientBuilder()`, no API server interaction, but
  still inside the envtest-booting package.
- The repo has **no log-assertion idiom at all** (no `funcr`, `testr`, `zaptest` or zap observer
  in any `_test.go`). Do NOT introduce one. Log lines are verified by extracting pure helpers and
  unit-testing those, plus the post-merge Loki queries in "Verification".
- There is **no `.golangci.yml`**, so `golangci-lint` runs its default set
  (errcheck/gosimple/govet/ineffassign/staticcheck/unused). No `exhaustive` linter: a `switch` on
  `MintOutcome` does not have to enumerate every member, but this plan enumerates them anyway for
  legibility.
- `obs.SweepSkippedTotal` already exists with labels `{project, activity, reason}`
  (`obs/sweep_metrics.go:101-104`) and its closed vocabulary lives in `obs.sweepSkipReasons`
  (`:108`), seeded per project by `SeedSweepErrorsForProject` (`:169-170`).
- `TestSweepSeedReasonsCoverEveryFailSite` (`obs/sweep_metrics_test.go`) scans `fail("...")` out
  of `sweep.go` source and fails BOTH ways. **Any new `fail(reason, ...)` call site in this MR
  must be added to `obs.sweepSeedReasons` in the same commit or that test goes red.** This plan
  adds exactly one: `resolve_live_owner`.

## The bug, in one paragraph (for the MR description)

`IsOrphanIssue` clause (b) calls `own.ControllerOwner(cr)`, which returns `owned=true` for any
ownerRef carrying `controller=true` and never checks that the named Task still exists. An Issue CR
whose owning Task was reaped keeps a dangling controller ownerRef forever, so the sweep treats it
as owned and skips it silently on every pass, forever. Live victims in `tatara/tatara-operator`:
`iss-tatara-operator-510`, `512`, `520`, `523`, `524`, each naming a Task that `kubectl get`
reports NotFound.

## The open question this MR RESOLVES but does NOT answer

Issues `502`, `503`, `505`, `521`, `525` carry **zero** ownerRefs, so clause (b) passes for them.
The reporter allowlist was ruled out (`botLogin` `szymonrychu-bot` covers the four bot-authored
ones, `szymonrychu` is a maintainer, no repo-level overrides) and the budget was ruled out
(`operator_sweep_mint_cap_hit_total` has no series at all). **Their skip branch is not determined.**
MR1's new per-issue skip logging (`action=sweep_skip_issue`) is what will state it outright on the
first pass after rollout. **The MR description must NOT assert a cause for these five.** Write:
"the branch these five take is not determinable from outside the operator; the new per-issue skip
log resolves it on the first pass after rollout."

## Collides with MR6 (state-machine redesign) - read before touching anything

This MR must NOT contain: the 8-state model, `status.parkReason`, any `TaskDone` change, the
`clarify` kind fold, or the deletion of `internal/controller/resume.go`. Those are MR6.

Two places where this MR touches code MR6 rewrites or deletes. Both are called out again at their
task:

1. **`internal/controller/resume.go` is DELETED in MR6.** It still exists now and must compile and
   behave correctly here. Task 6 changes exactly one call site in it (`resume.go:161`) plus the
   log line at `:164`. **Keep that diff to the loop body. Do not change `resumeOne`'s signature,
   do not change `resumeNoReentryParks`'s or `resumeNoReentryParksPaced`'s signatures, and do not
   add a requeue-plumbing path through resume.go** - every line of that is thrown away in MR6, and
   the sweep's own 30s tombstone requeue (Task 6) already covers the re-drive.
2. **`taskStillPushes` / `TaskDone` / `StageParked`.** Task 6 changes `createTaskRaceSafe`'s return
   TYPE but not its `existing.DeletionTimestamp != nil || tatarav1alpha1.TaskDone(existing)`
   predicate. MR6 changes what `TaskDone` MEANS (a parked Task stops being terminal). Leave the
   predicate byte-identical so MR6's diff on it is clean and reviewable.

## Regenerated files

**This MR does not touch `api/`.** No CRD field is added, removed or re-typed. Therefore
`make generate` produces no change to `api/v1alpha1/zz_generated.deepcopy.go` and `make manifests`
produces no change to `charts/tatara-operator/crd-bases/tatara.dev_*.yaml`. Task 8 runs both
anyway and asserts `git status --porcelain` is empty afterwards - if either writes a file, the
change escaped its scope fence and must be understood before merging, not committed blind.

RBAC: no new API group/verb. The operator already has `get;list;watch;update;patch` on
`tatara.dev/issues` (the sweep already Updates Issue CRs via `ownIssueForTask`) and `get;list;
watch` on `tasks`. The `rbac-drift` pre-commit hook (`make rbac-check`) fires on any
`internal/controller/*.go` change and will confirm this; it must stay green with no edit to
`charts/tatara-operator/templates/rbac.yaml`.

## Semver

**`semver:minor`.** Intent is a bugfix, but the MR ships NEW observable interface that
`tatara-observability` will alert against and dashboards will read:

- new metric `operator_sweep_orphan_stranded_seconds{project,repo,number}`
- new metric `operator_sweep_stale_owner_repaired_total{project,activity}`
- new metric `operator_intake_mint_outcome_total{kind,outcome}`
- six new values on the existing `operator_sweep_skipped_total{reason}` closed set
- a new log action `sweep_skip_issue`, and `repos` added to `sweep_pass`

Additive, backward compatible, no removals: that is minor. Per hard rule 7 the implementer owns
the level and a reviewer may raise it, never lower it. Set the literal `semver:minor` PR label (or
`change_significance: minor` on `change_summary`) BEFORE merge - CI cuts the tag from the label at
the merge commit, so a merge that lands before the label is a release that never gets tagged.

## File structure

**Created:**

| File | Responsibility |
|---|---|
| `internal/controller/sweep_orphan_liveness_test.go` | Every new sweep-side test: `resolveLiveOwner`, the end-to-end reaped-owner regression, skip counters, the stranded gauge, `repoNames`. |

**Modified:**

| File | Change |
|---|---|
| `internal/own/own.go` | New pure `DropOwner(obj, taskName) bool`. Package stays memory-only. |
| `internal/own/own_test.go` | `TestDropOwner` table. |
| `internal/controller/sweep.go` | Skip vocabulary constants; `IsOrphanIssue` -> `(bool, reason)` taking `liveOwner`; `Minter.resolveLiveOwner` + `Minter.dropStaleOwner`; `skipIssue`/`strandOrphan` helpers; `repoNames`; `repos=` on the pass log; `sweepIssues`/`sweepPRs`/`SweepProject` return a requeue duration; `MintOutcome` switches. |
| `internal/controller/sweep_test.go` | `TestOrphanIssuePredicate` rewritten for the new signature. |
| `internal/controller/intake.go` | `MintOutcome` type; `createTaskRaceSafe`, `MintForItem`, `MintIssueTask`, `MintReviewTask` return it; `MintOutcomeTotal` increments. |
| `internal/controller/intake_test.go`, `intake_selfheal_test.go`, `resume_deploy_test.go` | Call-site + assertion updates for `MintOutcome`. |
| `internal/controller/ensure_task.go` | `MintOutcome` switch; a tombstone outcome is an error, not a fabricated Task name. |
| `internal/controller/ownership.go` | `reMintReviewOwner` handles `MintTombstoneDeleted`. |
| `internal/controller/takeover_mint.go` | `MintOutcome` switch; bound the currently unbounded self-recursion. |
| `internal/controller/resume.go` | One call site + a truthful log line. Minimal - MR6 deletes this file. |
| `internal/controller/projectscan.go` | Consume `SweepProject`'s new requeue duration. |
| `internal/webhook/server.go` (x3), `internal/webhook/mirror_refresh.go` (x1) | `created bool` -> `outcome == MintCreated`; `MintTombstoneDeleted` becomes a 500 so the forge redelivers. |
| `internal/webhook/primary_mint_test.go` | Call-site update. |
| `internal/obs/sweep_metrics.go` | Vocabulary list; `resolve_live_owner` seed reason; `SweepStaleOwnerRepairedTotal`; `SweepOrphanStrandedSeconds` + `ClearSweepOrphanStranded`; `MintOutcomeTotal`. |
| `internal/obs/sweep_metrics_test.go` | Label tests, updated seed counts, source-scan sync test. |
| `MEMORY.md` | One dated line (Task 8). |

---

## Task 1: `own.DropOwner`

**Files:**
- Modify: `internal/own/own.go` (append after `OldestSurvivingOwner`, before `RepairZeroController`)
- Test: `internal/own/own_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `func DropOwner(obj client.Object, task string) bool` - removes every Task-kind owner
  ref naming `task` from `obj` IN MEMORY; returns true iff anything was removed. The caller owns
  the `Update`.

**Why it lives in `internal/own` and why it takes no `ctx`:** the package doc says every function
except `RepairZeroController` mutates in memory and returns, and that the caller owns the Update.
A `DropOwner` that did its own Get/Update would be the second exception and would drag a
`client.Client` into a package whose whole value is being dumb. The API Get that decides
*liveness* stays in `sweep.go` (Task 3).

- [ ] **Step 1: Write the failing test**

Append to `internal/own/own_test.go`:

```go
// TestDropOwner pins the tombstone-ref half of B.2. An ownerRef naming a Task
// the API server no longer has is not ownership, it is a dangling string, and
// #521 is what happens when it is merely IGNORED instead of removed: the same
// ref misroutes ownerTaskRequests (stage_controller.go), the reaper cascade and
// ourMR, so the sweep must DROP it, not read past it.
func TestDropOwner(t *testing.T) {
	ref := func(name string, controller bool) metav1.OwnerReference {
		r := metav1.OwnerReference{
			APIVersion: tataradevv1alpha1.GroupVersion.String(),
			Kind:       "Task",
			Name:       name,
			UID:        types.UID("u-" + name),
		}
		if controller {
			c := true
			r.Controller = &c
		}
		return r
	}
	projRef := metav1.OwnerReference{
		APIVersion: tataradevv1alpha1.GroupVersion.String(),
		Kind:       "Project",
		Name:       "proj",
		UID:        types.UID("u-proj"),
	}

	tests := map[string]struct {
		refs    []metav1.OwnerReference
		drop    string
		want    bool
		wantLen int
	}{
		"drops the named controller ref": {
			refs: []metav1.OwnerReference{ref("gone", true)}, drop: "gone", want: true, wantLen: 0,
		},
		"drops the named plain ref": {
			refs: []metav1.OwnerReference{ref("gone", false)}, drop: "gone", want: true, wantLen: 0,
		},
		"keeps every other Task ref": {
			refs: []metav1.OwnerReference{ref("gone", true), ref("alive", false)},
			drop: "gone", want: true, wantLen: 1,
		},
		"keeps non-Task refs": {
			refs: []metav1.OwnerReference{ref("gone", true), projRef},
			drop: "gone", want: true, wantLen: 1,
		},
		"absent name changes nothing": {
			refs: []metav1.OwnerReference{ref("alive", true)}, drop: "gone", want: false, wantLen: 1,
		},
		"no refs at all changes nothing": {
			refs: nil, drop: "gone", want: false, wantLen: 0,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			obj := &tataradevv1alpha1.Issue{
				ObjectMeta: metav1.ObjectMeta{Name: "iss-x-1", Namespace: "tatara", OwnerReferences: tc.refs},
			}
			if got := DropOwner(obj, tc.drop); got != tc.want {
				t.Fatalf("DropOwner = %v, want %v", got, tc.want)
			}
			if n := len(obj.GetOwnerReferences()); n != tc.wantLen {
				t.Fatalf("remaining owner refs = %d, want %d", n, tc.wantLen)
			}
			for _, r := range obj.GetOwnerReferences() {
				if r.Kind == "Task" && r.Name == tc.drop {
					t.Fatalf("DropOwner left the dropped ref %q behind", tc.drop)
				}
			}
		})
	}
}
```

If `own_test.go` does not already import `k8s.io/apimachinery/pkg/types`, add it.

- [ ] **Step 2: Run test to verify it fails**

Run: `mise exec -- go test ./internal/own/ -run TestDropOwner -v`
Expected: FAIL, `undefined: DropOwner`

- [ ] **Step 3: Write minimal implementation**

In `internal/own/own.go`, after `OldestSurvivingOwner`:

```go
// DropOwner removes EVERY Task-kind owner ref naming task from obj, IN MEMORY,
// and reports whether anything changed so the caller can skip its Update.
//
// It exists for the ONE case RepairZeroController does not cover, and the
// distinction is issue #521: RepairZeroController handles "no ref carries
// controller=true"; this handles "a ref names a Task that DOES NOT EXIST". A
// dangling controller ref is not ownership, it is a string, and it must be
// REMOVED rather than read past - the same ref also misroutes ownerTaskRequests
// (stage_controller.go), the reaper cascade and ourMR, so a caller that merely
// ignored it would leave three other consumers still wrong.
//
// LIVENESS IS NOT DECIDED HERE. This package is memory-only by its own package
// doc; the caller Gets the Task and decides. See Minter.resolveLiveOwner.
func DropOwner(obj client.Object, task string) bool {
	refs := obj.GetOwnerReferences()
	kept := make([]metav1.OwnerReference, 0, len(refs))
	for _, r := range refs {
		if isTaskRef(r) && r.Name == task {
			continue
		}
		kept = append(kept, r)
	}
	if len(kept) == len(refs) {
		return false
	}
	obj.SetOwnerReferences(kept)
	return true
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mise exec -- go test ./internal/own/ -run TestDropOwner -v`
Expected: PASS, all six subtests.

- [ ] **Step 5: Commit**

```bash
git add internal/own/own.go internal/own/own_test.go
git commit -m "feat(own): add DropOwner for tombstone owner refs (#521)"
```

---

## Task 2: The closed `SweepSkip` vocabulary and its sync test

**Files:**
- Modify: `internal/controller/sweep.go:95-98` (extend the existing const block)
- Modify: `internal/obs/sweep_metrics.go:108` (`sweepSkipReasons`)
- Test: `internal/obs/sweep_metrics_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: the exported constants `SweepSkipNone`, `SweepSkipIssueNotOpen`, `SweepSkipIssueOwned`,
  `SweepSkipReporterNotAllowed`, `SweepSkipMintBudget`, `SweepSkipAlreadyMinted`,
  `SweepSkipTombstoneDeleted` (all `string`), alongside the existing `SweepSkipMRClaimed`. Tasks 3,
  4 and 6 consume them.

**Value style, and a deliberate deviation from the design doc.** The design doc proposed
kebab-case values with a `skip-` prefix (`skip-owned`, `skip-budget`, ...). The existing member is
`SweepSkipMRClaimed = "mr_claimed_by_other_task"`, snake_case, no prefix, and `obs.sweepSkipReasons`
already carries that literal. The brief says to extend the existing pattern rather than invent a
new idiom, so **all values are snake_case with no prefix**. A single Prometheus label carrying two
naming conventions is exactly the drift `TestSweepSeedReasonsCoverEveryFailSite` was written to
kill. The `SweepSkip` Go prefix already says "skip"; repeating it in the value is noise.

`skip-owner-gone` from the design doc is deliberately **not** in this vocabulary. After Task 3 a
dead owner is no longer a skip: the ref is dropped and the SAME pass mints. It gets its own
counter (`operator_sweep_stale_owner_repaired_total`, Task 3), because "I repaired something" and
"I did nothing" must not share a series.

- [ ] **Step 1: Write the failing tests**

In `internal/obs/sweep_metrics_test.go`, replace `TestSeedSweepSkippedForProject`'s constant and
add the source-scan sync test:

```go
// TestSeedSweepSkippedForProject - const updated from 1 to 7.
	const wantPerProject = 7 // sweep x the closed SweepSkip* vocabulary
```

```go
// TestSweepSkipReasonsMatchSweepConstants is the skip-side twin of
// TestSweepSeedReasonsCoverEveryFailSite, and it exists for the same reason:
// sweepSkipReasons carries a "keep in sync with sweep.go's SweepSkip*
// constants" comment, and a comment cannot enforce that. An unseeded skip
// reason has NO series until its first skip, and a counter born AT its first
// skip has no earlier sample to increase from - so
// increase(operator_sweep_skipped_total{reason=...}[1h]) is blind to exactly
// the first skip after every pod roll, which is the observability hole issue
// #521 spent 19 hours inside. Fails BOTH ways: an unseeded constant, and a
// seeded reason with no constant left.
func TestSweepSkipReasonsMatchSweepConstants(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "controller", "sweep.go"))
	if err != nil {
		t.Fatalf("read sweep.go: %v", err)
	}
	seeded := map[string]bool{}
	for _, r := range sweepSkipReasons {
		seeded[r] = true
	}
	declared := map[string]bool{}
	for _, m := range regexp.MustCompile(`SweepSkip[A-Za-z]+\s*=\s*"([a-z_]+)"`).FindAllStringSubmatch(string(src), -1) {
		declared[m[1]] = true
	}
	if len(declared) == 0 {
		t.Fatal("found no SweepSkip* constants in sweep.go - the scan is broken, not the seed list")
	}
	for reason := range declared {
		if !seeded[reason] {
			t.Errorf("sweep.go declares skip reason %q but sweepSkipReasons does not seed it: "+
				"increase() cannot see its first increment", reason)
		}
	}
	for reason := range seeded {
		if !declared[reason] {
			t.Errorf("sweepSkipReasons seeds %q but no SweepSkip* constant declares it: "+
				"a permanently dead zero series", reason)
		}
	}
}
```

Note the regex only matches non-empty values, so `SweepSkipNone = ""` is correctly invisible to
both sides.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mise exec -- go test ./internal/obs/ -run 'TestSeedSweepSkippedForProject|TestSweepSkipReasonsMatchSweepConstants' -v`
Expected: `TestSeedSweepSkippedForProject` FAILs with "seeding added 1 skip series, want 7";
`TestSweepSkipReasonsMatchSweepConstants` FAILs with six "does not seed it" errors.

- [ ] **Step 3: Write minimal implementation**

In `internal/controller/sweep.go`, replace the existing `SweepSkipMRClaimed` block (`:95-98`):

```go
	// THE CLOSED SweepSkip VOCABULARY: every reason the sweep DELIBERATELY does
	// not do a piece of work. It is the {reason} label on
	// obs.SweepSkippedTotal and the `reason` field on the sweep_skip_issue /
	// sweep_skip_pr log lines, and it is kept in sync with obs.sweepSkipReasons
	// by TestSweepSkipReasonsMatchSweepConstants, which scans these constants
	// out of this file (the prose version of that instruction had already
	// drifted once for the fail() reasons - see issue #495).
	//
	// Values are snake_case with NO "skip" prefix, matching SweepSkipMRClaimed,
	// which predates the rest: one label carrying two naming conventions is the
	// drift this vocabulary exists to prevent, and the Go identifier already
	// says "skip".
	//
	// There is deliberately NO "owner gone" member. Before issue #521 a dead
	// owner WAS a silent skip - that is the whole bug. It is now a REPAIR
	// (obs.SweepStaleOwnerRepairedTotal) followed by a mint in the same pass,
	// and "I repaired something" must not share a series with "I did nothing".

	// SweepSkipNone is the empty reason: nothing was skipped.
	SweepSkipNone = ""
	// SweepSkipMRClaimed is clause 1b: an adoptable-by-shape PR whose
	// MergeRequest CR another Task already controller-owns.
	SweepSkipMRClaimed = "mr_claimed_by_other_task"
	// SweepSkipIssueNotOpen is IsOrphanIssue clause (a): the forge says the
	// issue is not open.
	SweepSkipIssueNotOpen = "issue_not_open"
	// SweepSkipIssueOwned is IsOrphanIssue clause (b): a LIVE Task
	// controller-owns the Issue CR. Since #521 "live" is load-bearing - the
	// clause used to accept a ref naming a Task that no longer existed.
	SweepSkipIssueOwned = "issue_owned"
	// SweepSkipReporterNotAllowed is IsOrphanIssue clause (c): the reporter
	// intake gate (issue #102) refused the author.
	SweepSkipReporterNotAllowed = "issue_reporter_not_allowed"
	// SweepSkipMintBudget: one of the two creation budgets bound, so this
	// orphan is deferred to the next pass. Paired with
	// obs.SweepMintCapHitTotal, which says WHICH cap bound, once per pass;
	// this says WHICH ISSUES paid for it.
	SweepSkipMintBudget = "mint_budget_bound"
	// SweepSkipAlreadyMinted: MintExistingLive. A webhook (or a concurrent
	// pass) already minted this natural key and its Task is alive.
	SweepSkipAlreadyMinted = "already_minted"
	// SweepSkipTombstoneDeleted: MintTombstoneDeleted. The deterministic name
	// was held by a dead twin, which has just been deleted; the mint is owed
	// and has NOT happened yet. The pass requeues (sweepRemintDelay) rather
	// than waiting a full sweep period.
	SweepSkipTombstoneDeleted = "tombstone_deleted"
```

In `internal/obs/sweep_metrics.go`, replace `sweepSkipReasons` (`:106-108`):

```go
// sweepSkipReasons is the closed skip-reason set. Keep in sync with sweep.go's
// SweepSkip* constants - enforced by TestSweepSkipReasonsMatchSweepConstants,
// which scans them out of the source, because the identical prose instruction
// on sweepSeedReasons had already drifted from four call sites (issue #495).
var sweepSkipReasons = []string{
	"mr_claimed_by_other_task",
	"issue_not_open",
	"issue_owned",
	"issue_reporter_not_allowed",
	"mint_budget_bound",
	"already_minted",
	"tombstone_deleted",
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mise exec -- go test ./internal/obs/ ./internal/controller/ -run 'TestSeedSweepSkippedForProject|TestSweepSkipReasonsMatchSweepConstants|TestOrphanIssuePredicate' -v`
Expected: PASS. Nothing consumes the new constants yet, but they are exported so `unused` does not
fire.

- [ ] **Step 5: Commit**

```bash
git add internal/controller/sweep.go internal/obs/sweep_metrics.go internal/obs/sweep_metrics_test.go
git commit -m "feat(sweep): close the SweepSkip reason vocabulary and pin it to obs (#521)"
```

---

## Task 3: THE FIX - `resolveLiveOwner` and `IsOrphanIssue -> (bool, reason)`

This is the outage fix. Everything else in the MR is legibility around it.

**Files:**
- Modify: `internal/controller/sweep.go:124-149` (`IsOrphanIssue`), `:834-836` (the sweep call
  site), and a new `Minter.resolveLiveOwner` / `Minter.dropStaleOwner` pair placed immediately
  after `IsOrphanIssue`
- Modify: `internal/controller/intake.go:135-141` (`MintForItem`'s issue branch)
- Modify: `internal/obs/sweep_metrics.go` (new `SweepStaleOwnerRepairedTotal`; add
  `resolve_live_owner` to `sweepSeedReasons`)
- Test: `internal/controller/sweep_test.go` (rewrite `TestOrphanIssuePredicate`)
- Test: `internal/controller/sweep_orphan_liveness_test.go` (new file)
- Test: `internal/obs/sweep_metrics_test.go`

**Interfaces:**
- Consumes: `own.DropOwner` (Task 1); `SweepSkip*` constants (Task 2).
- Produces:
  - `func IsOrphanIssue(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, iss scm.Issue, liveOwner string) (bool, string)`
  - `func (m *Minter) resolveLiveOwner(ctx context.Context, proj *tatarav1alpha1.Project, cr *tatarav1alpha1.Issue, activity string) (string, error)`
  - `var obs.SweepStaleOwnerRepairedTotal *prometheus.CounterVec` labels `{project, activity}`

**Why the signature changes to `liveOwner string` rather than mutating `cr`.** The alternative -
have `resolveLiveOwner` strip the ref off the caller's in-memory `cr` and leave `IsOrphanIssue`
reading owner refs - "works" but leaves the ORDER as a convention a caller can forget, which is
precisely the class of defect #521 is. Taking the resolved owner as a parameter makes the order
STRUCTURAL: there is no way to call the predicate without having already answered the liveness
question. It also keeps `IsOrphanIssue` a pure function of its arguments, which is what makes the
existing table test possible.

**Why the Task Get uses the UNCACHED reader.** Dropping an ownerRef is a WRITE, and a stale
informer cache that has not yet observed a freshly created Task would report NotFound and drop a
LIVE owner's ref - stealing an issue out from under a running Task. `createTaskRaceSafe` already
establishes this exact discipline for the same reason (`intake.go:325-331`: "a stale cache that
showed a still-LIVE twin as terminal would cascade a Delete"). `m.reader()` is the uncached
`APIReader`, falling back to `Client` when none is wired (unit tests).

- [ ] **Step 1: Write the failing tests**

First, rewrite `TestOrphanIssuePredicate` in `internal/controller/sweep_test.go` (replacing the
whole existing function, `sweep_test.go:137-208`). The `owned`/`ownerless`/`plainOnly` Issue CR
fixtures above it are no longer used by this test - **delete them from this function** (check with
grep that nothing else in the file uses them before deleting; if something does, leave them).

```go
// TestOrphanIssuePredicate pins the THREE clauses of B.4's ONE orphan
// predicate, and the REASON each returns. Clause (c) is the reporter intake
// gate (issue #102): v3 deleted it by omission, and its entire purpose is that
// an INJECTED issue never becomes a Task.
//
// Clause (b) now takes a RESOLVED LIVE owner rather than reading owner refs
// itself (issue #521). The old form called own.ControllerOwner, which returns
// owned=true for any ref carrying controller=true and never checks the named
// Task exists - so an Issue CR whose owning Task was reaped kept a dangling
// ref forever and was skipped silently on every pass. Taking the live owner as
// a PARAMETER makes "resolve liveness first" structural instead of a
// convention a caller can forget. Minter.resolveLiveOwner is what fills it in;
// TestResolveLiveOwner* pins that half.
func TestOrphanIssuePredicate(t *testing.T) {
	proj := sweepProject("orphan-proj")
	repo := sweepRepo("orphan-proj")

	gated := sweepProject("orphan-proj")
	gated.Spec.Scm.ReporterLogins = []string{"alice"}

	tests := map[string]struct {
		proj       *tatarav1alpha1.Project
		iss        scm.Issue
		liveOwner  string
		want       bool
		wantReason string
	}{
		"open, no owner, open allowlist": {
			proj: proj, iss: scm.Issue{Number: 1, State: "open", Author: "carol"},
			want: true, wantReason: SweepSkipNone,
		},
		"clause a: closed on the forge": {
			proj: proj, iss: scm.Issue{Number: 1, State: "closed", Author: "carol"},
			want: false, wantReason: SweepSkipIssueNotOpen,
		},
		"clause b: a LIVE controller owner": {
			proj: proj, iss: scm.Issue{Number: 1, State: "open", Author: "carol"},
			liveOwner: "owner-task", want: false, wantReason: SweepSkipIssueOwned,
		},
		"clause b: no live owner IS an orphan (the #521 case: the ref named a reaped Task)": {
			proj: proj, iss: scm.Issue{Number: 1, State: "open", Author: "carol"},
			liveOwner: "", want: true, wantReason: SweepSkipNone,
		},
		"clause a beats clause b: a closed issue with a live owner still reports not-open": {
			proj: proj, iss: scm.Issue{Number: 1, State: "closed", Author: "carol"},
			liveOwner: "owner-task", want: false, wantReason: SweepSkipIssueNotOpen,
		},
		"clause c: a non-allowlisted author mints NOTHING": {
			proj: gated, iss: scm.Issue{Number: 1, State: "open", Author: "mallory"},
			want: false, wantReason: SweepSkipReporterNotAllowed,
		},
		"clause c: an allowlisted author passes": {
			proj: gated, iss: scm.Issue{Number: 1, State: "open", Author: "alice"},
			want: true, wantReason: SweepSkipNone,
		},
		"clause c: an empty author fails CLOSED under an active gate": {
			proj: gated, iss: scm.Issue{Number: 1, State: "open", Author: ""},
			want: false, wantReason: SweepSkipReporterNotAllowed,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, reason := IsOrphanIssue(tc.proj, repo, tc.iss, tc.liveOwner)
			if got != tc.want {
				t.Fatalf("IsOrphanIssue orphan = %v, want %v", got, tc.want)
			}
			if reason != tc.wantReason {
				t.Fatalf("IsOrphanIssue reason = %q, want %q", reason, tc.wantReason)
			}
			if got && reason != SweepSkipNone {
				t.Fatalf("an orphan must carry no skip reason, got %q", reason)
			}
			if !got && reason == SweepSkipNone {
				t.Fatal("a non-orphan must NAME the clause that refused it")
			}
		})
	}
}
```

Now create `internal/controller/sweep_orphan_liveness_test.go`. All tests in this file use the
FAKE client (`fake.NewClientBuilder`), not `k8sClient` - no envtest interaction, though the
package's `TestMain` still boots the control plane for the package as a whole.

```go
package controller

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// issueOwnedBy builds a mirror Issue CR carrying a controller ownerRef to
// taskName - the exact shape the live victims of #521 carry
// (iss-tatara-operator-510/512/520/523/524, each naming a Task that
// `kubectl get` reports NotFound).
func issueOwnedBy(repo string, number int, taskName string) *tatarav1alpha1.Issue {
	ctrl := true
	return &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.IssueName(repo, number),
			Namespace: testNS,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: tatarav1alpha1.GroupVersion.String(),
				Kind:       "Task",
				Name:       taskName,
				UID:        types.UID("u-" + taskName),
				Controller: &ctrl,
			}},
		},
	}
}

func liveTask(name string) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, UID: types.UID("u-" + name)},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "orphan-proj"},
	}
}

func minterFor(c client.Client) *Minter {
	return &Minter{Client: c, APIReader: c, Scheme: c.Scheme()}
}

func getIssue(t *testing.T, c client.Client, name string) *tatarav1alpha1.Issue {
	t.Helper()
	var iss tatarav1alpha1.Issue
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &iss); err != nil {
		t.Fatalf("get issue %s: %v", name, err)
	}
	return &iss
}

// TestResolveLiveOwnerNilCR: no mirror yet is not ownership.
func TestResolveLiveOwnerNilCR(t *testing.T) {
	proj := sweepProject("orphan-proj")
	c := fake.NewClientBuilder().WithScheme(testScheme()).Build()
	got, err := minterFor(c).resolveLiveOwner(context.Background(), proj, nil, SweepActivity)
	if err != nil {
		t.Fatalf("resolveLiveOwner: %v", err)
	}
	if got != "" {
		t.Fatalf("resolveLiveOwner = %q, want \"\"", got)
	}
}

// TestResolveLiveOwnerNoControllerRef: fix H13 has a failed Task RELEASE its
// controller ownership, and per B.1 a zero-owner object is never collected. A
// plain-owner-only CR has no controller owner and is an orphan.
func TestResolveLiveOwnerNoControllerRef(t *testing.T) {
	proj := sweepProject("orphan-proj")
	iss := issueOwnedBy("tatara-operator", 1, "plain-task")
	iss.OwnerReferences[0].Controller = nil
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(iss).Build()

	got, err := minterFor(c).resolveLiveOwner(context.Background(), proj, iss, SweepActivity)
	if err != nil {
		t.Fatalf("resolveLiveOwner: %v", err)
	}
	if got != "" {
		t.Fatalf("resolveLiveOwner = %q, want \"\" (no controller ref)", got)
	}
	if n := len(getIssue(t, c, iss.Name).OwnerReferences); n != 1 {
		t.Fatalf("a plain owner ref must NOT be dropped, %d refs remain, want 1", n)
	}
}

// TestResolveLiveOwnerLiveTask: the ordinary owned case. The ref is returned
// and NOTHING is written - stealing an issue from a running Task is the one
// outcome strictly worse than #521.
func TestResolveLiveOwnerLiveTask(t *testing.T) {
	proj := sweepProject("orphan-proj")
	iss := issueOwnedBy("tatara-operator", 1, "owner-task")
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithObjects(iss, liveTask("owner-task")).Build()

	got, err := minterFor(c).resolveLiveOwner(context.Background(), proj, iss, SweepActivity)
	if err != nil {
		t.Fatalf("resolveLiveOwner: %v", err)
	}
	if got != "owner-task" {
		t.Fatalf("resolveLiveOwner = %q, want \"owner-task\"", got)
	}
	if _, owned := own.ControllerOwner(getIssue(t, c, iss.Name)); !owned {
		t.Fatal("a LIVE owner's controller ref must survive untouched")
	}
}

// TestResolveLiveOwnerDeadTaskDropsRef IS issue #521. An Issue CR whose owning
// Task was reaped keeps a dangling controller ownerRef forever; the old
// predicate read owned=true off it and skipped the issue silently on every
// pass, for 19 hours across five issues. The ref must be DROPPED (a write, not
// an in-memory ignore: the same ref misroutes ownerTaskRequests, the reaper
// cascade and ourMR), counted, and the caller told there is no live owner.
func TestResolveLiveOwnerDeadTaskDropsRef(t *testing.T) {
	proj := sweepProject("orphan-proj")
	iss := issueOwnedBy("tatara-operator", 510, "reaped-task")
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(iss).Build()

	before := testutil.ToFloat64(obs.SweepStaleOwnerRepairedTotal.WithLabelValues(proj.Name, SweepActivity))
	got, err := minterFor(c).resolveLiveOwner(context.Background(), proj, iss, SweepActivity)
	if err != nil {
		t.Fatalf("resolveLiveOwner: %v", err)
	}
	if got != "" {
		t.Fatalf("resolveLiveOwner = %q, want \"\" (the named Task does not exist)", got)
	}
	stored := getIssue(t, c, iss.Name)
	if len(stored.OwnerReferences) != 0 {
		t.Fatalf("the tombstone ref must be DROPPED IN ETCD, %d refs remain", len(stored.OwnerReferences))
	}
	after := testutil.ToFloat64(obs.SweepStaleOwnerRepairedTotal.WithLabelValues(proj.Name, SweepActivity))
	if after-before != 1 {
		t.Fatalf("SweepStaleOwnerRepairedTotal delta = %v, want 1", after-before)
	}
}

// TestResolveLiveOwnerDeadTaskKeepsOtherOwners: the drop is surgical. A plain
// ref to a DIFFERENT, still-live Task holds the GC open (B.1) and must survive.
func TestResolveLiveOwnerDeadTaskKeepsOtherOwners(t *testing.T) {
	proj := sweepProject("orphan-proj")
	iss := issueOwnedBy("tatara-operator", 512, "reaped-task")
	iss.OwnerReferences = append(iss.OwnerReferences, metav1.OwnerReference{
		APIVersion: tatarav1alpha1.GroupVersion.String(), Kind: "Task",
		Name: "other-task", UID: types.UID("u-other-task"),
	})
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithObjects(iss, liveTask("other-task")).Build()

	if _, err := minterFor(c).resolveLiveOwner(context.Background(), proj, iss, SweepActivity); err != nil {
		t.Fatalf("resolveLiveOwner: %v", err)
	}
	stored := getIssue(t, c, iss.Name)
	if len(stored.OwnerReferences) != 1 || stored.OwnerReferences[0].Name != "other-task" {
		t.Fatalf("owner refs after drop = %+v, want exactly the live plain owner", stored.OwnerReferences)
	}
}

// TestResolveLiveOwnerIsIdempotent: the sweep runs hourly forever. A second
// pass over an already-repaired CR must be a no-op, and must NOT re-count -
// a repair counter that ticks on every pass cannot distinguish "five issues
// were repaired once" from "one issue is being repaired forever".
func TestResolveLiveOwnerIsIdempotent(t *testing.T) {
	proj := sweepProject("orphan-proj")
	iss := issueOwnedBy("tatara-operator", 520, "reaped-task")
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(iss).Build()
	m := minterFor(c)

	if _, err := m.resolveLiveOwner(context.Background(), proj, iss, SweepActivity); err != nil {
		t.Fatalf("first resolveLiveOwner: %v", err)
	}
	mid := testutil.ToFloat64(obs.SweepStaleOwnerRepairedTotal.WithLabelValues(proj.Name, SweepActivity))
	if _, err := m.resolveLiveOwner(context.Background(), proj, getIssue(t, c, iss.Name), SweepActivity); err != nil {
		t.Fatalf("second resolveLiveOwner: %v", err)
	}
	after := testutil.ToFloat64(obs.SweepStaleOwnerRepairedTotal.WithLabelValues(proj.Name, SweepActivity))
	if after != mid {
		t.Fatalf("a second pass re-counted the repair: %v -> %v", mid, after)
	}
}

// TestSweepMintsIssueWhoseOwningTaskWasReaped is the END-TO-END #521
// regression, and the one test that would have caught the outage. An OPEN
// forge issue whose mirror carries a controller ref to a reaped Task must be
// minted BY THE SAME PASS that discovers the ref is a tombstone - not deferred,
// not skipped.
func TestSweepMintsIssueWhoseOwningTaskWasReaped(t *testing.T) {
	proj := sweepProject("orphan-proj")
	repo := sweepRepo("orphan-proj")
	iss := issueOwnedBy(repo.Name, 510, "reaped-task")
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithObjects(proj, repo, iss).WithStatusSubresource(&tatarav1alpha1.Task{}).Build()

	rd := &sweepReader{issues: []scm.IssueRef{{
		Number: 510, State: "open", Author: "szymonrychu", Title: "stranded",
		CreatedAt: time.Now().Add(-19 * time.Hour),
	}}}
	runSweep(t, c, proj, repo, rd)

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != 1 {
		t.Fatalf("minted %d tasks, want 1 (the reaped owner's tombstone ref must not block the mint)", len(tasks))
	}
	if len(getIssue(t, c, iss.Name).OwnerReferences) == 0 {
		t.Fatal("the mint must leave the fresh Task as controller owner, not zero owners")
	}
	if name, _ := own.ControllerOwner(getIssue(t, c, iss.Name)); name == "reaped-task" {
		t.Fatal("the tombstone ref survived the pass")
	}
}
```

`testScheme()` and `sweepReader`/`runSweep`/`sweepTasks`/`sweepProject`/`sweepRepo` already exist in
`sweep_test.go`. If the fake-client builder in `sweep_test.go` uses a differently-named scheme
helper, use that one - grep `fake.NewClientBuilder().` in `sweep_test.go:1344` and copy its exact
`WithScheme`/`WithStatusSubresource` chain rather than inventing one.

Finally, add the label test in `internal/obs/sweep_metrics_test.go`:

```go
func TestSweepStaleOwnerRepairedTotalLabels(t *testing.T) {
	SweepStaleOwnerRepairedTotal.WithLabelValues("label-test-proj", "sweep").Inc()
	assertLabelNames(t, gatheredLabelNames(t, SweepStaleOwnerRepairedTotal,
		"operator_sweep_stale_owner_repaired_total"),
		[]string{"activity", "project"})
}
```

- [ ] **Step 2: Run tests to verify they fail**

```
mise exec -- go test ./internal/obs/ -run TestSweepStaleOwnerRepairedTotalLabels -v
mise exec -- go test ./internal/controller/ -run 'TestOrphanIssuePredicate|TestResolveLiveOwner|TestSweepMintsIssueWhoseOwningTaskWasReaped' -v
```
Expected: compile failure - `undefined: obs.SweepStaleOwnerRepairedTotal`, `undefined:
resolveLiveOwner`, and `IsOrphanIssue` "assignment mismatch: 2 variables but 1 value".

- [ ] **Step 3: Write minimal implementation**

**(a)** `internal/obs/sweep_metrics.go` - add the counter, register it, seed it, and add the new
fail reason:

```go
// SweepStaleOwnerRepairedTotal counts Issue mirrors whose controller ownerRef
// named a Task the API server no longer has, and which the sweep therefore
// REPAIRED by dropping the ref (issue #521). It is deliberately NOT a
// SweepSkippedTotal reason: a repair is work done, and it is followed by a mint
// in the SAME pass, so sharing a series with "I did nothing" would hide the
// difference between the bug being fixed and the bug still running.
//
// Expected shape after the #521 rollout: a single burst (one per stranded
// issue, five in the tatara project) and then flat forever. A SUSTAINED rate is
// a reap that is not handing over ownership - go read B.5, not this counter.
var SweepStaleOwnerRepairedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_sweep_stale_owner_repaired_total",
	Help: "Issue mirrors whose controller ownerRef named a non-existent Task and was dropped, by project and activity (contract B.2/B.4, issue #521).",
}, []string{"project", "activity"})
```

Add `"resolve_live_owner"` to `sweepSeedReasons` (required by
`TestSweepSeedReasonsCoverEveryFailSite`, and bump `TestSeedSweepErrorsForProject`'s
`wantPerProject` from `17 + 1 + 2*2 + 3` to `18 + 1 + 2*2 + 3`, updating its comment from
"sweep x 17" to "sweep x 18 (+resolve_live_owner, issue #521)").

In `SeedSweepErrorsForProject`, add after the existing skip seeding:

```go
	SweepStaleOwnerRepairedTotal.WithLabelValues(project, "sweep")
```

Add `SweepStaleOwnerRepairedTotal` to the `init()` `MustRegister` list.

**(b)** `internal/controller/sweep.go` - rewrite `IsOrphanIssue` (`:124-149`):

```go
// IsOrphanIssue is THE orphan predicate. THREE clauses, all required:
//
//	a. issue.state == "open"                           (SCM truth)
//	b. no LIVE Task controller-owns the Issue CR
//	c. IsAllowedReporter(proj, repo, issue.author)     (fix C6)
//
// It returns the orphan verdict AND, when the verdict is false, the SweepSkip
// reason that produced it - the same refactor shape as stage.Unpark ->
// stage.UnparkDetailed, and for the same reason its doc gives: a guard that
// declines without recording a reason is how a high-stakes rule stalls work for
// a day with zero errors and zero logs. A caller cannot structurally skip an
// issue without naming the clause that refused it.
//
// liveOwner is the name of the Task that controller-owns the Issue CR AND STILL
// EXISTS, or "" when there is none. It is NOT read off the CR here, and issue
// #521 is why. The old form called own.ControllerOwner(cr), which returns
// owned=true for any ref carrying controller=true and NEVER checks the named
// Task exists - so an Issue CR whose owning Task was reaped kept a dangling
// controller ownerRef forever, the sweep read it as owned, and five open issues
// went 19 hours with no Task and no log line. Taking the resolved owner as a
// PARAMETER makes "resolve liveness first" structural rather than a convention
// the next caller can forget. Minter.resolveLiveOwner is the resolver, and it
// DROPS a tombstone ref rather than ignoring it.
//
// A zero-owner CR IS an orphan: fix H13 has a failed Task RELEASE its
// controller ownership and drop the ownerRef, and per B.1 an object with zero
// owners is never garbage collected - it is still there, and the mint ADOPTS
// it.
//
// Clause (c) is the reporter intake gate (issue #102, api/v1alpha1/logins.go).
// It is closed-by-default when configured (an empty allowlist preserves the open
// default; an empty LOGIN fails closed under an active gate) and its entire
// purpose is that an INJECTED issue never becomes a Task.
func IsOrphanIssue(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, iss scm.Issue, liveOwner string) (bool, string) {
	if iss.State != "open" {
		return false, SweepSkipIssueNotOpen
	}
	if liveOwner != "" {
		return false, SweepSkipIssueOwned
	}
	if !tatarav1alpha1.IsAllowedReporter(proj, repo, iss.Author) {
		return false, SweepSkipReporterNotAllowed
	}
	return true, SweepSkipNone
}

// resolveLiveOwner answers IsOrphanIssue clause (b): who controller-owns cr AND
// still exists. It returns "" when nobody does.
//
// THE TOMBSTONE BRANCH IS THE #521 FIX. A ref naming a Task the API server does
// not have is not ownership, it is a dangling string, and it is DROPPED - a
// write, not an in-memory ignore. Merely ignoring it would fix the sweep and
// leave three other consumers reading the same lie: ownerTaskRequests
// (stage_controller.go) would enqueue reconciles for a Task that does not
// exist, the reaper cascade would reason about it, and ourMR would key on it.
// Dropping it also lets THIS SAME PASS mint, because the very next call is
// IsOrphanIssue with liveOwner="".
//
// It lives HERE and not in internal/own because that package is memory-only by
// its own package doc: every function except RepairZeroController mutates an
// object in memory and returns, and the caller owns the Update. The API Get
// that decides liveness does not belong there. own.DropOwner is the pure
// mutation half.
//
// THE GET IS UNCACHED (m.reader()). A stale informer cache that has not yet
// observed a freshly created Task would report NotFound and this function would
// drop a LIVE owner's ref, stealing an issue out from under a running Task -
// strictly worse than the bug it fixes. createTaskRaceSafe already made exactly
// this call for exactly this reason (fix F3-1).
func (m *Minter) resolveLiveOwner(ctx context.Context, proj *tatarav1alpha1.Project,
	cr *tatarav1alpha1.Issue, activity string) (string, error) {

	if cr == nil {
		return "", nil
	}
	name, owned := own.ControllerOwner(cr)
	if !owned {
		return "", nil
	}
	var task tatarav1alpha1.Task
	err := m.reader().Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: name}, &task)
	if err == nil {
		return name, nil
	}
	if !apierrors.IsNotFound(err) {
		return "", fmt.Errorf("sweep: resolve owner task %q of %s: %w", name, cr.Name, err)
	}
	dropped, derr := m.dropStaleOwner(ctx, cr, name)
	if derr != nil {
		return "", derr
	}
	if dropped {
		obs.SweepStaleOwnerRepairedTotal.WithLabelValues(proj.Name, activity).Inc()
		log.FromContext(ctx).Info("sweep: dropped a controller ownerRef naming a Task that no longer exists; the issue is mintable again",
			"action", "sweep_stale_owner_repaired", "resource_id", cr.Name,
			"activity", activity, "project", proj.Name, "stale_owner", name)
	}
	return "", nil
}

// dropStaleOwner removes owner's ownerRef from the Issue CR in etcd, under
// RetryOnConflict against a FRESH copy, and mirrors the result onto cr so the
// caller's copy is not left carrying a ref that no longer exists. It reports
// whether it actually wrote, so the repair counter counts repairs and not
// passes. A concurrently reaped CR (NotFound) is not an error: there is nothing
// left to repair.
func (m *Minter) dropStaleOwner(ctx context.Context, cr *tatarav1alpha1.Issue, owner string) (bool, error) {
	key := client.ObjectKeyFromObject(cr)
	wrote := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		wrote = false
		var fresh tatarav1alpha1.Issue
		if err := m.Client.Get(ctx, key, &fresh); err != nil {
			return err
		}
		if !own.DropOwner(&fresh, owner) {
			cr.SetOwnerReferences(fresh.GetOwnerReferences())
			return nil
		}
		if err := m.Client.Update(ctx, &fresh); err != nil {
			return err
		}
		cr.SetOwnerReferences(fresh.GetOwnerReferences())
		wrote = true
		return nil
	})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("sweep: drop stale owner %q from %s: %w", owner, key.Name, err)
	}
	return wrote, nil
}
```

`sweep.go` already imports `context`, `fmt`, `apierrors`, `types`, `client`, `retry`, `log`, `obs`,
`own`. No new imports.

**(c)** `internal/controller/sweep.go:834-836` - the sweep call site. Replace:

```go
		if !IsOrphanIssue(proj, repo, ext, cr) {
			continue
		}
```

with:

```go
		liveOwner, lerr := r.minter().resolveLiveOwner(ctx, proj, cr, activity)
		if lerr != nil {
			fail("resolve_live_owner", lerr, "repo", repo.Name, "number", ref.Number)
			continue
		}
		orphan, skipReason := IsOrphanIssue(proj, repo, ext, liveOwner)
		if !orphan {
			continue
		}
		_ = skipReason // consumed by skipIssue in the next commit
```

The `_ = skipReason` line is a deliberate one-commit placeholder so this task stays independently
testable; Task 4 replaces it in the same file. If you are running Tasks 3 and 4 back to back,
write Task 4's `r.skipIssue(...)` line directly and skip the placeholder.

**(d)** `internal/controller/intake.go:135-141` - `MintForItem`'s issue branch. Replace:

```go
	cr, err := m.issueCR(ctx, proj, repo, item.Issue.Number)
	if err != nil {
		return nil, false, err
	}
	if !IsOrphanIssue(proj, repo, item.Issue, cr) {
		return nil, false, nil
	}
```

with:

```go
	cr, err := m.issueCR(ctx, proj, repo, item.Issue.Number)
	if err != nil {
		return nil, false, err
	}
	// Clause (b) resolves LIVENESS, not just presence, and repairs a tombstone
	// ref in place (issue #521). The webhook path needs this exactly as much as
	// the sweep does: a dangling controller ref made the primary mint a silent
	// no-op too, so a human opening an issue got nothing either.
	liveOwner, lerr := m.resolveLiveOwner(ctx, proj, cr, SweepActivity)
	if lerr != nil {
		return nil, false, lerr
	}
	if orphan, _ := IsOrphanIssue(proj, repo, item.Issue, liveOwner); !orphan {
		return nil, false, nil
	}
```

- [ ] **Step 4: Run tests to verify they pass**

```
mise exec -- go test ./internal/obs/ -v
mise exec -- go test ./internal/controller/ -run 'TestOrphanIssuePredicate|TestResolveLiveOwner|TestSweepMintsIssueWhoseOwningTaskWasReaped' -v
mise exec -- go build ./...
```
Expected: PASS. Then run the two whole packages to catch collateral:
`mise exec -- go test ./internal/controller/ ./internal/webhook/ ./internal/obs/ ./internal/own/`

- [ ] **Step 5: Commit**

```bash
git add internal/controller/sweep.go internal/controller/intake.go internal/controller/sweep_test.go \
        internal/controller/sweep_orphan_liveness_test.go internal/obs/sweep_metrics.go \
        internal/obs/sweep_metrics_test.go
git commit -m "fix(sweep): resolve owner LIVENESS and drop tombstone ownerRefs (#521)"
```

---

## Task 4: Per-issue skip logging and counting

**Files:**
- Modify: `internal/controller/sweep.go` (new `skipIssue` method placed next to `sweepIssues`; the
  call site written in Task 3)
- Test: `internal/controller/sweep_orphan_liveness_test.go`

**Interfaces:**
- Consumes: `SweepSkip*` (Task 2), `IsOrphanIssue`'s reason (Task 3).
- Produces: `func (r *ProjectReconciler) skipIssue(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int, activity, reason string)`. Task 6 also calls it.

**Both the log and the counter, and why.** The counter cannot say WHICH issue went unanswered -
that is exactly what cost 19 hours on #521. The log line cannot be alerted on cheaply at the rate
Prometheus can. The codebase already states this trade-off for `SweepSkippedTotal` in its own doc
comment; MR1 just applies it to the issue arm.

**Volume, because a reviewer will ask.** `sweepIssues` iterates `ListOpenIssues` only. On the live
cluster that is roughly 150 open issues across 3 projects, and the sweep runs on the `issueScan`
cadence (hourly). Worst case is therefore ~150 INFO lines per project per hour, and the steady
state is dominated by `issue_owned`. That is well inside what Loki already ingests for this
operator, and it is the exact stream that would have shown five issues being skipped for a reason
that was a lie. **Do not** demote `issue_owned` to `V(1)` to save volume: `issue_owned` being
WRONG is the entire bug.

- [ ] **Step 1: Write the failing test**

Append to `internal/controller/sweep_orphan_liveness_test.go`:

```go
// TestSweepCountsEverySkipReason pins that a skipped issue is COUNTED under the
// clause that refused it. The counter is half the answer (the log line names
// WHICH issue), and it is the half an alert can read: a skip rate that never
// returns to zero is a stuck intake, which is precisely what nobody could see
// for 19 hours on #521.
func TestSweepCountsEverySkipReason(t *testing.T) {
	tests := map[string]struct {
		setup      func(proj *tatarav1alpha1.Project) []client.Object
		issue      scm.IssueRef
		wantReason string
	}{
		"a live owner": {
			setup: func(proj *tatarav1alpha1.Project) []client.Object {
				return []client.Object{issueOwnedBy("tatara-operator", 7, "owner-task"), liveTask("owner-task")}
			},
			issue:      scm.IssueRef{Number: 7, State: "open", Author: "carol"},
			wantReason: SweepSkipIssueOwned,
		},
		"a reporter outside the allowlist": {
			setup: func(proj *tatarav1alpha1.Project) []client.Object {
				proj.Spec.Scm.ReporterLogins = []string{"alice"}
				return nil
			},
			issue:      scm.IssueRef{Number: 8, State: "open", Author: "mallory"},
			wantReason: SweepSkipReporterNotAllowed,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			proj := sweepProject("skip-proj")
			repo := sweepRepo("skip-proj")
			objs := []client.Object{proj, repo}
			objs = append(objs, tc.setup(proj)...)
			c := fake.NewClientBuilder().WithScheme(testScheme()).
				WithObjects(objs...).WithStatusSubresource(&tatarav1alpha1.Task{}).Build()

			before := testutil.ToFloat64(
				obs.SweepSkippedTotal.WithLabelValues(proj.Name, SweepActivity, tc.wantReason))
			runSweep(t, c, proj, repo, &sweepReader{issues: []scm.IssueRef{tc.issue}})
			after := testutil.ToFloat64(
				obs.SweepSkippedTotal.WithLabelValues(proj.Name, SweepActivity, tc.wantReason))

			if after-before != 1 {
				t.Fatalf("SweepSkippedTotal{reason=%q} delta = %v, want 1", tc.wantReason, after-before)
			}
			if n := len(sweepTasks(t, c, proj.Name)); n != 0 {
				t.Fatalf("a skipped issue minted %d tasks, want 0", n)
			}
		})
	}
}

// TestSweepCountsBudgetBoundSkip: maxNewTasksPerSweep=1 with two orphans means
// the second orphan is DEFERRED, and the issue that paid for the cap must be
// named. obs.SweepMintCapHitTotal already says WHICH cap bound, once per pass;
// this says WHICH ISSUES it cost.
func TestSweepCountsBudgetBoundSkip(t *testing.T) {
	proj := sweepProject("budget-proj")
	proj.Spec.MaxNewTasksPerSweep = 1
	repo := sweepRepo("budget-proj")
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithObjects(proj, repo).WithStatusSubresource(&tatarav1alpha1.Task{}).Build()

	before := testutil.ToFloat64(
		obs.SweepSkippedTotal.WithLabelValues(proj.Name, SweepActivity, SweepSkipMintBudget))
	runSweep(t, c, proj, repo, &sweepReader{issues: []scm.IssueRef{
		{Number: 11, State: "open", Author: "szymonrychu"},
		{Number: 12, State: "open", Author: "szymonrychu"},
	}})
	after := testutil.ToFloat64(
		obs.SweepSkippedTotal.WithLabelValues(proj.Name, SweepActivity, SweepSkipMintBudget))

	if after-before != 1 {
		t.Fatalf("SweepSkippedTotal{reason=mint_budget_bound} delta = %v, want 1", after-before)
	}
	if n := len(sweepTasks(t, c, proj.Name)); n != 1 {
		t.Fatalf("minted %d tasks under maxNewTasksPerSweep=1, want 1", n)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mise exec -- go test ./internal/controller/ -run 'TestSweepCountsEverySkipReason|TestSweepCountsBudgetBoundSkip' -v`
Expected: FAIL, "SweepSkippedTotal{reason=...} delta = 0, want 1" on every subtest.

- [ ] **Step 3: Write minimal implementation**

In `internal/controller/sweep.go`, add immediately above `sweepIssues`:

```go
// skipIssue records ONE deliberately-skipped issue: the counter for the alert,
// and the log line for the human. BOTH, and the reason is the one the codebase
// already gives for SweepSkippedTotal - a counter cannot say WHICH issue went
// unanswered, which is exactly the gap that let five open issues sit for 19
// hours with a green sweep heartbeat (issue #521).
//
// resource_id is the Issue MIRROR's name (iss-<repo>-<number>), not the
// project: the five live victims are named in kubectl output and in the CR's
// own metadata by that string, so it is what an operator greps for.
func (r *ProjectReconciler) skipIssue(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, activity, reason string) {

	obs.SweepSkippedTotal.WithLabelValues(proj.Name, activity, reason).Inc()
	log.FromContext(ctx).Info("sweep: issue skipped",
		"action", "sweep_skip_issue", "resource_id", tatarav1alpha1.IssueName(repo.Name, number),
		"activity", activity, "reason", reason, "project", proj.Name,
		"repo", repo.Name, "number", number)
}
```

Replace Task 3's placeholder in `sweepIssues`:

```go
		orphan, skipReason := IsOrphanIssue(proj, repo, ext, liveOwner)
		if !orphan {
			r.skipIssue(ctx, proj, repo, ref.Number, activity, skipReason)
			continue
		}
```

And the budget branch (currently `sweep.go:862-864`):

```go
		if !budget.allow(ctx, stg) {
			r.skipIssue(ctx, proj, repo, ref.Number, activity, SweepSkipMintBudget)
			continue
		}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mise exec -- go test ./internal/controller/ -run 'TestSweepCounts' -v`
Expected: PASS (3 subtests + 1 test).

- [ ] **Step 5: Commit**

```bash
git add internal/controller/sweep.go internal/controller/sweep_orphan_liveness_test.go
git commit -m "feat(sweep): log and count every per-issue skip with its reason (#521)"
```

---

## Task 5: `repos=[...]` on the pass-complete log line

**Files:**
- Modify: `internal/controller/sweep.go:790-794` and `:752-771` (the repos loop)
- Test: `internal/controller/sweep_orphan_liveness_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `func repoNames(repos []tatarav1alpha1.Repository) []string`.

**Why.** `SweepProject` is called with `dueRepos`, not all repos (`projectscan.go:1417`), so
"the sweep ran" has never meant "the sweep looked at repo X". Reconstructing which repos were in a
given pass required correlating the per-repo cadence config with wall-clock timestamps. That was a
19-hour diagnostic gap. One field closes it.

The repo has no log-assertion idiom (verified: no `funcr`, `testr` or zap observer in any test), so
the testable unit is the pure helper. Do not introduce a log sink.

- [ ] **Step 1: Write the failing test**

Append to `internal/controller/sweep_orphan_liveness_test.go`:

```go
// TestRepoNames pins the `repos` field on the sweep_pass log line.
// SweepProject is called with dueRepos, not with every repo
// (projectscan.go), so "the sweep ran" has never meant "the sweep looked at
// repo X" - reconstructing a pass's repo set from the per-repo cadence and
// wall-clock timestamps is what made #521 a 19-hour diagnosis.
func TestRepoNames(t *testing.T) {
	tests := map[string]struct {
		in   []tatarav1alpha1.Repository
		want []string
	}{
		"empty": {in: nil, want: []string{}},
		"preserves sweep order": {
			in: []tatarav1alpha1.Repository{
				{ObjectMeta: metav1.ObjectMeta{Name: "tatara-operator"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "tatara-cli"}},
			},
			want: []string{"tatara-operator", "tatara-cli"},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := repoNames(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("repoNames = %v, want %v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("repoNames = %v, want %v", got, tc.want)
				}
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mise exec -- go test ./internal/controller/ -run TestRepoNames -v`
Expected: FAIL, `undefined: repoNames`

- [ ] **Step 3: Write minimal implementation**

In `internal/controller/sweep.go`, add next to `skipIssue`:

```go
// repoNames is the `repos` field on the sweep_pass log line. SweepProject is
// handed dueRepos, NOT every repo in the project, so without this a pass-complete
// line cannot be read as "these repos were scanned" - which is a diagnostic
// gap, not a cosmetic one (issue #521 took 19 hours partly because the repo set
// of each pass had to be reconstructed from per-repo cadence config and
// timestamps). Sweep order is preserved: it is the order failures will appear in.
func repoNames(repos []tatarav1alpha1.Repository) []string {
	out := make([]string, 0, len(repos))
	for i := range repos {
		out = append(out, repos[i].Name)
	}
	return out
}
```

And in `SweepProject`, the pass-complete log (`:790-794`):

```go
	l.Info("sweep: pass complete",
		"action", "sweep_pass", "resource_id", proj.Name, "activity", activity,
		"repos", repoNames(repos),
		"minted_triaging", minted[tatarav1alpha1.StageTriaging],
		"minted_parked", minted[tatarav1alpha1.StageParked],
		"active_tasks", budget.active, "duration_ms", time.Since(now).Milliseconds())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mise exec -- go test ./internal/controller/ -run TestRepoNames -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/controller/sweep.go internal/controller/sweep_orphan_liveness_test.go
git commit -m "feat(sweep): name the repos in the pass-complete log line (#521)"
```

---

## Task 6: The `MintOutcome` enum and every caller

**Files:**
- Modify: `internal/controller/intake.go` (type + `createTaskRaceSafe` + `MintForItem` +
  `MintIssueTask` + `MintReviewTask`)
- Modify: `internal/controller/sweep.go` (`sweepIssues`, `sweepPRs`, `SweepProject`)
- Modify: `internal/controller/projectscan.go:1416-1442`
- Modify: `internal/controller/ensure_task.go:43`
- Modify: `internal/controller/ownership.go:336`
- Modify: `internal/controller/takeover_mint.go:127-141`
- Modify: `internal/controller/resume.go:160-166`
- Modify: `internal/webhook/server.go:669`, `:717`, `:810`
- Modify: `internal/webhook/mirror_refresh.go:203`
- Modify: `internal/obs/sweep_metrics.go` (`MintOutcomeTotal`)
- Test: `internal/controller/intake_test.go`, `intake_selfheal_test.go`, `resume_deploy_test.go`,
  `sweep_orphan_liveness_test.go`, `internal/webhook/primary_mint_test.go`,
  `internal/obs/sweep_metrics_test.go`

**This task is ATOMIC.** Go will not compile with a changed return type and unchanged callers, so
every site below lands in one commit. Work through them in the order listed.

**Interfaces:**
- Consumes: `SweepSkipAlreadyMinted`, `SweepSkipTombstoneDeleted` (Task 2), `skipIssue` (Task 4).
- Produces:
  - `type MintOutcome string` with `MintNotOwed`, `MintCreated`, `MintExistingLive`, `MintTombstoneDeleted`
  - `func (m *Minter) MintForItem(ctx, proj, repo, item ForgeItem, webhookOriginated bool, sp objbudget.Spiller) (*tatarav1alpha1.Task, MintOutcome, error)`
  - `func (m *Minter) MintIssueTask(ctx, proj, repo, ext scm.Issue, stg, reason string, sp objbudget.Spiller) (*tatarav1alpha1.Task, MintOutcome, error)`
  - `func (m *Minter) MintReviewTask(ctx, proj, repo, pr scm.PRRef, cr *tatarav1alpha1.MergeRequest, stg, reason string, sp objbudget.Spiller, expectFrom ...string) (*tatarav1alpha1.Task, MintOutcome, error)`
  - `func (m *Minter) createTaskRaceSafe(ctx, task) (MintOutcome, *tatarav1alpha1.Task, error)`
  - `func (r *ProjectReconciler) SweepProject(...) (time.Duration, error)` - the duration is a
    requeue-after, `0` for none
  - `var obs.MintOutcomeTotal *prometheus.CounterVec` labels `{kind, outcome}`
  - `const sweepRemintDelay = 30 * time.Second`

**The defect this closes.** `resume.go:160-166` calls `MintForItem`, discards its `created` bool,
and logs "re-minted the issue fresh" unconditionally. A bare bool cannot distinguish "nothing was
owed" from "I DESTROYED a Task holding this name, re-drive me". Four distinct states were being
squeezed into one bit.

**Where the immediate requeue lands, per caller.** `MintTombstoneDeleted` means work is OWED and
has not happened. Each caller re-drives on the mechanism it already has:

| Caller | On `MintTombstoneDeleted` |
|---|---|
| `sweepIssues` / `sweepPRs` | count `tombstone_deleted`, log, and return `sweepRemintDelay` up to `SweepProject`, which returns it to `projectscan.go`'s existing `consider(...)` requeue machinery. 30s, not a full sweep period. |
| webhook `handleIssueOpened` / `handleMROpened` / `handleIssueComment` | HTTP 500. The forge redelivers within seconds. This is already the file's stated policy for a mint error ("a mint error is a 5xx so GitHub redelivers, rather than a silent 202 that waits for the next sweep"), and a tombstone delete is materially a failed mint. |
| `mirror_refresh.go` (trigger-label path) | log at INFO and return. It is a best-effort backstop with no response to fail; the sweep's 30s requeue covers it. |
| `EnsureTaskForMRComment` | return an error. Its contract's `("", false, nil)` means "accepted-ignored", and returning a Task name for a Task that does not exist is exactly the lie this enum exists to make impossible. |
| `reMintReviewOwner` | return an error, so the MergeRequest reconciler requeues. |
| `MintOrUnparkTakeoverTask` | already retries once against the freed name. Keep that behaviour, BOUND the recursion (see below). |
| `resumeOne` | log truthfully at INFO and return nil. **No signature change - MR6 deletes this file.** The issue is now severed and ownerless, so it is an orphan the next sweep pass mints. |

**Why `sweepRemintDelay = 30s` and not 0.** The deleted tombstone Task's owned Issue/MergeRequest
mirrors cascade in the BACKGROUND (`DeletePropagationBackground`). Re-minting instantly can bind a
fresh Task to a mirror the GC is still collecting. 30s is comfortably past that and 120x better
than waiting a full hourly pass. It is a constant next to `sweepGoalLimit`, not a tunable.

**Boy-scout, hard rule 3.** `MintOrUnparkTakeoverTask` currently recurses into itself with NO
bound on the tombstone path (`takeover_mint.go:141`). Two mints racing the same dead name could
recurse indefinitely. The enum makes the branch legible, so bound it here.

- [ ] **Step 1: Write the failing tests**

`internal/obs/sweep_metrics_test.go`:

```go
func TestMintOutcomeTotalLabels(t *testing.T) {
	MintOutcomeTotal.WithLabelValues("clarify", "created").Inc()
	assertLabelNames(t, gatheredLabelNames(t, MintOutcomeTotal,
		"operator_intake_mint_outcome_total"),
		[]string{"kind", "outcome"})
}
```

Append to `internal/controller/sweep_orphan_liveness_test.go`:

```go
// TestMintOutcomeDistinguishesNothingOwedFromDestroyed is the defect this enum
// closes. A bare `created bool` collapsed four states into one bit, which is
// how resume.go logged "re-minted the issue fresh" for a mint that never
// happened. Each state must be individually nameable.
func TestMintOutcomeDistinguishesNothingOwedFromDestroyed(t *testing.T) {
	proj := sweepProject("outcome-proj")
	repo := sweepRepo("outcome-proj")
	ext := scm.Issue{Number: 30, State: "open", Author: "szymonrychu", Title: "t", URL: "https://example.invalid/30"}

	t.Run("a fresh name is MintCreated", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(testScheme()).
			WithObjects(proj, repo).WithStatusSubresource(&tatarav1alpha1.Task{}).Build()
		_, outcome, err := minterFor(c).MintIssueTask(context.Background(), proj, repo, ext,
			tatarav1alpha1.StageTriaging, "", nil)
		if err != nil {
			t.Fatalf("MintIssueTask: %v", err)
		}
		if outcome != MintCreated {
			t.Fatalf("outcome = %q, want %q", outcome, MintCreated)
		}
	})

	t.Run("a LIVE twin is MintExistingLive, not MintCreated", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(testScheme()).
			WithObjects(proj, repo).WithStatusSubresource(&tatarav1alpha1.Task{}).Build()
		m := minterFor(c)
		if _, _, err := m.MintIssueTask(context.Background(), proj, repo, ext,
			tatarav1alpha1.StageTriaging, "", nil); err != nil {
			t.Fatalf("first MintIssueTask: %v", err)
		}
		_, outcome, err := m.MintIssueTask(context.Background(), proj, repo, ext,
			tatarav1alpha1.StageTriaging, "", nil)
		if err != nil {
			t.Fatalf("second MintIssueTask: %v", err)
		}
		if outcome != MintExistingLive {
			t.Fatalf("outcome = %q, want %q", outcome, MintExistingLive)
		}
	})

	t.Run("a TERMINAL twin is MintTombstoneDeleted and the name is freed", func(t *testing.T) {
		name := tatarav1alpha1.IntakeTaskName(proj.Name, SweepIssueKind, repo.Name, ext.Number)
		dead := &tatarav1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
			Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: SweepIssueKind},
			Status:     tatarav1alpha1.TaskStatus{Stage: tatarav1alpha1.StageDelivered},
		}
		c := fake.NewClientBuilder().WithScheme(testScheme()).
			WithObjects(proj, repo, dead).WithStatusSubresource(&tatarav1alpha1.Task{}).Build()

		_, outcome, err := minterFor(c).MintIssueTask(context.Background(), proj, repo, ext,
			tatarav1alpha1.StageTriaging, "", nil)
		if err != nil {
			t.Fatalf("MintIssueTask: %v", err)
		}
		if outcome != MintTombstoneDeleted {
			t.Fatalf("outcome = %q, want %q", outcome, MintTombstoneDeleted)
		}
		var gone tatarav1alpha1.Task
		gerr := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &gone)
		if gerr == nil {
			t.Fatal("the terminal twin was not deleted, so the name is still blocked")
		}
	})

	t.Run("a non-orphan issue is MintNotOwed", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(testScheme()).
			WithObjects(proj, repo, issueOwnedBy(repo.Name, 31, "owner-task"), liveTask("owner-task")).
			WithStatusSubresource(&tatarav1alpha1.Task{}).Build()
		owned := scm.Issue{Number: 31, State: "open", Author: "szymonrychu"}
		_, outcome, err := minterFor(c).MintForItem(context.Background(), proj, repo,
			ForgeItem{Issue: owned}, false, nil)
		if err != nil {
			t.Fatalf("MintForItem: %v", err)
		}
		if outcome != MintNotOwed {
			t.Fatalf("outcome = %q, want %q", outcome, MintNotOwed)
		}
	})
}

// TestSweepRequeuesOnTombstoneDelete: MintTombstoneDeleted means work is OWED
// and has NOT happened. The pass must ask for a fast requeue rather than
// waiting a full sweep period - the "silent next-pass" is the shape of the
// defect, not an acceptable fallback.
func TestSweepRequeuesOnTombstoneDelete(t *testing.T) {
	proj := sweepProject("requeue-proj")
	repo := sweepRepo("requeue-proj")
	name := tatarav1alpha1.IntakeTaskName(proj.Name, SweepIssueKind, repo.Name, 40)
	dead := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: SweepIssueKind},
		Status:     tatarav1alpha1.TaskStatus{Stage: tatarav1alpha1.StageDelivered},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithObjects(proj, repo, dead).WithStatusSubresource(&tatarav1alpha1.Task{}).Build()

	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	rd := &sweepReader{issues: []scm.IssueRef{{Number: 40, State: "open", Author: "szymonrychu"}}}
	requeue, err := r.SweepProject(context.Background(), proj, rd, []tatarav1alpha1.Repository{*repo}, nil, SweepActivity)
	if err != nil {
		t.Fatalf("SweepProject: %v", err)
	}
	if requeue != sweepRemintDelay {
		t.Fatalf("requeueAfter = %v, want %v", requeue, sweepRemintDelay)
	}
}
```

This test file needs `"github.com/prometheus/client_golang/prometheus"` added to its imports.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mise exec -- go test ./internal/obs/ ./internal/controller/ -run 'TestMintOutcome|TestSweepRequeuesOnTombstoneDelete' -v`
Expected: compile failure - `undefined: MintOutcome`, `undefined: obs.MintOutcomeTotal`,
`undefined: sweepRemintDelay`, `SweepProject` "assignment mismatch: 2 variables but 1 value".

- [ ] **Step 3a: The type, the counter and the minter**

`internal/obs/sweep_metrics.go` - add and register:

```go
// MintOutcomeTotal counts every intake mint attempt by its typed outcome
// (controller.MintOutcome). It is the metric half of replacing a bare
// `created bool`, which collapsed FOUR states into one bit and let
// resume.go log a re-mint that never happened: "nothing was owed" and "I
// deleted a Task holding this name and the mint is still owed" were the same
// value. A non-zero rate on outcome="tombstone_deleted" is a re-mint-after-reap
// churn signal; a non-zero rate on outcome="existing_live" is the normal
// webhook-beats-sweep backstop.
var MintOutcomeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_intake_mint_outcome_total",
	Help: "Intake mint attempts by task kind and typed outcome (contract B.4).",
}, []string{"kind", "outcome"})
```

`internal/controller/intake.go` - add the type above `ForgeItem`:

```go
// MintOutcome is the CLOSED result vocabulary of the intake funnel. It replaces
// a bare `created bool`, which squeezed four materially different states into
// one bit:
//
//	MintNotOwed           classification says no Task is owed (bot PR, ignored,
//	                      already owned by a live Task). Nothing happened.
//	MintCreated           a new Task exists. This is the ONLY outcome a caller
//	                      may describe as "minted".
//	MintExistingLive      the deterministic natural key is held by a LIVE twin.
//	                      No new Task; the twin's binding was repaired.
//	MintTombstoneDeleted  the key was held by a DEAD twin, which this call just
//	                      DELETED. The mint is still OWED and has not happened.
//	                      A caller must re-drive, not report success.
//
// The last two are why the bool was a defect: resume.go:164 logged "re-minted
// the issue fresh" for every non-error return, including the one where a Task
// had just been destroyed and nothing replaced it. Every caller must now switch.
type MintOutcome string

const (
	// MintNotOwed: classification decided no Task is owed. Also the value
	// returned alongside a non-nil error, which callers must check FIRST.
	MintNotOwed MintOutcome = "not_owed"
	// MintCreated: a new Task exists.
	MintCreated MintOutcome = "created"
	// MintExistingLive: a live twin holds the natural key.
	MintExistingLive MintOutcome = "existing_live"
	// MintTombstoneDeleted: a dead twin held the key and was deleted. The mint
	// is OWED and has NOT happened; re-drive.
	MintTombstoneDeleted MintOutcome = "tombstone_deleted"
)
```

`createTaskRaceSafe` (`intake.go:305-341`) - change only the return type and the three literal
returns. **Leave the `existing.DeletionTimestamp != nil || tatarav1alpha1.TaskDone(existing)`
predicate byte-identical; MR6 changes what `TaskDone` means and its diff must stay clean.**

```go
func (m *Minter) createTaskRaceSafe(ctx context.Context, task *tatarav1alpha1.Task) (MintOutcome, *tatarav1alpha1.Task, error) {
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	existing := &tatarav1alpha1.Task{}
	if err := m.reader().Get(ctx, key, existing); err == nil {
		if existing.DeletionTimestamp == nil && !tatarav1alpha1.TaskDone(existing) {
			return MintExistingLive, existing, nil
		}
	} else if !apierrors.IsNotFound(err) {
		return MintNotOwed, nil, fmt.Errorf("intake: pre-check task %s: %w", key.Name, err)
	}

	err := m.Client.Create(ctx, task)
	if err == nil {
		return MintCreated, nil, nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return MintNotOwed, nil, fmt.Errorf("intake: create task %s: %w", key.Name, err)
	}
	if getErr := m.reader().Get(ctx, key, existing); getErr != nil {
		return MintNotOwed, nil, fmt.Errorf("intake: resolve existing task %s: %w", key.Name, getErr)
	}
	if existing.DeletionTimestamp != nil || tatarav1alpha1.TaskDone(existing) {
		if delErr := m.Client.Delete(ctx, existing); delErr != nil && !apierrors.IsNotFound(delErr) {
			return MintNotOwed, nil, fmt.Errorf("intake: delete stale terminal task %s: %w", key.Name, delErr)
		}
		log.FromContext(ctx).Info("intake: deleted stale terminal task on name collision; the mint is still OWED",
			"action", "intake_stale_delete", "resource_id", key.Name)
		return MintTombstoneDeleted, nil, nil
	}
	return MintExistingLive, existing, nil
}
```

Also update the doc comment above it: the sentence "it returns created=false rather than a second
Task" becomes "it returns MintExistingLive or MintTombstoneDeleted rather than a second Task", and
"re-mint on the next tick against the freed name" becomes "the caller re-drives; see MintOutcome".

`MintIssueTask` - change the signature to `(*tatarav1alpha1.Task, MintOutcome, error)`, replace
every `return nil, false, X` with `return nil, MintNotOwed, X`, and rewrite the create block:

```go
	outcome, existing, err := m.createTaskRaceSafe(ctx, task)
	if err != nil {
		return nil, MintNotOwed, err
	}
	if outcome != MintCreated {
		// Backstop: repair an interrupted mint that left the twin's Issue CR an
		// unbound stub (the MergeRequest analogue in MintReviewTask documents why).
		if existing != nil {
			if rerr := m.repairIssueBinding(ctx, proj, repo, ext, existing, sp); rerr != nil {
				return nil, MintNotOwed, rerr
			}
		}
		obs.MintOutcomeTotal.WithLabelValues(SweepIssueKind, string(outcome)).Inc()
		return task, outcome, nil
	}
```

and the two tail returns become `return nil, MintNotOwed, err` / and finally:

```go
	if m.Metrics != nil {
		m.Metrics.OrphanAdopted(SweepIssueKind)
	}
	obs.MintOutcomeTotal.WithLabelValues(SweepIssueKind, string(MintCreated)).Inc()
	return task, MintCreated, nil
```

`MintReviewTask` - identical treatment with `SweepReviewKind` as the `kind` label.

Note both keep returning the non-nil local `task` on the non-created path, exactly as today, so no
existing caller can nil-deref. **The outcome, not the pointer, is what a caller must read.**

`MintForItem` - signature to `(*tatarav1alpha1.Task, MintOutcome, error)`; the PR branch's
`default:` becomes `return nil, MintNotOwed, nil`; the issue branch's non-orphan return becomes
`return nil, MintNotOwed, nil`; error returns become `return nil, MintNotOwed, err`. Update its doc
comment: replace the "created=false means ..." sentence with "It returns a typed MintOutcome; see
MintOutcome for what each member obliges the caller to do."

- [ ] **Step 3b: The sweep callers and the requeue plumbing**

`internal/controller/sweep.go`:

Add next to `sweepGoalLimit`:

```go
	// sweepRemintDelay is how long the pass asks to be requeued after it
	// DELETED a stale terminal Task holding a natural key (MintTombstoneDeleted).
	// The mint is still OWED, and waiting a full sweep period for it is the
	// silent-next-pass shape issue #521 exists to kill.
	//
	// It is 30s and not 0 because the deleted tombstone's owned Issue/MergeRequest
	// mirrors cascade in the BACKGROUND (DeletePropagationBackground): re-minting
	// instantly could bind a fresh Task to a mirror the GC is still collecting.
	// 30s is comfortably past that, and 120x better than the hourly pass.
	sweepRemintDelay = 30 * time.Second
```

(`sweepGoalLimit` lives in a `const` block with untyped constants; `30 * time.Second` is typed, so
declare it in its own `const` block below, or as a `var`. Follow whichever the file's gofmt keeps
clean.)

`sweepIssues` gains a `now time.Time` parameter (for Task 7) and returns `time.Duration`:

```go
func (r *ProjectReconciler) sweepIssues(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	reader scm.SCMReader, owner, name string, issues []scm.IssueRef, budget *sweepBudget, minted map[string]int,
	sp objbudget.Spiller, activity string, now time.Time, fail func(string, error, ...any)) time.Duration {

	requeue := time.Duration(0)
	for _, ref := range issues {
		...
```

and its mint block replaces `sweep.go:865-885`:

```go
		task, outcome, merr := r.minter().MintIssueTask(ctx, proj, repo, ext, stg, reason, sp)
		if merr != nil {
			fail("mint_issue_task", merr, "repo", repo.Name, "number", ref.Number)
			continue
		}
		switch outcome {
		case MintCreated:
			// Spent, on the mint that read it - whichever stage that mint chose.
			if live {
				if cerr := r.clearWebhookOriginated(ctx, proj, repo, ref.Number); cerr != nil {
					fail("clear_webhook_marker", cerr, "repo", repo.Name, "number", ref.Number)
				}
			}
			budget.record(stg)
			minted[stg]++
			log.FromContext(ctx).Info("sweep: minted task for orphan issue",
				"action", "sweep_mint", "resource_id", task.Name, "activity", activity,
				"repo", repo.Name, "number", ref.Number, "stage", stg, "stage_reason", reason,
				"webhook_originated", live)
		case MintExistingLive:
			// A webhook already minted this natural key; the sweep's backstop no-ops.
			r.skipIssue(ctx, proj, repo, ref.Number, activity, SweepSkipAlreadyMinted)
		case MintTombstoneDeleted:
			// The mint is OWED and has NOT happened. Ask for a fast requeue
			// instead of waiting a full sweep period.
			r.skipIssue(ctx, proj, repo, ref.Number, activity, SweepSkipTombstoneDeleted)
			requeue = sweepRemintDelay
		case MintNotOwed:
			// Unreachable: MintIssueTask is called only past IsOrphanIssue, and
			// it never classifies. Named anyway so a future member of the
			// vocabulary cannot be added without meeting this switch.
			r.skipIssue(ctx, proj, repo, ref.Number, activity, SweepSkipAlreadyMinted)
		}
	}
	return requeue
}
```

`sweepPRs` gains the same `time.Duration` return, with `requeue := time.Duration(0)` declared
above its `for _, pr := range prs` loop and `return requeue` at the end; its `PRReview` arm's
`task, created, merr :=` becomes `task, outcome, merr :=` with:

```go
			switch outcome {
			case MintCreated:
				budget.record(stg)
				minted[stg]++
				l.Info("sweep: minted review task for human PR", ...)   // unchanged body
			case MintExistingLive, MintNotOwed:
				obs.SweepSkippedTotal.WithLabelValues(proj.Name, activity, SweepSkipAlreadyMinted).Inc()
			case MintTombstoneDeleted:
				obs.SweepSkippedTotal.WithLabelValues(proj.Name, activity, SweepSkipTombstoneDeleted).Inc()
				requeue = sweepRemintDelay
			}
```

`SweepProject` returns `(time.Duration, error)`. Every early `return fmt.Errorf(...)` becomes
`return 0, fmt.Errorf(...)`; `return firstErr` becomes `return requeue, firstErr`; the tail becomes
`return requeue, nil`. Inside the repos loop:

```go
		if ierr != nil {
			fail("list_issues", ierr, "repo", repo.Name)
		} else if d := r.sweepIssues(ctx, proj, repo, reader, owner, name, issues, budget, minted, sp, activity, now, fail); d > 0 {
			requeue = d
		}
		...
		if d := r.sweepPRs(ctx, proj, repo, reader, writer, token, owner, name, prs, budget, minted, sp, activity, fail); d > 0 {
			requeue = d
		}
```

Update `SweepProject`'s doc comment with a sentence: "It returns a non-zero requeue-after when the
pass deleted a stale terminal Task holding a natural key: that mint is still OWED and must not
wait a full sweep period (issue #521)."

`internal/controller/projectscan.go:1418-1431`:

```go
			if len(dueRepos) > 0 {
				sweepRequeue, serr := r.SweepProject(ctx, proj, reader, dueRepos, nil, SweepActivity)
				if serr != nil {
					// RE-REPORT ONLY, hence V(1) (issue #477). ... (comment unchanged)
					l.V(1).Info("scan: sweep returned an error (already logged and metered by the sweep)",
						"action", "scan_sweep_error", "resource_id", proj.Name,
						"activity", SweepActivity, "error", serr.Error())
				}
				if sweepRequeue > 0 {
					// The pass deleted a stale terminal Task holding a natural
					// key; that mint is still OWED (issue #521).
					consider(now.Add(sweepRequeue))
				}
```

`runSweep` in `sweep_test.go` becomes:

```go
	if _, err := r.SweepProject(context.Background(), proj, rd, []tatarav1alpha1.Repository{*repo}, nil, SweepActivity); err != nil {
		t.Fatalf("SweepProject: %v", err)
	}
```

- [ ] **Step 3c: The remaining callers**

`internal/controller/ensure_task.go:43`:

```go
	stg, reason := MintReviewStage(mr)
	task, outcome, err := m.MintReviewTask(ctx, proj, repo, pr, mr, stg, reason, m.spillerFor(proj))
	if err != nil {
		return "", false, err
	}
	if outcome == MintTombstoneDeleted {
		// The natural key was held by a DEAD twin, which the mint just deleted.
		// No Task exists, so naming one here would be a fabrication - which is
		// exactly the class of defect MintOutcome exists to make impossible.
		// Erroring makes the webhook 5xx and the forge redeliver.
		return "", false, fmt.Errorf("intake: mint for MR %s deleted a stale terminal task; the mint is still owed", mr.Name)
	}
	return task.Name, outcome == MintCreated, nil
```

Add `"fmt"` to its imports. Note the second return value's meaning tightens from "we minted or
adopted" to "we CREATED", which is what its doc comment already claims.

`internal/controller/ownership.go:336` (`reMintReviewOwner`):

```go
	_, outcome, err := d.minter().MintReviewTask(ctx, proj, repo, pr, mr, stg, reason, d.spiller(proj), prevOwner)
	if err != nil {
		return fmt.Errorf("flip: re-mint review task: %w", err)
	}
	if outcome == MintTombstoneDeleted {
		// The mint is still OWED: this MR has no review owner yet. Return an
		// error so the MergeRequest reconciler requeues rather than reporting a
		// hand-back that did not happen.
		return fmt.Errorf("flip: re-mint review task for %s deleted a stale terminal task; the mint is still owed", mr.Name)
	}
	return nil
```

`internal/controller/takeover_mint.go:127-141`:

```go
	outcome, twin, err := m.createTaskRaceSafe(ctx, task)
	if err != nil {
		return nil, err
	}
	switch outcome {
	case MintExistingLive:
		return twin, nil
	case MintTombstoneDeleted:
		// createTaskRaceSafe collided with a DEAD twin and just deleted the
		// tombstone. There is no "next tick" on this endpoint-driven path:
		// retry ONCE against the now-freed name rather than surfacing a false
		// negative to the caller. BOUNDED (issue #521 boy-scout): the recursion
		// used to be unbounded, so two mints racing the same dead name could
		// recurse indefinitely.
		if retried {
			return nil, fmt.Errorf("takeover: task %s name still held by a dead twin after one retry", name)
		}
		return m.mintOrUnparkTakeoverTask(ctx, proj, repo, mr, requestingUser, commentBody, sp, true, expectFrom...)
	}
```

To carry `retried` without changing the exported signature, rename the body to an unexported
`mintOrUnparkTakeoverTask(..., retried bool, expectFrom ...string)` and make the exported
`MintOrUnparkTakeoverTask` a one-line wrapper passing `false`. Keep the exported doc comment where
it is.

`internal/controller/resume.go:160-166` - **minimal, MR6 deletes this file:**

```go
	for _, j := range jobs {
		_, outcome, err := r.minter().MintForItem(ctx, proj, j.repo, j.item, false, nil)
		if err != nil {
			return err
		}
		// The outcome is READ, not discarded. This log line used to fire
		// unconditionally and claimed a re-mint for every non-error return,
		// including MintTombstoneDeleted - where a Task had just been DESTROYED
		// and nothing replaced it. That is the exact defect the typed outcome
		// closes (issue #521). The issue is severed and ownerless by this point,
		// so it is an orphan the next sweep pass mints.
		log.FromContext(ctx).Info("resumed a no-re-entry park from a human reply",
			"action", "resume_remint", "resource_id", j.name, "old_task", t.Name,
			"reason", t.Status.StageReason, "outcome", string(outcome))
	}
```

`internal/webhook/server.go` - all three sites take the same shape. At `:669`:

```go
	_, outcome, merr := s.minter().MintForItem(ctx, &proj, repo, item, true, s.cfg.SpillerFor(&proj))
	if merr != nil {
		s.log.ErrorContext(ctx, "issues: primary mint failed", "error", merr,
			"project", proj.Name, "issue_ref", ev.IssueRef)
		s.reject(w, http.StatusInternalServerError, "mint issue", provider, ev.Kind, ev.Action, "error")
		return
	}
	if outcome == controller.MintTombstoneDeleted {
		// A dead twin held the natural key and was just deleted; the mint is
		// still OWED. 500 so the forge redelivers within seconds, which is this
		// file's existing policy for a mint that did not land.
		s.log.ErrorContext(ctx, "issues: mint deleted a stale terminal task; the mint is still owed",
			"project", proj.Name, "issue_ref", ev.IssueRef)
		s.reject(w, http.StatusInternalServerError, "mint issue", provider, ev.Kind, ev.Action, "error")
		return
	}
	if outcome == controller.MintCreated {
		// ... existing ClearWebhookOriginated + log body, unchanged
	}
```

Same at `:717` (`handleMROpened`, no marker clear) and `:810` (`handleIssueComment`).

`internal/webhook/mirror_refresh.go:203` - best-effort backstop, so no 5xx to raise:

```go
	_, outcome, merr := s.minter().MintForItem(ctx, proj, repo, item, true, s.cfg.SpillerFor(proj))
	if merr != nil {
		s.log.ErrorContext(ctx, "issues: trigger-label mint failed", "error", merr,
			"project", proj.Name, "issue_ref", ev.IssueRef)
		return
	}
	if outcome == controller.MintTombstoneDeleted {
		s.log.InfoContext(ctx, "issues: trigger-label mint deleted a stale terminal task; the sweep re-drives it",
			"project", proj.Name, "issue_ref", ev.IssueRef)
		return
	}
	if outcome == controller.MintCreated {
		// ... existing body unchanged
	}
```

- [ ] **Step 3d: Test call sites**

Mechanical: replace `_, created, err := ...Mint...` with `_, outcome, err := ...Mint...` and
`if !created` / `if created` with `if outcome != MintCreated` / `if outcome == MintCreated`.
Sites, all verified present:

- `internal/controller/intake_test.go`: `:67`, `:89`, `:102`, `:105`, `:117`, `:131`, `:152`
- `internal/controller/intake_selfheal_test.go`: `:106`, `:132`, `:155`, `:189`, `:222`, `:261`
- `internal/controller/resume_deploy_test.go`: `:97`
- `internal/webhook/primary_mint_test.go`: `:109`

Prefer asserting the exact outcome over `!= MintCreated` where the test's name already says which
one it means (e.g. `TestMintForItem_OwnedIssue_NoOp` should assert `MintNotOwed`, and
`TestMintForItem_ConcurrentSameKey_OneTask` should assert exactly one `MintCreated` across its
goroutines). That is strictly more test than it had.

- [ ] **Step 4: Run the full suite**

```
mise exec -- go build ./...
mise exec -- go vet ./...
mise run test
```
Expected: PASS across all packages.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(intake): replace the created bool with a typed MintOutcome (#521)"
```

---

## Task 7: The `operator_sweep_orphan_stranded_seconds` deadman

**Files:**
- Modify: `internal/obs/sweep_metrics.go`
- Modify: `internal/controller/sweep.go` (`sweepIssues`)
- Test: `internal/obs/sweep_metrics_test.go`, `internal/controller/sweep_orphan_liveness_test.go`

**Interfaces:**
- Consumes: `MintOutcome` (Task 6).
- Produces: `var obs.SweepOrphanStrandedSeconds *prometheus.GaugeVec` labels
  `{project, repo, number}`; `func obs.ClearSweepOrphanStranded(project, repo string)`;
  `func (r *ProjectReconciler) strandOrphan(proj, repo, ref scm.IssueRef, now time.Time)`.

**Why a new metric and not an alert on an existing one.** The only sweep alert today is
`operator_sweep_last_success_timestamp_seconds`, a LIVENESS heartbeat, and its own doc comment says
it is stamped whenever the repos loop runs to completion even with per-item errors. It reported
green for 19 hours while five issues rotted. A heartbeat can never catch this class: the sweep WAS
running, correctly, doing nothing. The deadman has to measure the WORK, not the loop.

**Definition.** One series per issue that is orphan by the predicate (open, allowed author, no live
owning Task) and that finished a sweep pass WITHOUT a Task. Value is the issue's age in seconds
since its forge `CreatedAt`. The design doc phrases this as "max age"; per-issue series give that
via `max by (project) (...)` in the alert AND name the offender, which a pre-aggregated max cannot.

**Cardinality (contract K.1).** Series exist only for stranded issues, and the steady state is ZERO
series. Cleared per `(project, repo)` at the top of every `sweepIssues` call so a healed issue's
series disappears on the pass that heals it. This is exactly `MergeCursorStalledSeconds`'s
discipline (`obs/merge_metrics.go:20-28`, `ClearMergeCursorStalled`) - a gauge with a per-object
label that is never deleted is scraped forever and `/metrics` grows without bound. Clearing is
scoped to `(project, repo)` and NOT to `project`, because `SweepProject` is called with `dueRepos`:
clearing by project would wipe series for repos this pass never looked at.

- [ ] **Step 1: Write the failing tests**

`internal/obs/sweep_metrics_test.go`:

```go
func TestSweepOrphanStrandedSecondsLabels(t *testing.T) {
	SweepOrphanStrandedSeconds.WithLabelValues("label-test-proj", "tatara-operator", "510").Set(1)
	assertLabelNames(t, gatheredLabelNames(t, SweepOrphanStrandedSeconds,
		"operator_sweep_orphan_stranded_seconds"),
		[]string{"number", "project", "repo"})
}

// The gauge carries a per-issue label, so a healed issue's series MUST leave
// the registry or /metrics grows without bound (contract K.1 CARDINALITY, the
// same rule ClearMergeCursorStalled exists for). Clearing is scoped to
// (project, repo) because SweepProject is called with dueRepos, not every repo.
func TestClearSweepOrphanStranded(t *testing.T) {
	SweepOrphanStrandedSeconds.WithLabelValues("clear-proj", "repo-a", "1").Set(1)
	SweepOrphanStrandedSeconds.WithLabelValues("clear-proj", "repo-b", "2").Set(1)
	ClearSweepOrphanStranded("clear-proj", "repo-a")
	if n := testutil.CollectAndCount(SweepOrphanStrandedSeconds); n < 1 {
		t.Fatal("clearing one repo removed every series")
	}
	ClearSweepOrphanStranded("clear-proj", "repo-a")
	SweepOrphanStrandedSeconds.WithLabelValues("clear-proj", "repo-a", "1").Set(1)
	ClearSweepOrphanStranded("clear-proj", "repo-a")
	ClearSweepOrphanStranded("clear-proj", "repo-b")
}
```

`internal/controller/sweep_orphan_liveness_test.go`:

```go
// TestSweepStrandedGaugeMarksABudgetBoundOrphan is the deadman. The only sweep
// alert today is a liveness heartbeat that reported GREEN while five issues
// rotted for 19 hours - it cannot catch this class, because the sweep WAS
// running correctly and doing nothing. This gauge measures the WORK.
func TestSweepStrandedGaugeMarksABudgetBoundOrphan(t *testing.T) {
	proj := sweepProject("stranded-proj")
	proj.Spec.MaxNewTasksPerSweep = 1
	repo := sweepRepo("stranded-proj")
	obs.ClearSweepOrphanStranded(proj.Name, repo.Name)
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithObjects(proj, repo).WithStatusSubresource(&tatarav1alpha1.Task{}).Build()

	old := time.Now().Add(-19 * time.Hour)
	runSweep(t, c, proj, repo, &sweepReader{issues: []scm.IssueRef{
		{Number: 11, State: "open", Author: "szymonrychu", CreatedAt: old},
		{Number: 12, State: "open", Author: "szymonrychu", CreatedAt: old},
	}})

	minted := testutil.ToFloat64(obs.SweepOrphanStrandedSeconds.WithLabelValues(proj.Name, repo.Name, "11"))
	deferred := testutil.ToFloat64(obs.SweepOrphanStrandedSeconds.WithLabelValues(proj.Name, repo.Name, "12"))
	if minted != 0 {
		t.Fatalf("the MINTED issue must carry no stranded series, got %v", minted)
	}
	if deferred < 19*60*60 {
		t.Fatalf("the DEFERRED issue's stranded age = %v, want at least 19h in seconds", deferred)
	}
}

// TestSweepStrandedGaugeClearsWhenTheIssueIsMinted: the gauge must go away the
// pass the issue is served, or it is a permanent false alarm.
func TestSweepStrandedGaugeClearsWhenTheIssueIsMinted(t *testing.T) {
	proj := sweepProject("stranded-clear-proj")
	proj.Spec.MaxNewTasksPerSweep = 1
	repo := sweepRepo("stranded-clear-proj")
	obs.ClearSweepOrphanStranded(proj.Name, repo.Name)
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithObjects(proj, repo).WithStatusSubresource(&tatarav1alpha1.Task{}).Build()

	old := time.Now().Add(-19 * time.Hour)
	issues := []scm.IssueRef{
		{Number: 21, State: "open", Author: "szymonrychu", CreatedAt: old},
		{Number: 22, State: "open", Author: "szymonrychu", CreatedAt: old},
	}
	runSweep(t, c, proj, repo, &sweepReader{issues: issues})
	if v := testutil.ToFloat64(obs.SweepOrphanStrandedSeconds.WithLabelValues(proj.Name, repo.Name, "22")); v == 0 {
		t.Fatal("issue 22 should be stranded after the first budget-bound pass")
	}
	runSweep(t, c, proj, repo, &sweepReader{issues: issues}) // second pass, fresh budget
	if v := testutil.ToFloat64(obs.SweepOrphanStrandedSeconds.WithLabelValues(proj.Name, repo.Name, "22")); v != 0 {
		t.Fatalf("issue 22 stranded series survived the pass that minted it: %v", v)
	}
	if n := len(sweepTasks(t, c, proj.Name)); n != 2 {
		t.Fatalf("minted %d tasks over two passes, want 2", n)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mise exec -- go test ./internal/obs/ ./internal/controller/ -run 'StrandedSeconds|ClearSweepOrphanStranded|TestSweepStrandedGauge' -v`
Expected: compile failure, `undefined: SweepOrphanStrandedSeconds`.

- [ ] **Step 3: Write minimal implementation**

`internal/obs/sweep_metrics.go`:

```go
// SweepOrphanStrandedSeconds is the sweep's DEADMAN, and it exists because the
// only sweep alert before it was a LIVENESS heartbeat
// (SweepLastSuccessTimestamp), which reported green for 19 hours while five
// open issues in tatara/tatara-operator had no Task at all (issue #521). A
// heartbeat structurally cannot catch that class: the sweep WAS running,
// correctly, doing nothing. This measures the WORK instead - one series per
// open, allowed-author issue that FINISHED a sweep pass with no live owning
// Task, valued at that issue's age in seconds.
//
// Steady state is ZERO SERIES. Alert on max by (project) over a threshold well
// past one sweep period; the {repo, number} labels NAME the offender, which a
// pre-aggregated max cannot.
//
// CARDINALITY (contract K.1). Per-issue labels mean the series MUST be deleted
// when the issue is served, exactly like MergeCursorStalledSeconds: a gauge
// that is never deleted is scraped forever and /metrics grows without bound.
// ClearSweepOrphanStranded is called at the top of every per-repo sweep, and it
// is scoped to (project, repo) rather than project because SweepProject is
// called with dueRepos - clearing by project would wipe series for repos this
// pass never looked at.
var SweepOrphanStrandedSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "operator_sweep_orphan_stranded_seconds",
	Help: "Age in seconds of an open, allowed-author issue that finished a sweep pass with no live owning Task, by project, repo and number (contract B.4/K.1, issue #521).",
}, []string{"project", "repo", "number"})

// ClearSweepOrphanStranded deletes every stranded series for one (project,
// repo). Called at the top of each per-repo sweep, so an issue served by this
// pass loses its series on this pass.
func ClearSweepOrphanStranded(project, repo string) {
	SweepOrphanStrandedSeconds.DeletePartialMatch(prometheus.Labels{"project": project, "repo": repo})
}
```

Register it in `init()`.

`internal/controller/sweep.go` - add next to `skipIssue`:

```go
// strandOrphan marks ONE orphan issue as having finished a sweep pass with no
// live owning Task. See obs.SweepOrphanStrandedSeconds for why a heartbeat
// cannot replace it.
func (r *ProjectReconciler) strandOrphan(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	ref scm.IssueRef, now time.Time) {

	obs.SweepOrphanStrandedSeconds.
		WithLabelValues(proj.Name, repo.Name, strconv.Itoa(ref.Number)).
		Set(now.Sub(ref.CreatedAt).Seconds())
}
```

(`strconv` is already imported by `sweep.go`.)

At the very top of `sweepIssues`, before the loop:

```go
	// Clear FIRST, set per stranded issue below: an issue this pass serves must
	// lose its series on this pass, or the deadman is a permanent false alarm.
	obs.ClearSweepOrphanStranded(proj.Name, repo.Name)
```

Then in the two branches where an ORPHAN ends the pass without a Task:

```go
		if !budget.allow(ctx, stg) {
			r.skipIssue(ctx, proj, repo, ref.Number, activity, SweepSkipMintBudget)
			r.strandOrphan(proj, repo, ref, now)
			continue
		}
```

and in the `MintTombstoneDeleted` arm added in Task 6:

```go
		case MintTombstoneDeleted:
			r.skipIssue(ctx, proj, repo, ref.Number, activity, SweepSkipTombstoneDeleted)
			r.strandOrphan(proj, repo, ref, now)
			requeue = sweepRemintDelay
```

`MintCreated` and `MintExistingLive` both leave a LIVE Task owning the issue, so neither strands.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mise exec -- go test ./internal/obs/ ./internal/controller/ -run 'StrandedSeconds|ClearSweepOrphanStranded|TestSweepStrandedGauge' -v`
Expected: PASS. Then `mise run test` for the whole suite.

- [ ] **Step 5: Commit**

```bash
git add internal/obs/sweep_metrics.go internal/obs/sweep_metrics_test.go \
        internal/controller/sweep.go internal/controller/sweep_orphan_liveness_test.go
git commit -m "feat(obs): add the sweep orphan-stranded deadman gauge (#521)"
```

---

## Task 8: `MEMORY.md`, full verification, MR

**Files:**
- Modify: `MEMORY.md` (append one dated entry)

**Interfaces:** none.

`ROADMAP.md` is NOT touched: this MR completes no roadmap phase and re-scopes nothing. MR6 is where
the roadmap moves.

- [ ] **Step 1: Append the MEMORY.md entry**

One dated line, in the file's existing style (dense, one paragraph, states the non-obvious
decision and the dead end):

```markdown
- 2026-08-07 (#521, MR1) `IsOrphanIssue` clause (b) called `own.ControllerOwner(cr)`, which returns `owned=true` for ANY ownerRef carrying `controller=true` and never checks the named Task exists - so an Issue CR whose owning Task was reaped kept a dangling controller ownerRef forever and the sweep skipped it silently on every pass, for 19 hours across five open issues (`iss-tatara-operator-510/512/520/523/524`) with a GREEN sweep heartbeat. The fix is not "also check the Task" inside the predicate: the predicate now takes a RESOLVED `liveOwner string` and `Minter.resolveLiveOwner` fills it in, which makes "resolve liveness first" structural rather than a convention the next caller forgets - the same reason `stage.UnparkDetailed` exists. The stale ref is DROPPED, not ignored, because the identical ref also misroutes `ownerTaskRequests` (stage_controller.go), the reaper cascade and `ourMR`; ignoring it would have fixed one of four consumers. The liveness Get is UNCACHED for the same reason `createTaskRaceSafe`'s is (fix F3-1): a stale informer that has not observed a freshly created Task would make this drop a LIVE owner's ref and steal an issue out from under a running Task, which is strictly worse than the bug. `own.DropOwner` is the pure mutation half and stays in `internal/own`; the Get does NOT, because that package is memory-only by its own package doc. Also replaced the intake funnel's `created bool` with a typed `MintOutcome` - it collapsed FOUR states into one bit, which is how `resume.go:164` logged "re-minted the issue fresh" for a return where a Task had just been DESTROYED and nothing replaced it. `MintTombstoneDeleted` now requeues the sweep at 30s (`sweepRemintDelay`) rather than a full hourly pass; 30s and not 0 because the deleted tombstone's owned mirrors cascade in the BACKGROUND and an instant re-mint could bind a fresh Task to a mirror the GC is still collecting. Observability half: the only sweep alert was a LIVENESS heartbeat, which structurally cannot catch this class (the sweep WAS running, correctly, doing nothing), so `operator_sweep_orphan_stranded_seconds{project,repo,number}` measures the WORK, cleared per `(project, repo)` on every pass because `SweepProject` is called with `dueRepos` and a project-wide clear would wipe repos this pass never looked at. STILL OPEN at the time of writing: issues 502/503/505/521/525 carry ZERO ownerRefs, so clause (b) passed for them; the reporter allowlist and the creation budget were both ruled out from the live cluster, and their skip branch is not determinable from outside - the new `action=sweep_skip_issue` line is what states it on the first pass after rollout, and the MR description deliberately asserts no cause.
```

- [ ] **Step 2: Full local verification (mandatory, and every command's output must be read)**

```bash
# 1. Regenerated files must NOT move: this MR touches no api/ type.
mise exec -- make generate
mise exec -- make manifests
git status --porcelain
# EXPECTED: only the files this MR deliberately edits. If
# api/v1alpha1/zz_generated.deepcopy.go or
# charts/tatara-operator/crd-bases/tatara.dev_*.yaml appear, STOP: the change
# escaped its scope fence. Understand it before committing anything.

# 2. RBAC drift: no new group/verb, so this must be green with no chart edit.
mise exec -- make rbac-check

# 3. Lint.
mise exec -- make lint
# EXPECTED: no findings. (Makefile tolerates golangci exit code 5 = "no files".)

# 4. Full test suite, INCLUDING envtest. Downloads the 1.33.0 control plane on
#    first run via `setup-envtest use 1.33.0`, so allow several minutes.
mise exec -- make test
# equivalently: mise run test
# EXPECTED: ok for every package, no FAIL, no DATA RACE (the target runs -race).

# 5. Pre-commit, all files. The `go-test` hook is stage: pre-push and runs the
#    FULL suite again, so this is the slowest step; do not skip it, it is what
#    CI runs.
mise exec -- pre-commit run --all-files
mise exec -- pre-commit run --all-files --hook-stage pre-push
```

Targeted re-runs for the tests this MR adds, all of which must be green:

```bash
mise exec -- go test ./internal/own/ -run TestDropOwner -v
mise exec -- go test ./internal/obs/ -run 'TestSweepSkipReasonsMatchSweepConstants|TestSeedSweepSkippedForProject|TestSeedSweepErrorsForProject|TestSweepSeedReasonsCoverEveryFailSite|TestSweepStaleOwnerRepairedTotalLabels|TestMintOutcomeTotalLabels|TestSweepOrphanStrandedSecondsLabels|TestClearSweepOrphanStranded' -v
mise exec -- go test ./internal/controller/ -run 'TestOrphanIssuePredicate|TestResolveLiveOwner|TestSweepMintsIssueWhoseOwningTaskWasReaped|TestSweepCounts|TestRepoNames|TestMintOutcome|TestSweepRequeuesOnTombstoneDelete|TestSweepStrandedGauge' -v
mise exec -- go test ./internal/webhook/ -v
```

- [ ] **Step 3: Commit and open the MR**

```bash
git add MEMORY.md
git commit -m "docs(memory): record the #521 orphan-liveness fix and its dead ends"
```

Then follow `superpowers:finishing-a-development-branch`. MR body must contain:

- The bug paragraph from "The bug, in one paragraph" above.
- The five named victims and the fact they mint on the first pass after rollout.
- The open question, stated as open: "issues 502/503/505/521/525 carry zero ownerRefs, so clause
  (b) passed for them. The reporter allowlist and the creation budget were both ruled out against
  the live cluster. Their skip branch is not determinable from outside the operator; the new
  `action=sweep_skip_issue` log resolves it on the first pass after rollout." **Do not name a
  cause.**
- The new metric names, so `tatara-observability` can pick them up.
- `semver:minor` label set BEFORE merge.

- [ ] **Step 4: Post-merge cluster verification**

Namespace `tatara`, project `tatara`, repo `tatara-operator`. Run these once the new image is
rolled out and ONE sweep period (`issueScan` cadence) has elapsed.

**(a) The five dangling refs are gone and the issues have Tasks:**

```bash
# Every stale ref should be gone: this prints nothing on success.
for n in 510 512 520 523 524; do
  owner=$(kubectl -n tatara get issue "iss-tatara-operator-$n" \
    -o jsonpath='{.metadata.ownerReferences[?(@.controller==true)].name}')
  if [ -n "$owner" ] && ! kubectl -n tatara get task "$owner" >/dev/null 2>&1; then
    echo "STILL DANGLING: iss-tatara-operator-$n -> $owner (NotFound)"
  fi
done
```

```bash
# Each of the five must now be controller-owned by a Task that EXISTS.
kubectl -n tatara get tasks -o json | jq -r '
  .items[]
  | select(.spec.projectRef == "tatara")
  | select(.spec.source.number as $n | [510,512,520,523,524] | index($n))
  | [.metadata.name, (.spec.source.number|tostring), .spec.kind, .status.stage, (.status.stageReason // "")]
  | @tsv'
# EXPECTED: five rows. Task names are hashed (IntakeTaskName), so match on
# .spec.source.number, never on a name pattern.
```

**(b) Resolve the open question for the other five** (this is the whole reason the skip log
exists). Loki, via Grafana:

```
{namespace="tatara", pod=~"tatara-operator-.*"}
  | json
  | action = "sweep_skip_issue"
  | resource_id =~ "iss-tatara-operator-(502|503|505|521|525)"
```
Read the `reason` field. It will be one of the seven closed values, and that is the answer nobody
could get from outside. Record it on issue #521.

**(c) The repair fired exactly as many times as there were stranded issues:**

```promql
sum(increase(operator_sweep_stale_owner_repaired_total{project="tatara"}[24h]))
```
EXPECTED: a burst summing to 5 shortly after rollout, then flat. A SUSTAINED rate means a reap is
not handing over ownership - that is a B.5 bug, not this one.

**(d) The skip vocabulary has a zero baseline for every member** (the whole point of the seeding):

```promql
operator_sweep_skipped_total{project="tatara", activity="sweep"}
```
EXPECTED: seven series present, most at 0, from the operator's first Project reconcile - not born
at their first skip.

**(e) The deadman is empty:**

```promql
max by (project, repo, number) (operator_sweep_orphan_stranded_seconds)
```
EXPECTED: no series once the backlog converges. Any series names an issue with no Task.

**(f) Mints actually happened, and how:**

```promql
sum by (kind, outcome) (increase(operator_intake_mint_outcome_total[24h]))
```
EXPECTED: `{kind="clarify", outcome="created"}` non-zero. `outcome="tombstone_deleted"` should be
rare-to-zero; a sustained rate is re-mint-after-reap churn.

**(g) The pass log now names its repos:**

```
{namespace="tatara", pod=~"tatara-operator-.*"} | json | action = "sweep_pass"
```
EXPECTED: every line carries a `repos` array.

---

## Out of scope, noted rather than dropped

1. **The Grafana alert rule** for `operator_sweep_orphan_stranded_seconds` lives in
   `tatara-observability` (`alerts/tatara-*.yaml`), a different repo with its own terraform apply.
   The metric ships here; the rule is a companion change there. Suggested shape:
   `max by (project) (operator_sweep_orphan_stranded_seconds) > 7200` for 30m, warning - two hours
   is comfortably past one hourly sweep period plus a budget-bound pass.
2. **`ProjectStatus.ScanMarks` / `ScanMark` are dead** (zero readers, zero writers, only the type
   declaration and generated deepcopy reference them) - already tracked at the tail of `ROADMAP.md`.
   Touching them would drag `api/` into this MR and force a CRD regeneration, so no.
3. **MR2-MR7** of the #521 landing (skills pin, skill fold, `submit_outcome` schema, contract
   version 4, the 8-state model). MR1 ships alone and first, by design: the platform is currently
   doing zero work and the redesign must not gate the unblocking.
