# Refine-driven recovery of gave-up implementations - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Drive gave-up implementations to a terminal outcome (delivered, closed, or escalated) with a bounded auto-reroll and a refine judgment layer, tracked on the Task CR.

**Architecture:** Add a per-issue give-up counter to the durable lifecycle Task; increment it at the single park chokepoint; bound `recoverOrphans` to adopt-and-reroll while under the cap and stop above it; keep the counter alive via a reaper invariant; surface give-up via `task_list` + a `blocked` metric state; and rewrite the refine prompt to treat gave-up issues as a top-priority close/comment/escalate category.

**Tech Stack:** Go (operator, kubebuilder CRDs, controller-runtime, envtest), tatara-cli (MCP `task_list`), Prometheus metrics.

## Global Constraints

- Newest stable Go; KISS; no tech debt; JSON `log/slog` only; metrics for anything that counts/fails.
- Spec: `docs/superpowers/specs/2026-06-28-refine-recover-gaveup-implementations-design.md`.
- `maxImplGiveUps = 3`. In-scope give-up reasons (recoverable): `implement-failed`, `maxIterations`, `refused-no-explanation`, `deadline`. Out-of-scope (deliberate): `refused-declined`, `already_done`, `human-declined`, `triage-failed`.
- Increment the counter exactly once per give-up (transition `Implement -> Parked` with an in-scope reason); never per steady-state reconcile.
- No new GitHub label. Build operator via CI on merge, deploy via tatara-helmfile.
- Run operator tests with envtest: `KUBEBUILDER_ASSETS="$(go run sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.21 use 1.33.0 -p path)" go test ./...` or `make test`.

## File structure

- `api/v1alpha1/task_types.go` - new `Status.ImplementGiveUps int`; helper `IsRecoverableGiveup(reason string) bool`; `maxImplGiveUps` const lives in controller (not api).
- `internal/controller/lifecycle.go` - increment in `setLifecycleState` on the in-scope `Implement -> Parked` transition.
- `internal/controller/projectscan.go` - bound + adopt-in-place reroll in `recoverOrphans`; helper to find the matching terminal lifecycle task; adopt-at-Implement that preserves the counter.
- `internal/controller/reaper.go` - spare a parked-in-Implement task whose issue is still open (in `gcTerminalTasks`).
- `internal/controller/project_controller.go` - `issueStateFor` returns `blocked` for parked-in-Implement at cap.
- `internal/refine/goal.go` - gave-up priority category in `GoalProject`.
- tatara-cli MCP `task_list` - include `lifecycleState`, `parkReason`, `implementGiveUps` per task.
- CRD regen: `make manifests` updates `charts/tatara-operator/crd-bases/tatara.dev_tasks.yaml`.

---

### Task 1: Give-up counter field + recoverable-reason helper + increment chokepoint

**Files:**
- Modify: `api/v1alpha1/task_types.go` (add field near `ImplementEmptyRetries` ~line 328; add `IsRecoverableGiveup`)
- Modify: `internal/controller/lifecycle.go` (`setLifecycleState` ~line 226)
- Test: `api/v1alpha1/task_types_test.go` (or existing `types_fields_test.go`), `internal/controller/lifecycle_giveup_test.go` (new)
- Regen: `charts/tatara-operator/crd-bases/tatara.dev_tasks.yaml` via `make manifests`

**Interfaces:**
- Produces: `Task.Status.ImplementGiveUps int` (json `implementGiveUps`, `+optional`); `v1alpha1.IsRecoverableGiveup(reason string) bool` returning true for the in-scope set.

- [ ] **Step 1: Write the failing test for the reason helper**

In `api/v1alpha1/task_types_test.go`:
```go
func TestIsRecoverableGiveup(t *testing.T) {
	rec := []string{"implement-failed", "maxIterations", "refused-no-explanation", "deadline"}
	for _, r := range rec {
		if !IsRecoverableGiveup(r) {
			t.Errorf("%q should be recoverable", r)
		}
	}
	notRec := []string{"refused-declined", "already_done", "human-declined", "triage-failed", "implement-done", ""}
	for _, r := range notRec {
		if IsRecoverableGiveup(r) {
			t.Errorf("%q should NOT be recoverable", r)
		}
	}
}
```

- [ ] **Step 2: Run it, verify it fails** - `go test ./api/v1alpha1/ -run TestIsRecoverableGiveup` -> FAIL (undefined: IsRecoverableGiveup).

- [ ] **Step 3: Add the field + helper**

In `api/v1alpha1/task_types.go`, in `TaskStatus` next to `ImplementEmptyRetries`:
```go
	// ImplementGiveUps counts implementation attempts that gave up for this
	// issue's durable lifecycle Task (transition Implement->Parked with a
	// recoverable reason). Bounds the auto-reroll backstop. +optional
	ImplementGiveUps int `json:"implementGiveUps,omitempty"`
```
Add (package-level):
```go
// IsRecoverableGiveup reports whether a Parked reason represents an
// implementation that gave up and may be re-rolled (vs a deliberate decline).
func IsRecoverableGiveup(reason string) bool {
	switch reason {
	case "implement-failed", "maxIterations", "refused-no-explanation", "deadline":
		return true
	default:
		return false
	}
}
```

- [ ] **Step 4: Run it, verify pass** - `go test ./api/v1alpha1/ -run TestIsRecoverableGiveup` -> PASS.

- [ ] **Step 5: Write the failing increment test**

In `internal/controller/lifecycle_giveup_test.go` (envtest or a unit test that calls `setLifecycleState` against a fake client - mirror an existing lifecycle test's harness). Assert:
```go
// Implement -> Parked with implement-failed increments once.
// from=="Implement", reason recoverable -> ImplementGiveUps goes 0 -> 1.
// A second call with the task already Parked (no transition) does NOT increment.
// Implement -> Parked with reason "refused-declined" does NOT increment.
// Conversation -> Parked with "deadline" does NOT increment (from != Implement).
```
Use the same fake-client + TaskReconciler setup as the nearest existing `setLifecycleState`/lifecycle test in the package.

- [ ] **Step 6: Run it, verify it fails.**

- [ ] **Step 7: Implement the increment in `setLifecycleState`**

In `internal/controller/lifecycle.go`, inside `setLifecycleState(ctx, task, to, reason)`, BEFORE it overwrites `task.Status.LifecycleState`, capture `from := task.Status.LifecycleState` and, when persisting the status update, add:
```go
	if from == "Implement" && to == "Parked" && tatarav1alpha1.IsRecoverableGiveup(reason) {
		task.Status.ImplementGiveUps++
	}
```
Place it so it lands in the same status write as the state transition (read the function: it does a RetryOnConflict status update - increment on the `fresh` object inside that closure, guarded by `fresh.Status.LifecycleState == from` to stay idempotent under conflict retries).

- [ ] **Step 8: Run it, verify pass.** `go test ./internal/controller/ -run Giveup` (envtest).

- [ ] **Step 9: Regenerate CRDs** - `make manifests`; confirm `implementGiveUps` appears in `charts/tatara-operator/crd-bases/tatara.dev_tasks.yaml`.

- [ ] **Step 10: Commit** - `git add -A && git commit -m "feat: track ImplementGiveUps on Task CR (transition-guarded)"`

---

### Task 2: Bounded adopt-in-place reroll in recoverOrphans

**Files:**
- Modify: `internal/controller/projectscan.go` (`recoverOrphans` ~line 2293-2360; add a find-matching-terminal-task helper + adopt-at-Implement preserving counter)
- Test: `internal/controller/projectscan_recover_giveup_test.go` (new)

**Interfaces:**
- Consumes: `Task.Status.ImplementGiveUps` (Task 1), `taskMatchesItem` (task_types.go:52), `hasLiveLifecycleTaskForIssue` (projectscan.go:2113), `adoptLifecycleTask` (projectscan.go:258), `existingScanTasks`.
- Produces: behavior - for an open `tatara-implementation` issue with a terminal (Parked) matching task and no live task: reroll (adopt at Implement, counter preserved) iff `ImplementGiveUps < maxImplGiveUps`; else skip.
- Const `maxImplGiveUps = 3` near the other backstop consts (projectscan.go:47 area).

- [ ] **Step 1: Write the failing test**

In `internal/controller/projectscan_recover_giveup_test.go`, table-driven against a fake client seeded with a Project + a terminal lifecycle Task for `owner/repo#7` and an open issue `#7` labelled `tatara-implementation`:
```go
// case A: ImplementGiveUps=0 -> recoverOrphans re-enters Implement on the SAME
//   task (no new task created), task.Status.LifecycleState=="Implement", Phase=="".
// case B: ImplementGiveUps=1 -> rerolls (same task), state Implement.
// case C: ImplementGiveUps=3 -> NO reroll: task stays Parked, no new task, and
//   a ScanItem("backstop","skipped_giveup_cap") metric is recorded.
// case D: a LIVE (non-terminal, LifecycleState=Implement) task present -> no action.
// case E: no matching task at all -> falls back to createScanTask (fresh).
```
Assert by listing tasks after the call and checking counts/state. Reuse the harness from the existing `projectscan` recover/backstop tests.

- [ ] **Step 2: Run it, verify it fails.**

- [ ] **Step 3: Add `maxImplGiveUps` const + a finder helper**

Near projectscan.go:47:
```go
const maxImplGiveUps = 3
```
Add:
```go
// matchingTerminalLifecycleTask returns the terminal (Parked/Done/Stopped or
// Failed/Succeeded) lifecycle task for slug#number, or nil. Used to recover the
// durable per-issue task so its ImplementGiveUps counter is preserved.
func matchingTerminalLifecycleTask(existing []tatarav1alpha1.Task, slug string, number int) *tatarav1alpha1.Task {
	for i := range existing {
		t := &existing[i]
		if t.Spec.Kind != "issueLifecycle" {
			continue
		}
		if taskMatchesItem(t, slug, number) && tatarav1alpha1.TaskTerminal(t) {
			return t
		}
	}
	return nil
}
```
(Confirm `taskMatchesItem` signature at task_types.go:52 and adapt the call.)

- [ ] **Step 4: Wire the bound into recoverOrphans**

In `recoverOrphans`, in the `case hasLabel(iss.Labels, implementation):` branch (projectscan.go:2326), after the `hasLiveLifecycleTaskForIssue` continue-guard (2338) and before `createScanTask`, insert:
```go
	if entry == "Implement" && hasLabel(iss.Labels, implementation) {
		if tk := matchingTerminalLifecycleTask(existing, slug, iss.Number); tk != nil {
			if tk.Status.ImplementGiveUps >= maxImplGiveUps {
				r.Metrics.ScanItem("backstop", "skipped_giveup_cap")
				l.Info("backstop: implementation gave up at cap; not rerolling, awaiting refine/human",
					"action", "backstop_recover", "resource_id", proj.Name,
					"issue", fmt.Sprintf("%s#%d", slug, iss.Number),
					"give_ups", tk.Status.ImplementGiveUps)
				continue
			}
			// Reroll the SAME task so the counter persists; re-enter Implement.
			if err := r.adoptLifecycleTaskAt(ctx, proj, tk, "Implement"); err != nil {
				l.Error(err, "backstop: adopt-reroll failed", "action", "backstop_recover", "resource_id", proj.Name)
			} else {
				r.Metrics.ScanItem("backstop", "reroll_giveup")
			}
			continue
		}
	}
```

- [ ] **Step 5: Add `adoptLifecycleTaskAt`**

Generalize the existing `adoptLifecycleTask` (projectscan.go:258) - which resets to `Triage` - into a variant that re-enters at a given state and PRESERVES `ImplementGiveUps`:
```go
// adoptLifecycleTaskAt re-enters an existing terminal lifecycle task at `entry`
// (in-place, preserving identity and the ImplementGiveUps counter): clears
// Phase, sets LifecycleState=entry, resets ImplementEmptyRetries and the
// activity/deadline clocks, but does NOT touch ImplementGiveUps.
```
Implement by copying `adoptLifecycleTask`'s status-update closure, setting `LifecycleState = entry` instead of `"Triage"`, and explicitly NOT zeroing `ImplementGiveUps`. Keep `adoptLifecycleTask` as `adoptLifecycleTaskAt(ctx, proj, task, "Triage")` to stay DRY (verify its current callers still get Triage semantics).

- [ ] **Step 6: Run tests, verify pass.** `make test` (envtest) or the package run.

- [ ] **Step 7: Commit** - `git commit -am "feat: bound recoverOrphans reroll to maxImplGiveUps, adopt-in-place to preserve counter"`

---

### Task 3: Reaper invariant - keep the counter alive

**Files:**
- Modify: `internal/controller/reaper.go` (`gcTerminalTasks` ~line 330)
- Test: `internal/controller/reaper_giveup_test.go` (new)

**Interfaces:**
- Consumes: `TaskTerminal`, `Task.Status.LifecycleState`, the issue-open signal the reaper already has (or add a guard purely on state).
- Produces: a parked-in-Implement task (LifecycleState==Parked, last state Implement, ParkReason recoverable, ImplementGiveUps>0) is NOT GC'd while under cap.

- [ ] **Step 1: Write the failing test**

In `internal/controller/reaper_giveup_test.go`:
```go
// A Parked task with ParkReason recoverable AND ImplementGiveUps in [1,2]
//   older than the retention window is NOT deleted by gcTerminalTasks.
// A Parked task with ImplementGiveUps>=maxImplGiveUps (escalated) IS eligible
//   for normal GC (its recovery is done; refine/human owns it).
// A Parked task with a non-recoverable reason (e.g. refused-declined) GCs normally.
```
(Decision: keep the under-cap task; once at cap the counter no longer needs preserving because recoverOrphans will not reroll regardless - so normal retention applies. This keeps the live window bounded.)

- [ ] **Step 2: Run it, verify it fails.**

- [ ] **Step 3: Add the guard in `gcTerminalTasks`**

Before deleting a terminal Task, skip when it is an under-cap recoverable give-up:
```go
	if tk.Status.LifecycleState == "Parked" &&
		tatarav1alpha1.IsRecoverableGiveup(tk.Status.ParkReason) &&
		tk.Status.ImplementGiveUps > 0 && tk.Status.ImplementGiveUps < maxImplGiveUps {
		continue // preserve the give-up counter for recoverOrphans reroll
	}
```
(`ParkReason` only reflects the LAST park; a recoverable reason + a positive under-cap counter is sufficient and simple.)

- [ ] **Step 4: Run tests, verify pass.**

- [ ] **Step 5: Commit** - `git commit -am "feat: reaper preserves under-cap give-up tasks (counter durability)"`

---

### Task 4: `blocked` metric state

**Files:**
- Modify: `internal/controller/project_controller.go` (`issueStateFor` ~line 393)
- Test: `internal/controller/project_controller_test.go` (extend the issueStateFor tests)

**Interfaces:**
- Consumes: `Task.Status.LifecycleState`, `ImplementGiveUps`, `ParkReason`.
- Produces: `issueStateFor` returns `"blocked"` for a parked-in-Implement task at cap; existing states unchanged.

- [ ] **Step 1: Write the failing test**

Extend the existing `issueStateFor` test table:
```go
// LifecycleState=Parked, ParkReason recoverable, ImplementGiveUps>=3 -> "blocked"
// LifecycleState=Parked, ImplementGiveUps<3 -> existing behavior (whatever it returns today for Parked)
// LifecycleState=Implement -> "implementing" (unchanged)
```

- [ ] **Step 2: Run it, verify it fails.**

- [ ] **Step 3: Implement**

In `issueStateFor`, before the existing state mapping returns:
```go
	if t.Status.LifecycleState == "Parked" &&
		tataradevv1alpha1.IsRecoverableGiveup(t.Status.ParkReason) &&
		t.Status.ImplementGiveUps >= maxImplGiveUps {
		return "blocked"
	}
```
(Confirm the import alias used in this file - `tataradevv1alpha1` per the grep - and that `maxImplGiveUps` is reachable; if it lives in `controller` package it is.)

- [ ] **Step 4: Run tests, verify pass.**

- [ ] **Step 5: Commit** - `git commit -am "feat: tatara_issue_state blocked state for capped give-ups"`

---

### Task 5: Surface give-up fields in `task_list` MCP

**Files:**
- Modify: tatara-cli `task_list` tool (find: `grep -rn "task_list\|TaskList\|lifecycleState" internal/mcp` in tatara-cli) - add `lifecycleState`, `parkReason`, `implementGiveUps` to each task entry IF not already present.
- Modify (if needed): operator side that backs the task list (the operator serves Task data the cli renders - confirm whether cli reads Task CRs directly or via an operator endpoint).
- Test: the cli tool's existing test for `task_list` output shape.

**Interfaces:**
- Produces: each `task_list` entry includes `lifecycleState` (string), `parkReason` (string), `implementGiveUps` (int).

- [ ] **Step 1: Locate the task_list field assembly** - `cd ../../tatara-cli` (or the cli checkout) and `grep -rn "implementGiveUps\|parkReason\|lifecycleState\|func.*[Tt]askList" internal/`. Identify the struct/marshal that builds a task entry.

- [ ] **Step 2: Write the failing test** asserting the rendered entry contains the three fields for a task with `ImplementGiveUps=2, ParkReason="implement-failed", LifecycleState="Parked"`.

- [ ] **Step 3: Run it, verify it fails.**

- [ ] **Step 4: Add the fields** to the entry struct + populate from the Task status. Keep names camelCase matching the CRD json tags.

- [ ] **Step 5: Run tests, verify pass.**

- [ ] **Step 6: Commit** (in the cli repo) - `git commit -am "feat: task_list exposes lifecycleState, parkReason, implementGiveUps"`. Note: this requires a cli release + wrapper pin bump to ship; flag in the integration notes. If `task_list` already returns these (verify in Step 1), skip this task and note it.

---

### Task 6: Refine prompt - gave-up issues as top priority

**Files:**
- Modify: `internal/refine/goal.go` (`GoalProject` ~line 25-56)
- Test: `internal/refine/goal_test.go` (golden/string-content test)

**Interfaces:**
- Consumes: nothing new at compile time; the prompt instructs the agent to read `task_list` give-up fields (Task 5).
- Produces: prompt text containing the gave-up priority category with close/comment/escalate rules and the live-vs-gaveup distinction.

- [ ] **Step 1: Write the failing test**

In `internal/refine/goal_test.go`:
```go
func TestGoalProject_GaveUpCategory(t *testing.T) {
	g := GoalProject([]string{"szymonrychu/tatara-cli"}, 14)
	for _, want := range []string{
		"gave up", "implementGiveUps", "Parked",
		"close", "refined scope", "escalate",
		"never act on a live", // live-vs-gaveup guard
	} {
		if !strings.Contains(g, want) {
			t.Errorf("GoalProject missing %q", want)
		}
	}
}
```

- [ ] **Step 2: Run it, verify it fails.**

- [ ] **Step 3: Edit the prompt**

In `GoalProject`, add a FIRST priority section before the existing categories. Content (adapt to the file's existing voice/format):
```
PRIORITY 0 - gave-up implementations (do these first):
Read task_list. For each OPEN issue whose lifecycle task is terminal
(LifecycleState=Parked) with a recoverable parkReason (implement-failed,
maxIterations, refused-no-explanation, deadline) - NEVER act on a live /
in-progress implementation (non-terminal task):
- If the work is already delivered, a duplicate, or obsolete -> close the issue
  with a one-line reason (regardless of implementGiveUps).
- Else if implementGiveUps < 3 -> comment a refined, concrete, actionable scope
  that addresses why prior attempts failed, so the next auto-reroll can succeed.
  Do NOT close. Do NOT create a task (the operator re-rolls).
- Else (implementGiveUps >= 3) -> comment a failure summary that escalates to a
  human: what was attempted, why it failed, and exactly what input/decision is
  needed. Do NOT close, do NOT reroll.
```
Also remove/soften any existing line that frames in-flight implementations as entirely out-of-scope so terminal/parked ones are explicitly in-scope (keep the "do not create tasks" rule and the live-implementation hands-off rule).

- [ ] **Step 4: Run tests, verify pass.**

- [ ] **Step 5: Commit** - `git commit -am "feat: refine prompt prioritizes recovering gave-up implementations"`

---

## Integration + verification (after all tasks)

- [ ] `gofmt -l` clean; `go build ./...`; `go vet ./...`; `make test` (envtest) all green in the operator worktree.
- [ ] `make manifests` shows only the `implementGiveUps` CRD addition.
- [ ] requesting-code-review on the full branch diff; fix critical/high; re-run tests.
- [ ] Merge operator PR -> CI publishes operator image + both charts. If Task 5 touched the cli, merge cli + bump the wrapper cli pin.
- [ ] tatara-helmfile: bump tatara-operator + tatara-project chart pins + operator image tag to the merged SHA (and wrapper image if cli changed). Diff -> merge -> apply.
- [ ] Live verify: a Task that gives up shows `ImplementGiveUps` incrementing; recoverOrphans rerolls under cap and stops at cap; `tatara_issue_state{state="blocked"}` appears for a capped issue; a refine turn's prompt includes the gave-up category (operator pod logs / a refine pod).

## Self-review notes

- Spec coverage: component 1->Task1, 2->Task2, 3(task_list)->Task5, 4(refine)->Task6, 5(metric)->Task4, 6(reaper)->Task3. All covered.
- Counter semantics consistent: increment once per in-scope Implement->Parked transition (Task1); read by Task2/3/4; bound `maxImplGiveUps=3` used identically in Tasks 2/3/4.
- Naming consistent: `ImplementGiveUps`, `IsRecoverableGiveup`, `maxImplGiveUps`, `adoptLifecycleTaskAt`, `matchingTerminalLifecycleTask` across tasks.
- Task 5 may be a no-op if `task_list` already surfaces the fields - verify first, skip if so (noted in the task).
