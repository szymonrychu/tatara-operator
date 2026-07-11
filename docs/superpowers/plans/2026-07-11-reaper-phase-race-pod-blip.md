# Reaper phase-race pod blip + leader-gated maintenance ticker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the orphan reaper deleting live wrapper pods mid-continuation, and stop the poll+reap ticker running in every operator replica.

**Architecture:** Two independent fixes in `code/tatara-operator`. (1) In `reaper.go` `orphanReason`, gate the phase-terminal reap on an empty `DeployState` so a lifecycle task showing transient `phase=Succeeded` between a turn-batch drain and `resetAgentRun` keeps its warm pod. (2) Split `CallbackServer.Start`'s ticker into a new `RunMaintenance` method registered as a leader-only manager runnable, leaving the HTTP handler running on every replica.

**Tech Stack:** Go (controller-runtime), envtest (`internal/controller` suite), `mise` task runner.

## Global Constraints

- Newest stable Go, pinned to the exact minor in `go.mod`. Build/test/lint via `mise run {build,test,lint}` or `mise exec -- go ...`, never bare `go`.
- JSON logs (`log/slog`); metrics for anything that counts/fails; KISS, no tech-debt.
- `gofmt` + `golangci-lint` clean. Wrap errors with `fmt.Errorf("context: %w", err)`. Table-driven tests with `t.Run` where natural.
- Agents never merge PRs. Declare `change_significance` on the change summary (this is a `patch`: bug fix).
- Spec: `docs/superpowers/specs/2026-07-11-reaper-phase-race-pod-blip-design.md`.

---

### Task 1: Lifecycle-gate the phase-terminal reap

**Files:**
- Modify: `internal/controller/reaper.go` (`orphanReason`, the `isTerminal(task.Status.Phase)` branch ~line 168)
- Test: `internal/controller/reaper_test.go`

**Interfaces:**
- Consumes: existing `orphanReason(pod *corev1.Pod, tasks map[string]*Task) (string, bool)`, helpers `mkTaskProject`, `mkTaskRepository`, `mkTask`, `getTask`, `setTaskPhase`, `mkWrapperPodSvc`, `podExists`, `reaperServer()` (all in `reaper_test.go`).
- Produces: no signature change. Behavior change: a task with `phase` terminal AND non-empty non-terminal `DeployState` is no longer an orphan by the phase branch.

- [ ] **Step 1: Write the failing test** (append to `internal/controller/reaper_test.go`, after `TestReapOrphans_TerminalPhase`)

```go
// TestReapOrphans_TerminalPhaseWithLiveLifecycleKept covers the pod-blip fix:
// a task shows phase Succeeded transiently between a turn-batch drain
// (NoPendingSubtasks) and resetAgentRun reviving it, while its lifecycle
// DeployState is still live (Conversation). The reaper must NOT phase-reap it -
// that would kill the warm pod mid-continuation.
func TestReapOrphans_TerminalPhaseWithLiveLifecycleKept(t *testing.T) {
	mkTaskProject(t, "p-reap-phlc", 3)
	mkTaskRepository(t, "r-reap-phlc", "p-reap-phlc")
	mkTask(t, "t-reap-phlc", "p-reap-phlc", "r-reap-phlc")
	setTaskPhase(t, "t-reap-phlc", "Succeeded")
	tk := getTask(t, "t-reap-phlc")
	tk.Status.DeployState = "Conversation"
	if err := k8sClient.Status().Update(context.Background(), tk); err != nil {
		t.Fatalf("set lifecycle: %v", err)
	}
	mkWrapperPodSvc(t, "reap-phlc", "t-reap-phlc", string(tk.UID))

	reaperServer().ReapOrphans(context.Background())
	if !podExists(t, "reap-phlc") {
		t.Error("expected pod for phase-Succeeded task with live Conversation lifecycle to be kept")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mise exec -- go test ./internal/controller/ -run TestReapOrphans_TerminalPhaseWithLiveLifecycleKept -count=1`
Expected: FAIL (pod reaped by the current unconditional phase branch).

- [ ] **Step 3: Write minimal implementation** — edit `internal/controller/reaper.go` `orphanReason`. Replace the block:

```go
	if isTerminal(task.Status.Phase) {
		return fmt.Sprintf("task phase %s", task.Status.Phase), true
	}
	if isLifecycleTerminal(task.Status.DeployState) {
		return fmt.Sprintf("task lifecycle %s", task.Status.DeployState), true
	}
```

with:

```go
	// A phase-terminal task is reaped promptly only when it has NO lifecycle to
	// continue: an empty DeployState marks a one-shot (non-conversational) task
	// with nothing left to run. A lifecycle task that shows phase Succeeded (a
	// turn batch drained: NoPendingSubtasks) may be about to be revived by
	// resetAgentRun to continue in the SAME warm pod (front-half implement
	// handoff, conversation reactivation); phase-reaping it there kills the pod
	// mid-continuation (the blip). Its DeployState is non-empty, so this branch
	// skips it - the lifecycle-terminal branch below reaps it once genuinely
	// finished, and the idle backstop reaps it if it goes idle.
	if task.Status.DeployState == "" && isTerminal(task.Status.Phase) {
		return fmt.Sprintf("task phase %s", task.Status.Phase), true
	}
	if isLifecycleTerminal(task.Status.DeployState) {
		return fmt.Sprintf("task lifecycle %s", task.Status.DeployState), true
	}
```

- [ ] **Step 4: Run the new test + the two adjacent existing tests to verify pass and no regression**

Run: `mise exec -- go test ./internal/controller/ -run 'TestReapOrphans_TerminalPhase|TestReapOrphans_TerminalPhaseWithLiveLifecycleKept|TestReapOrphans_TerminalLifecycle|TestReapOrphans_IdleNoLiveTurn' -count=1 -v`
Expected: PASS. (`TestReapOrphans_TerminalPhase` uses an empty `DeployState`, so it still reaps; `TestReapOrphans_TerminalLifecycle` reaps via the lifecycle branch; idle backstop unchanged.)

- [ ] **Step 5: Run the full reaper + controller suite**

Run: `mise exec -- go test ./internal/controller/ -count=1`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/controller/reaper.go internal/controller/reaper_test.go
git commit -m "fix: reap wrapper pod on phase-terminal only when lifecycle is idle

A conversational task shows phase=Succeeded transiently between a turn-batch
drain and resetAgentRun; the reaper was deleting its warm pod mid-continuation
(the issue-203 pod blip). Gate the phase branch on empty DeployState; live
lifecycle tasks are left to the lifecycle-terminal branch and the idle backstop."
```

---

### Task 2: Leader-gate the maintenance ticker

**Files:**
- Modify: `internal/controller/turncallback.go` (`Start`, ~line 777) - extract ticker into `RunMaintenance`
- Modify: `cmd/manager/wire.go` (add `maintenanceRunnable`, register it, ~line 412)
- Test: `internal/controller/turncallback_maintenance_test.go` (new), `cmd/manager/wire_test.go`

**Interfaces:**
- Consumes: `CallbackServer` with fields `Session`, and methods `PollOnce(ctx)`, `ReapOrphans(ctx)`; package const `pollRequeue`.
- Produces:
  - `func (s *CallbackServer) RunMaintenance(ctx context.Context) error` - blocks on a `pollRequeue` ticker running `PollOnce` (when `Session != nil`) then `ReapOrphans`; returns `nil` on `ctx.Done()`.
  - `func (s *CallbackServer) Start(ctx context.Context, addr string) error` - now serves only the HTTP server (no ticker).
  - `maintenanceRunnable{srv *controller.CallbackServer}` in `package main` with `Start`/`NeedLeaderElection()==true`.

- [ ] **Step 1: Write the failing test** for `RunMaintenance` returning on ctx cancel (new file `internal/controller/turncallback_maintenance_test.go`)

```go
package controller

import (
	"context"
	"testing"
	"time"
)

// TestRunMaintenance_ReturnsOnCtxCancel verifies the extracted maintenance loop
// exits cleanly when its context is cancelled (no goroutine/ticker leak).
func TestRunMaintenance_ReturnsOnCtxCancel(t *testing.T) {
	s := &CallbackServer{} // fields unused: pre-cancelled ctx means the ticker body never fires
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled: the ctx.Done() select arm must win before the first tick
	done := make(chan error, 1)
	go func() { done <- s.RunMaintenance(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunMaintenance returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunMaintenance did not return on cancelled context")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mise exec -- go test ./internal/controller/ -run TestRunMaintenance_ReturnsOnCtxCancel -count=1`
Expected: FAIL (compile error: `RunMaintenance` undefined).

- [ ] **Step 3: Implement `RunMaintenance` and slim `Start`** in `internal/controller/turncallback.go`. Replace the current `Start`:

```go
// Start runs the callback HTTP server (callback + push-metrics + health) until
// ctx is done. It serves on every replica (see maintenanceRunnable for the
// leader-only poll/reap loop). Implements manager.Runnable.
func (s *CallbackServer) Start(ctx context.Context, addr string) error {
	srv := &http.Server{Addr: addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		// Use a bounded context to avoid blocking shutdown forever if an
		// in-flight handler is stuck (finding 7, mirrors webhook/server.go:823).
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("callback server: %w", err)
	}
	return nil
}

// RunMaintenance drives the periodic poll backstop and orphan reaper on a
// pollRequeue ticker until ctx is done. It is registered as a LEADER-ONLY
// manager runnable (maintenanceRunnable): only the elected leader polls for
// missed turn callbacks and reaps orphan pods, so N replicas no longer each
// run full-namespace Lists + deletes every cycle. Implements manager.Runnable.
func (s *CallbackServer) RunMaintenance(ctx context.Context) error {
	t := time.NewTicker(pollRequeue)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if s.Session != nil {
				s.PollOnce(ctx)
			}
			// Backstop the one-shot teardown: reap wrapper pods whose Task
			// is gone or terminal. Runs regardless of Session (orphans
			// outlive their session).
			s.ReapOrphans(ctx)
		}
	}
}
```

- [ ] **Step 4: Run the maintenance test to verify it passes**

Run: `mise exec -- go test ./internal/controller/ -run TestRunMaintenance_ReturnsOnCtxCancel -count=1`
Expected: PASS.

- [ ] **Step 5: Write the failing runnable-wiring test** (append to `cmd/manager/wire_test.go`)

```go
func TestMaintenanceRunnable_IsLeaderOnly(t *testing.T) {
	var m maintenanceRunnable
	if !m.NeedLeaderElection() {
		t.Error("maintenanceRunnable must require leader election (leader-only poll/reap)")
	}
	var c callbackRunnable
	if c.NeedLeaderElection() {
		t.Error("callbackRunnable must NOT require leader election (HTTP serves on every replica)")
	}
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `mise exec -- go test ./cmd/manager/ -run TestMaintenanceRunnable_IsLeaderOnly -count=1`
Expected: FAIL (compile error: `maintenanceRunnable` undefined).

- [ ] **Step 7: Add and register `maintenanceRunnable`** in `cmd/manager/wire.go`. After the existing `callbackRunnable` registration block (`mgr.Add(callbackRunnable{...})`), add a second `mgr.Add`:

```go
	if err := mgr.Add(callbackRunnable{srv: cbServer, addr: cfg.InternalAddr}); err != nil {
		return nil, fmt.Errorf("add callback server: %w", err)
	}
	if err := mgr.Add(maintenanceRunnable{srv: cbServer}); err != nil {
		return nil, fmt.Errorf("add maintenance runnable: %w", err)
	}
	return seq, nil
```

and define the type next to `callbackRunnable`:

```go
// maintenanceRunnable runs the CallbackServer's poll-backstop + orphan-reaper
// ticker. Unlike the callback HTTP server (callbackRunnable, every replica),
// this is LEADER-ONLY: NeedLeaderElection true so only the elected leader
// polls for missed turn callbacks and reaps orphan pods. When leader election
// is disabled (single replica), controller-runtime still runs it.
type maintenanceRunnable struct {
	srv *controller.CallbackServer
}

func (m maintenanceRunnable) Start(ctx context.Context) error {
	return m.srv.RunMaintenance(ctx)
}

func (m maintenanceRunnable) NeedLeaderElection() bool { return true }
```

- [ ] **Step 8: Run the wiring test + package build to verify pass**

Run: `mise exec -- go test ./cmd/manager/ -run TestMaintenanceRunnable_IsLeaderOnly -count=1`
Expected: PASS.

- [ ] **Step 9: Full build + controller + manager suites**

Run: `mise exec -- go build ./... && mise exec -- go test ./internal/controller/ ./cmd/manager/ -count=1`
Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add internal/controller/turncallback.go internal/controller/turncallback_maintenance_test.go cmd/manager/wire.go cmd/manager/wire_test.go
git commit -m "fix: run poll+reap maintenance loop on the leader only

CallbackServer.Start ran its pollRequeue ticker (PollOnce + ReapOrphans) in
every replica because the callback runnable is non-leader (the HTTP handler
must serve everywhere). Split the ticker into RunMaintenance, registered as a
leader-only runnable; the HTTP handler stays on every replica."
```

---

### Task 3: Full gate + spec/roadmap bookkeeping

**Files:**
- Modify: `MEMORY.md` (one dated line), `ROADMAP.md` (if it lists this)
- The design spec `docs/superpowers/specs/2026-07-11-reaper-phase-race-pod-blip-design.md` is already in the tree; commit it with this task.

- [ ] **Step 1: Run the full gate**

Run: `mise run generate && mise run manifests && mise exec -- gofmt -l internal cmd && mise run lint && mise run test && mise run build`
Expected: `gofmt -l` prints nothing; generate/manifests produce no diff (no API change); lint 0 issues; tests + build PASS.

- [ ] **Step 2: Add a MEMORY.md entry** (one dated line at the top of the list) summarizing: reaper phase-race pod-blip fix (empty-DeployState gate on the phase branch) + leader-gated maintenance ticker, with the issue-203 evidence and the "idle backstop covers the abandoned case" rationale.

- [ ] **Step 3: Commit the spec, plan, and bookkeeping**

```bash
git add docs/superpowers/specs/2026-07-11-reaper-phase-race-pod-blip-design.md docs/superpowers/plans/2026-07-11-reaper-phase-race-pod-blip.md MEMORY.md ROADMAP.md
git commit -m "docs: reaper phase-race pod-blip design, plan, MEMORY note"
```

## Notes for the implementer

- Do NOT change `isTerminal` (`task_controller.go:89`) - it is used by ~10 call sites; only the reaper's use of it changes.
- Do NOT touch the issue-derived pod naming (`podNameSuffix`) - out of scope; the `task-uid` label already disambiguates incarnations.
- `mise run test` runs the envtest controller suite; it needs the envtest assets `mise` provisions. If `-race` is unavailable locally (no gcc in sandbox), note it - CI runs `-race`.
