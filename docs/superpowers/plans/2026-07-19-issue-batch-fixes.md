# Issue-batch fixes implementation plan (2026-07-19)

Executes the approved design at
`docs/superpowers/specs/2026-07-19-issue-batch-fixes-design.md`. TDD order per
task: failing test first, then implementation, then the per-task verification
command. All line numbers below were re-verified against the worktree code and
corrected where the spec drifted (see "Spec corrections" at the end).

## Affected repos

- **tatara-operator** - WP1..WP10 (this worktree's bulk). Branch `fix-issue-batch`.
  Full gate: `make generate manifests test lint build`.
- **tatara-observability** - one alert-threshold change (WP7 companion). Edit
  YAML only under `alerts/`.
- **tatara-helmfile** - one cron-schedule addition (WP6 companion).

All three worktrees are under
`/Users/szymonri/Documents/tatara-new/.worktrees/fix-issue-batch/<repo>` on
branch `fix-issue-batch`.

## Parallelization map

Wave 1 - fully independent, dispatch in parallel:

- **WP1** (assignment.go) | **WP2** (issue_types.go + outcome.go + handlers.go)
  | **WP3** (grafana.go + server.go + config.go) | **WP4** (gitlab_review.go +
  github.go + reviewpost.go) | **WP5** (stage.go) | **WP9** (main.go + wire.go +
  queue_controller.go)
- **T-OBS** (tatara-observability alert) and **T-HELM** (tatara-helmfile cron) -
  different repos, independent of everything.

Wave 2 - controller package, `reaper.go` and `ProjectReconciler` overlaps.
Do NOT run these in parallel with each other where they share a file:

- **`reaper.go` is edited by WP6 (INFO log at the `doc_reference` Inc site,
  ~line 485) and by WP10 (ReapTerminal pacing at the caller).** Serialize:
  land **WP6 first, then WP10**. WP8 only *reads* `reaper.go`'s `release()` /
  `dropOwnerRef` as a pattern; it edits `sever.go` and can run alongside either.
- **`ProjectReconciler` struct (project_controller.go) is edited by WP10**
  (adds per-block `lastRun` maps); **WP7 edits projectscan.go** (`runScans`) in
  the same package but touches neither the struct nor project_controller.go.
  Land WP10's struct change, then WP7, or run WP7 and WP10 by one owner. WP7 and
  WP8 are file-disjoint from each other and from WP6, so WP7+WP8 may run in
  parallel; keep WP6/WP10 serialized on `reaper.go`.

Recommended: Wave-1 subagents in one dispatch; Wave-2 as `WP6 -> WP10` serial
with `WP7`, `WP8` parallel around them.

---

## WP1 - #397 skills directive keyed on wrong kind

**Files:** `internal/controller/assignment.go`.
**Parallel-group:** Wave 1, standalone.

`assignmentFor` (line 76) builds the job from `agentKind` (line 83:
`agentJob(agentKind)`) but the skills directive on line 84 reads
`task.Spec.Kind`. On an incident Task whose current stage is clarify, `agentKind`
is `clarify` but `Spec.Kind` is `incident`, so the agent is told to invoke
`tatara-incident-investigation`, absent from the clarify profile.

**Test first** (`internal/controller/assignment_test.go`): table over
agentKind in {clarify, implement, review} with `task.Spec.Kind = "incident"`.
Assert `assignmentFor(agentKind, task)` does NOT contain
`tatara-incident-investigation` and DOES contain the agentKind's own required
skill (e.g. `tatara-clarify-conversation` for clarify,
`tatara-implement-workflow` for implement). Run - fails today (incident skill
leaks into every kind).

**Implementation:** line 84, change `skillsDirective(task.Spec.Kind)` to
`skillsDirective(agentKind)`. One-line change.

**Verify:** `mise exec -- go test ./internal/controller/ -run Assignment`.

---

## WP2 - #398 line=0 finding fails CRD validation, surfaces as 500

**Files:** `api/v1alpha1/issue_types.go`, `internal/restapi/outcome.go`,
`internal/restapi/handlers.go`, generated CRD/deepcopy.
**Parallel-group:** Wave 1, standalone. (Requires `make generate manifests`.)

`ReviewFinding` (issue_types.go lines 107-115): `Line int` on lines 109-110
carries `+kubebuilder:validation:Minimum=1` and a bare `json:"line"` (required).
A file-level finding submitted without a line becomes 0, the apiserver rejects
the status write as Invalid, and `writeClientErr` (handlers.go line 42) collapses
every non-NotFound error to a bare 500.

**Tests first:**
1. `internal/restapi/handlers_test.go`: table-driven `writeClientErr` -
   `apierrors.NewNotFound(...)` -> 404; `apierrors.NewInvalid(...)` -> 422 with
   the validation detail in the body; a generic error -> 500. Fails today
   (Invalid -> 500).
2. `internal/restapi/outcome_test.go` (review path): submit a `request_changes`
   finding with no `line` (envelope omits it) -> write succeeds, persisted
   `ReviewFinding.Line == nil`; submit with `line: 5` -> round-trips to `*5`.
   Fails today (Invalid rejection / int zero).

**Implementation:**
- issue_types.go: `Line int` -> `Line *int` with `json:"line,omitempty"`; drop
  the `+kubebuilder:validation:Minimum=1` marker. (Field stays optional.)
- outcome.go: `reviewFindingPayload.Line` (line 76) -> `*int`
  `json:"line,omitempty"`; `findingsFor` (line 1046-1057) copies the pointer
  through unchanged (`Line: f.Line`). Any read site that formats a line for a
  forge comment or the review-post body treats nil as "file-level finding"
  (no `:line` suffix) - grep `\.Line` across `internal/restapi` and
  `internal/scm` review-post rendering and guard each deref.
- handlers.go `writeClientErr`: add, before the generic 500,
  `if apierrors.IsInvalid(err) { writeError(w, http.StatusUnprocessableEntity, err.Error()) ; return }`.
- `make generate manifests` to regenerate CRD YAML + `zz_generated.deepcopy.go`
  for the `*int` field.

**Note (WP4 dependency):** WP4 also reads `ReviewFinding.Line`; the `*int`
change lands the nil-handling contract WP4's GitLab anchoring relies on. If both
run in parallel, WP2 owns the type change and WP4 consumes it - coordinate the
`ReviewFinding.Line` signature.

**Verify:** `mise exec -- go test ./internal/restapi/... ./api/...` then
`make generate manifests` (clean tree after).

---

## WP3 - incident dedup key unstable across co-firing composition

**Files:** `internal/webhook/grafana.go`, `internal/webhook/server.go`,
`internal/config/config.go`, `internal/webhook/grafana_dedup_test.go`.
**Parallel-group:** Wave 1, standalone.

`incidentDedupKey` (grafana.go line 112) hashes `project + alertRuleName +
sortedKV(stable CommonLabels)`. `CommonLabels` is the intersection of whatever
instances co-fire that evaluation; when the member set changes the hash changes
and the open tracker is bypassed, spawning a fresh investigation (#398: 4
repeats of one rule ~35 min apart).

**Test first** (grafana_dedup_test.go): two `GrafanaAlert`s, same project + same
`alertname`, DIFFERENT `CommonLabels` member sets (e.g. one adds a `severity`
common label, one drops it) must produce the SAME `incidentDedupKey`. A
different `alertname` must still differ. Fails today (member-set churn -> key
churn). Keep the existing #320-vs-#328 collapse case green.

**Implementation:**
- grafana.go: `incidentDedupKey(a, project)` drops the `denylist` parameter and
  the `stable`-labels loop; body becomes
  `h := sha256.Sum256([]byte(project + "\x00" + alertRuleName(a)))` (keep the
  16-hex truncation). Update the doc comment (lines 105-111) to state the key is
  project + rule name only, and why (co-firing composition instability).
- Delete the now-dead denylist machinery for this key: `defaultVolatileDenylist`
  (lines 87-90), `denylistSet` (lines 94-103). `correlationSet` /
  `incidentGroupKey` are for the SEPARATE group key - leave them.
- server.go: drop the `dedupDenylist` field and the `denylistSet(...)` init at
  line 124; the call at line 853 becomes `incidentDedupKey(alert, proj.Name)`.
- config.go: remove `IncidentDedupVolatileLabels` (lines 198, 579) and its
  `INCIDENT_DEDUP_VOLATILE_LABELS` `getCSVList` load; drop the corresponding
  `config_test.go` case (line 589).
- Update `grafana_dedup_test.go` / `incident_admission_test.go` /
  `incident_correlation_test.go` call sites to the two-arg signature.

**MEMORY.md:** one dated line recording the collapse trade-off (same-rule
different-workload refires now share one tracker; workload distinction covered
by refire comments + the escalation valve threshold 10 / stale 48h + cross-rule
group linking).

**Verify:** `mise exec -- go test ./internal/webhook/... ./internal/config/...`.

---

## WP4 - #394 GitLab inline discussion 400 retried forever

**Files:** `internal/scm/gitlab_review.go`, `internal/scm/github.go`,
`internal/controller/reviewpost.go`, tests
`internal/scm/postreview_test.go` (+ a gitlab_review test),
`internal/controller/reviewpost_test.go`.
**Parallel-group:** Wave 1, standalone. Consumes WP2's `ReviewFinding.Line *int`.

Two compounding defects:
- `postDiscussion` (gitlab_review.go lines 239-255) always sends `new_line:
  f.Line` with `new_path == old_path == f.Path` and no `old_line`; any finding
  GitLab cannot anchor to a diff hunk answers 400 "line_code can't be blank".
- `classifyReviewPostError` (github.go lines 889-904) switches only
  401/403/422 to terminal `ErrReviewRefused`; **400 falls through to the
  retryable default**, so `reviewpost.go` (line 145 routes `ErrReviewRefused`
  to `ReasonReviewPostRefused`; otherwise line 166 returns a plain error that
  hot-requeues) loops the identical POST forever. `PostReview` (gitlab_review.go
  line 214) also returns on the first `postDiscussion` error, so one bad finding
  blocks the whole round.

**Tests first:**
1. `postreview_test.go`: a 400 `HTTPError` carrying "line_code" maps to
   `ErrReviewRefused` (alongside the existing 401/403/422 cases); a rate-limit
   403 stays retryable (existing case stays green). Fails today (400 stays
   retryable).
2. gitlab_review test: a finding whose `Line` cannot be anchored to a fetched
   hunk range degrades to a non-inline note (a `postMRNote`, WARN + metric) and
   the round still completes; PostReview does NOT abort on it.
3. `reviewpost_test.go`: `PostReview` returning `ErrReviewRefused` drives the
   Task to `parked / ReasonReviewPostRefused`, not a raw requeue error.

**Implementation, three layers (per spec):**
1. gitlab_review.go: in `PostReview` (line 165), after `glDiffRefsOf`, fetch the
   MR diff once (new `glDiffOf` / reuse discussions listing) and build a hunk
   lookup. Resolve each finding's `(path, line)` against real new-side hunk
   ranges; anchorable -> `postDiscussion`, unanchorable -> a plain note carrying
   the finding body (`postMRNote`) + `slog.Warn` + a degrade metric. Handle the
   WP2 nil `Line` as file-level -> always the non-inline path.
2. github.go: add `http.StatusBadRequest` to the terminal case in
   `classifyReviewPostError` (line 894-900), keeping the `ErrRateLimited`
   escape.
3. gitlab_review.go: per-finding non-fatal posting - the loop (lines 206-223)
   catches each `postDiscussion` error, logs, continues, and still posts the
   round body note; only a structural 4xx classified terminal aborts.

**Verify:** `mise exec -- go test ./internal/scm/... ./internal/controller/ -run Review`.

---

## WP5 - #393 unpark re-enters reviewing on merged MRs

**Files:** `internal/stage/stage.go`, `internal/stage/stage_test.go`.
**Parallel-group:** Wave 1, standalone.

In `Unpark` (stage.go line 911) two sites re-enter `StageReviewing` without
checking merged state:
- the `kindReview` branch of `ReasonAwaitingHuman` (lines 950-966),
- `ReasonHandoffStalled` (lines 1040-1062).

A pod spawned into reviewing on an already-merged MR has no legal outcome
(`postOutcome` review answers "this task owns no open MR", outcome.go line 851),
so the Task is trapped.

**Decision on behavior (per task brief): refuse re-entry when any MR is merged.**
Rationale, verified from the code: the review-terminal edge is computed by
`reviewAdvanceEdge` (reviewpost.go line 343), which lives in **package
`controller`**; `controller` imports `stage`, so `stage.Unpark` cannot call it
(import cycle) and the edge is not reusable inside Unpark. The "correct" terminal
for a merged MR is genuinely ambiguous (a human-merged review PR vs an
operator-merged delivery), and routing to merging risks re-running deploy side
effects. Refuse-re-entry exactly mirrors the existing `ReasonNoOutcome` guard at
stage.go line 1031 (`if anyMerged(in.MRs) { return "", false }`): the Task ages
out at `parkRetention` and is reaped, which is the right terminal for a Task
whose MR already shipped. `anyMerged` is defined at stage.go line 1138.

**Test first** (stage_test.go): table cases -
- `ReasonAwaitingHuman`, `Spec.Kind = review`, a merged MR in `in.MRs`, a
  non-bot pending event -> `Unpark` returns `("", false)` and does NOT touch
  `HumanReviewRounds`. Without a merged MR (open MR) -> unchanged
  (`StageReviewing`, `HumanReviewRounds++`).
- `ReasonHandoffStalled`, a merged MR + non-bot event -> `("", false)`; without a
  merged MR -> unchanged (`StageReviewing`).

Fails today (both yield `StageReviewing` on merged MRs).

**Implementation:** at the top of the `kindReview` block (after the
`hasNonBotEvent` check, before the `HumanReviewRounds` cap, ~line 958) add
`if anyMerged(in.MRs) { return "", false }`. Same guard at the top of the
`ReasonHandoffStalled` case (after `hasNonBotEvent`, ~line 1054). Keep scope to
these two sites (the non-review `ReasonAwaitingHuman` path is out of scope per
spec).

**Verify:** `mise exec -- go test ./internal/stage/ -run Unpark`.

---

## WP6 - #392 GC blocked forever by doc_reference on docs-less projects

**Files:** `internal/controller/docbatch.go`, `internal/controller/reaper.go`,
tests `internal/controller/docbatch_test.go` / `reaper_test.go`.
**Parallel-group:** Wave 2. **Shares `reaper.go` with WP10 - land WP6 first.**

`needsDocumenting` (docbatch.go lines 245-262) never checks whether the owning
Project actually runs doc batching. A project with `documentation.enabled` but
no cron schedule structurally never mints a batch, so delivered Tasks with merged
MRs are pinned by the `doc_reference` GC block forever (reaper.go line 485,
`obs.GCBlockedDocReference`). Callers: docbatch.go line 68, reaper.go line 480.

**Test first:** `needsDocumenting` (via both callers) -
- docs disabled (`Documentation == nil` / `!Enabled` / empty `Repo`) -> false
  (not blocked).
- docs enabled + repo set but `Cron.Documentation.Schedule == ""` -> false.
- fully configured (enabled + repo + non-empty schedule) + all MRs merged ->
  true (regression: still blocked).
- zero merged MRs -> false (unaffected).

Fails today (empty-schedule project returns true and pins the Task).

**Implementation:**
- Thread `proj` into `needsDocumenting(ctx, proj, t)` (both callers have it:
  docbatch.go's `mintDocBatch` scope, reaper.go `reapDelivered` param).
- Early-return false when `proj.Spec.Documentation` is nil / `!Enabled` /
  `Repo == ""` OR `proj.Spec.Scm.Cron.Documentation.Schedule == ""` - symmetric
  to the existing zero-merged-MR exemption (lines 253-260). Guard nil `Scm` /
  `Cron` on the path.
- reaper.go: at the `doc_reference` Inc site (line 485) add an INFO log naming
  the Task and its MR merge states (`action: reap_blocked`, reason
  `doc_reference`), so a future pin is diagnosable.

Backfill is automatic on the next sweep.

**Verify:** `mise exec -- go test ./internal/controller/ -run 'Doc|Reap'`.

---

## WP7 - #386 sweep heartbeat gauge dies on every redeploy

**Files:** `internal/controller/projectscan.go`, tests
`internal/controller/projectscan_test.go` (new sweep-gauge test).
**Parallel-group:** Wave 2. Same package as WP10; edits projectscan.go only
(not the ProjectReconciler struct) - coordinate compile with WP10.

`SweepLastSuccessTimestamp` (sweep_metrics.go line 49) is process-local and only
stamped when a pass freshly runs (`stampScan` line 474; sweep.go line 633). The
issueScan cron (`0 */4 * * *`) is slower than the push-CD redeploy cadence, so a
new pod almost never re-stamps before replacement; the alert fires on a false
liveness proxy while `Status.LastIssueScan` in etcd tracks real progress.

**Test first:** in `runScans` (projectscan.go line 1066) -
- `proj.Status.LastIssueScan == nil` -> the `issueScan`/`sweep` gauges are left
  unset (true NoData; assert no Set call / gauge value 0 for a never-scanned
  project). Use a registry probe or a seam over `SweepLastSuccessTimestamp`.
- `LastIssueScan` non-nil with zero due repos -> `issueScan` and `sweep` gauges
  reflect the persisted `LastIssueScan.Unix()` (rehydrated, not left stale).
- `LastBrainstorm` / `LastDocumentation` non-nil -> their gauges rehydrate.

Fails today (no rehydrate; gauge only set on a fresh stamp).

**Implementation:** at the top of `runScans` (after the early `return 0, nil`
guard, before any due-check, ~line 1072) rehydrate from persisted stamps:
```
if proj.Status.LastIssueScan != nil {
    ts := float64(proj.Status.LastIssueScan.Unix())
    obs.SweepLastSuccessTimestamp.WithLabelValues("issueScan").Set(ts)
    obs.SweepLastSuccessTimestamp.WithLabelValues(SweepActivity).Set(ts) // "sweep"
}
if proj.Status.LastBrainstorm != nil {
    obs.SweepLastSuccessTimestamp.WithLabelValues("brainstorm").Set(float64(proj.Status.LastBrainstorm.Unix()))
}
if proj.Status.LastDocumentation != nil {
    obs.SweepLastSuccessTimestamp.WithLabelValues("documentation").Set(float64(proj.Status.LastDocumentation.Unix()))
}
```
Correct the `SweepLastSuccessTimestamp` doc comment (sweep_metrics.go lines
34-52) and the `stampScan` doc comment (projectscan.go lines 444-452) to note
the gauge is now rehydrated from persisted stamps on every reconcile, not only
success-stamped.

**Verify:** `mise exec -- go test ./internal/controller/ -run 'Scan|SweepGauge'`.

**Companion - tatara-observability (T-OBS below).**

---

## WP8 - #369 SeverOrphan drops controller owner without handover

**Files:** `internal/controller/sever.go`, tests
`internal/controller/sever_test.go`.
**Parallel-group:** Wave 2. Edits `sever.go` only (reads `reaper.go` /
`own` as pattern) - parallel-safe with WP7; file-disjoint from WP6/WP10.

`SeverOrphan` (sever.go line 111) unconditionally `dropOwnerRef`s, leaving a CR
with a plain owner but no controller owner when another live Task still owns it
(B.2 rule-5 violation, masked at runtime by `RepairZeroController`).

**Test first** (sever_test.go): an Issue owned by two Tasks (Task-A controller,
Task-B plain owner, both live). `SeverIssueFromTask(..., taskA, SeverOrphan)`:
the surviving Task-B is promoted to `controller=true` (via
`own.ControllerOwner`), NO zero-controller state, no bare drop. Single-owner path
(only Task-A) -> `dropOwnerRef` unchanged (leaves an ownerless orphan for
re-adoption). Fails today (two-owner case drops the controller flag).

**Implementation:** in the `SeverOrphan` branch (lines 105-140), inside the
`RetryOnConflict` after `Get`, mirror `reaper.go` `release()` (line 703) and
`RepairZeroController` (own.go line 154):
1. `owner, ok := own.ControllerOwner(&iss)`; if `!ok || owner != task.Name`,
   leave the ref handling to the existing drop (no controller flag to hand over).
2. Build a `live map[string]bool` of the Issue's OTHER Task owners via inline
   `Get` (NotFound -> false, ok -> true), exactly as `RepairZeroController`
   lines 172-184 do.
3. `heir, hasHeir := own.OldestSurvivingOwner(&iss, live)`; if `hasHeir`,
   `own.HandOverController(&iss, task, &Task{ObjectMeta:{Name: heir, ...}})`
   then `Update`; else `dropOwnerRef(&iss, task.Name)` then `Update`.
Keep the subsequent tatara-parked label strip (lines 122-137) unchanged. Update
the sever.go doc comment (lines 26-52) to record the handover-before-drop rule.

**Verify:** `mise exec -- go test ./internal/controller/ -run Sever`.

---

## WP9 - #395 alert admission gap across rollouts

**Files:** `cmd/manager/main.go`, `cmd/manager/wire.go`,
`internal/controller/queue_controller.go`, tests `cmd/manager/main_test.go`,
`cmd/manager/wire_test.go`, `internal/controller/queue_controller_test.go`.
**Parallel-group:** Wave 1, standalone.

`DispatcherReconciler` (queue_controller.go line 33) is watch-driven, one worker,
no leadership-acquired backstop; `LeaderElectionReleaseOnCancel` is unset (the
outgoing leader holds its lease through SIGTERM); client-go default QPS=5 slows
cold-start cache fill. Rollout bursts left a 7m22s alert-admission gap.

**Tests first:**
1. `main_test.go`: `managerOptions(cfg, scheme)` (main.go line 47) returns
   `LeaderElectionReleaseOnCancel: true`. `buildManager` path sets the REST
   config QPS=50 / Burst=100 (assert via a seam over the `rest.Config` the
   manager is built from). Fails today.
2. `queue_controller_test.go`: a leader-only backstop enqueues a Reconcile for a
   queued/pending `QueuedEvent` with no watch trigger fired; the ticker fires
   independently of watch events.

**Implementation:**
- main.go: add `LeaderElectionReleaseOnCancel: true` to `manager.Options` (line
  48-66). Raise REST QPS/Burst: in `buildManager` (line 69), take
  `cfg := ctrl.GetConfigOrDie()`, set `cfg.QPS = 50; cfg.Burst = 100`, pass to
  `ctrl.NewManager(cfg, ...)`.
- New leader-only runnable following the `maintenanceRunnable` pattern
  (**cmd/manager/wire.go lines 564-577**, `NeedLeaderElection() bool { return
  true }`): on `Start` and every 60s it lists Projects and, for each, lists its
  queued `QueuedEvent`s and pushes a `reconcile.Request` per pending QE into a
  `source.Channel` wired into `DispatcherReconciler.SetupWithManager`
  (queue_controller.go line 845, add `.WatchesRawSource(source.Channel(ch,
  &handler.EnqueueRequestForObject{}))` or a generic-event channel). Note: the
  reconcile key is a QueuedEvent (Reconcile line 653 Gets `req.NamespacedName`
  as a QE), so "per project" enqueue means iterating each Project's pending QEs.
  Register the runnable via `mgr.Add(...)` alongside `maintenanceRunnable` (wire
  line 544). Make admission deterministic instead of replay-timing luck.

**Verify:** `mise exec -- go test ./cmd/manager/... ./internal/controller/ -run 'Dispatcher|Manager|LeaderElection'`.

---

## WP10 - #367 hot Project.Reconcile amplification

**Files:** `internal/controller/project_controller.go`,
`internal/controller/resume.go`, `internal/controller/reaper.go`, tests
`internal/controller/*_test.go` (pacing tests, mirror `unpark_pacing_test.go`).
**Parallel-group:** Wave 2. **Shares `reaper.go` with WP6 (land WP6 first) and
the ProjectReconciler struct with WP7's package.**

Every Reconcile pass re-runs all per-pass blocks; three do full namespace Lists
each time - `computeProjectCounts` (project_controller.go line 317, called line
199), `resumeNoReentryParks` (resume.go line 32, called line 268),
`ReapTerminal` (reaper.go line 324, called line 279) - while owned-memory-stack
watch events plus the 10s provisioning requeue drive sub-10s pass cadence.

**Test first** (new `*_pacing_test.go`, pattern from `unpark_pacing_test.go`):
for each of the three paced wrappers, calling it twice inside the min interval
executes the underlying block ONCE and returns the remaining interval as the
requeue; a third call past the floor executes again. Assert list-call counts
stay bounded under a rapid trigger loop. Fails today (runs every pass).

**Implementation:** replicate the `driveUnparksPaced` pattern (unpark.go line
165: `r.UnparkDriveInterval` + `r.lastDriveUnparks map[string]time.Time`, keyed
per project, default `defaultUnparkDriveInterval` = 60s):
- Add three per-project `lastRun` maps to the `ProjectReconciler` struct
  (project_controller.go, near line 143) and a shared/default 60s min interval
  (reuse a `default...Interval` const each).
- Wrap each block in a `...Paced(ctx, proj, now) (time.Duration, error)` that
  short-circuits within the interval (returning `interval - elapsed`) and folds
  its requeue into `soonestRequeue` at the call sites (lines 199, 268, 279).
  `computeProjectCounts` currently returns void - its paced wrapper returns the
  requeue and, when skipped, leaves the prior `project.Status` counts (the
  persist block re-applies the values read at the top of Reconcile, a no-op).
- Do NOT add status-filtering watch predicates and KEEP the 10s memory requeue
  (it is the CNPG readiness poll) - pacing the expensive blocks removes the
  amplification without deafening the watch.

**Verify:** `mise exec -- go test ./internal/controller/ -run 'Pacing|Reconcile'`.

---

## T-OBS - tatara-observability sweep heartbeat threshold (WP7 companion)

**File:**
`/Users/szymonri/Documents/tatara-new/.worktrees/fix-issue-batch/tatara-observability/alerts/tatara-operator.yaml`.
**Parallel-group:** Wave 1, independent repo.

The design cites Grafana rule uid `ffrz32fcqvta8d`; that uid is Grafana-assigned
and is NOT stored in the YAML. The editable rule is **"Operator sweep heartbeat
stale"** (tatara-operator.yaml, `threshold: 7200` at line 924, expr
`time() - operator_sweep_last_success_timestamp_seconds{...}`). Raise
`threshold: 7200` -> `threshold: 21600` (6h, above the 4h issueScan cron).

**Do NOT touch** `tatara-cd.yaml` line 41 (also `threshold: 7200`) - that is a
different CD rule.

**Verify:** confirm the single edited line is under the "Operator sweep heartbeat
stale" rule; no terraform run locally (CI plans on PR).

---

## T-HELM - tatara-helmfile documentation cron (WP6 companion)

**File:**
`/Users/szymonri/Documents/tatara-new/.worktrees/fix-issue-batch/tatara-helmfile/values/project-tatara/common.yaml`.
**Parallel-group:** Wave 1, independent repo.

Verified: `scm.cron` (line 106) has `brainstorm`, `issueScan`, `refine` but NO
`documentation` key; top-level `documentation` (enabled + repo) is set at line
64 - so the intent was docs-on and the schedule omission is the oversight (the
exact WP6 defect). Add a `documentation` block under `scm.cron`, matching the
neighboring key shape (8-space key, 10-space `schedule:` scalar, same as
`brainstorm`/`issueScan`):
```
        documentation:
          schedule: 0 3 * * *
```

**Verify:** `yamllint` / `helmfile -e default template` (CI diffs on PR); confirm
2-space nesting matches `brainstorm`/`issueScan`.

---

## Final integration task

1. In the tatara-operator worktree run the full gate:
   `make generate manifests test lint build`. The tree must be clean after
   `make generate manifests` (WP2 CRD/deepcopy regen committed).
2. **MEMORY.md** (tatara-operator) - add dated lines:
   - WP3: the incident-dedup collapse trade-off (key is now project + rule name
     only; same-rule different-workload refires share one tracker; workload
     distinction rests on refire comments + escalation valve + group linking).
   - WP7: the doc-comment correction rationale for `SweepLastSuccessTimestamp`
     (gauge rehydrated from persisted `Status.Last*Scan` on every reconcile;
     success-only stamping was a false liveness proxy across redeploys).
3. **ROADMAP.md** - if a phase/line-item covers this issue-batch, mark it done;
   otherwise no change (batch fix, not a roadmap phase).
4. Out-of-scope handling (no code): close #377 with a crediting comment (done by
   #378); file the comment_issue append rate-limit idea as a new follow-up issue.

Delivery: operator PR `change_significance` minor (CRD schema loosening);
observability + helmfile PRs patch. PRs cross-linked; review loop + merge per the
standing start-development flow.

---

## Spec corrections (verified against the worktree)

1. **WP2 line refs:** the `ReviewFinding` struct is lines **107-115** (spec said
   107-114); `Line int` + `Minimum=1` is on lines **109-110**. `writeClientErr`
   is handlers.go **line 42** (spec gave no line).
2. **WP3 scope:** dropping the `CommonLabels` contribution also removes
   `defaultVolatileDenylist` + `denylistSet` (grafana.go 87-103), the
   `dedupDenylist` field + init (server.go 124) and its call at 853, and
   `IncidentDedupVolatileLabels` / `INCIDENT_DEDUP_VOLATILE_LABELS`
   (config.go 198, 579; config_test.go 589). `correlationSet` / `incidentGroupKey`
   are the SEPARATE group key and stay.
3. **WP5 helper/decision:** `anyMerged` is *defined* at stage.go **line 1138**;
   the existing *use* the spec means (the precedent) is at **line 1031**
   (`ReasonNoOutcome`). The two edit sites are the `kindReview` branch at
   **950** and `ReasonHandoffStalled` at **1040** - both verified. Decision:
   **refuse re-entry** on merged, because `reviewAdvanceEdge` lives in package
   `controller` (reviewpost.go 343) and `stage.Unpark` cannot import it (import
   cycle: controller imports stage); refuse-re-entry mirrors the line-1031
   precedent.
4. **WP6 Inc site:** the `doc_reference` counter is `obs.GCBlockedDocReference`
   at reaper.go **line 485** (the INFO log lands there). `needsDocumenting` is
   docbatch.go **245-262** as stated; callers docbatch.go 68 + reaper.go 480.
5. **WP7 companion:** rule uid `ffrz32fcqvta8d` is not in the YAML; edit the
   "Operator sweep heartbeat stale" rule at
   `alerts/tatara-operator.yaml` **line 924** (`threshold: 7200`). `tatara-cd.yaml`
   line 41 has the same value but is a different rule - leave it.
6. **WP9 pattern location:** `maintenanceRunnable` is in **cmd/manager/wire.go**
   (lines 564-577), not `internal/controller/wire.go` (which does not exist).
   `DispatcherReconciler.Reconcile` keys on a `QueuedEvent`, so the backstop
   enqueues per pending QE (the spec's "per project" = iterate each Project's
   pending QEs).
7. **WP6 companion (helmfile):** confirmed there is no `cron.documentation` key
   today; neighbors use camelCase keys with a nested `schedule:` scalar at
   8-/10-space indent.
