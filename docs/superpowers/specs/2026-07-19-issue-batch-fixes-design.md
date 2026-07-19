# Issue-batch fixes design (2026-07-19)

Batch fix of every confirmed defect behind the open tatara-operator issues,
verified by per-issue triage against main @ 8374da2. One PR per repo:
tatara-operator (this spec's bulk), tatara-observability (alert threshold),
tatara-helmfile (doc cron schedule). Issue #377 is already implemented by
PR #378 and is closed with a comment, not fixed again.

## WP1 - #397 skills directive keyed on wrong kind (S)

`internal/controller/assignment.go:84` passes `task.Spec.Kind` to
`skillsDirective`; the job half on line 83 uses the stage-derived `agentKind`
parameter. A clarify pod on an incident Task is told to invoke
`tatara-incident-investigation`, which its profile does not register.

Fix: `skillsDirective(agentKind)`. Regression test: for an incident-kind Task
with agentKind clarify/implement/review, `assignmentFor` must not name
`tatara-incident-investigation` and must name the agentKind's own skills.

## WP2 - #398 line=0 finding fails CRD validation, surfaces as 500 (M)

`api/v1alpha1/issue_types.go:107-114`: `ReviewFinding.Line int` carries
`Minimum=1` and is required; a file-level finding submitted without `line`
becomes 0, the apiserver rejects the status write as Invalid, and
`internal/restapi/handlers.go` `writeClientErr` turns everything but NotFound
into a bare 500. Scope was already owner-approved in the issue thread:

- `ReviewFinding.Line` becomes `*int` with `json:"line,omitempty"`, drop
  `Minimum=1` and the `required` membership, `make generate manifests`.
- `internal/restapi/outcome.go` payload/`findingsFor` keep nil when omitted;
  all read sites (review post rendering, forge comment formatting) handle nil
  as "file-level finding".
- `writeClientErr` gains an `apierrors.IsInvalid` branch returning 422 with
  the validation detail.

Tests: table-driven `writeClientErr` (NotFound/Invalid/other); outcome test
submitting a finding without `line` (write succeeds, nil) and with `line>=1`
(round-trips).

## WP3 - incident dedup key unstable across co-firing composition (S)

`internal/webhook/grafana.go` `incidentDedupKey` hashes `CommonLabels`, the
intersection of whatever alert instances co-fire in the group that
evaluation. When the member set changes, the hash changes, and the same rule
spawns a fresh full investigation despite an open tracker (#398 comment
timeline: 4 repeat investigations of rule afq61w81lyps1f ~35min apart).

Fix: key = sha256(project + rule name) only; delete the CommonLabels
contribution (the label denylist machinery for this key goes with it).
Same-rule refires while a tracker is open always take the cheap
refire-comment path; workload distinction is covered by refire comments, the
escalation valve (threshold 10 / stale 48h), and cross-rule group linking.
Update the doc comment; record the collapse trade-off in MEMORY.md.

Tests: two webhook deliveries of the same rule with different co-firing
member sets must produce the same dedup key (regression for the observed
leak); different rules still differ.

## WP4 - #394 GitLab inline discussion 400 retried forever (M)

Two compounding defects in `internal/scm/`:

- `gitlab_review.go:239-255` builds `position` naively (always `new_line`,
  never `old_line`, same path both sides). Any finding GitLab cannot anchor
  to a diff hunk answers 400 "line_code can't be blank".
- `github.go:889-904` `classifyReviewPostError` treats 400 as retryable, so
  `reviewpost.go` requeues the identical POST forever; and PostReview stops
  at the first error, so one bad finding blocks the whole round.

Fix, three layers:
1. Fetch the MR diff once per PostReview (alongside `glDiffRefsOf`), resolve
   each finding against real hunk ranges; unanchorable findings degrade to a
   non-inline note (WARN + metric) instead of failing the round.
2. Classify structural 4xx from the discussions POST (not rate limits) as
   terminal `scm.ErrReviewRefused`, routing to the existing
   parked/ReasonReviewPostRefused path.
3. Per-finding non-fatal posting: catch-and-continue, still post the round
   body note.

Tests: unanchorable-line finding degrades gracefully; 400 "line_code" maps to
ErrReviewRefused alongside the 401/403/422 cases; reviewpost routes it to
parked instead of returning a raw error.

## WP5 - #393 unpark re-enters reviewing on merged MRs (S)

`internal/stage/stage.go` `Unpark`: `ReasonHandoffStalled` (~1040) and the
`kindReview` branch of `ReasonAwaitingHuman` (~950) re-enter `StageReviewing`
without checking `in.MRs` for merged state. The agent then has no legal
outcome ("this task owns no open MR" 400) and the Task is trapped.

Fix: reuse the existing `anyMerged` helper (stage.go:1031) at the top of both
sites. When merged: route to the review-kind terminal stage mirroring
`advanceAfterReview`'s edge; if that edge cannot be derived from Unpark's
inputs alone, return no-unpark (stay parked). Tests: table cases in
stage_test.go - merged MR present must not yield StageReviewing; without a
merged MR, behavior unchanged.

## WP6 - #392 GC blocked forever by doc_reference on docs-less projects (M)

`internal/controller/docbatch.go:245-262` `needsDocumenting` never checks
whether the owning Project actually runs doc batching; a project with
`documentation.enabled` but no cron schedule structurally never mints a
batch, so delivered Tasks with merged MRs block GC forever.

Fix: thread `proj` into `needsDocumenting(ctx, proj, t)` (both callers have
it in scope); early-return false when `Documentation` is nil/disabled/empty
repo or `Cron.Documentation.Schedule` is empty - symmetric to the existing
zero-merged-MR exemption. Add an INFO log at the `doc_reference` Inc site in
reaper.go naming Task + MR states. Backfill is automatic on the next sweep.

Tests: docs disabled / empty schedule -> not blocked; fully configured ->
blocked (regression); zero merged MRs unaffected.

Companion (tatara-helmfile): add `cron.documentation.schedule: "0 3 * * *"`
to `values/project-tatara/common.yaml` - the enabled+repo config intent was
docs-on; the schedule omission is the oversight.

## WP7 - #386 sweep heartbeat gauge dies on every redeploy (M)

`internal/obs/sweep_metrics.go` `SweepLastSuccessTimestamp` is process-local
and only stamped when a pass freshly runs; the issueScan cron (0 */4 * * *)
is slower than the push-CD redeploy cadence, so a pod almost never re-stamps
before it is replaced. The alert fires on a false liveness proxy while
`Status.LastIssueScan` in etcd tracks real progress. The 00:00:10Z wedge
hypothesis in the issue is a red herring.

Fix: on every runScans reconcile, rehydrate the `issueScan` and `sweep`
gauges from `proj.Status.LastIssueScan`, and `brainstorm`/`documentation`
from their persisted stamps, before any due-check. Correct the
success-only-stamping doc comment.

Tests: nil stamp -> gauge unset (true NoData); non-nil stamp with zero due
repos -> gauge reflects persisted value.

Companion (tatara-observability): raise rule ffrz32fcqvta8d "Operator sweep
heartbeat stale" threshold 7200s -> 21600s (6h), above the 4h cron.

## WP8 - #369 SeverOrphan drops controller owner without handover (S)

`internal/controller/sever.go:111` `SeverOrphan` unconditionally
`dropOwnerRef`s, leaving a CR with a plain owner but no controller owner when
another live Task still owns it (B.2 rule-5 violation, masked at runtime by
`RepairZeroController`). Fix: mirror `reaper.go` `release()` - resolve
`own.ControllerOwner`, find `own.OldestSurvivingOwner` (inline Get-based
liveness, as `RepairZeroController` does), `own.HandOverController` when a
survivor exists, `dropOwnerRef` only when none does. Update the sever.go doc
comment. Test: two-owner Issue severed -> surviving Task promoted, no
zero-controller state; single-owner path unchanged.

## WP9 - #395 alert admission gap across rollouts (M)

`DispatcherReconciler` is watch-driven with one worker and no
leadership-acquired backstop; `LeaderElectionReleaseOnCancel` is unset so the
outgoing leader holds its lease through SIGTERM; client-go default QPS=5
slows cold-start cache fill. Rollout bursts left a 7m22s alert-admission gap.

Fix:
1. `cmd/manager/main.go`: `LeaderElectionReleaseOnCancel: true`; raise REST
   client QPS/Burst to 50/100.
2. New leader-only runnable (pattern: `maintenanceRunnable` in wire.go) that
   on start and every 60s lists Projects and enqueues a dispatcher Reconcile
   per project, making admission deterministic instead of replay-timing luck.

Tests: hook admits a queued alert-class event with no watch trigger; ticker
fires independent of events.

## WP10 - #367 hot Project.Reconcile amplification (M)

Every Reconcile pass re-runs all per-pass blocks; three do full namespace
Lists each time (`resumeNoReentryParks`, `ReapTerminal`,
`computeProjectCounts`) while watch events on owned memory-stack resources
plus the 10s provisioning requeue drive sub-10s pass cadence.

Fix: replicate the `driveUnparksPaced` pattern (per-project lastRun map +
min interval, default 60s) for those three blocks. Keep the 10s memory
requeue (it is the readiness poll) and do NOT add status-filtering watch
predicates - CNPG readiness transitions arrive via status churn; pacing the
expensive blocks removes the amplification without deafening the watch.

Tests: paced block executes once per interval across repeated Reconcile
calls; list-call counts stay bounded under a rapid trigger string.

## Out of scope

- #377: implemented by #378 (correlation, escalation, comment_issue). Action:
  close the issue with a crediting comment - no code.
- comment_issue append rate-limit (idea surfaced in #398 triage): genuinely
  new design, noted for a follow-up issue, not silently included.

## Delivery

Branch `fix-issue-batch` in all three repos. Operator PR carries
change_significance minor (CRD schema loosening). Observability and helmfile
PRs are one-line-class, patch. PRs cross-linked. Platform review loop and
merge per standing start-development flow.
