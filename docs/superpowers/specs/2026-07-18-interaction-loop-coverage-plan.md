# Implementation plan: interaction-loop coverage (WS3)

Design: 2026-07-18-interaction-loop-coverage-design.md (APPROVED). This plan is
implementation ORDER only; it does not re-decide anything.

Verified against CURRENT branch tip 6e1ac33 (design was traced to dd0e32f;
later commits ad1c34e/590923d/157e1e4/6e1ac33 reworked handleReview guards,
bounded-FIFO dedup, orphan mint !IsPR, ensureStagePod skip guard - adapted, not
reverted).

## Commit 1 - foundation (API + stage machine)
- api/v1alpha1/task_types.go: add `issue_edited` to TaskEvent.Kind enum.
- api/v1alpha1/issue_types.go: add `LastDeployTimeoutCommentAt *metav1.Time`.
- internal/stage/stage.go: `ReasonIssueClosed` const + Reasons slice; 9 new
  `rejected(issue-closed)` edges (triaging, brainstorming, clarifying,
  investigating, refining, approved, implementing, reviewing, merging - NOT
  deploying/documenting). `AllowsIssueClosedStop(stage) bool` helper.
- stage_test.go: each edge Legal from source, NOT from deploying/documenting/
  terminal; reason valid; Unpark(issue-closed)=no re-entry.
- `make generate manifests`.

## Commit 2 - sever op + I3 stop + IssueReconciler wiring + re-sever hardening
- internal/controller/sever.go (new): SeverMode{DeleteCR,Orphan};
  SeverIssueFromTask - IssueRefs clear FIRST, then CR delete / orphan+label-strip.
- internal/controller/issue_apply.go (new): ApplyIssueClosedStop (Enter rejected,
  sever DeleteCR, DeleteWrapper).
- issue_controller.go: owned+closed+live-9-stage -> ApplyIssueClosedStop;
  owned+closed+rejected(issue-closed)+CR-present -> finish DeleteCR (addition a).
- webhook: issues.closed (!IsPR) -> mirror State refresh.
- Tests: sever both modes + crash-between-steps; I3 each live stage; deploying
  excluded; reopen-after-delete; re-sever completes.

## Commit 3 - webhook parse + folds (I1, I2, M1, M4, trigger-label, PR closed/merged)
- scm.go: WebhookEvent add `Merged bool`.
- github.go: `edited` in ghNormalizeAction; pull_request_review_comment case;
  parse pull_request.merged.
- gitlab.go: `merge`->"merged" action; MR synchronize head already exposed.
- webhook/server.go routing: issue edited/synchronize/labeled/unlabeled ->
  handleIssueEdited (mirror diff + issue_edited event, NO driveCommentUnpark);
  issue labeled==trigger -> mint; mr synchronize -> mirror head; mr closed/merged
  -> mirror state; M4 fold in handleReview NotFound branch.
- Tests: I1 parse+fold; I2 body-diff GH+GL (label-only no event, combined fires);
  M1 head refresh no transition; trigger-label guards; M4 fold; PR closed/merged.

## Commit 4 - I4 resume + I5 deploy comment + M5 empty SHA
- ProjectReconciler.resumeNoReentryParks driver (Reconcile loop, leader-only):
  parked(no-re-entry,non-backlog) + fresh non-bot event + owns open issue ->
  SeverIssueFromTask(Orphan) + MintForItem from mirror CR (comments end w/ reply).
- task_stage.go reconcileClocks: on entering parked(deploy-timeout), enqueue ONE
  PendingComment per owned open issue w/ LastDeployTimeoutCommentAt marker.
- review_apply.go: M5 effective-SHA short-circuit before any MR write.
- Tests: I4 one-reply resume (no tatara-parked, humanHasLastWord, old IssueRefs
  cleared, re-entry park still unparks); I5 first-only + no clobber of
  LastRefireCommentAt; M5 fallback + no half-apply.

## M2 - documentation only (already ignored; add accepted-ignored inventory comment
in server.go routing).
