package controller

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// THE F.6 RE-ENTRY DRIVER (fix W3).
//
// stage.Unpark carries a full re-entry body for SIX park reasons, but before
// this file its ONLY production caller was the webhook's reverifyParked, gated to
// identity-unverified. unparkFires (reaper.go) merely CHECKS whether a park would
// re-enter, on a DeepCopy, so the reaper does not collect a re-entryable Task - it
// never APPLIED the transition. So awaiting-human, merge-timeout, deploy-timeout,
// no-outcome and backlog-sweep all had a re-entry body and NO driver: a
// reviewed+approved delivery whose CI stayed red past its budget parked at
// merge-timeout and was stranded forever.
//
// This driver applies stage.Unpark. identity-unverified USED TO BE EXCLUDED, on
// the reasoning that it re-enters only against a freshly SYNCED forge thread
// evaluated by the C.6 grammar (ReVerifyParked, the webhook's job) and that
// driving it here with GrammarPassed=false would never pass. That reasoning was
// correct and the exclusion was still a defect: it made the webhook's ONE
// evaluation the only one that would ever happen, so a fast path that lost a
// cache race stalled the Task permanently with the approval label already
// visible on the forge (2026-07-27, gitlab helmfile#26).
//
// It is included now because the verdict is DURABLE. The webhook stamps
// Task.status.approvalVerdict the moment the grammar passes, whether or not the
// un-park in that same request succeeds. This driver never re-runs the grammar -
// it cannot - it reads that record and re-derives the rest (are all open owned
// Issues approved?) from LIVE cluster state, which it can.
//
// The verdict is SCOPED TO THE CURRENT PARK (2026-07-27 security review
// finding): stage.Enter never clears ApprovalVerdict, so a Task approved once,
// moved on, and later re-parked identity-unverified for an UNRELATED reason (a
// newly acquired, unapproved owned Issue; a later clarify round) still carries
// the OLD verdict. grammarPassedFor (below) only honours a verdict stamped
// AFTER the CURRENT park's StageEnteredAt - a verdict already consumed by an
// earlier park can never satisfy a later one - and refuses a verdict with no
// Author, so a schema-legal-but-empty {"at": "..."} verdict cannot authorize
// anything either.
//
// A Task with no verdict, or only a stale one, declines with grammar-not-passed
// and stays exactly where it is. ONE CARVE-OUT: under
// Project.spec.AutoApproveTataraProposals, a bot-authored, anchor-verified
// proposal with ZERO maintainer comments auto-approves (autoApproveApplies /
// autoApprovalEvidence, approval_grammar.go) and verdictFrom records that as a
// verdict with Author=AutoApproveLogin and no CommentExternalID - by design, not
// a gap this driver introduces. Outside that flag, there is no path here that
// re-enters implementing without a recorded maintainer approval.

// UnparkDecline classifies WHY ApplyUnpark refused to re-enter (target==""),
// so callers can tell an anomalous drift bail from a normal steady-state
// refusal apart (finding: guard-decline and rule-decline used to collapse
// into the same target=="",err==nil shape, which is exactly what let the
// cache-lag decline hide as an unremarkable steady-state outcome).
type UnparkDecline string

const (
	// DeclineNone means ApplyUnpark did not decline (target != "").
	DeclineNone UnparkDecline = ""
	// DeclineGuard means the live Task's Stage/StageReason no longer matched
	// what the caller believed was parked (raced past by another writer, or
	// re-parked under a different reason). Rare and anomalous: the caller's
	// view of the world had already drifted from the apiserver.
	DeclineGuard UnparkDecline = "guard"
	// DeclineRule is the FALLBACK for a stage-level decline code this package
	// does not recognise. It is a bug-catcher: every code stage.UnparkDetailed
	// can return has its own constant below, and a "rule" label appearing in
	// operator_unpark_declined_total means the two vocabularies have drifted.
	DeclineRule UnparkDecline = "rule"

	// The rest MIRROR stage's decline vocabulary 1:1, so the metric's `kind`
	// label carries WHICH condition refused rather than a shrug. They are
	// declared here, not aliased, because internal/controller owns the metric
	// and must not leak a stage-package identifier into a label value by
	// accident.
	DeclineNoHumanEvent     UnparkDecline = stage.DeclineNoHumanEvent
	DeclineOverCap          UnparkDecline = stage.DeclineOverCap
	DeclineGrammarNotPassed UnparkDecline = stage.DeclineGrammarNotPassed
	DeclineNoOpenIssues     UnparkDecline = stage.DeclineNoOpenIssues
	DeclineNotAllApproved   UnparkDecline = stage.DeclineNotAllApproved
	DeclineMergedMR         UnparkDecline = stage.DeclineMergedMR
	DeclineRoundsExhausted  UnparkDecline = stage.DeclineRoundsExhausted
	DeclineTurnsExhausted   UnparkDecline = stage.DeclineTurnsExhausted
	DeclineWrongParkedFrom  UnparkDecline = stage.DeclineWrongParkedFrom
	DeclineIllegalEdge      UnparkDecline = stage.DeclineIllegalEdge
	DeclineNoReentry        UnparkDecline = stage.DeclineNoReentry
)

// DeclineFor maps a stage-package decline code onto this package's typed
// vocabulary. An unknown code falls back to DeclineRule, which is a bug-catcher
// rather than a normal outcome.
func DeclineFor(code string) UnparkDecline {
	switch code {
	case stage.DeclineNone:
		return DeclineNone
	case stage.DeclineNoHumanEvent, stage.DeclineOverCap, stage.DeclineGrammarNotPassed,
		stage.DeclineNoOpenIssues, stage.DeclineNotAllApproved, stage.DeclineMergedMR,
		stage.DeclineRoundsExhausted, stage.DeclineTurnsExhausted, stage.DeclineWrongParkedFrom,
		stage.DeclineIllegalEdge, stage.DeclineNoReentry:
		return UnparkDecline(code)
	default:
		return DeclineRule
	}
}

// grammarPassedFor scopes t's C.6 verdict to t's CURRENT park, for the ONE
// stageReason (identity-unverified) that reads it. It is the SINGLE definition
// of that scoping rule, called from both ApplyUnpark (on the freshly-read Task,
// inside the retry loop) and reaper.go's unparkFires (on its probe DeepCopy),
// so the periodic driver and the reap-eligibility probe cannot disagree about
// whether a given verdict counts (2026-07-27 security review finding 3).
//
// Two refusals, both fail-closed:
//
//  1. v.At must be AFTER t.Status.StageEnteredAt. stage.Enter never clears
//     ApprovalVerdict but DOES re-stamp StageEnteredAt on every transition,
//     including into parked(identity-unverified) - so "the verdict postdates
//     the current park" is exactly "this verdict was produced FOR this park,
//     not carried over from an earlier one already consumed". Without this, an
//     Issue approved once (sticky - verifyOneIssue never revokes it) could
//     satisfy a LATER, unrelated park caused by a different, since-closed
//     owned Issue (finding 1).
//  2. v.Author must be non-empty. Task.Status.ApprovalVerdict's CRD schema
//     leaves author/commentExternalId optional (evolution headroom), so a
//     hand-written or corrupted {"at": "..."} verdict is schema-valid; Author is
//     the one field every verdict this codebase writes always carries (a real
//     maintainer login, or AutoApproveLogin for the guarded proposal
//     carve-out - see verdictFrom), so requiring it makes the zero-value
//     struct unreachable by construction here regardless of what a given CR's
//     admission did or did not enforce (finding/minor 6).
//
// fallback is returned for every OTHER stageReason, where GrammarPassed is
// ignored entirely (see stage.UnparkInput's doc comment).
//
// metav1.Time round-trips through the apiserver at WHOLE-SECOND precision (its
// MarshalJSON drops sub-second digits), so two timestamps written within the
// same second are indistinguishable by the time either is read back. That
// never happens for a genuine identity-unverified verdict: a human must
// physically comment on the forge AFTER the park, which is always seconds to
// days later, never the same second. Refusing on equality (strict After, not
// After-or-equal) is deliberately the conservative side of that non-issue.
func grammarPassedFor(t *tatarav1alpha1.Task, fallback bool) bool {
	if t.Status.StageReason != stage.ReasonIdentityUnverified {
		return fallback
	}
	v := t.Status.ApprovalVerdict
	if v == nil || v.Author == "" || t.Status.StageEnteredAt == nil {
		return false
	}
	return v.At.After(t.Status.StageEnteredAt.Time)
}

// ApplyUnpark runs stage.Unpark for one parked Task and persists the re-entry
// under optimistic concurrency. It is the SINGLE application of stage.Unpark,
// shared by the project-reconcile driver (driveUnparks, the time-based reasons)
// and the webhook comment-driven paths (awaiting-human, backlog-sweep), so every
// F.6 re-entry flows through one place. activeTasks / maxOpen are supplied by the
// caller (computed once per pass) so a bulk promotion cannot exceed maxOpenTasks
// on a stale count. target is "" when the park did not re-enter; decline then
// says why (DeclineGuard vs DeclineRule) so the caller can log/count them
// differently. decline is always DeclineNone when target != "" or err != nil.
//
// reader is the manager's UNCACHED APIReader (same idiom as TaskReconciler's
// mintedAlready/refreshTaskFromAPI, #347/#348). The in-loop Get MUST use it,
// not the cached c: driveCommentUnpark's caller just wrote a pendingEvent via
// AppendTaskEvent's Status().Update microseconds earlier, and the cached
// informer has not observed that write yet. A cached Get here silently threw
// the fresh state away - fresh.Status.PendingEvents came back empty,
// hasNonBotEvent returned false, and the un-park was refused with ok=false, a
// normal (non-error) outcome, for a Task a human had just told to proceed
// (issue: comment-driven unpark lost the cache-lag race 2/2 in prod). Nil
// reader (unit tests that do not wire one) falls back to c.
func ApplyUnpark(ctx context.Context, c client.Client, reader client.Reader, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, activeTasks, maxOpen int, grammarPassed bool, now time.Time) (string, UnparkDecline, error) {

	// c (cached) is safe here, unlike the retry-loop Task Get below: by the time
	// driveCommentUnpark reaches this call the owning Issue CR is guaranteed to
	// already exist (resolveMirrorTarget/deliverPendingEvent already Got it and
	// early-returns on NotFound), the same-request write that preceded this call
	// only touches the Issue's Status.Comments, and stage.Unpark's
	// openIssues/allApproved read Status.State/Status.Status - fields that write
	// never touched - so no cache-lag can make this Get see a stale approval
	// state; a stale (or genuinely unapproved) read only ever routes into
	// clarifying, never into the silent decline this fix is about.
	issues, err := loadTaskIssues(ctx, c, task)
	if err != nil {
		return "", DeclineNone, err
	}
	mrs, err := loadTaskMRs(ctx, c, task)
	if err != nil {
		return "", DeclineNone, err
	}
	maxTurns := taskMaxTurns(proj, task)
	botLogin := botLoginOf(proj)

	getter := reader
	if getter == nil {
		getter = c
	}

	// Task 9's two conversational F.6 rules (awaiting-human, identity-unverified)
	// need the SAME ceiling answer Task 11 already computes for the live-stage
	// entry path (EnterConversing) - reused here, not reimplemented, so the
	// webhook fast path and the periodic backstop can never disagree about how
	// full a project's conversing lane is. Computed once per ApplyUnpark call
	// (like issues/mrs above), off the same uncached-when-available getter: a
	// stale ceiling read only ever costs an extra conversation slot for one pass,
	// never a security decision, so this does not need the retry loop's freshness.
	conversingRoom, err := ConversingHasRoom(ctx, getter, proj)
	if err != nil {
		return "", DeclineNone, fmt.Errorf("unpark: conversing capacity check on %s: %w", task.Name, err)
	}

	var target string
	var decline UnparkDecline
	key := client.ObjectKeyFromObject(task)
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := getter.Get(ctx, key, fresh); err != nil {
			return err
		}
		// Raced past this park by another writer (or already un-parked): nothing to
		// do. The reason must also still match, or a different park is in play.
		if fresh.Status.Stage != tatarav1alpha1.StageParked ||
			fresh.Status.StageReason != task.Status.StageReason {
			target = ""
			decline = DeclineGuard
			return nil
		}
		// grammarPassedFor reads the verdict off fresh - the UNCACHED read this
		// closure just did - not off the caller's (possibly cached, possibly
		// stale) task argument. See grammarPassedFor's own doc for why the
		// verdict must additionally be scoped to fresh's CURRENT park.
		to, code := stage.UnparkDetailed(stage.UnparkInput{
			Task:              fresh,
			Issues:            issues,
			MRs:               mrs,
			ActiveTasks:       activeTasks,
			MaxOpenTasks:      maxOpen,
			BotLogin:          botLogin,
			GrammarPassed:     grammarPassedFor(fresh, grammarPassed),
			MaxTurnsPerTask:   maxTurns,
			ConversingHasRoom: conversingRoom,
			Now:               now,
		})
		if to == "" {
			target = ""
			decline = DeclineFor(code)
			return nil
		}
		if err := c.Status().Update(ctx, fresh); err != nil {
			return err
		}
		target = to
		decline = DeclineNone
		return nil
	})
	if err != nil {
		return "", DeclineNone, fmt.Errorf("unpark: apply on %s: %w", task.Name, err)
	}
	if target != "" {
		log.FromContext(ctx).Info("unparked task",
			"action", "unpark", "resource_id", task.Name, "stage", target,
			"reason_from", task.Status.StageReason)
	}
	return target, decline, nil
}

// driveUnparksPaced runs driveUnparks for proj at most once per
// UnparkDriveInterval (default defaultUnparkDriveInterval), decoupled from
// whatever cadence Reconcile() happens to run at. Fix for the 2026-07-18
// operator_unpark_declined_total burst (tatara-operator#368): a Project stuck
// in memory phase=Provisioning forced Reconcile() onto a 10s-or-faster
// cadence (tatara-operator#367), and driveUnparks - with no pacing of its
// own - re-declined the full parked backlog on every single one of those
// passes. Returns the requeue interval to fold into soonestRequeue so a
// genuinely-eligible F.6 re-entry is never starved past the floor even once
// Reconcile()'s OTHER drivers stop forcing frequent passes. Keyed per project
// (like memoryUnhealthyCycles): two live Projects must not throttle each
// other's floor.
func (r *ProjectReconciler) driveUnparksPaced(ctx context.Context, proj *tatarav1alpha1.Project, now time.Time) (time.Duration, error) {
	interval := r.UnparkDriveInterval
	if interval <= 0 {
		interval = defaultUnparkDriveInterval
	}
	if last, ok := r.lastDriveUnparks[proj.Name]; ok {
		if elapsed := now.Sub(last); elapsed < interval {
			return interval - elapsed, nil
		}
	}
	if err := r.driveUnparks(ctx, proj, now); err != nil {
		return 0, err
	}
	if r.lastDriveUnparks == nil {
		r.lastDriveUnparks = map[string]time.Time{}
	}
	r.lastDriveUnparks[proj.Name] = now
	return interval, nil
}

// driveUnparks applies stage.Unpark to every parked Task in proj whose park
// reason has an F.6 re-entry rule, INCLUDING identity-unverified: the grammar
// verdict it needs comes from the durable Task.status.approvalVerdict (see the
// file header), never re-evaluated here. activeTasks is computed ONCE and then
// advanced as each Task re-enters an active stage, so a bulk re-entry never
// exceeds maxOpenTasks (H8).
func (r *ProjectReconciler) driveUnparks(ctx context.Context, proj *tatarav1alpha1.Project, now time.Time) error {
	var tl tatarav1alpha1.TaskList
	if err := r.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		return fmt.Errorf("unpark: list tasks: %w", err)
	}
	active, err := r.activeTaskCount(ctx, proj)
	if err != nil {
		return err
	}
	maxOpen := proj.Spec.MaxOpenTasks
	if maxOpen <= 0 {
		maxOpen = 6
	}

	var firstErr error
	for i := range tl.Items {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		t := &tl.Items[i]
		if t.Spec.ProjectRef != proj.Name || t.Status.Stage != tatarav1alpha1.StageParked {
			continue
		}
		// The verdict is the ONE input this pass cannot reconstruct, and ApplyUnpark
		// resolves it itself (grammarPassedFor, on its own UNCACHED read) rather than
		// this loop computing it off t - t comes from the List above (r.Client,
		// cached) and can lag exactly the write this whole feature exists to survive.
		// false here is the fallback ApplyUnpark uses for every OTHER stageReason,
		// where GrammarPassed is ignored entirely.
		target, decline, err := ApplyUnpark(ctx, r.Client, r.APIReader, proj, t, active, maxOpen, false, now)
		if err != nil {
			log.FromContext(ctx).Error(err, "unpark: apply failed",
				"action", "unpark_error", "resource_id", t.Name, "reason", t.Status.StageReason)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		// GUARD declines are always anomalous (the live object had already
		// drifted from what this pass believed was parked) and worth surfacing
		// here too, unlike RULE declines: driveUnparks sweeps every parked Task
		// every pass, and a RULE decline is the expected steady state for most
		// of them (e.g. merge-timeout still waiting) - logging every one would
		// be pure log spam, not a signal.
		if decline == DeclineGuard {
			log.FromContext(ctx).Info("unpark: declined (drift guard)",
				"action", "unpark_declined", "resource_id", t.Name, "reason", t.Status.StageReason, "decline", string(decline))
		}
		if decline != DeclineNone && r.Metrics != nil {
			r.Metrics.UnparkDeclined(t.Status.StageReason, string(decline))
		}
		if target != "" {
			if t.Status.StageReason == stage.ReasonIdentityUnverified {
				// This is the backstop catching what the fast path dropped. It is the
				// single most important line in this file for diagnosing a repeat of
				// the 2026-07-27 stall: if it fires often, the webhook fast path is
				// losing its cache race routinely and the fast path is what to fix.
				//
				// t (this loop's cached List item) is diagnostic best-effort here, not
				// the security decision (that already happened, inside ApplyUnpark,
				// against an uncached read) - so t.Status.ApprovalVerdict may be nil or
				// stale even though the re-entry above genuinely succeeded; guard it
				// rather than deref a verdict the cache never observed.
				fields := []any{"action", "unpark_verdict_backstop", "resource_id", t.Name, "stage", target}
				if v := t.Status.ApprovalVerdict; v != nil {
					fields = append(fields, "verdict_comment", v.CommentExternalID, "verdict_issue", v.IssueRef)
				}
				log.FromContext(ctx).Info("unpark: identity-unverified retried from the persisted grammar verdict", fields...)
			}
			active++ // the re-entered Task is now active; keep the cap honest this pass.
		}
	}
	return firstErr
}

// loadTaskIssues / loadTaskMRs resolve status.issueRefs / status.mrRefs to their
// CRs. A ref whose CR is gone is skipped (the mirror is not authoritative). They
// are the standalone twins of ProjectReconciler.ownedIssues/ownedMRs so the
// webhook package can drive an un-park without a ProjectReconciler.
func loadTaskIssues(ctx context.Context, c client.Client, t *tatarav1alpha1.Task) ([]tatarav1alpha1.Issue, error) {
	out := make([]tatarav1alpha1.Issue, 0, len(t.Status.IssueRefs))
	for _, name := range t.Status.IssueRefs {
		var iss tatarav1alpha1.Issue
		err := c.Get(ctx, types.NamespacedName{Namespace: t.Namespace, Name: name}, &iss)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("unpark: get issue %s: %w", name, err)
		}
		out = append(out, iss)
	}
	return out, nil
}

func loadTaskMRs(ctx context.Context, c client.Client, t *tatarav1alpha1.Task) ([]tatarav1alpha1.MergeRequest, error) {
	return LoadTaskMRsFor(ctx, c, t)
}

// LoadTaskMRsFor resolves status.mrRefs to their CRs. It is the exported,
// client.Reader-taking twin of loadTaskMRs, so a caller with only a Reader (the
// webhook's driveConversingEntry, which prefers the uncached APIReader for the
// same cache-lag reason every other webhook read does) can load a Task's owned
// MergeRequests without a ProjectReconciler. A ref whose CR is gone is skipped -
// the mirror is not authoritative.
func LoadTaskMRsFor(ctx context.Context, r client.Reader, t *tatarav1alpha1.Task) ([]tatarav1alpha1.MergeRequest, error) {
	out := make([]tatarav1alpha1.MergeRequest, 0, len(t.Status.MRRefs))
	for _, name := range t.Status.MRRefs {
		var mr tatarav1alpha1.MergeRequest
		err := r.Get(ctx, types.NamespacedName{Namespace: t.Namespace, Name: name}, &mr)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("unpark: get mergerequest %s: %w", name, err)
		}
		out = append(out, mr)
	}
	return out, nil
}

// CountActiveTasks counts the non-terminal Tasks in proj. It is the standalone
// twin of ProjectReconciler.activeTaskCount, for the webhook's backlog-sweep cap
// check (H8): a promotion is not a mint, so the cap must be re-checked at re-entry.
func CountActiveTasks(ctx context.Context, c client.Client, proj *tatarav1alpha1.Project) (int, error) {
	var tl tatarav1alpha1.TaskList
	if err := c.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		return 0, fmt.Errorf("unpark: list tasks for active count: %w", err)
	}
	n := 0
	for i := range tl.Items {
		if tl.Items[i].Spec.ProjectRef == proj.Name && StageActive(&tl.Items[i]) {
			n++
		}
	}
	return n, nil
}
