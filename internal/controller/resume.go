package controller

import (
	"context"
	"fmt"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// AnnResumeReleasing marks a Task whose no-re-entry resume has already SEVERED
// at least one owned Issue and is therefore committed to being collected this
// pass. It closes the ONE crash window resumeOne has: the candidacy test asks
// "does this parked Task still own an OPEN Issue", and the sever is exactly
// what makes that answer no. Without the marker, an operator restart between
// the sever and the delete leaves a live parked Task holding the deterministic
// intake name forever while the Issue it no longer owns is re-adopted straight
// back to it by repairIssueBinding.
const AnnResumeReleasing = "tatara.dev/resume-releasing"

// resumeTriggerHumanReply / resumeTriggerAutoReentry name WHO asked for a
// resume. resumeOne is one mechanism with two callers - the one-reply guarantee
// and C.3's automatic pickup - and every log line it writes carries the trigger,
// because "tatara restarted this by itself" and "a maintainer restarted this"
// are the two facts an operator reading the resume trail actually needs to tell
// apart. It is a closed vocabulary; nothing derives control flow from it.
const (
	resumeTriggerHumanReply  = "human-reply"
	resumeTriggerAutoReentry = "auto-reentry"
	// resumeTriggerAutoCollect is the one trigger that RE-MINTS NOTHING: every
	// owned Issue is already closed, so resumeOne severs, closes the Task's own
	// bot PRs and collects, and its mint loop has no jobs to run.
	resumeTriggerAutoCollect = "auto-collect"
)

// resumeNoReentryParks restores the ONE-REPLY GUARANTEE for the UnparkNever
// (and, since O3, UnparkRetired)
// population: one human reply to a Task parked with a reason nobody un-parks is
// enough, and the human never has to comment a SECOND time.
//
// WHY THIS EXISTS AT ALL, given the branch's claim that it does not need to.
// The claim was that with TaskDone == {done, rejected} a parked Task is no
// longer terminal, "so the sweep's ordinary re-mint handles the Issue". It does
// not. The sweep re-mints an ORPHAN Issue, and IsOrphanIssue's clause (b)
// (sweep.go) asks resolveLiveIssueOwner, which finds the still-LIVE parked Task
// as the Issue's controller owner and answers issue_owned. Nothing re-mints
// until the reaper releases the Task at ParkRetention - seven days, by which
// time the reply is long gone from pendingEvents.
//
// WHY A DRIVER AND NOT AN INTAKE/SWEEP-SEAM TWEAK. Teaching IsOrphanIssue to
// treat a parked UnparkNever owner as orphan does not work, and would be worse
// than doing nothing. The fresh mint's name is the DETERMINISTIC IntakeTaskName
// for (project, kind, repo, number) - which is the SAME name the parked owner
// already holds. Because TaskDone(parked) is now false, createTaskRaceSafe
// takes its live-twin branch: the mint returns MintExistingLive, creates
// nothing, and repairIssueBinding re-binds the Issue straight back to the
// parked Task. The old Task has to GO before a fresh one can exist, and a seam
// predicate cannot make it go.
//
// SO THIS IS AN EARLY REAP, and that is the honest name for it. A parked
// UnparkNever Task has exactly ONE exit in the #521 model - the reaper's
// collection at ParkRetention (stage.Unpark returns DeclineNoReentry for every
// one of these reasons, and stage.Enter refuses to move a Task that is still
// parked). This driver answers that clock EARLY when a human replies, instead
// of inventing a state transition the model does not have. It is NOT a
// re-entry: the old Task is never un-parked, its pod is never resurrected, and
// what the human gets is a FRESH Task minted through the shared MintForItem
// funnel, which re-runs every gate (tatara-parked, allowed-reporter,
// MintStage). An exhaustion cap cannot be escaped one lap at a time through
// here, because there is no lap - there is a new Task with new counters, which
// is precisely what the UnparkNever classification says should happen, only
// without the seven-day wait. THAT is the ordinary case; resumeOne's own
// switch on MintForItem's outcome (step 5) is what keeps the log honest on
// the rarer paths where the gates it re-runs answer something other than a
// fresh Task (finding B5) - "the human gets a fresh Task" is a claim this
// driver now backs with a typed check, not an assumption baked into one
// unconditional log line.
//
// LEADER-ONLY (it runs from the project reconcile, alongside driveUnparks; the
// reaper remains the ultimate backstop). It never touches the re-entry surface:
// stage.UnparkHuman and stage.UnparkTimer reasons belong to driveUnparks and
// are skipped here.
func (r *ProjectReconciler) resumeNoReentryParks(ctx context.Context, proj *tatarav1alpha1.Project, now time.Time) error {
	var tl tatarav1alpha1.TaskList
	if err := r.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		return fmt.Errorf("resume: list tasks: %w", err)
	}

	// live is EVERY Task that currently exists, for the B.5 handover inside
	// deleteReapedTask - the same map ReapTerminal builds for the same reason.
	// releaseOwnership excludes the dying Task itself.
	live := make(map[string]bool, len(tl.Items))
	for i := range tl.Items {
		live[tl.Items[i].Name] = true
	}

	var firstErr error
	for i := range tl.Items {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		t := &tl.Items[i]
		if t.Spec.ProjectRef != proj.Name || !tatarav1alpha1.Parked(t) {
			continue
		}
		class, ok := stage.UnparkClassFor(t.Status.ParkReason)
		if !ok || (class != stage.UnparkNever && class != stage.UnparkRetired) {
			continue // a human- or timer-un-parked reason: driveUnparks owns it.
		}
		// UnparkRetired IS INCLUDED, and leaving it out would have been a silent
		// regression of the one-reply guarantee. O3 moved three reasons out of
		// UnparkNever into that migration class, and stage.Unpark still declines
		// them (DeclineNoReentry) - so to a human replying on the issue they behave
		// EXACTLY like an UnparkNever park, and this is still their only same-pass
		// answer. driveRetiredUnparks does not overlap: it is a one-shot,
		// annotation-latched sweep bounded at 48h that never looks at
		// pendingEvents. Past that window - or on a Task it already migrated once -
		// this path is the ONLY thing a reply can reach.
		if !hasNonBotPendingEvent(t, botLoginOf(proj)) {
			// No HUMAN reply waiting. The bot's own park comment must never
			// resume the Task it parked, so authorship is the whole test here.
			continue
		}
		if err := r.resumeOne(ctx, proj, t, live, resumeTriggerHumanReply); err != nil {
			log.FromContext(ctx).Error(err, "resume: no-re-entry park resume failed",
				"action", "resume_error", "resource_id", t.Name, "reason", t.Status.ParkReason)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// resumeNoReentryParksPaced runs resumeNoReentryParks for proj at most once per
// defaultResumeNoReentryInterval, decoupled from whatever cadence Reconcile()
// happens to run at (tatara-operator#367, mirrors driveUnparksPaced in
// unpark.go: without a floor this does a full-namespace Task List on every
// single Reconcile pass). Keyed per project (like lastDriveUnparks): two live
// Projects must not throttle each other's floor.
func (r *ProjectReconciler) resumeNoReentryParksPaced(ctx context.Context, proj *tatarav1alpha1.Project, now time.Time) (time.Duration, error) {
	interval := defaultResumeNoReentryInterval
	if last, ok := r.lastResumeNoReentryParks[proj.Name]; ok {
		if elapsed := now.Sub(last); elapsed < interval {
			return interval - elapsed, nil
		}
	}
	if err := r.resumeNoReentryParks(ctx, proj, now); err != nil {
		return 0, err
	}
	if r.lastResumeNoReentryParks == nil {
		r.lastResumeNoReentryParks = map[string]time.Time{}
	}
	r.lastResumeNoReentryParks[proj.Name] = now
	return interval, nil
}

// resumeOne resumes ONE no-re-entry parked Task off a human reply.
//
// ORDER IS LOAD-BEARING, and every step is independently idempotent so a crash
// anywhere leaves a state the next pass finishes rather than one it cannot see:
//
//  1. STAMP AnnResumeReleasing, before anything else mutates. See that constant:
//     step 3 destroys the candidacy test, so the marker is what carries the
//     commitment across a restart.
//  2. CLOSE the old Task's own bot PRs, with NO issue-side effect
//     (closeTaskBotMRs never posts the terminal issue comment and never stamps
//     tatara-parked). It runs BEFORE step 4's delete, whose background
//     propagation cascades any mirror still owner-reffed to the Task: a closed
//     forge PR with no mirror is recoverable, an OPEN one with no mirror is the
//     split state nothing ever collects.
//  3. SEVER every owned OPEN Issue (Orphan): IssueRefs cleared, CR ownerRef
//     dropped, tatara-parked stripped from the mirror - so the fresh mint lands
//     ACTIVE via humanHasLastWord.
//  4. COLLECT the old Task through the reaper's OWN delete (deleteReapedTask:
//     B.5 handover first, then Delete). releaseTerminal is deliberately NOT
//     called - its steps 1 and 2 post "tatara has stopped working this issue"
//     and stamp tatara-parked, which is the exact opposite of what is
//     happening, and the label would force the fresh mint straight back to
//     parked(backlog-sweep). Step 2 already did the only part of the terminal
//     sequence that is still owed (our own PRs), and step 3 already released
//     the Issues.
//  5. MINT, through the shared MintForItem funnel, from the MIRROR CR so the
//     ForgeItem's Comments end with the human reply. The returned MintOutcome
//     is switched on, not stringified into a field on an unconditional
//     success log (finding B5): only MintCreated is a resume, and the other
//     three members are logged as the incomplete outcomes they are.
//
// All severs happen before any mint (step 3 before step 5), so no mint can
// observe a half-detached Task.
func (r *ProjectReconciler) resumeOne(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, live map[string]bool, trigger string) error {

	issues, err := r.ownedIssues(ctx, t)
	if err != nil {
		return err
	}
	// A CLOSED owned Issue IS severed and DOES collect the Task early (C.4); it
	// is only the MINT that it does not get. The old code bailed outright when
	// no owned Issue was open, and that bail was a silent vanish: a Task parked
	// with a reason nobody un-parks, holding one closed issue, had NO exit left
	// at all. resumeNoReentryParks skipped it here, the reaper's ParkRetention
	// clock deleted it at T+7d and cascade-deleted the mirror with it, and
	// IsOrphanIssue (which needs state == "open") could never see the issue
	// again. Severing a closed issue costs nothing - nothing is re-minted from
	// it, because forgeItemFromMirror describes an OPEN item and MintStage would
	// be lying about a closed one - and it frees the Task to be collected NOW
	// rather than sitting parked for a week doing nothing.
	var openNames, allNames []string
	for i := range issues {
		allNames = append(allNames, issues[i].Name)
		if issues[i].Status.State == "open" {
			openNames = append(openNames, issues[i].Name)
		}
	}
	releasing := t.Annotations[AnnResumeReleasing] == "true"
	if len(allNames) == 0 && !releasing {
		// The Task owns no Issue mirror at all, so there is nothing to sever and
		// nothing to re-mint: the reaper's ordinary ParkRetention clock still
		// owns this Task.
		return nil
	}
	if !releasing {
		if err := r.annotateTask(ctx, t, AnnResumeReleasing, "true"); err != nil {
			return err
		}
	}

	// Step 2. Retry-safe and idempotent (the AnnTerminalClosed marker, shared
	// with the reaper's closeOwnMRs); a failure returns before anything is
	// severed, so the whole resume retries cleanly next pass.
	if err := r.closeTaskBotMRs(ctx, proj, t, trigger); err != nil {
		return err
	}

	// Step 3. Each Issue is LIVE-READ off the uncached APIReader so the
	// webhook's just-appended human reply is ALWAYS visible at mint time
	// (the #348/#352 live-read discipline): a lagging cache would hide the
	// reply, humanHasLastWord would be false, the fresh Task would mint
	// parked(backlog-sweep) and - IssueRefs already cleared - the human would
	// need a SECOND reply. The live read is what preserves the guarantee.
	type mintJob struct {
		item ForgeItem
		repo *tatarav1alpha1.Repository
		name string
	}
	var jobs []mintJob
	open := make(map[string]bool, len(openNames))
	for _, name := range openNames {
		open[name] = true
	}
	for _, name := range allNames {
		liveIss, err := r.liveIssue(ctx, proj.Namespace, name)
		if err != nil {
			return err
		}
		if liveIss == nil {
			continue // already gone (concurrent reap): nothing to re-adopt.
		}
		if !open[name] {
			// CLOSED: severed so the mirror survives the Task's collection as a
			// zero-owner CR, never re-minted. See the openNames/allNames comment.
			if err := SeverIssueFromTask(ctx, r.Client, t, name, SeverOrphan); err != nil {
				return err
			}
			continue
		}
		r.stripForgeParkedLabel(ctx, proj, liveIss) // best-effort operator-on-promotion
		if err := SeverIssueFromTask(ctx, r.Client, t, name, SeverOrphan); err != nil {
			return err
		}
		repo, err := r.repositoryFor(ctx, proj.Namespace, liveIss.Spec.RepositoryRef)
		if err != nil {
			return err
		}
		jobs = append(jobs, mintJob{item: forgeItemFromMirror(liveIss), repo: repo, name: name})
	}

	// Step 4. This is what frees the deterministic IntakeTaskName, without which
	// step 5 can only ever return MintExistingLive.
	if err := r.deleteReapedTask(ctx, proj, t, live); err != nil {
		return err
	}

	// Step 5. The outcome is SWITCHED ON, not stringified into a field on one
	// shared, unconditionally-"resumed" line (finding B5): that used to log a
	// completed resume for every non-error return, including MintNotOwed -
	// where the old Task is already deleted, its bot PRs already closed, and
	// the Issue already severed, yet NOTHING was minted. That is verbatim the
	// #521 shape MintOutcome's own doc (intake.go) says every caller must now
	// switch on. MintCreated is the ONLY member this loop may describe as a
	// resume; the other three leave the Issue orphaned this pass (a later
	// sweep re-mints it as an ordinary orphan, EXCEPT when MintNotOwed's own
	// classification reason - e.g. the reporter allowlist - would decline it
	// again forever), and are logged as the incomplete outcomes they are.
	l := log.FromContext(ctx)
	for _, j := range jobs {
		_, outcome, err := r.minter().MintForItem(ctx, proj, j.repo, j.item, false, nil)
		if err != nil {
			return err
		}
		switch outcome {
		case MintCreated:
			// The only case that IS a resume: a fresh Task now exists on the
			// deterministic name and owns the Issue again.
			l.Info("resumed a no-re-entry park from a human reply",
				"action", "resume_remint", "resource_id", j.name, "old_task", t.Name,
				"reason", t.Status.ParkReason)
		case MintExistingLive:
			// No new Task: a live twin already held the natural key, and
			// MintIssueTask's backstop repaired its binding onto the Issue.
			// Not a resume - the twin was already there before this pass ran.
			l.Info("resume: a live twin already held the mint key; its binding was repaired, not a re-mint",
				"action", "resume_existing_live", "resource_id", j.name, "old_task", t.Name,
				"reason", t.Status.ParkReason)
		case MintTombstoneDeleted:
			// A dead twin held the key and was just DELETED (createTaskRaceSafe
			// already logged that delete on its own line). Nothing replaced it:
			// the mint is still OWED and the Issue sits ownerless until a later
			// sweep pass re-mints it as an orphan. This must never read as a
			// completed resume.
			l.Error(fmt.Errorf("resume: mint for %s deleted a stale terminal task; the mint is still owed", j.name),
				"resume: no task replaced the deleted terminal twin; the issue is orphaned",
				"action", "resume_mint_owed", "resource_id", j.name, "old_task", t.Name,
				"reason", t.Status.ParkReason)
		case MintNotOwed:
			// Nothing was minted at all. The old Task is gone, its bot PRs are
			// closed, and the Issue was just severed - it is now orphaned with
			// no owner and no re-mint in flight, possibly permanently (e.g. a
			// reporter-allowlist decline reclassifies the same way on every
			// later sweep pass too). This is the #521 defect's exact shape and
			// must be loud, not logged as a success.
			l.Error(fmt.Errorf("resume: mint for %s decided nothing is owed after severing the issue", j.name),
				"resume: no task was minted; the issue is orphaned",
				"action", "resume_not_owed", "resource_id", j.name, "old_task", t.Name,
				"reason", t.Status.ParkReason)
		}
	}
	return nil
}

// liveIssue reads an Issue mirror through the UNCACHED APIReader (falling back
// to the cached Client when none is wired, as in unit tests), so the re-mint
// sees the webhook's just-appended human reply. A NotFound returns (nil, nil):
// the issue was reaped concurrently and there is nothing to re-adopt.
func (r *ProjectReconciler) liveIssue(ctx context.Context, ns, name string) (*tatarav1alpha1.Issue, error) {
	var rdr client.Reader = r.Client
	if r.APIReader != nil {
		rdr = r.APIReader
	}
	iss := &tatarav1alpha1.Issue{}
	if err := rdr.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, iss); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("resume: live-read issue %s: %w", name, err)
	}
	return iss, nil
}

// closeTaskBotMRs closes every OPEN bot PR the Task OWNS on the forge, with NO
// issue-side effect (unlike releaseTerminal, which also posts the terminal issue
// comment and stamps tatara-parked - both wrong for a resume, see resumeOne
// step 4). It is the retry-safe, idempotent PR-close half of the resume and
// shares ourMR + the AnnTerminalClosed marker with the reaper's closeOwnMRs.
func (r *ProjectReconciler) closeTaskBotMRs(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, trigger string) error {
	mrs, err := r.ownedMRs(ctx, t)
	if err != nil {
		return err
	}
	provider := providerOf(proj)
	for i := range mrs {
		mr := &mrs[i]
		if mr.Status.State != "open" || mr.Annotations[AnnTerminalClosed] != "" || !ourMR(proj, t, mr) {
			continue
		}
		writer, token, err := r.scanWriter(ctx, proj)
		if err != nil {
			return fmt.Errorf("resume: scm writer: %w", err)
		}
		repo, err := r.repositoryFor(ctx, proj.Namespace, mr.Spec.RepositoryRef)
		if err != nil {
			return err
		}
		// The wording follows the TRIGGER, because "restarting" is false on the
		// collect path: every owned issue is already closed and nothing is being
		// re-minted, so a PR closed there is being cleaned up, not superseded.
		body := fmt.Sprintf("Closing: tatara is restarting this issue (%s) after the previous attempt ended in `%s`.",
			trigger, t.Status.ParkReason)
		if trigger == resumeTriggerAutoCollect {
			body = fmt.Sprintf("Closing: the issue this PR was opened for is closed, and tatara's task ended in `%s`.",
				t.Status.ParkReason)
		}
		closeErr := writer.ClosePR(ctx, repo.Spec.URL, token, mr.Spec.Number, body)
		RecordSCM(r.Metrics, provider, "close_pr", closeErr)
		if closeErr != nil && !isPermanentTargetGone(closeErr) {
			return fmt.Errorf("resume: close bot MR %s#%d: %w", repo.Name, mr.Spec.Number, closeErr)
		}
		if err := r.annotateMR(ctx, mr, AnnTerminalClosed, t.Name); err != nil {
			return err
		}
		log.FromContext(ctx).Info("resume: closed the old task's bot PR before re-minting",
			"action", "resume_close_mr", "resource_id", t.Name, "repo", repo.Name, "number", mr.Spec.Number)
	}
	return nil
}

// stripForgeParkedLabel removes tatara-parked from the forge issue IF the mirror
// shows it present (operator-on-promotion). Best-effort: a failure is logged,
// never fatal - sever already strips the MIRROR label, which is what the mint
// decision reads.
func (r *ProjectReconciler) stripForgeParkedLabel(ctx context.Context, proj *tatarav1alpha1.Project, iss *tatarav1alpha1.Issue) {
	if !slices.Contains(iss.Status.Labels, TataraParkedLabel) {
		return
	}
	writer, token, err := r.scanWriter(ctx, proj)
	if err != nil {
		log.FromContext(ctx).Error(err, "resume: scm writer for label strip", "resource_id", iss.Name)
		return
	}
	repo, err := r.repositoryFor(ctx, proj.Namespace, iss.Spec.RepositoryRef)
	if err != nil {
		log.FromContext(ctx).Error(err, "resume: repository for label strip", "resource_id", iss.Name)
		return
	}
	slug, err := scm.RepoSlugFromURL(repo.Spec.URL)
	if err != nil {
		return
	}
	issueRef := fmt.Sprintf("%s#%d", slug, iss.Spec.Number)
	rmErr := writer.RemoveLabel(ctx, token, issueRef, TataraParkedLabel)
	RecordSCM(r.Metrics, providerOf(proj), "remove_label", rmErr)
	if rmErr != nil && !isPermanentTargetGone(rmErr) {
		log.FromContext(ctx).Error(rmErr, "resume: strip forge tatara-parked label", "resource_id", iss.Name, "issue_ref", issueRef)
	}
}

// forgeItemFromMirror builds a ForgeItem from a mirror Issue CR so the re-mint's
// MintStage runs humanHasLastWord against the SAME comments the mirror holds -
// which end with the human reply the webhook's AppendCommentToMirror landed.
// tatara-parked is filtered out (sever stripped it) so MintStage never parks it.
func forgeItemFromMirror(iss *tatarav1alpha1.Issue) ForgeItem {
	comments := make([]scm.IssueComment, 0, len(iss.Status.Comments))
	for _, c := range iss.Status.Comments {
		comments = append(comments, scm.IssueComment{
			ExternalID: c.ExternalID, Author: c.Author, Body: c.Body, CreatedAt: c.CreatedAt.Time,
		})
	}
	labels := make([]string, 0, len(iss.Status.Labels))
	for _, l := range iss.Status.Labels {
		if l != TataraParkedLabel {
			labels = append(labels, l)
		}
	}
	return ForgeItem{Issue: scm.Issue{
		Number: iss.Spec.Number, State: "open", Author: iss.Status.Author,
		Title: iss.Status.Title, Body: iss.Status.Body, Labels: labels, URL: iss.Spec.URL,
		Comments: comments,
	}}
}

// hasNonBotPendingEvent reports whether t carries a HUMAN pending event.
func hasNonBotPendingEvent(t *tatarav1alpha1.Task, botLogin string) bool {
	for i := range t.Status.PendingEvents {
		if t.Status.PendingEvents[i].Author != botLogin {
			return true
		}
	}
	return false
}
