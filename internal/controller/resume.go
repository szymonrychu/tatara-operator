package controller

import (
	"context"
	"fmt"
	"slices"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// resumeNoReentryParks is the WS3-I4 driver. A human reply to a Task parked with a
// NO-RE-ENTRY reason (stage-deadline, review-loop-exhausted, review-post-refused,
// implement-declined, admission-starved, fold-adoption-unverified, doc-timeout,
// handoff-stalled, agent-contract-mismatch, object-too-large) must not vanish and
// must not smuggle a re-entry past the C.6 gate. Instead it triggers an immediate,
// gate-respecting fresh start: sever(Orphan) the owned issue and re-mint it as a
// fresh ACTIVE clarify Task via the shared MintForItem funnel.
//
// LEADER-ONLY (runs from the project reconcile, alongside driveUnparks; the
// reaper is the ultimate backstop). It never touches the F.6 re-entry surface
// (backlog-sweep/awaiting-human/identity-unverified/merge-timeout/deploy-timeout/
// no-outcome are stage.HasReentry and handled by driveUnparks/reverifyParked).
func (r *ProjectReconciler) resumeNoReentryParks(ctx context.Context, proj *tatarav1alpha1.Project, now time.Time) error {
	var tl tatarav1alpha1.TaskList
	if err := r.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		return fmt.Errorf("resume: list tasks: %w", err)
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
		if stage.HasReentry(t.Status.StageReason) {
			continue // an F.6 re-entry reason: driveUnparks / reverifyParked owns it.
		}
		if !hasNonBotPendingEvent(t, botLoginOf(proj)) {
			continue // no human reply waiting: nothing to resume.
		}
		if err := r.resumeOne(ctx, proj, t); err != nil {
			log.FromContext(ctx).Error(err, "resume: no-re-entry park resume failed",
				"action", "resume_error", "resource_id", t.Name, "reason", t.Status.StageReason)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// resumeOne resumes ONE no-re-entry parked Task off a human reply.
//
// ORDER is load-bearing. The old Task's bot PR is closed FIRST (idempotently, WITH
// NO issue-side effect - closeTaskBotMRs never posts the terminal issue comment),
// so that once the issue is severed and re-minted, the deterministic-name
// collision that makes MintForItem delete the old stale-terminal Task cannot leak
// an OPEN forge PR (the split state the sever section warns about). THEN the issue
// is severed (Orphan): IssueRefs cleared, CR ownerRef dropped, tatara-parked
// stripped - so the fresh mint lands ACTIVE via humanHasLastWord and the old
// Task's later reap posts NO spurious comment and re-stamps NO label. THEN
// MintForItem re-adopts the now-orphan OPEN issue, building the ForgeItem from the
// MIRROR CR so its Comments end with the human reply.
func (r *ProjectReconciler) resumeOne(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task) error {

	issues, err := r.ownedIssues(ctx, t)
	if err != nil {
		return err
	}
	open := make([]tatarav1alpha1.Issue, 0, len(issues))
	for i := range issues {
		if issues[i].Status.State == "open" {
			open = append(open, issues[i])
		}
	}
	if len(open) == 0 {
		return nil // the reply is not on a live owned issue (a closed issue is the I3 path).
	}

	// Close the old Task's own bot PR FIRST, no issue side effect. Retry-safe and
	// idempotent (AnnTerminalClosed marker); a failure returns before anything is
	// severed, so the whole resume retries cleanly next pass.
	if err := r.closeTaskBotMRs(ctx, proj, t); err != nil {
		return err
	}

	for i := range open {
		iss := &open[i]
		r.stripForgeParkedLabel(ctx, proj, iss) // best-effort operator-on-promotion (F.6)
		if err := SeverIssueFromTask(ctx, r.Client, t, iss.Name, SeverOrphan); err != nil {
			return err
		}
		repo, err := r.repositoryFor(ctx, proj.Namespace, iss.Spec.RepositoryRef)
		if err != nil {
			return err
		}
		if _, _, err := r.minter().MintForItem(ctx, proj, repo, forgeItemFromMirror(iss), false, nil); err != nil {
			return err
		}
		log.FromContext(ctx).Info("resumed a no-re-entry park from a human reply: re-minted the issue fresh",
			"action", "resume_remint", "resource_id", iss.Name, "old_task", t.Name, "reason", t.Status.StageReason)
	}
	return nil
}

// closeTaskBotMRs closes every OPEN bot PR the Task OWNS on the forge, with NO
// issue-side effect (unlike releaseTerminal, which also posts the terminal issue
// comment + stamps tatara-parked). It is the retry-safe, idempotent PR-close half
// of the WS3-I4 resume: it must land BEFORE the issue is severed so the eventual
// stale-terminal delete of the old Task cannot cascade an OPEN PR's mirror away.
// It shares ourMR + the AnnTerminalClosed marker with the reaper's closeOwnMRs.
func (r *ProjectReconciler) closeTaskBotMRs(ctx context.Context, proj *tatarav1alpha1.Project, t *tatarav1alpha1.Task) error {
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
		body := fmt.Sprintf("Closing: tatara is restarting this issue from a human reply after the previous attempt ended in `%s`.", t.Status.StageReason)
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
// shows it present (F.6 operator-on-promotion). Best-effort: a failure is logged,
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
