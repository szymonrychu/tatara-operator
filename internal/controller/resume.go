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

// resumeNoReentryParks is the WS3-I4 driver. A human reply to a Task parked with a
// NO-RE-ENTRY reason (stage-deadline, review-loop-exhausted, review-post-refused,
// implement-declined, admission-starved, fold-adoption-unverified, doc-timeout,
// agent-contract-mismatch, object-too-large) must not vanish and must not
// smuggle a re-entry past the C.6 gate. Instead it triggers an immediate,
// gate-respecting fresh start: sever(Orphan) the owned issue and re-mint it as a
// fresh ACTIVE clarify Task via the shared MintForItem funnel.
//
// LEADER-ONLY (runs from the project reconcile, alongside driveUnparks; the
// reaper is the ultimate backstop). It never touches the F.6 re-entry surface
// (backlog-sweep/awaiting-human/identity-unverified/merge-timeout/deploy-timeout/
// no-outcome/handoff-stalled are stage.HasReentry and handled by
// driveUnparks/reverifyParked).
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
	var openNames []string
	for i := range issues {
		if issues[i].Status.State == "open" {
			openNames = append(openNames, issues[i].Name)
		}
	}
	if len(openNames) == 0 {
		return nil // the reply is not on a live owned issue (a closed issue is the I3 path).
	}

	// Close the old Task's own bot PR FIRST, no issue side effect. Retry-safe and
	// idempotent (AnnTerminalClosed marker); a failure returns before anything is
	// severed, so the whole resume retries cleanly next pass.
	if err := r.closeTaskBotMRs(ctx, proj, t); err != nil {
		return err
	}

	// Sever ALL owned open issues BEFORE any MintForItem. On the collision path
	// (old Task kind == clarify, same IntakeTaskName) the FIRST mint deletes the old
	// stale-terminal Task, which would make a later in-loop sever hard-fault; doing
	// every Task-side IssueRefs clear up front removes that window (and sever now
	// tolerates a gone Task anyway). Each issue is LIVE-READ off the uncached
	// APIReader so the webhook's just-appended human reply is ALWAYS visible at mint
	// time (mirrors the #348/#352 live-read discipline): otherwise, on the
	// direct-mint path (old Task kind != clarify, so MintForItem creates the fresh
	// Task in THIS pass), a lagging cache hides the reply, humanHasLastWord is false,
	// the fresh Task mints parked(backlog-sweep), and - IssueRefs already cleared -
	// the human needs a SECOND reply. The live read preserves the one-reply guarantee.
	type mintJob struct {
		item ForgeItem
		repo *tatarav1alpha1.Repository
		name string
	}
	var jobs []mintJob
	for _, name := range openNames {
		live, err := r.liveIssue(ctx, proj.Namespace, name)
		if err != nil {
			return err
		}
		if live == nil {
			continue // already gone (concurrent reap): nothing to re-adopt.
		}
		r.stripForgeParkedLabel(ctx, proj, live) // best-effort operator-on-promotion (F.6)
		if err := SeverIssueFromTask(ctx, r.Client, t, name, SeverOrphan); err != nil {
			return err
		}
		repo, err := r.repositoryFor(ctx, proj.Namespace, live.Spec.RepositoryRef)
		if err != nil {
			return err
		}
		jobs = append(jobs, mintJob{item: forgeItemFromMirror(live), repo: repo, name: name})
	}

	for _, j := range jobs {
		if _, _, err := r.minter().MintForItem(ctx, proj, j.repo, j.item, false, nil); err != nil {
			return err
		}
		log.FromContext(ctx).Info("resumed a no-re-entry park from a human reply: re-minted the issue fresh",
			"action", "resume_remint", "resource_id", j.name, "old_task", t.Name, "reason", t.Status.StageReason)
	}
	return nil
}

// liveIssue reads an Issue mirror through the UNCACHED APIReader (falling back to
// the cached Client when none is wired, as in unit tests), so the WS3-I4 re-mint
// sees the webhook's just-appended human reply. A NotFound returns (nil, nil): the
// issue was reaped concurrently and there is nothing to re-adopt.
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
