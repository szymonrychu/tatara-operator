package controller

import (
	"context"
	"fmt"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// EnsureTaskForMRComment guarantees a Task owns the MR before its comment is
// delivered - the MR arm of the general "every human comment yields a Task
// update or creation" pipeline (the ISSUE arm is MintForItem's orphan-issue
// branch, called inline from handleIssueComment; MEMORY W3's driveCommentUnpark
// is what refreshes/unparks an EXISTING owner once delivery lands).
//
// An existing LIVE controller owner is returned unchanged - deliverPendingEvent's
// driveCommentUnpark refreshes/unparks it, this function does not need to.
// LIVE is load-bearing (issue #521): own.ControllerOwner alone returns true for
// a ref naming a Task the API server no longer has, so a reaped review Task's
// name was handed straight back to the webhook, which then delivered the
// human's comment as a TaskEvent to nothing. resolveLiveMROwner DROPS that ref,
// so the mint below runs on this same call. An
// orphan OPEN MR with a NON-BOT author in PR reaction scope mints its review
// Task inline via the SAME PRReview rule ClassifyPR/the sweep use
// (MintReviewStage + MintReviewTask), race-safe with the sweep through
// createTaskRaceSafe. A bot-authored comment, a closed/merged MR, or an
// out-of-scope PR mints nothing and returns ("", false, nil) - the caller
// treats that as accepted-ignored, not an error.
func (m *Minter) EnsureTaskForMRComment(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest, author string) (string, bool, error) {

	ownerName, err := m.resolveLiveMROwner(ctx, proj, mr, m.activity())
	if err != nil {
		return "", false, err
	}
	if ownerName != "" {
		return ownerName, false, nil
	}
	if mr.Status.State != "open" {
		return "", false, nil
	}
	bot := botLoginOf(proj)
	if bot != "" && author == bot {
		return "", false, nil
	}
	pr := prRefFromMR(repo, mr)
	if !prInReactionScope(proj, repo, prCandidate(pr), bot) {
		return "", false, nil
	}
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
}

// prRefFromMR adapts a MergeRequest mirror CR onto the scm.PRRef the intake
// funnel's classify/mint predicates (ClassifyPR, prCandidate,
// prInReactionScope, MintReviewTask) all consume. It is mrSnapshot's inverse:
// mrSnapshot builds a mirror-upsert snapshot FROM a PRRef the sweep/webhook
// already listed from the forge; this builds one FROM the mirror CR itself,
// for the comment path where the caller has the CR but not a fresh forge
// listing. The mirror carries no Labels (MergeRequestStatus has none), so
// prInReactionScope's labeledOrMentioned scope can only admit this comment via
// the trusted-author check or an @-mention in the body, never the trigger
// label - matching what a comment-only signal can actually know.
func prRefFromMR(repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest) scm.PRRef {
	pr := scm.PRRef{
		Repo:       repoSlug(repo),
		Number:     mr.Spec.Number,
		Author:     mr.Status.Author,
		HeadSHA:    mr.Status.HeadSHA,
		HeadBranch: mr.Status.HeadBranch,
		Body:       mr.Status.Body,
	}
	if mr.Status.UpdatedAt != nil {
		pr.UpdatedAt = mr.Status.UpdatedAt.Time
	}
	return pr
}
