package controller

import (
	"context"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// EnsureTaskForMRComment guarantees a Task owns the MR before its comment is
// delivered - the MR arm of the general "every human comment yields a Task
// update or creation" pipeline (the ISSUE arm is MintForItem's orphan-issue
// branch, called inline from handleIssueComment; MEMORY W3's driveCommentUnpark
// is what refreshes/unparks an EXISTING owner once delivery lands).
//
// An existing controller owner is returned unchanged - deliverPendingEvent's
// driveCommentUnpark refreshes/unparks it, this function does not need to. An
// orphan OPEN MR with a NON-BOT author in PR reaction scope mints its review
// Task inline via the SAME PRReview rule ClassifyPR/the sweep use
// (MintReviewStage + MintReviewTask), race-safe with the sweep through
// createTaskRaceSafe. A bot-authored comment, a closed/merged MR, or an
// out-of-scope PR mints nothing and returns ("", false, nil) - the caller
// treats that as accepted-ignored, not an error.
func (m *Minter) EnsureTaskForMRComment(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest, author string) (string, bool, error) {

	if ownerName, ok := own.ControllerOwner(mr); ok {
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
	task, _, err := m.MintReviewTask(ctx, proj, repo, pr, mr, stg, reason, m.spillerFor(proj))
	if err != nil {
		return "", false, err
	}
	return task.Name, true, nil
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
