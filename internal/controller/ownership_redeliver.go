package controller

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// redeliverMRComments is OP12's job: replaying comments an external MR missed
// while it had no owning Task in scope, or that simply arrived between sweeps.
// Called only by ReconcileOwnership, with incoming already narrowed to
// comments newer than the mirror's LastMirroredCommentID cursor
// (listPRCommentsAfter, sweep.go) - never a re-derivation of that filter here.
//
// For every new comment it: mirrors it onto the MR CR
// (AppendCommentToMirror - the SAME C.5 write-back helper the webhook uses),
// and - if the MR carries a controller owner - delivers it as an mr_comment
// TaskEvent via AppendTaskEvent, the SAME capped-append the webhook fast path
// uses (OP12 relocated it to this package precisely so the two paths share
// one implementation, never two that can drift). The cursor advances to the
// newest comment actually processed, in ONE write at the end, so a crash
// mid-loop re-runs as a partial no-op (already-mirrored comments dedup on
// ExternalID) rather than a lost cursor advance.
//
// Belt-and-suspenders (the OP6 convergence amendment): sweepPRs already mints
// the review Task for an in-scope orphan MR on the PRReview classification
// (ClassifyPR -> MintReviewTask) - the SAME rule EnsureTaskForMRComment (OP6)
// uses on the webhook fast path - so by the time this runs, an owner normally
// already exists. If ControllerOwner is still empty (a caller reaching here
// without going through that mint), EnsureTaskForMRComment - the exact
// function the webhook calls - is run here too, so the webhook fast path and
// this sweep convergence path never diverge on how an owner gets minted.
func (d *StageDriver) redeliverMRComments(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest, incoming []scm.IssueComment) error {

	ownerName, hasOwner := own.ControllerOwner(mr)
	if !hasOwner && len(incoming) > 0 {
		// incoming[0].Author is the mint's author-of-record for this belt-and-
		// suspenders path; it relies on the sweep's prior PRReview
		// classification (ClassifyPR -> MintReviewTask, see above) having
		// already filtered out bot-first pages, making a bot incoming[0] here
		// unreachable in practice.
		newOwner, _, err := d.minter().EnsureTaskForMRComment(ctx, proj, repo, mr, incoming[0].Author)
		if err != nil {
			return err
		}
		if newOwner != "" {
			ownerName, hasOwner = newOwner, true
		}
	}

	newest := mr.Status.LastMirroredCommentID
	for _, c := range incoming {
		if c.ExternalID == "" || c.ExternalID == mr.Status.LastMirroredCommentID {
			continue
		}
		if proj.Spec.Scm != nil && c.Author == proj.Spec.Scm.BotLogin {
			newest = c.ExternalID
			continue // never redeliver the bot's own comment
		}
		cmt := tatarav1alpha1.Comment{
			ExternalID: c.ExternalID, Author: c.Author, Body: c.Body, CreatedAt: metav1.NewTime(c.CreatedAt),
		}
		if err := AppendCommentToMirror(ctx, d.Client, d.spiller(proj), mr, cmt); err != nil {
			return err
		}
		if hasOwner {
			var task tatarav1alpha1.Task
			if err := d.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: ownerName}, &task); err == nil {
				ev := tatarav1alpha1.TaskEvent{
					At: metav1.Now(), Kind: "mr_comment", Repo: repo.Name,
					Number: mr.Spec.Number, Author: c.Author, Body: c.Body,
				}
				if err := AppendTaskEvent(ctx, d.Client, &task, ev); err != nil {
					return err
				}
			} else if !apierrors.IsNotFound(err) {
				return err
			}
		}
		newest = c.ExternalID
	}
	if newest != mr.Status.LastMirroredCommentID {
		key := client.ObjectKeyFromObject(mr)
		return objbudget.FitMergeRequest(ctx, d.Client, d.spiller(proj), key, func(m *tatarav1alpha1.MergeRequest) {
			m.Status.LastMirroredCommentID = newest
		})
	}
	return nil
}
