package controller

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// OwnershipMarker is the hidden, greppable idempotency marker for an ownership
// announcement, keyed on the flip timestamp so each distinct flip is announced
// exactly once (same two-sided design as the review-round markers: forge
// marker as the idempotency key, checked by threadCarriesMarker).
func OwnershipMarker(direction string, changedAt metav1.Time) string {
	return fmt.Sprintf("<!-- tatara-ownership dir=%s at=%s -->", direction, changedAt.UTC().Format("2006-01-02T15:04:05Z"))
}

const (
	takeoverAnnounce  = "Taking ownership of this MR at @%s's request - I'll handle conflicts, requested changes, and merge on green review. Push to the branch and I'll stand down."
	standdownAnnounce = "A commit outside tatara landed on this branch - standing down from pushing. I'll keep reviewing, and the operator will still merge on an approved review. Comment 'take over' to hand it back."
)

// trimTakeoverUser reads the requesting user back out of a
// "takeover-requested-by:<user>" ownershipReason.
func trimTakeoverUser(reason string) string {
	return strings.TrimPrefix(reason, "takeover-requested-by:")
}

// DrainOwnershipAnnouncement posts a single announcement comment for the MR's
// CURRENT ownership state, if one has not already been posted for this flip
// timestamp. Convergent and idempotent: every flip writer (the takeover REST
// endpoint's to-tatara flip, ReconcileOwnership's to-external flip) only
// mutates status; this drain, running on every MergeRequest reconcile, does
// the forge write, gated on the forge already carrying OwnershipMarker for
// this flip's changedAt.
//
// A never-flipped MR (ownershipChangedAt == nil) and the initial classification
// (reason "initial"/"") are not flips and are never announced.
func (d *StageDriver) DrainOwnershipAnnouncement(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest) error {

	if mr.Status.OwnershipChangedAt == nil || mr.Status.OwnershipReason == "initial" || mr.Status.OwnershipReason == "" {
		return nil
	}
	direction := "to-external"
	body := standdownAnnounce
	if mr.Status.Ownership == tatarav1alpha1.OwnershipTatara {
		direction = "to-tatara"
		body = fmt.Sprintf(takeoverAnnounce, trimTakeoverUser(mr.Status.OwnershipReason))
	}
	marker := OwnershipMarker(direction, *mr.Status.OwnershipChangedAt)

	writer, token, provider, err := d.forge(ctx, proj)
	if err != nil {
		return err
	}
	reader, err := d.reader(provider, token)
	if err != nil {
		return err
	}
	owner, name, err := scm.OwnerRepo(repo.Spec.URL)
	if err != nil {
		return fmt.Errorf("ownership announce: owner/repo for %s: %w", repo.Name, err)
	}
	thread, err := listThreadComments(ctx, reader, mr, owner, name, mr.Spec.Number)
	if err != nil {
		return err
	}
	if threadCarriesMarker(thread, marker) {
		return nil // already announced this flip
	}
	slug, _, err := repoSlugFromURL(repo.Spec.URL, provider)
	if err != nil {
		return fmt.Errorf("ownership announce: repo slug for %s: %w", repo.Name, err)
	}
	if err := writer.Comment(ctx, token, commentRef(slug, provider, mr.Spec.Number, true), marker+"\n"+body); err != nil {
		RecordSCM(d.Metrics, provider, "comment", err)
		return fmt.Errorf("ownership announce %s: %w", mr.Name, err)
	}
	RecordSCM(d.Metrics, provider, "comment", nil)
	log.FromContext(ctx).Info("ownership announcement posted", "action", "ownership_announce",
		"resource_id", mr.Name, "direction", direction)
	return nil
}
