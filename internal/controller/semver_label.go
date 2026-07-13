package controller

import (
	"context"
	"fmt"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// AnnSemverLabel records the semver:<level> label the operator has PROJECTED
// onto the forge PR (value: the label itself). It is the projection's
// idempotency key.
//
// It is an ANNOTATION and not a status field because MergeRequestStatus has no
// room for one, and the alternative - re-reading the PR's live label set on
// every reconcile pass - spends a forge call per MR per cadence tick against the
// same rate limit the merge loop needs.
const AnnSemverLabel = "tatara.dev/semver-label"

// semverLabels is the closed set of levels, in the order the projection strips
// the ones it did not want.
var semverLabels = []string{semverLabelMajor, semverLabelMinor, semverLabelPatch}

// semverLabelColors is the managed palette for the release lever. It is the ONE
// table: managedLabelColors folds it in, so the label the projection ensures and
// the label the project-scan pre-colours can never drift apart.
var semverLabelColors = map[string]string{
	semverLabelMajor: "b60205", // major - red
	semverLabelMinor: "d93f0b", // minor - orange
	semverLabelPatch: "0e8a16", // patch - green
}

// semverLabelFor maps a declared change significance onto its managed label. An
// unknown or empty significance maps to "" - the operator NEVER invents one.
func semverLabelFor(significance string) string {
	switch significance {
	case "major":
		return semverLabelMajor
	case "minor":
		return semverLabelMinor
	case "patch":
		return semverLabelPatch
	default:
		return ""
	}
}

// prLabelRef renders the label-write ref for a PULL REQUEST. GitHub labels a PR
// through its issue route (owner/repo#N). GitLab routes on the sigil: '!' is the
// merge request, '#' is the ISSUE with the same iid - so a '#' ref would label an
// unrelated issue and leave the MR bare.
func prLabelRef(slug, provider string, number int) string {
	if provider == "gitlab" {
		return fmt.Sprintf("%s!%d", slug, number)
	}
	return fmt.Sprintf("%s#%d", slug, number)
}

// ProjectSemverLabel projects MergeRequest.status.significance onto the forge
// PR's semver:<level> label (contract H.4).
//
// THE RELEASE TRAIN RUNS ON THIS LABEL. CI cuts the release tag FROM IT: no
// label -> no tag -> nothing published -> no version pin propagates ->
// tatara-helmfile applies nothing -> status.deployedAt is never stamped -> the
// Task never leaves deploying, and the whole platform wedges there.
//
// It is a ONE-WAY projection of status.significance, the same shape as the C.6
// Issue label projection: no label is ever READ to produce significance.
// status.significance is IMPLEMENT-OWNED and a review may only ESCALATE it
// (internal/restapi/outcome.go), so a RAISE arrives here as a CHANGED value and
// REPLACES the label - the superseded level is stripped, never left beside the
// new one, because a PR carrying both lets CI key the tag off either. A LOWER
// never arrives: /outcome refuses it, so the value is unchanged and this is a
// no-op.
//
// An EMPTY significance writes NOTHING. That is the human PR (a kind=review Task
// mirrors a PR the operator did not author): H.4 says a HUMAN sets the label
// there, and the operator never invents a level. ReconcileMerging is what makes
// an empty significance on a PR the operator is about to MERGE loud.
func (d *StageDriver) ProjectSemverLabel(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest) error {
	if d.SCMFor == nil || mr.Status.State == "closed" {
		return nil
	}
	want := semverLabelFor(mr.Status.Significance)
	// IDEMPOTENT: re-applying must not duplicate the label (GitHub 422s a second
	// identical review; a second AddLabel is merely a wasted call, and both are
	// avoidable).
	if want == "" || mr.Annotations[AnnSemverLabel] == want {
		return nil
	}

	writer, token, provider, err := d.forge(ctx, proj)
	if err != nil {
		return err
	}
	slug, _, err := repoSlugFromURL(repo.Spec.URL, provider)
	if err != nil {
		return fmt.Errorf("semver label: repo slug for %s: %w", repo.Name, err)
	}
	ref := prLabelRef(slug, provider, mr.Spec.Number)
	level := mr.Status.Significance
	l := log.FromContext(ctx)

	// The label must EXIST with its managed colour first: GitHub auto-creates an
	// unknown label with a random colour, GitLab 404s.
	ensureErr := writer.EnsureLabel(ctx, repo.Spec.URL, token, want, semverLabelColors[want])
	RecordSCM(d.Metrics, provider, "ensure_label", ensureErr)
	if ensureErr != nil {
		obs.SemverLabelTotal.WithLabelValues(mr.Spec.RepositoryRef, level, "error").Inc()
		return fmt.Errorf("semver label: ensure %s on %s: %w", want, repo.Name, ensureErr)
	}
	addErr := writer.AddLabel(ctx, token, ref, want)
	RecordSCM(d.Metrics, provider, "add_label", addErr)
	if addErr != nil {
		if isPermanentTargetGone(addErr) {
			l.Info("semver: the label target is gone; skipping",
				"action", "semver_label", "resource_id", mr.Name, "pr_ref", ref)
			return nil
		}
		obs.SemverLabelTotal.WithLabelValues(mr.Spec.RepositoryRef, level, "error").Inc()
		return fmt.Errorf("semver label: add %s to %s: %w", want, ref, addErr)
	}
	// A RAISE REPLACES. RemoveLabel is best-effort on both providers: removing a
	// label the PR does not carry is a no-op.
	for _, other := range semverLabels {
		if other == want {
			continue
		}
		removeErr := writer.RemoveLabel(ctx, token, ref, other)
		RecordSCM(d.Metrics, provider, "remove_label", removeErr)
		if removeErr != nil && !isPermanentTargetGone(removeErr) {
			l.Error(removeErr, "semver: removing a superseded semver label failed",
				"action", "semver_label", "resource_id", mr.Name, "pr_ref", ref, "label", other)
		}
	}
	if err := annotateMergeRequest(ctx, d.Client, mr, AnnSemverLabel, want); err != nil {
		return fmt.Errorf("semver label: record %s on %s: %w", want, mr.Name, err)
	}

	obs.SemverLabelTotal.WithLabelValues(mr.Spec.RepositoryRef, level, "applied").Inc()
	l.Info("semver: projected the declared significance onto the PR label",
		"action", "semver_label_applied", "resource_id", mr.Name, "repo", mr.Spec.RepositoryRef,
		"pr", mr.Spec.Number, "significance", level, "label", want, "provider", provider)
	return nil
}

// annotateMergeRequest persists ONE metadata marker on a MergeRequest CR. It is
// a metadata write, not a status write: nothing here competes with the status
// subresource, so the marker can never lose a race with a reconciler.
func annotateMergeRequest(ctx context.Context, c client.Client, mr *tatarav1alpha1.MergeRequest, key, value string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cur tatarav1alpha1.MergeRequest
		if err := c.Get(ctx, client.ObjectKeyFromObject(mr), &cur); err != nil {
			return err
		}
		if cur.Annotations == nil {
			cur.Annotations = map[string]string{}
		}
		cur.Annotations[key] = value
		if err := c.Update(ctx, &cur); err != nil {
			return err
		}
		mr.SetAnnotations(cur.Annotations)
		mr.SetResourceVersion(cur.GetResourceVersion())
		return nil
	})
}
