package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// THE RED-CI GATE (issue #476).
//
// A review verdict says nothing about CI. helmfile!1358 went red at 01:41:30Z,
// the review approved it at 01:44:21Z and the Task entered merging at 01:44:24Z
// - 2m54s AFTER the change had become provably unmergeable. merging is POD-LESS,
// so nothing in there can fix a failing test; it re-read the same verdict every
// 60s until the 4h budget parked it, un-parked straight back in, and burned ~16h
// and four alert firings to reach a conclusion that was decided in three
// minutes.
//
// The state needed to avoid all of that is one live read. It is done LIVE, never
// from mr.status.ciStatus: the mirror is synced on push, and in the incident it
// had last synced at 01:41:25Z - five seconds BEFORE the job failed - so a
// mirror-based guard would have read "pending" and promoted anyway.
//
// ONLY "failure" trips the gate. "" (no CI at all) and "pending"/"running" are
// NOT verdicts, and treating them as one would bounce every Task whose checks
// are merely still running.
//
// THE TWO SITES ARE NOT EQUALLY LOAD-BEARING, and that asymmetry is deliberate.
// The merging-side gate is the one that must hold: it sits on a 60s requeue, so
// a read that fails is simply re-read a minute later. The reviewing-side gate is
// an OPTIMISATION on top of it - it saves the pointless promotion - and it
// therefore FAILS OPEN. A reviewing advance that returned an error instead would
// wedge the Task: reconcileClocks evaluates this before the HandoffDeadline and
// before stage.ArmedClock, so an error there is a Task that reaches neither, in
// a stage whose whole contract is "no stage without an exit deadline". Nothing
// is lost by promoting on a failed read - merging re-checks within 60s and
// leaves through this same edge.

// ciRedAtReviewedHead returns the owned MergeRequest whose LIVE CI has FAILED at
// the head that was REVIEWED, or nil when none has.
//
// A red head that is NOT the reviewed head is deliberately ignored: someone
// pushed after the review, and that is the head-moved bounce's business (it
// re-reviews rather than re-implements). Merged and closed MRs are skipped -
// their CI can no longer block anything.
//
// IT FOLDS A LIVE-MERGED OBSERVATION ONTO THE IN-MEMORY MIRROR COPY. The routing
// in stage.CIRed reads anyMerged(mrs), which is a MIRROR read, and the mirror
// sweep is hourly; an MR merged out of band (the C.9 accepted risk) would
// otherwise still read "open" here and let a red sibling route the Task to
// implementing - re-proposing merged code and recreating deleted branches, the
// one boundary this gate must not cross. ReconcileMerging gets this for free
// from stampMerged; the reviewing-side callers get it from here. The write is
// in-memory only: it exists to make the DECISION honest, and the mirror sync
// owns persisting it.
//
// A nil scmFor (unit tests with no forge wiring) yields nil: the gate is a
// refinement of the promotion, not a precondition for it, so an unwired driver
// behaves exactly as it did before this gate existed.
func ciRedAtReviewedHead(ctx context.Context, c client.Client, scmFor func(string) (scm.SCMWriter, error),
	m *obs.OperatorMetrics, proj *tatarav1alpha1.Project,
	mrs []tatarav1alpha1.MergeRequest) (*tatarav1alpha1.MergeRequest, error) {

	if scmFor == nil || len(mrs) == 0 {
		return nil, nil
	}
	provider := "github"
	if proj.Spec.Scm != nil && proj.Spec.Scm.Provider != "" {
		provider = proj.Spec.Scm.Provider
	}
	writer, err := scmFor(provider)
	if err != nil {
		return nil, fmt.Errorf("ci gate: scm writer: %w", err)
	}
	token, err := mirrorSCMToken(ctx, c, proj)
	if err != nil {
		return nil, err
	}
	var red *tatarav1alpha1.MergeRequest
	for i := range mrs {
		mr := &mrs[i]
		if mr.Status.State == "merged" || mr.Status.State == "closed" {
			continue
		}
		var repo tatarav1alpha1.Repository
		if err := c.Get(ctx, types.NamespacedName{Namespace: mr.Namespace, Name: mr.Spec.RepositoryRef}, &repo); err != nil {
			return nil, fmt.Errorf("ci gate: get repository %s: %w", mr.Spec.RepositoryRef, err)
		}
		st, err := writer.GetPRState(ctx, repo.Spec.URL, token, mr.Spec.Number)
		RecordSCM(m, provider, "get_pr_state", err)
		if err != nil {
			return nil, fmt.Errorf("ci gate: get pr state %s!%d: %w", mr.Spec.RepositoryRef, mr.Spec.Number, err)
		}
		if st.Merged {
			mr.Status.State = "merged"
			continue
		}
		if st.CIStatus != "failure" {
			continue
		}
		if mr.Status.ReviewedSHA != "" && st.HeadSHA != "" && st.HeadSHA != mr.Status.ReviewedSHA {
			continue // red on a head nobody reviewed: the head-moved bounce owns it
		}
		if red == nil {
			// Keep going: a later MR may be live-merged, and the merged/finished
			// boundary outranks the red one.
			red = mr
		}
	}
	return red, nil
}

// enterCIRed applies stage.CIRed and persists status.ciRedReentries in the SAME
// write as the stage. The counter is the bound on cycle 5, and a bound written
// by a second call is a bound a crash between the two can lose.
//
// It appends an operator NOTE first, for the same reason the request_changes
// path appends reviewBeltNote: the bundle an implement pod renders carries the
// MIRROR's ci status, whose staleness is this whole gate's premise, so without
// the note the pod boots with no statement that CI is red, no PR, no SHA - three
// laps of guessing that end at failed(ci-blocked). The note is idempotent on its
// body and carries the lap number, so a retry does not duplicate it and lap 2
// does not dedup against lap 1.
//
// The counter, the log and the gauge cleanup all land AFTER the write. EnterStage
// can fail and be retried, and transition.go is explicit that a metric emitted
// inside the retried region counts once per retry.
func enterCIRed(ctx context.Context, c client.Client, sp objbudget.Spiller, m *obs.OperatorMetrics,
	task *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest,
	red *tatarav1alpha1.MergeRequest, now time.Time) error {

	from := task.Status.Stage
	edge, _ := stage.CIRed(task, mrs, tatarav1alpha1.MaxCIRedReentries)
	reentries := task.Status.CIRedReentries
	if err := appendOperatorNoteTo(ctx, c, sp, task, ciRedNote(red, reentries), now); err != nil {
		return err
	}
	if err := EnterStage(ctx, c, sp, m, task, mrs, edge.To, edge.Reason, now,
		func(t *tatarav1alpha1.Task) { t.Status.CIRedReentries = reentries }); err != nil {
		return err
	}
	obs.ClearMergeCursorStalled(task.Name)
	obs.CIRedExitTotal.WithLabelValues(red.Spec.RepositoryRef, from, edge.To).Inc()
	log.FromContext(ctx).Info("ci gate: the required checks are RED at the reviewed head; leaving the merge path",
		"action", "ci_red_exit", "resource_id", task.Name, "from", from,
		"to", edge.To, "stage_reason", edge.Reason, "repo", red.Spec.RepositoryRef,
		"pr", red.Spec.Number, "sha", red.Status.ReviewedSHA, "ci_red_reentries", reentries)
	return nil
}

// ciRedNote is what the next implement pod actually reads. It names the repo, the
// PR and the exact SHA whose checks failed, because the bundle's own ci field is
// the stale mirror value.
func ciRedNote(red *tatarav1alpha1.MergeRequest, lap int) string {
	sha := red.Status.ReviewedSHA
	if sha == "" {
		sha = red.Status.HeadSHA
	}
	return fmt.Sprintf(
		"CI is RED on %s!%d at the reviewed head %s (attempt %d of %d). The change was reviewed "+
			"and cannot merge until the required checks pass. Read the failing job on the forge, fix it, "+
			"and submit again - do not re-open the review discussion.",
		red.Spec.RepositoryRef, red.Spec.Number, sha, lap, tatarav1alpha1.MaxCIRedReentries)
}
