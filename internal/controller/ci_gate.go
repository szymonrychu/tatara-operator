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
// THE SITES ARE NOT EQUALLY LOAD-BEARING, and that asymmetry is deliberate.
// The merging-side gate is the one that must hold: it sits on a 60s requeue, so
// a read that fails is simply re-read a minute later. Every other site is an
// OPTIMISATION on top of it - it saves a pointless promotion, or a pointless
// pod - and every one of them therefore FAILS OPEN. A reviewing advance that
// returned an error instead would wedge the Task: reconcileClocks evaluates this
// before the HandoffDeadline and before stage.ArmedClock, so an error there is a
// Task that reaches neither, in a stage whose whole contract is "no stage
// without an exit deadline". Nothing is lost by promoting on a failed read -
// merging re-checks within 60s and leaves through this same edge.
//
// THE SCOPE, AS OF PR B. This comment used to say the gate fired ONLY on
// edge.To == StateMerged - the merge corridor, after an approve verdict - and
// that was honest and was the whole bug. A pipeline that went red at 09:00 was
// not noticed until the change had been reviewed, approved, and promoted; a
// review pod spent a full turn reading a diff that could never land; and the
// implement pod that could have fixed it in ninety seconds had been gone for
// twenty minutes. There are now FOUR sites, in the order a change meets them:
//
//  1. THE IMPLEMENT SUBMISSION (restapi/readiness.go). Red at the pushed head
//     REFUSES the outcome while the agent is still in-turn. Pending HOLDS the
//     Task on status.ciWaitSince instead of advancing.
//  2. THE CI HOLD (reconcileCIWait, task_stage.go). Resolves that hold: green
//     advances to awaiting-review, red bounces through enterCIRed, and
//     CIWaitDeadline advances anyway so the hold is never a dead end.
//  3. AWAITING-REVIEW (reconcileClocks). A pipeline that goes red AFTER the
//     review pod spawned bounces the Task back to the agent that can fix it,
//     instead of letting the round finish and re-discovering the verdict in the
//     merge corridor. Triggered on the MIRROR's ciStatus - which PR A made real
//     and refreshes on a 5m backstop - and only CONFIRMED live, so the
//     30s-requeue reconcile loop does not turn into one forge call per Task per
//     30s.
//  4. THE MERGE CORRIDOR (merge.go, reviewpost.go, task_stage.go's re-drive),
//     unchanged, still the one that must hold.
//
// SITES 1-3 COVER A TAKEN-OVER MR FOR FREE, and that is worth stating because it
// reads like an omission. A maintainer takeover flips
// MergeRequest.status.ownership to "external"; it does NOT delete the mirror, so
// the MR is still an owned open MergeRequest and every fold below still sees it.
// Ownership gates who may PUSH and who may MERGE, never whether CI is read.

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

// The MIRROR verdict vocabulary. It is folded from
// MergeRequest.status.ciStatus, which PR A gave three real writers, and it is
// the CHEAP TRIGGER for sites 2 and 3: a mirror read costs nothing, so it can
// run on every reconcile, and only a mirror that says "red" ever pays for a live
// confirmation.
const (
	ciVerdictClear   = "clear"
	ciVerdictPending = "pending"
	ciVerdictRed     = "red"
)

// ciMirrorVerdict folds status.ciStatus across the still-open owned MRs.
//
// RED WINS OVER PENDING, and both win over clear. One repo of a three-repo
// change whose pipeline failed makes the whole change unmergeable, and the merge
// order means the failing one may not even be the one being merged first.
//
// "none" AND "" FOLD TO CLEAR, NOT TO PENDING. A repo with no CI configured at
// all is the normal case for a docs repo, and treating it as pending would hold
// every such Task for the full CIWaitDeadline before failing open. The
// distinction that matters is between a verdict and the absence of a pipeline,
// not between a verdict and the absence of an observation.
//
// Merged and closed MRs are skipped: their CI can no longer block anything.
func ciMirrorVerdict(mrs []tatarav1alpha1.MergeRequest) string {
	out := ciVerdictClear
	for i := range mrs {
		mr := &mrs[i]
		if mr.Status.State == "merged" || mr.Status.State == "closed" {
			continue
		}
		switch mr.Status.CIStatus {
		case scm.CIMirrorRed:
			return ciVerdictRed
		case scm.CIMirrorPending, scm.CIMirrorRunning:
			out = ciVerdictPending
		}
	}
	return out
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

	return enterCIRedWith(ctx, c, sp, m, task, mrs, red, now, nil)
}

// enterCIRedWith is enterCIRed's body with one extra status mutation folded into
// the SAME write, for the site that has one: the CI hold, which must clear
// status.ciWaitSince and revoke the conditional acceptance atomically with the
// bounce.
//
// IT HAS A THIRD BRANCH THE ORIGINAL DID NOT NEED. stage.CIRed's transition
// outcome targets under-implementation, and site 2 is ALREADY at
// under-implementation - so EnterStage takes its to==from silent-no-op path
// (transition.go), which returns before FitTask and drops the mutate with it.
// The counter and the caller's extra would be silently lost. On a self-edge the
// mutation is therefore applied directly, which is correct rather than merely
// equivalent: the Task genuinely is not moving, and re-stamping StateEnteredAt /
// PodStartedAt is exactly what #403 forbade.
func enterCIRedWith(ctx context.Context, c client.Client, sp objbudget.Spiller, m *obs.OperatorMetrics,
	task *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest,
	red *tatarav1alpha1.MergeRequest, now time.Time, extra func(*tatarav1alpha1.Task)) error {

	from := task.Status.State
	edge, _ := stage.CIRed(task, mrs, tatarav1alpha1.MaxCIRedReentries)
	reentries := task.Status.CIRedReentries
	if err := appendOperatorNoteTo(ctx, c, sp, task, ciRedNote(red, reentries), now); err != nil {
		return err
	}
	mutate := func(t *tatarav1alpha1.Task) {
		t.Status.CIRedReentries = reentries
		if extra != nil {
			extra(t)
		}
	}
	// stage.CIRed returns a PARK for three of its four outcomes (kind=review,
	// an already-merged sibling, and the spent re-entry budget) and a
	// TRANSITION for the fourth. Parking is not an edge any more (#521), so
	// EnterStage refuses stage.ParkTarget by design and an unbranched applier
	// would 500 on the majority outcome.
	switch edge.To {
	case stage.ParkTarget:
		if err := ParkTask(ctx, c, sp, m, task, edge.Reason, now, mutate); err != nil {
			return err
		}
	case from:
		if err := objbudget.FitTask(ctx, c, sp, client.ObjectKeyFromObject(task), mutate); err != nil {
			return err
		}
	default:
		if err := EnterStage(ctx, c, sp, m, task, mrs, edge.To, edge.Reason, now, mutate); err != nil {
			return err
		}
	}
	obs.ClearMergeCursorStalled(task.Name)
	obs.CIRedExitTotal.WithLabelValues(red.Spec.RepositoryRef, from, edge.To).Inc()
	log.FromContext(ctx).Info("ci gate: the required checks are RED at the reviewed head; leaving the merge path",
		"action", "ci_red_exit", "resource_id", task.Name, "from", from,
		"to", edge.To, "reason", edge.Reason, "repo", red.Spec.RepositoryRef,
		"pr", red.Spec.Number, "sha", red.Status.ReviewedSHA, "ci_red_reentries", reentries)
	return nil
}

// ciRedNote is what the next implement pod actually reads. It names the repo, the
// PR and the exact SHA whose checks failed, because the bundle's own ci field is
// the stale mirror value.
//
// IT SPLITS ON WHETHER THE HEAD WAS EVER REVIEWED. Before PR B the only way to
// reach this note was through the merge corridor, so "the change was reviewed"
// was always true and the closing "do not re-open the review discussion" was
// always the right instruction. Sites 1-3 can now fire on a head no reviewer has
// ever seen, and telling that agent not to re-open a discussion that does not
// exist is the kind of stale operator prose an agent spends a lap trying to obey.
func ciRedNote(red *tatarav1alpha1.MergeRequest, lap int) string {
	if sha := red.Status.ReviewedSHA; sha != "" {
		return fmt.Sprintf(
			"CI is RED on %s!%d at the reviewed head %s (attempt %d of %d). The change was reviewed "+
				"and cannot merge until the required checks pass. Read the failing job on the forge, fix it, "+
				"and submit again - do not re-open the review discussion.",
			red.Spec.RepositoryRef, red.Spec.Number, sha, lap, tatarav1alpha1.MaxCIRedReentries)
	}
	return fmt.Sprintf(
		"CI is RED on %s!%d at the head you pushed, %s (attempt %d of %d). Nothing has reviewed this "+
			"change yet and nothing will until the required checks pass. Read the failing job "+
			"(`scm_read(kind=\"ci\", repo=%q, number=%d)`), fix it, push, and submit again.",
		red.Spec.RepositoryRef, red.Spec.Number, red.Status.HeadSHA, lap,
		tatarav1alpha1.MaxCIRedReentries, red.Spec.RepositoryRef, red.Spec.Number)
}
