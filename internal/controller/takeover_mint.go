package controller

import (
	"context"
	"fmt"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// takeoverKind is Task.Spec.Kind for a maintainer-gated takeover of an existing
// MR (MR ownership design). It is its own kind - not review, not clarify - so
// its deterministic natural-key name never collides with the review Task or a
// clarify/issue Task minted for the same forge number.
const takeoverKind = "takeover"

// takeoverTaskName is the deterministic natural-key name for the ONE
// full-lifecycle takeover Task an MR ever gets, across every flip round: a
// maintainer can take-over/stand-down/take-over many times over an MR's life,
// and every round must land on the SAME Task so its history (turn count,
// review rounds, MR ownership) is never reset by a fresh mint.
func takeoverTaskName(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int) string {
	return tatarav1alpha1.IntakeTaskName(proj.Name, takeoverKind, repo.Name, number)
}

// TakeoverTaskName is the exported form of takeoverTaskName. OP9's takeover
// REST endpoint (internal/restapi) needs the identical deterministic name to
// reason about the takeover Task without re-deriving the "takeover" kind
// string itself, which would risk the two packages drifting on the naming
// scheme.
func TakeoverTaskName(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int) string {
	return takeoverTaskName(proj, repo, number)
}

// MintOrUnparkTakeoverTask is the ONE entry point a maintainer's "take over"
// comment drives (OP9's takeover endpoint calls this). It returns the single
// full-lifecycle takeover Task bound to mr:
//
//   - No Task exists yet for (proj, takeover, repo, mr.Spec.Number): mint one
//     straight into `approved` (the Create->approved edge stage.go carries for
//     exactly this), carrying the MR's existing head branch as
//     AnnTakeoverHeadBranch so the implement pod PUSHES to it instead of a
//     derived tatara/* branch, and controller-own the MR mirror.
//   - The Task already exists and is parked(ownership-lost) - a re-take after
//     a stand-down: re-enter `approved` (parked->approved(ownership-lost)) so
//     the SAME Task resumes pushing, rather than minting a duplicate.
//   - The Task already exists in any OTHER live stage (including a fresh
//     mint that raced us here): return it unchanged. Idempotent.
//
// The OTHER exit from parked(ownership-lost) - parked->merging - is driven by
// DrainStandDownMerge (OP11) on an approved review; that is a different
// trigger (a maintainer's REVIEW, not a maintainer's COMMENT) and does not
// belong on this path, which always targets approved (RESUME PUSHING).
func (m *Minter) MintOrUnparkTakeoverTask(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest, requestingUser, commentBody string,
	sp objbudget.Spiller) (*tatarav1alpha1.Task, error) {

	name := takeoverTaskName(proj, repo, mr.Spec.Number)

	var existing tatarav1alpha1.Task
	err := m.Client.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: name}, &existing)
	if err == nil {
		if existing.Status.Stage == tatarav1alpha1.StageParked && existing.Status.StageReason == stage.ReasonOwnershipLost {
			if eerr := EnterStage(ctx, m.Client, sp, m.Metrics, &existing, nil,
				tatarav1alpha1.StageApproved, stage.ReasonOwnershipLost, time.Now(), nil); eerr != nil {
				return nil, fmt.Errorf("takeover: re-enter approved on %s: %w", existing.Name, eerr)
			}
		}
		return &existing, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("takeover: get task %s: %w", name, err)
	}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: proj.Namespace,
			Annotations: map[string]string{
				tatarav1alpha1.AnnTakeoverHeadBranch: mr.Status.HeadBranch,
			},
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj.Name,
			RepositoryRef: repo.Name,
			Kind:          takeoverKind,
			Goal:          takeoverGoal(mr, requestingUser, commentBody),
			InitialStage:  tatarav1alpha1.StageApproved,
			// InitialStageReason is left empty: approved is not a reason-required
			// target (reasonRequired only gates parked/failed/rejected) and
			// "requested by <user>" is not an F.5 closed-set reason - stamping it
			// here would make the create-edge's stage.Enter reject the mint with
			// UnknownReasonError. The requester and trigger comment live in Goal
			// instead, which is free text.
			MergeOrder: []string{repo.Name},
			Source: &tatarav1alpha1.TaskSource{
				Provider:    providerOf(proj),
				IssueRef:    mr.Spec.URL,
				URL:         mr.Spec.URL,
				Number:      mr.Spec.Number,
				IsPR:        true,
				HeadSHA:     mr.Status.HeadSHA,
				Title:       mr.Status.Title,
				AuthorLogin: mr.Status.Author,
			},
		},
	}
	if err := controllerutil.SetControllerReference(proj, task, m.Scheme); err != nil {
		return nil, fmt.Errorf("takeover: set task ownerref: %w", err)
	}
	created, twin, err := m.createTaskRaceSafe(ctx, task)
	if err != nil {
		return nil, err
	}
	if !created {
		if twin != nil {
			return twin, nil
		}
		// createTaskRaceSafe collided with a DEAD twin and just deleted the
		// tombstone (its "re-mint on the next tick" case). There is no "next
		// tick" on this endpoint-driven path: retry once against the now-freed
		// name rather than surfacing a false negative to the caller.
		return m.MintOrUnparkTakeoverTask(ctx, proj, repo, mr, requestingUser, commentBody, sp)
	}

	ext := mrExtFromMR(mr)
	if err := m.bindMRToTask(ctx, proj, repo, ext, task, sp); err != nil {
		return nil, err
	}
	mrName := tatarav1alpha1.MergeRequestName(repo.Name, mr.Spec.Number)
	if err := m.stampMintStatus(ctx, task, func(fresh *tatarav1alpha1.Task) {
		if !slices.Contains(fresh.Status.MRRefs, mrName) {
			fresh.Status.MRRefs = append(fresh.Status.MRRefs, mrName)
		}
	}); err != nil {
		return nil, err
	}
	return task, nil
}

// mrExtFromMR builds the scm.MergeRequest snapshot bindMRToTask's SyncMergeRequest
// upserts from the MergeRequest CR mint already has in hand, instead of routing
// through mrSnapshot's scm.PRRef (that path reconstructs the URL from a
// provider+owner/repo slug MintReviewTask gets from a forge PR listing; a
// takeover always has an authoritative mr.Spec.URL already, so reconstructing it
// would risk diverging from the one on record). It is still the SAME mirror
// writer (SyncMergeRequest, via bindMRToTask) - just fed from the CR instead of
// a fresh forge listing row.
func mrExtFromMR(mr *tatarav1alpha1.MergeRequest) scm.MergeRequest {
	return scm.MergeRequest{
		Number:     mr.Spec.Number,
		URL:        mr.Spec.URL,
		Title:      mr.Status.Title,
		Author:     mr.Status.Author,
		Body:       mr.Status.Body,
		State:      "open",
		HeadBranch: mr.Status.HeadBranch,
		HeadSHA:    mr.Status.HeadSHA,
		CIStatus:   mr.Status.CIStatus,
		Mergeable:  mr.Status.Mergeable,
	}
}

func takeoverGoal(mr *tatarav1alpha1.MergeRequest, user, comment string) string {
	return fmt.Sprintf("Take over %s at @%s's request. Triggering comment:\n\n%s",
		mr.Spec.URL, user, comment)
}
