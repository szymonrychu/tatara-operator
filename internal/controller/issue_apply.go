package controller

import (
	"context"
	"fmt"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ApplyIssueClosedStop is the WS3-I3 stop edge: a human closed the driving issue
// mid-flight, so the operator stops the Task at rejected(issue-closed), severs
// the closed Issue mirror (DELETING it, unless it is a brainstorm proposal - see
// recordProposalDecline), and tears down the wrapper pod. The existing terminal
// reaper then closes the bot PR with its standard note.
//
// LEADER-ONLY: it is driven from the IssueReconciler (leader-only), never from
// the webhook goroutine, so it adds no new webhook-goroutine stage mutation
// (#353 / F6-1). It mirrors review_apply.go's applier shape.
//
// It re-reads the Task live inside RetryOnConflict and re-checks the live-stage
// gate: an approval or a park that landed since the caller's read must not be
// overwritten. It returns stopped=false (no error) when the Task is no longer in
// a live source stage (raced past, or the operator's own C.4 deploying-close).
func ApplyIssueClosedStop(ctx context.Context, c client.Client, task *tatarav1alpha1.Task,
	issueName string, now time.Time) (stopped bool, err error) {

	key := client.ObjectKeyFromObject(task)
	var prevStage string
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		stopped = false
		fresh := &tatarav1alpha1.Task{}
		if err := c.Get(ctx, key, fresh); err != nil {
			return err
		}
		if !stage.AllowsIssueClosedStop(fresh.Status.Stage) {
			return nil // raced off a live source stage (approval/park/deploying); fold
		}
		prevStage = fresh.Status.Stage
		if err := stage.Enter(fresh, nil, tatarav1alpha1.StageRejected, stage.ReasonIssueClosed, now); err != nil {
			return nil // guard refused; leave untouched
		}
		if err := c.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*task = *fresh
		stopped = true
		return nil
	}); err != nil {
		return false, fmt.Errorf("issue-closed: stop task %s: %w", task.Name, err)
	}
	if !stopped {
		return false, nil
	}

	// Tear the wrapper pod down inline, leader-safe (same idiom as the review
	// appliers). The in-flight turn is abandoned; no half-finished branch is pushed.
	//
	// IT GOES FIRST, AHEAD OF EVERY OTHER FALLIBLE STEP, and that ordering is the
	// only thing that makes it reliable. This function is UNREACHABLE on re-entry:
	// the stage is now rejected, so AllowsIssueClosedStop is false and
	// handleIssueClosed routes to the re-sever branch, which tears down nothing and
	// no longer knows prevStage. Anything fallible placed above this line therefore
	// leaks a LIVE interactive agent pod against a stopped Task until the Task is
	// reaped - up to DeclinedProposalRetention now that the decline hold exists.
	// Keep it first; do not add fallible work above it.
	if stage.AgentKindFor(prevStage) != "" {
		if err := agent.DeleteWrapper(ctx, c, task.Namespace, task); err != nil {
			return true, fmt.Errorf("issue-closed: delete wrapper pod for %s: %w", task.Name, err)
		}
	}

	// Sever the closed issue from its Task. For an ORDINARY issue the mirror is a
	// rebuildable projection: delete it (the reopen mint re-creates it via
	// SyncIssue off the live open event), which is also the leak fix - a bare
	// ownerRef drop would leave the closed CR un-owned AND un-cascadable. For a
	// BRAINSTORM PROPOSAL the mirror is retained instead: see
	// recordProposalDecline. Both are recoverable on re-entry via the re-sever
	// branch, which is why they sit below the teardown rather than above it.
	if _, err := recordProposalDecline(ctx, c, task, issueName, now); err != nil {
		return true, err
	}
	log.FromContext(ctx).Info("issue closed mid-flight: stopped the task",
		"action", "issue_closed_stop", "resource_id", task.Name,
		"from_stage", prevStage, "issue", issueName)
	return true, nil
}

// severedButStillOwned is the RETAINED SHAPE predicate: t is iss's CONTROLLER
// OWNER and yet no longer LISTS it in Status.IssueRefs.
//
// BOTH conjuncts are evaluated HERE. An earlier version took the issue NAME and
// tested only the ref list, leaving the ownership half to whatever the caller
// happened to do first - which both callers did do, but a name that asserts a
// conjunct the body never evaluates is a trap for the third one. Taking the
// *Issue costs nothing: every caller already holds it.
//
// SeverRetainCR is the only thing that produces this combination DURABLY: the
// other two sever modes leave no ownerRef behind, one by deleting the CR and one
// by dropping the ref. It is not, however, the only thing that can produce it at
// all - every own-then-list mint (mintIssueCRs, MintForItem, mintIssueCR,
// linkArtifact) writes the ownerRef before appending the ref, so a crash between
// the two leaves the same shape.
//
// THE VERDICT CLAUSE IS THE CALLER'S, and deliberately so: the reaper pairs this
// with state=="closed", the reopen undo with status=="rejected", and those are
// genuinely different questions that cannot collapse into one predicate. Either
// one excludes a mint in flight - a mint is by definition creating an OPEN issue,
// and it seeds status "new" - and every one of those crash windows self-heals on
// the next reconcile anyway. Paired with a verdict, this is the durable,
// STAGE-INDEPENDENT signature of "this Task's only remaining relationship to this
// mirror is that its reap will collect it". DO NOT USE IT WITHOUT A VERDICT
// CLAUSE.
//
// Keying on the shape rather than on rejected(issue-closed) is what lets a
// decline recorded against a PARKED owner get the same retention window and the
// same reopen recovery as one recorded against a stopped Task.
func severedButStillOwned(t *tatarav1alpha1.Task, iss *tatarav1alpha1.Issue) bool {
	owner, owned := own.ControllerOwner(iss)
	return owned && owner == t.Name && !slices.Contains(t.Status.IssueRefs, iss.Name)
}

// proposalBotLogin resolves the owning Project's bot login for iss - the
// unforgeable authorship anchor effectiveProposalKind's body-marker fallback
// needs (a marker in a forge-editable body is not, on its own, provenance).
//
// A gone Project answers "" rather than an error: the Issue is cascading with
// its owners anyway. "" fails CLOSED - only a Spec-stamped proposal is then
// recognised - which is the safe direction for a retention decision, since the
// alternative branch DELETES the CR.
func proposalBotLogin(ctx context.Context, c client.Client, iss *tatarav1alpha1.Issue) (string, error) {
	var proj tatarav1alpha1.Project
	err := c.Get(ctx, types.NamespacedName{Namespace: iss.Namespace, Name: iss.Spec.ProjectRef}, &proj)
	if apierrors.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("proposal: get project %s: %w", iss.Spec.ProjectRef, err)
	}
	return botLoginOf(&proj), nil
}

// isBrainstormProposal is the ONE retention predicate. It is deliberately
// narrower than "has a proposal kind": only BRAINSTORM proposals are counted by
// the backlog control law and only they are rendered in the <proposal_history>
// block, so retaining an incident tracker's mirror would grow Issue CRs for no
// reader. Everything else keeps the pre-O9 delete-and-re-mint projection.
//
// It reads through effectiveProposalKind, so a legacy proposal filed before
// Spec.ProposalKind existed is retained too, under that function's bot-
// authorship and anchor gates. Do NOT relax those gates here: they close a
// backlog-inflation vector and an auto-approve escalation.
func isBrainstormProposal(ctx context.Context, c client.Client, iss *tatarav1alpha1.Issue) (bool, error) {
	botLogin, err := proposalBotLogin(ctx, c, iss)
	if err != nil {
		return false, err
	}
	return effectiveProposalKind(iss, botLogin) == tatarav1alpha1.ProposalKindBrainstorm, nil
}

// recordProposalDecline severs a CLOSED issue from its Task, retaining the
// mirror when it is a brainstorm proposal and deleting it when it is not
// (plan conflict C3). retained reports which branch ran.
//
// For an ORDINARY issue the mirror is a rebuildable projection and the delete is
// the leak fix: a bare ownerRef drop would leave the closed CR un-owned AND
// un-cascadable, leaking forever with the IssueReconciler mirror-syncing it.
//
// For a BRAINSTORM PROPOSAL the mirror is the ONLY record that the proposal was
// discarded and WHY (the maintainer's comments). Deleting it makes the
// <proposal_history> block's declined rows unreachable and a killed idea
// invisible to the next brainstorm session - the immortal-PR failure mode the
// block exists to close. So: stamp the verdict and RETAIN the CR, keeping its
// ownerRef on the now rejected Task so it cascades with that Task's reap rather
// than leaking.
//
// The stamp is what makes Issue.Status.Status = "rejected" a value production
// code actually writes (plan conflict C2), and it agrees with
// proposalDisplayStatus, which reads rejected OR closed-and-not-approved as
// declined.
//
// It is idempotent: an already-decided mirror is a no-op and a missing CR is
// success. An APPROVED proposal a maintainer later closes is NEVER rewritten to
// rejected - the approval is a verified, single-use evidence record.
func recordProposalDecline(ctx context.Context, c client.Client,
	task *tatarav1alpha1.Task, issueName string, now time.Time) (retained bool, err error) {

	key := types.NamespacedName{Namespace: task.Namespace, Name: issueName}
	var iss tatarav1alpha1.Issue
	if err := c.Get(ctx, key, &iss); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil // already gone; idempotent
		}
		return false, fmt.Errorf("issue-closed: get issue %s: %w", issueName, err)
	}
	proposal, err := isBrainstormProposal(ctx, c, &iss)
	if err != nil {
		return false, err
	}
	if !proposal {
		return false, SeverIssueFromTask(ctx, c, task, issueName, SeverDeleteCR)
	}
	return true, retainProposalDecline(ctx, c, task, &iss, now)
}

// recordParkedProposalDecline is the same decline record for an owner Task the
// WS3-I3 stop edge does not act on - a parked one, overwhelmingly (human ruling
// 2026-07-27).
//
// It exists because the stop edge and the decline record are SEPARATE CONCERNS,
// and conflating them left the dominant decline shape uncovered: a maintainer
// closing a proposal without commenting first leaves the owner parked,
// AllowsIssueClosedStop is false, nothing was recorded, and the reaper cascaded
// the mirror on its next pass - a declined row that survived minutes.
//
// IT NEVER TOUCHES THE TASK'S STAGE. No stage.Enter, no wrapper teardown, no
// terminal sequence: a parked Task stays exactly as parked as it was. The only
// Task write is the sever's Status.IssueRefs clear, which is what hands the
// mirror to the retention window instead of to the next reap.
//
// A NON-PROPOSAL issue is left completely alone here - no sever, no delete, no
// write of any kind - which is the difference from recordProposalDecline. This
// path is reached for every closed issue whose owner is not stoppable, so
// deleting an ordinary parked issue's mirror from it would be a platform-wide
// behaviour change dressed up as a brainstorm fix.
func recordParkedProposalDecline(ctx context.Context, c client.Client,
	task *tatarav1alpha1.Task, iss *tatarav1alpha1.Issue, now time.Time) error {

	proposal, err := isBrainstormProposal(ctx, c, iss)
	if err != nil || !proposal {
		return err
	}
	return retainProposalDecline(ctx, c, task, iss, now)
}

// retainProposalDecline is the retain-and-stamp half, shared by both callers.
// PRECONDITION: iss is a closed BRAINSTORM proposal (the callers own that gate,
// because they disagree about what to do when it fails).
//
// It is idempotent: an already-decided mirror is a no-op and a missing CR is
// success. An APPROVED proposal a maintainer later closes is NEVER rewritten to
// rejected - the approval is a verified, single-use evidence record.
func retainProposalDecline(ctx context.Context, c client.Client,
	task *tatarav1alpha1.Task, iss *tatarav1alpha1.Issue, now time.Time) error {

	key := client.ObjectKeyFromObject(iss)
	// Task side FIRST, the same ORDER guarantee SeverIssueFromTask documents:
	// drop the ref so the reaper never walks a stale one, then stamp the verdict.
	// Clearing the ref is ALSO what puts the mirror into the retained shape the
	// reaper's retention exception recognises (holdsDeclinedProposal).
	if err := SeverIssueFromTask(ctx, c, task, iss.Name, SeverRetainCR); err != nil {
		return err
	}
	stamped := false
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		stamped = false
		var fresh tatarav1alpha1.Issue
		if err := c.Get(ctx, key, &fresh); err != nil {
			return err
		}
		// "done" is in this guard for the same reason "approved" is: it is a
		// verdict the platform already reached and a decline must never overwrite
		// one. It is the DELIVERY stamp (CloseIssuesOnDelivery), so without it a
		// shipped proposal would be rewritten as discarded. Defence in depth only -
		// the caller's stage gate is what actually keeps this path away from
		// delivered work, because the sever above has already run by the time we
		// get here.
		switch fresh.Status.Status {
		case "rejected", "approved", "done":
			return nil
		}
		fresh.Status.Status = "rejected"
		if err := c.Status().Update(ctx, &fresh); err != nil {
			return err
		}
		stamped = true
		return nil
	}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("issue-closed: record the decline on %s: %w", iss.Name, err)
	}
	// Anchor the retention window on the DECLINE. UNCONDITIONAL on purpose:
	// stampDeclinedAt is write-once against the live Task, so this is a no-op once
	// anchored and a RETRY until it lands.
	//
	// It must NOT be gated on "we just stamped the verdict". Keyed that way a
	// failed Update was never retried - re-entry finds status=rejected and
	// short-circuits above - while the FALLBACK anchor for a parked owner is the
	// park time, which is typically already expired, so the mirror is cascaded on
	// the very next reap pass. That is the exact symptom this whole window exists
	// to kill, reached by the one path that looked harmless.
	if err := stampDeclinedAt(ctx, c, task, now); err != nil {
		return fmt.Errorf("issue-closed: stamp the decline clock on %s: %w", task.Name, err)
	}
	if !stamped {
		return nil
	}
	log.FromContext(ctx).Info("retained a discarded brainstorm proposal's mirror",
		"action", "proposal_declined", "resource_id", iss.Name, "task", task.Name,
		"owner_stage", task.Status.Stage, "owner_stage_reason", task.Status.StageReason,
		"proposal_kind", tatarav1alpha1.ProposalKindBrainstorm, "comments", len(iss.Status.Comments))
	return nil
}

// stampDeclinedAt records the decline time on the owner Task, WRITE-ONCE. It is
// the free function twin of (*ProjectReconciler).annotateTask - this path holds a
// bare client.Client, not the reconciler.
//
// The write-once test is made against cur, the AUTHORITATIVE live copy fetched
// inside the retry loop, not against the caller's possibly-stale in-memory Task.
// That is what makes "cannot slide the window forward" a property of this
// function rather than of caller discipline: a caller that has never read the
// annotation, or read it before another writer landed one, still cannot overwrite
// it. Callers therefore call unconditionally; an already-stamped Task is a no-op.
//
// KNOWN, BOUNDED CONSEQUENCE of write-once being per TASK: a Task that declines a
// SECOND proposal keeps the first decline's clock, so the second mirror gets the
// remainder of that window instead of a fresh one. It cannot EXTEND a window,
// which is the direction that would matter, and in practice a proposal gets its
// own clarify Task (mintClarifyTask, one per proposal), so a Task with two
// retained mirrors is not a shape the propose path produces.
func stampDeclinedAt(ctx context.Context, c client.Client, task *tatarav1alpha1.Task, now time.Time) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cur tatarav1alpha1.Task
		if err := c.Get(ctx, client.ObjectKeyFromObject(task), &cur); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if cur.Annotations[AnnProposalDeclinedAt] != "" {
			task.SetAnnotations(cur.Annotations) // already anchored; refresh the caller's copy
			return nil
		}
		if cur.Annotations == nil {
			cur.Annotations = map[string]string{}
		}
		cur.Annotations[AnnProposalDeclinedAt] = now.UTC().Format(time.RFC3339)
		if err := c.Update(ctx, &cur); err != nil {
			return err
		}
		task.SetAnnotations(cur.Annotations)
		return nil
	})
}

// reopenRetainedProposal undoes the retention when a maintainer REOPENS a
// declined proposal, and it is the price of retention rather than an extra.
//
// A retained mirror carries status=rejected and keeps a dead rejected Task as
// its controller owner. Reopened and left that way it would be: invisible to the
// sweep's orphan intake, so nothing ever mints a Task for it; unapprovable
// forever, because ApprovalInScope refuses a rejected thread; and uncounted by
// the backlog law, so the controller refills over a proposal the maintainer just
// revived. The pre-O9 delete-and-re-create had none of those problems, so the
// reopen has to put the mirror back in the same shape: verdict cleared, mirror
// orphaned for the sweep to re-adopt.
//
// THE STEP ORDER IS CRASH SAFETY, not style. Orphaning FIRST and clearing the
// verdict SECOND means the only interruptible intermediate state is
// rejected + ownerless, which BOTH re-entry paths still recognise (the reconciler
// gate is state=open + status=rejected; the sweep backstop keys on
// status=rejected alone). Clearing first would leave open + new + owned-by-a-dead
// Task, which no gate can re-enter, no sweep can adopt, and the reaper then
// cascades - the exact wedge this function exists to prevent.
//
// It writes Status.State itself so BOTH callers converge on one op: the webhook
// path has already stamped open, and the sweep backstop has not (it knows the
// forge lists the issue as open, which is the same truth from the other side).
//
// botLogin is threaded in by the caller, which always already holds the Project.
//
// KNOWN COSMETIC RESIDUAL: the forge keeps the declined label the decline
// projected onto it, because the label projection only writes for
// approved/rejected/done. It is projection-only and nothing reads it for control
// flow (C.6, fix 16), so it is left alone rather than adding a second label
// write path.
//
// KNOWN BEHAVIOURAL DELTA, stated because it is now reachable: the retained
// mirror keeps its Spec.ProposalBodyHash, so a proposal CLOSED WITH NO
// maintainer comment and then reopened still satisfies autoApproveApplies'
// anchor factor. Pre-O9 the reopen re-minted an anchorless CR, which failed that
// factor closed. Under autoApproveTataraProposals a triage-role reopen can
// therefore resurrect a proposal a maintainer vetoed by closing it silently. The
// anchor is unforgeable and the reopen is still an attributable forge action by
// an allowed reporter, so this is defensible - but it is a real widening of the
// auto-approve surface and must not be discovered later.
func reopenRetainedProposal(ctx context.Context, c client.Client, iss *tatarav1alpha1.Issue, botLogin string) error {
	if effectiveProposalKind(iss, botLogin) != tatarav1alpha1.ProposalKindBrainstorm {
		return nil
	}
	if iss.Status.Status != "rejected" {
		return nil // nothing retained to undo
	}

	// STEP 1: hand the mirror back to the sweep, but ONLY away from the Task that
	// the decline itself left owning it. Any other owner is live work and keeps it.
	//
	// The test is the RETAINED SHAPE, not the owner's stage: a decline is now
	// recorded against a parked owner as well as a rejected one, and keying this on
	// rejected(issue-closed) would leave every parked-owner decline unreopenable -
	// the same wedge, one shape over.
	if ownerName, owned := own.ControllerOwner(iss); owned {
		var owner tatarav1alpha1.Task
		err := c.Get(ctx, types.NamespacedName{Namespace: iss.Namespace, Name: ownerName}, &owner)
		switch {
		case apierrors.IsNotFound(err):
			// The owner is already gone; the mirror is cascading and there is nothing
			// to sever from.
		case err != nil:
			return fmt.Errorf("proposal-reopened: get owning task %s: %w", ownerName, err)
		case severedButStillOwned(&owner, iss):
			if err := SeverIssueFromTask(ctx, c, &owner, iss.Name, SeverOrphan); err != nil {
				return err
			}
		}
	}

	// STEP 2: clear the verdict and mirror the reopen.
	key := client.ObjectKeyFromObject(iss)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh tatarav1alpha1.Issue
		if err := c.Get(ctx, key, &fresh); err != nil {
			return err
		}
		if fresh.Status.Status != "rejected" && fresh.Status.State == "open" {
			return nil
		}
		// "new" is exactly what mintIssueCR seeds a freshly filed proposal with, so
		// the reopened thread is indistinguishable from an untriaged one.
		fresh.Status.Status = "new"
		fresh.Status.State = "open"
		return c.Status().Update(ctx, &fresh)
	}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("proposal-reopened: clear the decline on %s: %w", iss.Name, err)
	}
	iss.Status.Status, iss.Status.State = "new", "open"

	log.FromContext(ctx).Info("a declined brainstorm proposal was reopened: undid the retention",
		"action", "proposal_reopened", "resource_id", iss.Name, "proposal_kind", tatarav1alpha1.ProposalKindBrainstorm)
	return nil
}
