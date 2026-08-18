package controller

import (
	"context"
	"fmt"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// IssueReconciler keeps one Issue CR - one mirrored forge issue - converged.
//
// It is THIN by construction: it runs the B.2 rule 5 repair guard, re-reads the
// thread at the B.4 cadence, projects status.status onto the forge's labels, and
// requeues. It NEVER reads a label to produce status: labels are a ONE-WAY
// PROJECTION (C.6, fix 16). The ONE label read anywhere in the control path is
// tatara-parked (B.4), and it decides COST, never AUTHORITY - forging it buys an
// attacker a Task that stays parked (fails SAFE); forging an approval label
// would buy them prod.
type IssueReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// SCMFor returns the SCMWriter for a provider name (token passed per call).
	// Nil disables the label projection.
	SCMFor func(provider string) (scm.SCMWriter, error)
	// ReaderFor returns a token-bound scm.SCMReader. Nil disables the cadence
	// thread sync (the sweep still writes the mirror on its own pass).
	ReaderFor func(provider, token string) (scm.SCMReader, error)
	// SpillerFor returns the tatara-memory spiller for a Project (its
	// status.memory.endpoint), used by the A.7 byte-budget guard.
	SpillerFor func(proj *tatarav1alpha1.Project) objbudget.Spiller
	// Driver is the operator egress. It drains status.pendingComments - the
	// deferred issue_write(comment|edit|close) intents (C.2.12). Nil disables the
	// drain.
	Driver *StageDriver
	// Now is the clock, injectable in tests.
	Now func() time.Time
}

func (r *IssueReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *IssueReconciler) spiller(proj *tatarav1alpha1.Project) objbudget.Spiller {
	if r.SpillerFor == nil {
		return nil
	}
	return r.SpillerFor(proj)
}

// metrics returns the shared OperatorMetrics off Driver, or nil when the
// reconciler was built without one (RecordSCM is nil-safe).
func (r *IssueReconciler) metrics() *obs.OperatorMetrics {
	if r.Driver == nil {
		return nil
	}
	return r.Driver.Metrics
}

// +kubebuilder:rbac:groups=tatara.dev,resources=issues,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tatara.dev,resources=issues/status,verbs=get;update;patch

// Reconcile converges one Issue. The body is doReconcile; this wrapper is the
// #538 shutdown-cancellation boundary (see classifyReconcileShutdown): the
// mirror read at issue_controller.go's cadence sync and the label projection
// below are the two calls a SIGTERM caught mid-flight in production.
func (r *IssueReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	res, err := r.doReconcile(ctx, req)
	return classifyReconcileShutdown(ctx, "issue", req.Name, res, err)
}

func (r *IssueReconciler) doReconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var iss tatarav1alpha1.Issue
	if err := r.Get(ctx, req.NamespacedName, &iss); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !iss.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// B.2 rule 5: an Issue must NEVER have zero controller owners. With plain
	// owners and no controller owner it is worked by nobody and re-minted by
	// nobody - the sweep's orphan predicate sees an OWNED Issue.
	if _, err := own.RepairZeroController(ctx, r.Client, &iss); err != nil {
		return ctrl.Result{}, err
	}

	var proj tatarav1alpha1.Project
	if err := r.Get(ctx, types.NamespacedName{Namespace: iss.Namespace, Name: iss.Spec.ProjectRef}, &proj); err != nil {
		if apierrors.IsNotFound(err) {
			// The Project is gone; the Issue cascades with its owners.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("issue: get project %s: %w", iss.Spec.ProjectRef, err)
	}
	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: iss.Namespace, Name: iss.Spec.RepositoryRef}, &repo); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("issue: get repository %s: %w", iss.Spec.RepositoryRef, err)
	}

	cadence := MirrorCadence(mirrorOwnerTask(ctx, r.Client, &iss))

	if r.ReaderFor != nil && mirrorSyncDue(iss.Status.LastSyncedAt, cadence, r.now()) {
		reader, err := mirrorReaderFor(ctx, r.Client, r.ReaderFor, &proj)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := syncIssueThread(ctx, r.Client, r.spiller(&proj), reader, &proj, &repo, &iss); err != nil {
			provider := providerOf(&proj)
			recordSCMReadError(r.metrics(), provider, "list_issue_comments", err)
			// tatara-operator#621's latent Issue half. The MergeRequest side reaches
			// markMergeRequestGone here; this side deliberately does NOT mark the
			// mirror closed, because Issue.Status.State="closed" drives
			// handleIssueClosed and the WS3-I3 stop edge, which stops the owner Task.
			// A failed READ is not entitled to make a Task-lifecycle decision. Skip
			// the sync and fall through: the label projection and the closed branch
			// still owe this CR work.
			if !upstreamThreadGone(ctx, reader, &repo, provider, err) {
				return ctrl.Result{}, err
			}
			// THE STAMP IS THE RATE LIMIT, and it is the only one left. mirrorSyncDue
			// keys ONLY on LastSyncedAt, so an unstamped mirror is due on EVERY
			// reconcile - and this arm now returns nil, which removes the
			// controller-runtime backoff that used to bound the cost. Steady state is
			// cadence-driven via the tail requeue either way, but every watch-triggered
			// reconcile (a webhook comment append, the sweep, /outcome) would otherwise
			// re-pay ListIssueComments plus the repo probe. Status.State is still not
			// touched: the stamp records that the thread was CHECKED, which is exactly
			// what it means on the MergeRequest side too.
			if serr := stampIssueMirrorChecked(ctx, r.Client, r.spiller(&proj), &iss, r.now()); serr != nil {
				return ctrl.Result{}, serr
			}
			log.FromContext(ctx).Info("issue: upstream thread permanently gone; skipped mirror sync, mirror left open",
				"action", "issue_gone_mirror_read", "resource_id", iss.Name,
				"status", scm.ErrorStatus(err))
		}
	}

	// Placed before the closed branch, which can delete the CR and return early.
	// The cadence sync above does NOT feed this: syncIssueThread refreshes only
	// Status.Comments/LastSyncedAt/conditions. Status.Body and Status.Author are
	// written by SyncIssue on the webhook and scan intake paths, so the backfill
	// reads whatever those last wrote.
	if err := r.stampProposalKind(ctx, &iss, botLoginOf(&proj)); err != nil {
		return ctrl.Result{}, err
	}

	// O9: a maintainer REOPENED a proposal the operator had retained as declined.
	// It runs before the closed branch (the two states are exclusive) and after
	// the mirror sync, so the reopen is acted on the same reconcile it is seen.
	// The state it gates on is written by handleIssueOpened's stampIssueState;
	// sweepIssues carries the backstop for a lost or gated-out delivery.
	if iss.Status.State == "open" && iss.Status.Status == "rejected" {
		if err := reopenRetainedProposal(ctx, r.Client, &iss, botLoginOf(&proj)); err != nil {
			return ctrl.Result{}, err
		}
	}

	// WS3-I3: a human closed the driving issue mid-flight. Leader-only stop edge.
	// Runs after the mirror sync so a cadence-detected close acts the same
	// reconcile as a webhook-stamped one. When it acts, the Task is stopped (and
	// for the stop case the mirror CR is deleted), so return early.
	if iss.Status.State == "closed" {
		handled, err := r.handleIssueClosed(ctx, &iss)
		if err != nil {
			return ctrl.Result{}, err
		}
		if handled {
			return ctrl.Result{}, nil
		}
	}

	// The deferred issue_write intents (C.2.12): the agent's tool call persisted
	// one, and this is what performs it on the forge.
	//
	// The terminal guard is the same one the MergeRequest reconciler has carried
	// around its drain steps since #436, and this side needs it for the same
	// reason: every arm of the drain re-reads the thread (listThreadComments) to
	// dedup its own marker, so on a permanently-gone issue the drain 404s exactly
	// where the mirror sync above did. Without it #621's loop survives the fix for
	// any Issue carrying a pending intent - it merely moves twenty lines down and
	// costs an extra forge call per pass. The intent is NOT dropped: an agent's
	// durable intent is not something a failed forge read is entitled to discard,
	// so it is retried at the next cadence and stays visible in status.
	//
	// IT IS SCOPED TO THE WHOLE DRAIN, not to the thread re-read that motivates
	// it, because the drain is one call and the caller cannot see which arm
	// failed. So it also swallows a 404/410 out of CloseIssue, Comment, AddLabel
	// and EditIssue - and the drain returns at the FIRST failing intent, so every
	// later intent on this Issue is head-of-line blocked behind it. That is the
	// drain's pre-existing shape on any error; what this guard removes is the
	// ERROR line. recordSCMReadError is therefore not optional here: without it a
	// repo silently refusing every forge write leaves no signal at all outside the
	// raw status.pendingComments array.
	if r.Driver != nil {
		if err := r.Driver.DrainPendingComments(ctx, &iss); err != nil {
			if !isPermanentTargetGone(err) {
				return ctrl.Result{}, err
			}
			recordSCMReadError(r.metrics(), providerOf(&proj), "drain_pending_comments", err)
			log.FromContext(ctx).Info("issue: upstream permanently gone; stopped draining pending intents this pass",
				"action", "issue_gone_drain", "resource_id", iss.Name,
				"status", scm.ErrorStatus(err))
		}
	}

	if err := r.projectLabels(ctx, &proj, &repo, &iss); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: cadence}, nil
}

// stampProposalKind is the ONE-TIME migration backfill for Spec.ProposalKind.
// Issues minted before the field existed carry an empty kind and would count as
// 0 pending on rollout, so the operator would refill to N on top of the existing
// backlog.
//
// It is WRITE-ONCE and Spec-only: a non-empty kind is never recomputed, so a
// forge-side body edit after the fact cannot flip or clear it. Proposals minted
// before the body marker existed stay unmarked and simply never count - a
// bounded, shrinking set that self-corrects as the backlog turns over. No
// migration job, no startup sweep, no manual step.
//
// It reads the marker ONLY on an issue that is bot-authored AND already carries a
// ProposalBodyHash. Both gates are the read path's, verbatim - see
// effectiveProposalKind (proposalcount.go) for why each exists. They must be
// identical on both paths: the read gate stops a forgery being COUNTED in the
// passes before this runs, and this one stops it being stamped PERMANENTLY.
func (r *IssueReconciler) stampProposalKind(ctx context.Context, iss *tatarav1alpha1.Issue, botLogin string) error {
	if iss.Spec.ProposalKind != "" {
		return nil
	}
	if !issueAuthoredByBot(iss, botLogin) || iss.Spec.ProposalBodyHash == "" {
		return nil
	}
	kind := tatarav1alpha1.ProposalKindFromBody(iss.Status.Body)
	if kind == "" {
		return nil
	}
	// Both errors are wrapped. RetryOnConflict still sees a conflict through the
	// wrapper: apierrors.IsConflict resolves the APIStatus with errors.As.
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Issue{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(iss), fresh); err != nil {
			return fmt.Errorf("issue: get %s for provenance backfill: %w", iss.Name, err)
		}
		if fresh.Spec.ProposalKind != "" {
			iss.Spec.ProposalKind = fresh.Spec.ProposalKind
			iss.ResourceVersion = fresh.ResourceVersion
			return nil
		}
		fresh.Spec.ProposalKind = kind
		if err := r.Update(ctx, fresh); err != nil {
			return fmt.Errorf("issue: backfill provenance on %s: %w", iss.Name, err)
		}
		iss.Spec.ProposalKind = kind
		iss.ResourceVersion = fresh.ResourceVersion
		log.FromContext(ctx).Info("issue: backfilled proposal provenance",
			"action", "issue_proposal_kind_backfill", "resource_id", iss.Name, "proposal_kind", kind)
		return nil
	})
}

// projectLabels writes the ONE-WAY projection of Issue.status.status onto the
// forge's labels (C.6):
//
//	status=approved -> +approvedLabel, -declinedLabel
//	status=rejected -> +declinedLabel, -approvedLabel
//	status=done     -> both stripped
//
// No label is EVER read to produce status. status.labels is consulted here ONLY
// to skip a redundant forge write; it can never move status.status.
func (r *IssueReconciler) projectLabels(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, iss *tatarav1alpha1.Issue) error {
	if r.SCMFor == nil || iss.Status.Status == "" || iss.Status.Status == "new" {
		return nil
	}
	_, approved, _, declined := lifecycleLabels(proj.Spec.Scm)

	var want string
	switch iss.Status.Status {
	case "approved":
		want = approved
	case "rejected":
		want = declined
	case "done":
		want = ""
	default:
		return nil
	}

	provider := providerForRemote(ctx, repo.Spec.URL)
	if proj.Spec.Scm != nil && proj.Spec.Scm.Provider != "" {
		provider = proj.Spec.Scm.Provider
	}
	writer, err := r.SCMFor(provider)
	if err != nil {
		return fmt.Errorf("issue: scm writer: %w", err)
	}
	token, err := mirrorSCMToken(ctx, r.Client, proj)
	if err != nil {
		return err
	}
	slug, err := scm.RepoSlugFromURL(repo.Spec.URL)
	if err != nil {
		return fmt.Errorf("issue: repo slug for %s: %w", repo.Name, err)
	}
	issueRef := fmt.Sprintf("%s#%d", slug, iss.Spec.Number)

	present := make(map[string]bool, len(iss.Status.Labels))
	for _, l := range iss.Status.Labels {
		present[l] = true
	}

	l := log.FromContext(ctx)
	if want != "" && !present[want] {
		addErr := writer.AddLabel(ctx, token, issueRef, want)
		RecordSCM(r.metrics(), provider, "add_label", addErr)
		if addErr != nil {
			if isPermanentTargetGone(addErr) {
				l.Info("issue: label projection target is gone; skipping",
					"action", "issue_label_projection", "resource_id", iss.Name, "issue_ref", issueRef)
				return nil
			}
			return fmt.Errorf("issue: add label %s to %s: %w", want, issueRef, addErr)
		}
		l.Info("issue: projected status onto label",
			"action", "issue_label_projection", "resource_id", iss.Name,
			"issue_ref", issueRef, "status", iss.Status.Status, "label", want)
	}
	for _, l2 := range []string{approved, declined} {
		if l2 == want || !present[l2] {
			continue
		}
		// RemoveLabel is best-effort: the label may already be gone.
		removeErr := writer.RemoveLabel(ctx, token, issueRef, l2)
		RecordSCM(r.metrics(), provider, "remove_label", removeErr)
		if removeErr != nil && !isPermanentTargetGone(removeErr) {
			l.Error(removeErr, "issue: removing a projected label failed",
				"action", "issue_label_projection", "resource_id", iss.Name, "issue_ref", issueRef, "label", l2)
		}
	}
	return nil
}

// handleIssueClosed routes a closed, owned Issue CR through the WS3-I3 stop edge
// (a live, non-deploying source stage) or, as the review re-sever hardening,
// finishes a crash-interrupted sever on a rejected(issue-closed) owner.
// handled=true means the caller must stop this reconcile (the Task was stopped
// and/or the mirror CR was deleted). A RETAINED brainstorm-proposal mirror is
// explicitly NOT handled: it survives, so the rest of the reconcile still owes
// it a label projection and a cadence requeue.
func (r *IssueReconciler) handleIssueClosed(ctx context.Context, iss *tatarav1alpha1.Issue) (bool, error) {
	ownerName, owned := own.ControllerOwner(iss)
	if !owned {
		return false, nil
	}
	var task tatarav1alpha1.Task
	if err := r.Get(ctx, types.NamespacedName{Namespace: iss.Namespace, Name: ownerName}, &task); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("issue-closed: get owning task %s: %w", ownerName, err)
	}

	// Live, non-deployed source state: stop the Task now.
	//
	// A PARKED Task is EXCLUDED, and that exclusion is what #521 made necessary.
	// ApplyIssueClosedStop enters rejected(issue-closed), and stage.Enter refuses
	// a parked Task by design - so for a parked owner this call silently no-ops
	// and the unconditional `return` swallowed the parked-decline branch below
	// with it. A parked brainstorm proposal whose issue a human closes is the
	// commonest shape there is, and it must reach recordParkedProposalDecline.
	if !tatarav1alpha1.Parked(&task) && stage.AllowsIssueClosedStop(task.Status.State) {
		return ApplyIssueClosedStop(ctx, r.Client, &task, iss.Name, r.now())
	}

	// Re-sever hardening: a rejected(issue-closed) owner Task with the closed CR
	// still present means a crash interrupted the DeleteCR between clearing
	// IssueRefs and deleting the mirror. Finish it (restores prompt reopen).
	//
	// O9 makes that same state the STEADY state of every retained brainstorm
	// proposal, so it MUST route through recordProposalDecline: an unguarded
	// re-sever would delete the mirror C3 exists to keep, on the very next
	// reconcile. A retained mirror also reports handled=FALSE, so the reconcile
	// continues and the rejected verdict still projects onto the declined label.
	if task.Status.State == tatarav1alpha1.StateRejected && task.Status.StateReason == stage.ReasonIssueClosed {
		retained, err := recordProposalDecline(ctx, r.Client, &task, iss.Name, r.now())
		if err != nil {
			return false, err
		}
		return !retained, nil
	}

	// A PARKED owner, which is the shape that dominates real declines: a maintainer
	// closes the proposal without commenting first, so nothing ever unparked it
	// into clarifying. The stop edge correctly does nothing here, and before the
	// 2026-07-27 ruling so did everything else, which meant that decline was never
	// recorded and the reaper cascaded its mirror on the next pass.
	//
	// THE STAGE GATE IS EXPLICIT AND IT IS LOAD-BEARING. Everything that is not a
	// live source stage and not mid-sever reaches this line, and that includes the
	// SUCCESS path: CloseIssuesOnDelivery closes the driving issue itself and
	// stamps state=closed/status=done while the Task is at deploying, then
	// delivered. Ungated, a proposal that SHIPPED would be severed from its
	// delivered Task, have its "done" overwritten to "rejected", be logged as a
	// decline and relabelled tatara-declined on the forge. The verdict guard alone
	// cannot save it, because the sever runs first.
	//
	// Recording is NOT stopping: the Task's state, park reason and pod are
	// untouched, and a non-proposal issue is left entirely alone. handled stays
	// FALSE either way, so the reconcile still projects labels and requeues.
	if !tatarav1alpha1.Parked(&task) {
		return false, nil
	}
	return false, recordParkedProposalDecline(ctx, r.Client, &task, iss, r.now())
}

// SetupWithManager registers the Issue reconciler.
func (r *IssueReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tatarav1alpha1.Issue{}).
		Named("issue").
		Complete(r)
}

// mirrorReaderFor resolves the token-bound SCMReader for a Project.
func mirrorReaderFor(ctx context.Context, c client.Client, readerFor func(provider, token string) (scm.SCMReader, error),
	proj *tatarav1alpha1.Project) (scm.SCMReader, error) {
	token, err := mirrorSCMToken(ctx, c, proj)
	if err != nil {
		return nil, err
	}
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	reader, err := readerFor(provider, token)
	if err != nil {
		return nil, fmt.Errorf("mirror: scm reader for %s: %w", proj.Name, err)
	}
	return reader, nil
}
