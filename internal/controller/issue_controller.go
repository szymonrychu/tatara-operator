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

// Reconcile converges one Issue.
func (r *IssueReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
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
	if r.Driver != nil {
		if err := r.Driver.DrainPendingComments(ctx, &iss); err != nil {
			return ctrl.Result{}, err
		}
	}

	if err := r.projectLabels(ctx, &proj, &repo, &iss); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: cadence}, nil
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
// finishes a crash-interrupted SeverDeleteCR on a rejected(issue-closed) owner.
// handled=true means the caller must stop this reconcile (the Task was stopped
// and/or the mirror CR was deleted).
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

	// Live, non-deploying source stage: stop the Task now.
	if stage.AllowsIssueClosedStop(task.Status.Stage) {
		return ApplyIssueClosedStop(ctx, r.Client, &task, iss.Name, r.now())
	}

	// Re-sever hardening: a rejected(issue-closed) owner Task with the closed CR
	// still present means a crash interrupted the DeleteCR between clearing
	// IssueRefs and deleting the mirror. Finish it (restores prompt reopen).
	if task.Status.Stage == tatarav1alpha1.StageRejected && task.Status.StageReason == stage.ReasonIssueClosed {
		if err := SeverIssueFromTask(ctx, r.Client, &task, iss.Name, SeverDeleteCR); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
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
