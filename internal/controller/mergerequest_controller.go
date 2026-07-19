package controller

import (
	"context"
	"fmt"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// MergeRequestReconciler keeps one MergeRequest CR - one mirrored forge PR/MR -
// converged. It is the Issue reconciler's twin. The C.6 lifecycle label
// vocabulary projects Issue.status.status only; the ONE label this reconciler
// projects is the H.4 semver:<level> release lever, off status.significance.
//
// It writes NO status.status: that is written ONLY from an ACCEPTED review
// submit_outcome (C.5). It writes NO merge decision: status.headSHA here is the
// MIRROR's last-synced head, and both the merge and the approval re-read the
// head LIVE (fix 10) - a merge pinned to an hour-stale SHA is a TOCTOU hole on
// the repo that deploys the cluster.
type MergeRequestReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// ReaderFor returns a token-bound scm.SCMReader. Nil disables the cadence
	// thread sync.
	ReaderFor func(provider, token string) (scm.SCMReader, error)
	// SpillerFor returns the tatara-memory spiller for a Project, used by the A.7
	// byte-budget guard.
	SpillerFor func(proj *tatarav1alpha1.Project) objbudget.Spiller
	// Driver is the operator egress (C.5.3 phase 2). It drains status.pendingReview
	// - the ONLY thing that posts a review and the ONLY thing that advances a Task
	// off reviewing - and status.pendingComments. Nil disables both drains.
	Driver *StageDriver
	// Now is the clock, injectable in tests.
	Now func() time.Time
}

func (r *MergeRequestReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *MergeRequestReconciler) spiller(proj *tatarav1alpha1.Project) objbudget.Spiller {
	if r.SpillerFor == nil {
		return nil
	}
	return r.SpillerFor(proj)
}

// +kubebuilder:rbac:groups=tatara.dev,resources=mergerequests,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tatara.dev,resources=mergerequests/status,verbs=get;update;patch

// Reconcile converges one MergeRequest.
func (r *MergeRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var mr tatarav1alpha1.MergeRequest
	if err := r.Get(ctx, req.NamespacedName, &mr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !mr.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// B.2 rule 5: never zero controller owners.
	if _, err := own.RepairZeroController(ctx, r.Client, &mr); err != nil {
		return ctrl.Result{}, err
	}

	var proj tatarav1alpha1.Project
	if err := r.Get(ctx, types.NamespacedName{Namespace: mr.Namespace, Name: mr.Spec.ProjectRef}, &proj); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("mergerequest: get project %s: %w", mr.Spec.ProjectRef, err)
	}
	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: mr.Namespace, Name: mr.Spec.RepositoryRef}, &repo); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("mergerequest: get repository %s: %w", mr.Spec.RepositoryRef, err)
	}

	cadence := MirrorCadence(mirrorOwnerTask(ctx, r.Client, &mr))

	if r.ReaderFor != nil && mirrorSyncDue(mr.Status.LastSyncedAt, cadence, r.now()) {
		reader, err := mirrorReaderFor(ctx, r.Client, r.ReaderFor, &proj)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := syncMergeRequestThread(ctx, r.Client, r.spiller(&proj), reader, &proj, &repo, &mr); err != nil {
			return ctrl.Result{}, err
		}
	}

	// THE DURABLE INTENTS (C.5.3 phase 2). /outcome and the MCP writes are pure
	// etcd: they persist an intent and return. THIS is what performs it, and it is
	// the only thing that does.
	if r.Driver != nil {
		if err := r.Driver.DrainPendingComments(ctx, &mr); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Driver.DrainPendingReview(ctx, &mr); err != nil {
			return ctrl.Result{}, err
		}
		// The H.4 semver:<level> label. It lands the moment the implement outcome
		// stamps status.significance - not at merge time, and not on some sweep an
		// hour later: it is what CI cuts the release tag from, and a human looking
		// at the PR is entitled to see the level the agent declared.
		if err := r.Driver.ProjectSemverLabel(ctx, &proj, &repo, &mr); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{RequeueAfter: cadence}, nil
}

// SetupWithManager registers the MergeRequest reconciler.
func (r *MergeRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tatarav1alpha1.MergeRequest{}).
		Named("mergerequest").
		Complete(r)
}
