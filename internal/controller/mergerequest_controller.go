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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
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

// metrics returns the shared OperatorMetrics off Driver, or nil when the
// reconciler was built without one. Twin of IssueReconciler.metrics.
func (r *MergeRequestReconciler) metrics() *obs.OperatorMetrics {
	if r.Driver == nil {
		return nil
	}
	return r.Driver.Metrics
}

// +kubebuilder:rbac:groups=tatara.dev,resources=mergerequests,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tatara.dev,resources=mergerequests/status,verbs=get;update;patch

// Reconcile converges one MergeRequest.
// The body is doReconcile; this wrapper is the #538 shutdown-cancellation
// boundary (see classifyReconcileShutdown).
func (r *MergeRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	res, err := r.doReconcile(ctx, req)
	return classifyReconcileShutdown(ctx, "mergerequest", req.Name, res, err)
}

func (r *MergeRequestReconciler) doReconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
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

	owner := mirrorOwnerTask(ctx, r.Client, &mr)
	cadence := MirrorCadence(owner)
	ciCadence := CIRefreshCadence(owner)

	// THE TERMINAL EXCLUSION, same one refreshCI makes at :239. A merged or closed
	// mirror's thread can no longer gate anything, so re-reading it on cadence
	// forever is pure forge load - and it is the ONLY route by which a 404 could
	// reach the gone branch below and rewrite a MERGE that already happened.
	// mr.Status.State is not mirror-local: "merged" -> "closed" leaves
	// stage.AllMRsTerminal true while AllMRsMerged goes false, which is exactly
	// what terminalMREdge/OwnMRsShippedEdge finalize the owner Task
	// rejected(mr-closed-externally) on, with a public terminal comment on the
	// driving issue. syncMergeRequestThread itself stays unguarded: it is shared
	// with SyncMergeRequestOnDemand, the deliberate off-cadence path.
	terminalMirror := mr.Status.State == "merged" || mr.Status.State == "closed"
	if r.ReaderFor != nil && !terminalMirror && mirrorSyncDue(mr.Status.LastSyncedAt, cadence, r.now()) {
		reader, err := mirrorReaderFor(ctx, r.Client, r.ReaderFor, &proj)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := syncMergeRequestThread(ctx, r.Client, r.spiller(&proj), reader, &proj, &repo, &mr); err != nil {
			provider := providerOf(&proj)
			recordSCMReadError(r.metrics(), provider, "list_pr_comments", err)
			// tatara-operator#621: the #436 terminal-404 guard below only covers the
			// forge WRITES. A permanently-gone upstream MR discovered by this READ
			// returned raw from here and requeued forever, never reaching it.
			//
			// scm.IsNotFound (404 ONLY), not isPermanentTargetGone (404/410), even
			// though upstreamThreadGone accepts both: the disposition here is NOT
			// local to the mirror. status.state="closed" makes stage.MRTerminal true,
			// which is what externalTerminalEdge finalizes the OWNER TASK on
			// (rejected/mr-closed-externally, plus its terminal comment on the driving
			// issue). A predicate that terminates somebody's Task must be the narrow
			// one, and 404 is what a deleted PR actually answered throughout the
			// incident. 410 is GitHub's answer for a deleted ISSUE and is also what it
			// returns for a repo with Issues DISABLED - repo-wide, and invisible to the
			// repo-readable probe, which reads /repos and not /issues. The Issue side
			// keeps 404/410 because its disposition is a skipped read and nothing else.
			if scm.IsNotFound(err) && upstreamThreadGone(ctx, reader, &repo, provider, err) {
				// The stamp is not cosmetic. markMergeRequestGone writes state only, so
				// mirrorSyncDue stays true forever and this branch re-reads the gone
				// thread AND re-probes the repo on every CI tick - 5 minutes for an
				// actively-worked MR, i.e. a HIGHER forge rate than the exponential
				// backoff being removed, with the drains starved on every pass. Stamped,
				// the gone thread costs 2 calls per MIRROR cadence and the drains run on
				// every pass in between.
				if serr := markMergeRequestThreadGone(ctx, r.Client, r.spiller(&proj), &mr, r.now()); serr != nil {
					return ctrl.Result{}, serr
				}
				log.FromContext(ctx).Info("mergerequest: upstream thread permanently gone; marked closed, skipped mirror sync",
					"action", "mr_gone_mirror_read", "resource_id", mr.Name,
					"status", scm.ErrorStatus(err))
				return ctrl.Result{RequeueAfter: min(cadence, ciCadence)}, nil
			}
			return ctrl.Result{}, err
		}
	}

	// THE MISSED-WEBHOOK BACKSTOP (PR A item 3). The CI webhook is the primary
	// writer of status.ciStatus; this is what covers a delivery the forge dropped
	// and, just as often, one that arrived while this mirror still carried the
	// PREVIOUS head - a stamp keyed on headSHA has nothing to match, so it lands
	// nowhere and only a live re-read ever recovers it.
	r.refreshCI(ctx, &proj, &repo, &mr, ciCadence)

	// THE DURABLE INTENTS (C.5.3 phase 2). /outcome and the MCP writes are pure
	// etcd: they persist an intent and return. THIS is what performs it, and it is
	// the only thing that does.
	//
	// Steps run in order: DrainOwnershipAnnouncement/DrainStandDownMerge run AFTER
	// DrainPendingReview so a review Task's approve verdict - which settles
	// status.status/reviewedSHA - is already visible on mr before
	// DrainStandDownMerge decides whether to re-drive the parked owner Task into
	// merging. ReconcileOwnership is the webhook fast path for MR ownership
	// convergence: mr.Status.HeadSHA is the webhook-stamped mirror head (OP7), so
	// a human push that moves it off LastBotHeadSHA drives the flip right here,
	// not just on the cron sweep (newComments is nil - comment redelivery (OP12)
	// is the sweep's own job).
	if r.Driver != nil {
		steps := []func() error{
			func() error { return r.Driver.DrainPendingComments(ctx, &mr) },
			func() error { return r.Driver.DrainPendingReview(ctx, &mr) },
			func() error {
				_, err := r.Driver.ReconcileOwnership(ctx, &proj, &repo, &mr, mr.Status.HeadSHA, nil)
				return err
			},
			func() error { return r.Driver.DrainOwnershipAnnouncement(ctx, &proj, &repo, &mr) },
			func() error { return r.Driver.DrainStandDownMerge(ctx, &proj, &repo, &mr) },
			// The H.4 semver:<level> label. It lands the moment the implement outcome
			// stamps status.significance - not at merge time, and not on some sweep
			// an hour later: it is what CI cuts the release tag from, and a human
			// looking at the PR is entitled to see the level the agent declared.
			func() error { return r.Driver.ProjectSemverLabel(ctx, &proj, &repo, &mr) },
		}
		for _, step := range steps {
			if err := step(); err != nil {
				// A permanent 404 (the upstream MR was deleted at the forge) never
				// clears on retry: returning it would requeue with exponential
				// backoff forever (tatara-operator#426). Mark the mirror closed -
				// the same terminal state a real forge close would leave - and stop
				// draining this pass instead of looping.
				if scm.IsNotFound(err) {
					if serr := markMergeRequestGone(ctx, r.Client, r.spiller(&proj), &mr); serr != nil {
						return ctrl.Result{}, serr
					}
					log.FromContext(ctx).Info("mergerequest: upstream permanently gone (404); marked closed, stopped forge drains",
						"action", "mr_gone_404", "resource_id", mr.Name)
					return ctrl.Result{RequeueAfter: min(cadence, ciCadence)}, nil
				}
				return ctrl.Result{}, err
			}
		}
	}

	return ctrl.Result{RequeueAfter: min(cadence, ciCadence)}, nil
}

// refreshCI re-reads LIVE CI at this MR's mirrored head and stamps
// status.ciStatus / status.ciUpdatedAt.
//
// THE READ IS AGGREGATE. GetCommitCIStatus folds every check-run and every
// commit status on the commit before it answers, which is what entitles this
// path - unlike a single check_run webhook - to write green. It is also why it
// is the authority that corrects a webhook-stamped green down when a second
// check suite is still red.
//
// IT RETURNS NOTHING AND SWALLOWS EVERY ERROR, deliberately. It is a backstop:
// the drains that run after it (DrainPendingReview above all) are the only thing
// that advances a Task off reviewing, and a forge that 502s on a CI read must
// not be able to wedge them. The next cadence tick simply asks again.
func (r *MergeRequestReconciler) refreshCI(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest, cadence time.Duration) {

	if r.ReaderFor == nil || mr.Status.HeadSHA == "" {
		return
	}
	// A merged or closed MR's CI can no longer gate anything (same exclusion
	// ciRedAtReviewedHead makes), and re-reading it forever is pure forge load.
	if mr.Status.State == "merged" || mr.Status.State == "closed" {
		return
	}
	if !mirrorSyncDue(mr.Status.CIUpdatedAt, cadence, r.now()) {
		return
	}

	l := log.FromContext(ctx)
	reader, err := mirrorReaderFor(ctx, r.Client, r.ReaderFor, proj)
	if err != nil {
		l.Info("mergerequest: ci backstop skipped, no reader", "resource_id", mr.Name, "err", err.Error())
		return
	}
	owner, name, err := ciOwnerRepo(repo.Spec.URL, providerOf(proj))
	if err != nil {
		l.Info("mergerequest: ci backstop skipped, unparseable repo url", "resource_id", mr.Name, "err", err.Error())
		return
	}
	status, err := reader.GetCommitCIStatus(ctx, owner, name, mr.Status.HeadSHA)
	if err != nil {
		l.Info("mergerequest: ci backstop read failed (non-fatal)",
			"action", "ci_backstop_read", "resource_id", mr.Name, "head_sha", mr.Status.HeadSHA, "err", err.Error())
		return
	}

	mirrored := scm.MirrorCIStatus(status)
	now := metav1.NewTime(r.now())
	if err := objbudget.FitMergeRequest(ctx, r.Client, r.spiller(proj), client.ObjectKeyFromObject(mr),
		func(m *tatarav1alpha1.MergeRequest) {
			m.Status.CIStatus = mirrored
			m.Status.CIUpdatedAt = &now
		}); err != nil {
		l.Info("mergerequest: ci backstop write failed (non-fatal)",
			"action", "ci_backstop_write", "resource_id", mr.Name, "err", err.Error())
		return
	}
	// Fold the observation onto the in-memory copy the drains below still read.
	mr.Status.CIStatus = mirrored
	mr.Status.CIUpdatedAt = &now
}

// ciOwnerRepo resolves the (owner, repo) pair GetCommitCIStatus wants for this
// provider. GitLab's CI reads take the FULL project path as the single "owner"
// argument - its notion of a project id - and an empty repo; splitting a
// subgroup path into owner/name reads the wrong project or none. Same resolution
// as releaseGreen in merge.go and gatherRepoCIState in projectscan.go.
func ciOwnerRepo(repoURL, provider string) (string, string, error) {
	owner, name, err := scm.OwnerRepo(repoURL)
	if err != nil {
		return "", "", err
	}
	if provider == "gitlab" {
		if pp, perr := scm.GitLabProjectPath(repoURL); perr == nil {
			return pp, "", nil
		}
	}
	return owner, name, nil
}

// mrMergeIsRecorded reports whether this mirror already records a completed
// merge, which no forge 404 may overwrite.
//
// "closed" is a strictly weaker claim than "merged" and rewriting one into the
// other is not a mirror-local edit: stage.AllMRsTerminal stays true while
// AllMRsMerged goes false, so terminalMREdge/OwnMRsShippedEdge finalize the
// owner Task rejected(mr-closed-externally) - and WithTerminalIssueRelease posts
// that verdict publicly on the driving issue - instead of done. A 404 proves the
// forge will not answer for this number; it does not disprove a merge the
// operator itself performed and stamped. MergedAt is checked too because it is
// the merge path's own receipt (stampMerged, merge.go) and is the field
// review_apply.go and stage.go already treat as the durable one.
func mrMergeIsRecorded(mr *tatarav1alpha1.MergeRequest) bool {
	return mr.Status.State == "merged" || mr.Status.MergedAt != nil
}

// markMergeRequestGone stamps a permanently-404ing MergeRequest's mirror as
// closed, the same terminal state a real forge close leaves. Idempotent: a
// second 404 on an already-closed mirror is a no-op write.
func markMergeRequestGone(ctx context.Context, c client.Client, sp objbudget.Spiller, mr *tatarav1alpha1.MergeRequest) error {
	if mr.Status.State == "closed" || mrMergeIsRecorded(mr) {
		return nil
	}
	key := client.ObjectKeyFromObject(mr)
	return objbudget.FitMergeRequest(ctx, c, sp, key, func(m *tatarav1alpha1.MergeRequest) {
		m.Status.State = "closed"
	})
}

// markMergeRequestThreadGone is markMergeRequestGone for the MIRROR READ path
// (#621): it stamps the terminal state AND the cadence stamp the sync never got
// to write, in ONE status update.
//
// Both facts belong to the same write. Without the stamp mirrorSyncDue never
// goes false, and the first cut of this fix silenced the loop by making it
// FASTER: the early return re-read the thread it had just proven gone and
// re-probed the repo on every requeue - min(cadence, ciCadence) = 5 minutes for
// an actively-worked MR, against the ~3.6 calls/h the exponential backoff had
// settled at, with refreshCI and all six drains starved on every pass.
//
// The caller's terminal exclusion at :116 now skips the whole block once the
// state lands, so the stamp no longer carries that on its own. Keep it anyway:
// it is the honest record of when this thread was last checked, and it is the
// only thing standing between a future caller without that exclusion and the
// same regression.
//
// It refuses a recorded merge for the reason mrMergeIsRecorded gives - here
// rather than at the call site, because the invariant belongs to the write.
func markMergeRequestThreadGone(ctx context.Context, c client.Client, sp objbudget.Spiller,
	mr *tatarav1alpha1.MergeRequest, now time.Time) error {

	if mrMergeIsRecorded(mr) {
		return nil
	}
	stamp := metav1.NewTime(now)
	return objbudget.FitMergeRequest(ctx, c, sp, client.ObjectKeyFromObject(mr), func(m *tatarav1alpha1.MergeRequest) {
		m.Status.State = "closed"
		m.Status.LastSyncedAt = &stamp
	})
}

// SetupWithManager registers the MergeRequest reconciler.
func (r *MergeRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tatarav1alpha1.MergeRequest{}).
		Named("mergerequest").
		Complete(r)
}
