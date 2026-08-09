// Package controller holds the tatara-operator reconcilers.
package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/prometheus/client_golang/prometheus"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/grafanamcp"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// defaultGaugeRecomputeInterval is how often updateMemoryStackCounts and
// updateIssueStateCounts run. Both do a full ProjectList / TaskList scan;
// running them on every per-Project reconcile is O(N) per cycle per project.
// 60 s is coarse-grained enough to avoid list pressure while still converging
// the gauges quickly after any phase/state change.
const defaultGaugeRecomputeInterval = 60 * time.Second

// defaultUnparkDriveInterval floors how often driveUnparks re-sweeps a
// Project's parked-Task backlog (see UnparkDriveInterval, tatara-operator#368).
// 30s keeps time-based re-entries (merge-timeout/deploy-timeout/no-outcome/
// stage-deadline, all minutes-to-hours granularity per stage.go) and the
// comment-driven backstop (driveCommentUnpark handles the prompt path
// directly; this floor is only its cron-cadence fallback) unaffected in
// practice, while capping decline volume to (parked backlog)/30s regardless
// of how fast something else forces Reconcile() to run.
const defaultUnparkDriveInterval = 30 * time.Second

// defaultComputeProjectCountsInterval floors how often computeProjectCounts
// re-Lists Repository+Task in full to recompute RepositoryCount/
// OpenIssuesCount/OpenIncidentsCount (tatara-operator#367): a Project pinned
// to a fast Reconcile cadence by something ELSE (owned-memory-stack watch
// events, reconcileMemory's 10s provisioning requeue) must not turn this
// per-pass full-namespace List into a per-pass cost too. 60s matches
// defaultGaugeRecomputeInterval's reasoning.
const defaultComputeProjectCountsInterval = 60 * time.Second

// defaultResumeNoReentryInterval floors how often resumeNoReentryParks
// re-Lists Tasks in full to sweep for a human reply on a no-re-entry park
// (tatara-operator#367), same reasoning as defaultComputeProjectCountsInterval.
const defaultResumeNoReentryInterval = 60 * time.Second

// defaultReapTerminalInterval floors how often ReapTerminal re-Lists Tasks in
// full to reap terminal Tasks (tatara-operator#367), same reasoning as
// defaultComputeProjectCountsInterval.
const defaultReapTerminalInterval = 60 * time.Second

// ProjectReconciler validates a Project's SCM secret and publishes its
// webhook URL.
type ProjectReconciler struct {
	client.Client
	// APIReader is the manager's UNCACHED reader. driveUnparks passes it through
	// to ApplyUnpark, whose F.6 re-entry Get must never be served from a
	// cache that lags an external write (same #347/#348 idiom as
	// TaskReconciler.APIReader). Nil (unit tests) falls back to Client.
	APIReader           client.Reader
	Scheme              *runtime.Scheme
	Metrics             *obs.OperatorMetrics
	ExternalWebhookBase string
	MemoryConfig        memory.Config
	GrafanaConfig       grafanamcp.Config
	// ReaderFor returns a token-bound scm.SCMReader for a provider name and token.
	// Nil in tests that do not exercise scanning; wired in wire.go at runtime.
	ReaderFor func(provider, token string) (scm.SCMReader, error)
	// SCMFor returns the SCMWriter for a provider name (token passed per call).
	SCMFor func(provider string) (scm.SCMWriter, error)
	// SpillerFor returns the tatara-memory spiller for a Project (its
	// status.memory.endpoint), used by the A.7 byte-budget guard. Threaded
	// through minter() and driver() (mirroring TaskReconciler/IssueReconciler)
	// so a sweep-driven mint or ReconcileOwnership call spills exactly like the
	// webhook fast path does, instead of writing an un-budgeted mirror. Nil in
	// tests that do not exercise spilling.
	SpillerFor func(proj *tataradevv1alpha1.Project) objbudget.Spiller
	// Seq provides durable per-project sequence numbers for QueuedEvents created
	// by cron scans. Wired in wire.go; tests create via &queue.SeqSource{Client, Namespace}.
	Seq *queue.SeqSource
	// Tasks is the *TaskReconciler enforceConversingCeiling reaches
	// conversingHandoffAndPark through - the eviction path takes the SAME
	// handoff-turn-then-park sequence idle expiry does (Task 10), rather than a
	// second copy of it. Wired in wire.go to the SAME *TaskReconciler the
	// manager registers, not a second instance: two independent TaskReconcilers
	// would mean two independently-configured Session/SpillerFor/PodConfig
	// stacks for what must behave as one stopper. Nil (a test that never drives
	// the ceiling) is never dereferenced.
	Tasks *TaskReconciler

	// GaugeRecomputeInterval controls how often the cluster-wide gauge scans
	// (updateMemoryStackCounts + updateIssueStateCounts) run. Defaults to
	// defaultGaugeRecomputeInterval when zero. MaxConcurrentReconciles=1 means
	// this field is read/written under the controller's serialised call path;
	// no mutex required.
	GaugeRecomputeInterval time.Duration
	lastGaugeRecompute     time.Time

	// LightragHTTP is the client used to read per-project lightrag document
	// counts during the gauge recompute. Nil falls back to a short-timeout
	// default; tests inject an httptest-backed client. LightragBaseURL, when set,
	// overrides the in-cluster Service DNS (tests point it at httptest).
	LightragHTTP    *http.Client
	LightragBaseURL func(project string) string

	// MemoryHTTP is the client used by updateMemoryRetrievalProbe to probe each
	// Ready project's tatara-memory retrieval surface. Nil falls back to a
	// short-timeout default; tests inject an httptest-backed client. MemoryBaseURL,
	// when set, overrides the in-cluster Service DNS (tests point it at httptest).
	MemoryHTTP    *http.Client
	MemoryBaseURL func(project string) string

	// MemoryToken mints a memory-audience bearer token for the authenticated
	// retrieval probe (updateMemoryRetrievalProbe). Wired in wire.go to a cached
	// client-credentials TokenSource; tests inject a stub. When nil the probe
	// sends no token, so tatara-memory's auth gate answers 401 and every route is
	// classified "unauthorized" (unhealthy): an operator that cannot authenticate
	// to memory has no basis to report MemoryReady, so the gap surfaces rather
	// than hides.
	MemoryToken func(ctx context.Context) (string, error)

	// memoryUnhealthyCycles tracks, per project, the number of consecutive
	// updateMemoryRetrievalProbe cycles whose retrieval surface probed unhealthy.
	// reconcileMemory folds a sustained run (>= memoryRetrievalUnhealthyThreshold)
	// into the MemoryReady condition. Read/written only on the serialised reconcile
	// path (MaxConcurrentReconciles=1); no mutex required.
	memoryUnhealthyCycles map[string]int

	// memoryApplyTransientSince records, per project, when the current run of
	// RETRYABLE applyMemoryStack failures started (issue #439). reconcileMemory
	// absorbs those failures - no phase=Failed, no returned reconcile error -
	// until the run outlasts memoryApplyTransientGrace. Read/written only on the
	// serialised reconcile path (MaxConcurrentReconciles: 1); no mutex required,
	// same as memoryUnhealthyCycles.
	memoryApplyTransientSince map[string]time.Time

	// evictionFailures records, per victim Task (namespace/name),
	// enforceLivePodCeiling's own eviction-attempt failures (#564) so a victim
	// whose eviction just errored is SKIPPED, not re-picked, until its backoff
	// elapses - letting the pass fall through to the next-best candidate
	// instead of spending its one eviction slot on the same poisoned victim
	// every single pass forever. Keyed by types.NamespacedName rather than UID:
	// a Task's UID is assigned by the API server and this map must work
	// identically against a fake client in tests that never populate one:
	// two distinct un-UID'd Tasks would otherwise collide on the same
	// zero-value key and each be treated as the other's backoff. The (narrow)
	// cost is a deleted-and-recreated Task of the same name inheriting a stale
	// backoff record instead of starting clean - bounded by
	// evictionFailureBackoffMax and self-healing, so it costs a few minutes of
	// eviction delay at worst, never a stuck state. In-memory and not a
	// status/annotation marker: it resets on operator restart, which is
	// acceptable because eviction only ever runs on the leader
	// (MaxConcurrentReconciles=1, one leader replica), so a restart just means
	// the backoff resets to zero rather than churning the API server with a
	// write on every failed attempt. Read/written only on the serialised
	// reconcile path (MaxConcurrentReconciles=1); no mutex required, same as
	// memoryUnhealthyCycles.
	evictionFailures map[types.NamespacedName]*evictionFailure

	// ToolSurfaceHTTP is the client used by updateToolSurfaceProbe to probe the
	// operator-write tool backend. Nil falls back to a short-timeout
	// default; tests inject an httptest-backed client.
	ToolSurfaceHTTP *http.Client
	// OperatorURL is the in-cluster REST base URL of the operator-write surface
	// (the TATARA_OPERATOR_URL agent pods receive); updateToolSurfaceProbe probes
	// a representative read here. Empty disables the operator-backend probe.
	OperatorURL string

	// toolSurfaceUnhealthyCycles tracks, per backend, the number of consecutive
	// updateToolSurfaceProbe cycles that probed unhealthy. probeToolSurface only
	// meters the failing result once a backend's run reaches
	// toolSurfaceUnhealthyThreshold, so the operator's own rollout churn does not
	// trip the tool-surface alert against a still-serving backend. Read/written
	// only on the serialised reconcile path (MaxConcurrentReconciles=1); no mutex
	// required.
	toolSurfaceUnhealthyCycles map[string]int

	// UnparkDriveInterval floors how often driveUnparks re-sweeps a given
	// Project's parked-Task backlog, decoupled from whatever cadence
	// Reconcile() otherwise runs at. Defaults to defaultUnparkDriveInterval
	// when zero. Fix for the 2026-07-18 operator_unpark_declined_total burst
	// (tatara-operator#368): a Project stuck in memory phase=Provisioning
	// forces reconcileMemory's 10s RequeueAfter floor onto the WHOLE
	// Reconcile() (amplified further by Owns() watch retriggers - see
	// tatara-operator#367), and driveUnparks had no pacing of its own, so it
	// re-declined the full parked backlog on every single pass. Keyed per
	// project (like memoryUnhealthyCycles), never a single cluster-wide
	// clock: two live Projects must not throttle each other.
	UnparkDriveInterval time.Duration
	lastDriveUnparks    map[string]time.Time

	// lastComputeProjectCounts / lastResumeNoReentryParks / lastReapTerminal pace
	// computeProjectCounts, resumeNoReentryParks and ReapTerminal, one map each,
	// keyed per project (like lastDriveUnparks): full-namespace-List blocks that
	// used to re-run on every single Reconcile pass regardless of cadence
	// (tatara-operator#367). Read/written only on the serialised reconcile path
	// (MaxConcurrentReconciles=1); no mutex required.
	lastComputeProjectCounts map[string]time.Time
	lastResumeNoReentryParks map[string]time.Time
	lastReapTerminal         map[string]time.Time

	// lastBrainstormPausedLogged paces brainstorm()'s "no refill this pass"
	// line at INFO (rather than V(1)) for reason=="paused", once per
	// brainstormResyncInterval per project (I5 fix round). A paused project
	// must be log-visible at INFO somewhere besides the one-shot pause-stamp
	// line in outcome.go, but the decision itself now runs on every ~30s
	// event-driven pass, so logging it at INFO unconditionally would be the
	// exact continuous-line-at-INFO problem this same package already avoids
	// for "at-target" via V(1). Keyed per project, same idiom as
	// lastDriveUnparks.
	lastBrainstormPausedLogged map[string]time.Time
}

// +kubebuilder:rbac:groups=tatara.dev,resources=projects,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tatara.dev,resources=projects/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=clusters/status,verbs=get;list;watch
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=scheduledbackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors;prometheusrules;podmonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch

// Reconcile validates spec.scmSecretRef and sets status.webhookURL plus the
// Ready condition. A missing or malformed secret is reported via the Ready
// condition (status False), not returned as an error.
// The body is doReconcile; this wrapper is the #538 shutdown-cancellation
// boundary (see classifyReconcileShutdown). The project reconcile fans out into
// the scans, the reaper and the un-park drive, so it is the widest surface a
// SIGTERM can abort mid-flight.
func (r *ProjectReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	res, err := r.doReconcile(ctx, req)
	return classifyReconcileShutdown(ctx, "project", req.Name, res, err)
}

func (r *ProjectReconciler) doReconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var project tataradevv1alpha1.Project
	if err := r.Get(ctx, req.NamespacedName, &project); err != nil {
		if apierrors.IsNotFound(err) {
			// Retract this deleted Project's obs.SweepNextExpectedTimestamp
			// children (round-2 review finding): the gauge is a `time() - gauge`
			// alert input, so a frozen series left behind by a deleted Project
			// pages permanently until the pod restarts, same failure mode as an
			// enabled-then-disabled activity (finding 2) but via deletion instead
			// of a spec flip. Scoped to ONLY this metric on purpose -
			// SweepLastSuccessTimestamp and SweepErrorsTotal have the identical
			// leak but tearing them down too is out of scope here (see MEMORY.md).
			obs.SweepNextExpectedTimestamp.DeletePartialMatch(prometheus.Labels{"project": req.Name})
			// obs.SweepOrphanStrandedSeconds is retracted for the SAME reason and
			// is NOT out of scope: it carries a per-ISSUE label, so a deleted
			// Project leaks one permanently-frozen series per stranded issue, and
			// its alert (max by (project) over a threshold) then pages forever for
			// a project that no longer exists. runScans covers the three
			// in-life transitions; this covers deletion.
			obs.RetainSweepOrphanStranded(req.Name, nil)
			return ctrl.Result{}, nil
		}
		r.Metrics.ReconcileResult("Project", "error")
		return ctrl.Result{}, fmt.Errorf("get project: %w", err)
	}

	// Seed this Project's closed (activity x reason) SweepErrorsTotal set. The
	// seeding used to run in obs.init(); `project` joined that metric's label set
	// in issue #441 and project names are not known at process start, so it moved
	// here. Idempotent: WithLabelValues returns the existing child.
	obs.SeedSweepErrorsForProject(project.Name)

	reason, message, ready := r.validateSecret(ctx, &project)

	project.Status.WebhookURL = fmt.Sprintf("%s/%s", r.ExternalWebhookBase, project.Name)
	status := metav1.ConditionTrue
	if !ready {
		status = metav1.ConditionFalse
	}
	meta.SetStatusCondition(&project.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: project.Generation,
	})

	requeueAfter, memErr := r.reconcileMemory(ctx, &project)

	grafanaRequeueAfter, grafErr := r.reconcileGrafanaMCP(ctx, &project)
	if grafErr != nil {
		l.Error(grafErr, "grafana-mcp reconcile failed (non-blocking)", "resource_id", project.Name)
	}
	requeueAfter = soonestRequeue(requeueAfter, grafanaRequeueAfter)

	countsRequeue, _ := r.computeProjectCountsPaced(ctx, &project, time.Now())
	requeueAfter = soonestRequeue(requeueAfter, countsRequeue)

	// Persist the computed status under optimistic-concurrency retry. A routine
	// 409 ("the object has been modified") - amplified during operator rollouts
	// when two replicaset pods reconcile the same Project - otherwise bubbles up
	// as a "Reconciler error" log line (tatara-operator#387). Every other
	// status-write site in this package already retries; this was the lone
	// outlier. Only the fields THIS reconcile computes are re-applied onto the
	// freshly-read object (WebhookURL, the Ready/Memory conditions, Memory,
	// Grafana, the counts): fields owned by later/other write paths (LastIssueScan
	// et al. from stampScan below, ScanMarks, TokenBudget, LastRefine) must not be
	// reverted to the values read at the top of Reconcile. Mirrors stampScan /
	// patchQueuedEventStatus.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tataradevv1alpha1.Project{}
		if err := r.Get(ctx, req.NamespacedName, fresh); err != nil {
			return err
		}
		fresh.Status.WebhookURL = project.Status.WebhookURL
		fresh.Status.Conditions = project.Status.Conditions
		fresh.Status.Memory = project.Status.Memory
		fresh.Status.Grafana = project.Status.Grafana
		fresh.Status.RepositoryCount = project.Status.RepositoryCount
		fresh.Status.OpenIssuesCount = project.Status.OpenIssuesCount
		fresh.Status.OpenIncidentsCount = project.Status.OpenIncidentsCount
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		project = *fresh
		return nil
	}); err != nil {
		r.Metrics.ReconcileResult("Project", "error")
		return ctrl.Result{}, fmt.Errorf("update project status: %w", err)
	}

	if memErr != nil {
		r.Metrics.ReconcileResult("Project", "error")
		return ctrl.Result{}, memErr
	}

	r.maybeRecomputeGauges(ctx)

	// Make this project's queue-saturation gauges EXIST (at 0) even before its
	// first QueuedEvent reconcile writes one, so "idle" reads 0 instead of the
	// series vanishing (issue #496). Idempotent and value-preserving.
	for _, class := range []string{tataradevv1alpha1.QueueClassNormal, tataradevv1alpha1.QueueClassAlert} {
		r.Metrics.SeedQueueGauges(project.Name, class)
	}

	r.ensureLabelColors(ctx, &project)

	// Consume any standing resume signal BEFORE runScans. The manual-override
	// annotation (tatara.dev/brainstorm-resume) is the escape hatch every
	// prior-art system lacks, and it needs nothing runScans computes - it only
	// reads/writes Status.BrainstormPausedAt and the annotation itself. Running
	// it before runScans (rather than after, gated behind runScans' scanErr
	// early return) means it is consumed even on a project whose runScans
	// errors PERSISTENTLY (bad ScmSecretRef, a listing failure, anything):
	// previously that project could never consume the annotation at all (I4
	// fix round). This also keeps the original intent - a resume and the
	// session it unblocks landing in the SAME reconcile - since the refill
	// pass still runs after runScans, later in this function.
	if err := r.clearBrainstormPauseIfRequested(ctx, &project); err != nil {
		r.Metrics.ReconcileResult("Project", "error")
		return ctrl.Result{}, err
	}

	scanRequeue, scanRepos, scanExisting, scanRdr, scanErr := r.runScans(ctx, &project)
	if scanErr != nil {
		r.Metrics.ReconcileResult("Project", "error")
		return ctrl.Result{}, scanErr
	}

	// EVENT-DRIVEN REFILL (design section 4, task O6). runScans owns the CRON
	// path, gated on the schedule being due; this is the LEVEL-TRIGGERED pass
	// that reacts to the Watches(&Issue{}) edge above - a maintainer verdict
	// landing on a proposal Issue. It is a no-op unless the backlog is
	// actually short: the control law reads the same Issue CRs either way, so
	// running it even on a reconcile where the cron path JUST refilled costs
	// nothing extra - brainstormRefillDecision's deficit clamp and the
	// brainstorm QueuedEvent's dedup key both make a double-refill impossible.
	//
	// Reuses scanRepos/scanExisting/scanRdr, the SAME repos/existing-tasks/
	// reader runScans already fetched this pass, instead of re-listing or
	// re-resolving them a second time (O6 review Minor 3). A pass where
	// runScans could not resolve the reader (bad scmSecretRef, ReaderFor
	// unwired, etc) leaves scanRdr nil here too; runScans has already
	// logged that failure at ERROR (scan_reader_error), so this simply skips
	// rather than re-attempting and re-logging the identical failure (O6
	// review Minor 4).
	if cron := brainstormCronSpec(&project); cron != nil && cron.Enabled {
		if scanRdr != nil {
			// Stamp Status.LastBrainstorm whenever THIS path actually dispatches a
			// session (C2 fix round): the event-driven wake used to never touch it
			// at all, so the new cooldown gate in brainstormRefillDecision had
			// nothing durable to read between events, and a skip (which files no
			// Issue, leaves the deficit positive, and wakes this same path
			// immediately via brainstormCycleFinishedPredicate) could otherwise
			// re-dispatch back to back bounded only by the 30s self-requeue.
			// Best-effort: losing this stamp costs one cooldown window, not
			// correctness, so it is logged and tolerated rather than failing the
			// reconcile - matching stampScan's own cron-path callers.
			if created := r.brainstorm(ctx, &project, scanRdr, scanRepos, scanExisting, *cron); created {
				if serr := r.stampScan(ctx, &project, "brainstorm"); serr != nil {
					l.Error(serr, "scan: persist brainstorm stamp failed (event-driven refill)",
						"action", "scan_stamp_error", "resource_id", project.Name, "activity", "brainstorm")
					obs.SweepErrorsTotal.WithLabelValues(project.Name, "brainstorm", "stamp_failed").Inc()
				}
			}
		}
		// The periodic resync is the BACKSTOP poll interval for a lost watch
		// event, not the workqueue's error rate limiter (reconcile.Result
		// godoc) - folded into scanRequeue so a brainstorm-enabled project
		// always re-checks the backlog level at least this often even if no
		// Issue write ever lands.
		scanRequeue = soonestRequeue(scanRequeue, brainstormResyncInterval)
	}
	requeueAfter = soonestRequeue(requeueAfter, scanRequeue)

	// THE F.6 RE-ENTRY DRIVER (fix W3). Applies stage.Unpark to every parked Task
	// whose reason has a re-entry rule - time-based merge/deploy/no-outcome plus
	// the comment-driven awaiting-human/backlog-sweep/identity-unverified backstop.
	// identity-unverified is INCLUDED, so a comment the webhook fast path dropped
	// on a cache race still gets a second look on the reconcile cadence. READ
	// THIS BEFORE RELYING ON IT FOR MORE THAN THAT: NO grammar verdict is fed to
	// stage.Unpark by anything (agent-judged-approval-gate step A deleted every
	// writer of Task.status.approvalVerdict, step B the reader, step C the
	// UnparkInput slot, step D the API type), so this loop can only ever move a
	// parked(identity-unverified) Task to conversing, never to implementing. The approval decision itself now happens live, in restapi's
	// verifyApprovalScope on submit_outcome(decision=implement), against a Task
	// with a running pod. Paced independently of Reconcile()'s
	// other drivers (tatara-operator#368: an unrelated 10s-or-faster forced
	// cadence was hammering this path across the full parked backlog every
	// single pass). A persist failure requeues.
	unparkRequeue, unparkErr := r.driveUnparksPaced(ctx, &project, time.Now())
	if unparkErr != nil {
		r.Metrics.ReconcileResult("Project", "error")
		return ctrl.Result{}, fmt.Errorf("drive unparks: %w", unparkErr)
	}
	requeueAfter = soonestRequeue(requeueAfter, unparkRequeue)

	// The live-pod ceiling's level-triggered backstop. The webhook's
	// LiveHasRoom check is the fast path that usually keeps a project's
	// live pod count in range; this converges whatever raced it, and it
	// is also what keeps operator_live_pods honest for a project whose
	// comments have stopped arriving (a pure webhook-side gauge would freeze at
	// its last reading forever). Blocking, matching driveUnparksPaced/
	// ReapTerminalPaced above: an eviction failure
	// (a write conflict, a forge outage mid-handoff) must requeue rather than
	// be silently swallowed, the same as every other paced backstop in this loop.
	// enforceLivePodCeiling caps how much overflow it evicts in ONE call
	// (maxLivePodEvictionsPerPass, livepods.go) so a large overflow cannot
	// wedge this single MaxConcurrentReconciles=1 loop for hours; the returned
	// duration asks for a quick re-drive when work remains.
	ceilingRequeue, err := r.enforceLivePodCeiling(ctx, &project, time.Now())
	if err != nil {
		r.Metrics.ReconcileResult("Project", "error")
		return ctrl.Result{}, fmt.Errorf("enforce live pod ceiling: %w", err)
	}
	requeueAfter = soonestRequeue(requeueAfter, ceilingRequeue)

	// THE ONE-REPLY GUARANTEE for the UnparkNever population (finding H8). A
	// human reply to a Task parked under one of the reasons NOBODY un-parks
	// severs its owned open Issues, collects the old Task early, and re-mints
	// the Issues fresh through MintForItem - a new, gate-respecting Task, never
	// a re-entry of the old one.
	//
	// It is NOT redundant with the sweep, whatever "TaskDone is false now" might
	// suggest. The sweep re-mints an ORPHAN Issue, and a parked Task RETAINS its
	// controller ownership (fix H13 does not release it on a park), so
	// resolveLiveIssueOwner finds a live owner, IsOrphanIssue answers
	// issue_owned, and nothing re-mints until the reaper releases the Task at
	// ParkRetention - seven days, by which time the reply is gone from
	// pendingEvents and the maintainer has to comment a SECOND time. See
	// resume.go for why the fix cannot live at the intake/sweep seam instead.
	//
	// Runs AFTER driveUnparks so a re-entryable park has already been resumed
	// and is no longer parked here; the reaper is still the ultimate backstop.
	resumeRequeue, err := r.resumeNoReentryParksPaced(ctx, &project, time.Now())
	if err != nil {
		r.Metrics.ReconcileResult("Project", "error")
		return ctrl.Result{}, fmt.Errorf("resume no-re-entry parks: %w", err)
	}
	requeueAfter = soonestRequeue(requeueAfter, resumeRequeue)

	// THE B.6 TERMINAL REAPER (fix W2). Releases and GCs terminal Tasks
	// (failed/rejected/parked/delivered) and their Issues/MRs, and stamps the
	// tatara-parked label that keeps the M25 re-mint loop closed. It was
	// implemented and tested but had NO production caller. A blocking step failure
	// (e.g. a 403 on the terminal comment/label) returns an error and requeues, per
	// B.6/M25 - the reap must not proceed without the label landing.
	reapRequeue, err := r.ReapTerminalPaced(ctx, &project, time.Now())
	if err != nil {
		r.Metrics.ReconcileResult("Project", "error")
		return ctrl.Result{}, fmt.Errorf("reap terminal: %w", err)
	}
	requeueAfter = soonestRequeue(requeueAfter, reapRequeue)

	memPhase := ""
	if project.Status.Memory != nil {
		memPhase = project.Status.Memory.Phase
	}
	l.Info("reconciled project",
		"action", "reconcile_project",
		"resource_id", project.Name,
		"ready", ready,
		"reason", reason,
		"memory_phase", memPhase)
	r.Metrics.ReconcileResult("Project", "success")
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// soonestRequeue returns the smaller positive duration; 0 means "no requeue"
// and loses to any positive value.
func soonestRequeue(a, b time.Duration) time.Duration {
	switch {
	case a == 0:
		return b
	case b == 0:
		return a
	case a < b:
		return a
	default:
		return b
	}
}

// computeProjectCounts fills RepositoryCount/OpenIssuesCount/OpenIncidentsCount
// on project.Status from a namespace-scoped List + Go filter (item 7).
// Homelab scale: unindexed lists here are cheap (same pattern reaper/projectscan
// already use for Task lists).
func (r *ProjectReconciler) computeProjectCounts(ctx context.Context, project *tataradevv1alpha1.Project) {
	var repos tataradevv1alpha1.RepositoryList
	if err := r.List(ctx, &repos, client.InNamespace(project.Namespace)); err == nil {
		count := 0
		for i := range repos.Items {
			if repos.Items[i].Spec.ProjectRef == project.Name {
				count++
			}
		}
		project.Status.RepositoryCount = count
	}
	var tasks tataradevv1alpha1.TaskList
	if err := r.List(ctx, &tasks, client.InNamespace(project.Namespace)); err == nil {
		issues, incidents := 0, 0
		for i := range tasks.Items {
			t := &tasks.Items[i]
			if t.Spec.ProjectRef != project.Name || tataradevv1alpha1.TaskDone(t) {
				continue
			}
			switch t.Spec.Kind {
			case "incident":
				incidents++
			case SweepIssueKind:
				issues++
			}
		}
		project.Status.OpenIssuesCount = issues
		project.Status.OpenIncidentsCount = incidents
	}
}

// computeProjectCountsPaced runs computeProjectCounts for project at most once
// per defaultComputeProjectCountsInterval, decoupled from whatever cadence
// Reconcile() happens to run at (tatara-operator#367, mirrors
// driveUnparksPaced/unpark.go). When skipped, project.Status's counts are left
// exactly as read at the top of Reconcile; the persist block re-applies that
// value verbatim, which is a no-op, not a stale write. Keyed per project (like
// lastDriveUnparks): two live Projects must not throttle each other's floor.
func (r *ProjectReconciler) computeProjectCountsPaced(ctx context.Context, project *tataradevv1alpha1.Project, now time.Time) (time.Duration, error) {
	interval := defaultComputeProjectCountsInterval
	if last, ok := r.lastComputeProjectCounts[project.Name]; ok {
		if elapsed := now.Sub(last); elapsed < interval {
			return interval - elapsed, nil
		}
	}
	r.computeProjectCounts(ctx, project)
	if r.lastComputeProjectCounts == nil {
		r.lastComputeProjectCounts = map[string]time.Time{}
	}
	r.lastComputeProjectCounts[project.Name] = now
	return interval, nil
}

// maybeRecomputeGauges runs updateMemoryStackCounts and updateIssueStateCounts
// at most once per GaugeRecomputeInterval (defaultGaugeRecomputeInterval when
// zero). Calling it on every Reconcile is safe: it skips the expensive full
// ProjectList + TaskList scans until the interval has elapsed.
func (r *ProjectReconciler) maybeRecomputeGauges(ctx context.Context) {
	interval := r.GaugeRecomputeInterval
	if interval == 0 {
		interval = defaultGaugeRecomputeInterval
	}
	if !r.lastGaugeRecompute.IsZero() && time.Since(r.lastGaugeRecompute) < interval {
		return
	}
	r.updateMemoryStackCounts(ctx)
	r.updateIssueStateCounts(ctx)
	r.updateTaskStageGauges(ctx)
	r.updateQueueAgeGauge(ctx)
	r.updateLightragDocCounts(ctx)
	r.updateMemoryRetrievalProbe(ctx)
	r.updateToolSurfaceProbe(ctx)
	r.updateBotRoundsGauge(ctx)
	r.lastGaugeRecompute = time.Now()
}

// updateBotRoundsGauge recomputes operator_bot_rounds from authoritative
// cluster state: the MAXIMUM live Task.status.botRounds per project, matching
// the metric's own help text ("Highest consecutive agent-authored comment
// rounds ... by project"). A Reset() before each pass ensures a project whose
// highest-round Task since resolved (a human commented and reset it, the
// conversation parked, the Task went terminal) reads its current true value
// instead of retaining whatever bot-authored event happened to write the
// gauge last (2026-07-28 final review IMPORTANT 5) - the webhook fast path no
// longer writes this gauge directly for exactly that reason.
func (r *ProjectReconciler) updateBotRoundsGauge(ctx context.Context) {
	if r.Metrics == nil {
		return
	}
	var list tataradevv1alpha1.TaskList
	if err := r.List(ctx, &list); err != nil {
		return
	}
	r.Metrics.ResetBotRounds()
	maxByProject := make(map[string]int)
	for i := range list.Items {
		t := &list.Items[i]
		if t.Status.BotRounds > maxByProject[t.Spec.ProjectRef] {
			maxByProject[t.Spec.ProjectRef] = t.Status.BotRounds
		}
	}
	for project, n := range maxByProject {
		r.Metrics.SetBotRounds(project, float64(n))
	}
}

// memoryStackPhases is the full set of phases Project.Status.Memory reports.
// updateMemoryStackCounts sets every one per project each pass so a phase a
// project has left reads 0 rather than retaining its last value.
// "Disabled" is in the set so a project with spec.memory.enabled=false reports
// an explicit, queryable 1 on its own phase and a 0 on Failed/Degraded - the
// phases TataraMemoryStackFailed / TataraMemoryStackDegraded key on - rather
// than dropping out of the gauge entirely.
var memoryStackPhases = []string{
	"Provisioning", "Ready", "Failed", "Degraded", tataradevv1alpha1.MemoryPhaseDisabled,
}

// updateMemoryStackCounts lists all Projects and sets operator_memory_stacks to
// 1 for each project's current memory phase and 0 for the others (issue #425).
// The per-pass Reset also drops the series of a project that has been deleted.
// Projects without status.memory are not reported at all.
func (r *ProjectReconciler) updateMemoryStackCounts(ctx context.Context) {
	var list tataradevv1alpha1.ProjectList
	if err := r.List(ctx, &list); err != nil {
		return
	}
	r.Metrics.ResetMemoryStacks()
	for i := range list.Items {
		p := &list.Items[i]
		if p.Status.Memory == nil {
			continue
		}
		for _, phase := range memoryStackPhases {
			v := 0.0
			if p.Status.Memory.Phase == phase {
				v = 1.0
			}
			r.Metrics.SetMemoryStackPhase(p.Name, phase, v)
		}
	}
}

// lightragDocStatuses is the full set of ingestion statuses lightrag reports for
// a document (lightrag v1.4.16). updateLightragDocCounts Sets every one each
// pass (0 when absent) so a status that drains reads 0 rather than retaining its
// last value, mirroring updateMemoryStackCounts.
var lightragDocStatuses = []string{"PENDING", "PROCESSING", "PROCESSED", "FAILED"}

// lightragQueryTimeout bounds a single document-count read so a wedged lightrag
// cannot stall the serialised reconcile path.
const lightragQueryTimeout = 3 * time.Second

// updateLightragDocCounts refreshes operator_lightrag_documents for every
// Project whose memory stack is Ready by reading lightrag's cheap
// /documents/status_counts endpoint. It is best-effort: a project whose lightrag
// is unreachable or erroring is counted in operator_lightrag_query_errors_total
// and skipped, never failing the reconcile. Only Ready stacks are queried so a
// still-provisioning lightrag is not hammered.
func (r *ProjectReconciler) updateLightragDocCounts(ctx context.Context) {
	if r.Metrics == nil {
		return
	}
	var list tataradevv1alpha1.ProjectList
	if err := r.List(ctx, &list); err != nil {
		return
	}
	httpc := r.LightragHTTP
	if httpc == nil {
		httpc = &http.Client{Timeout: lightragQueryTimeout}
	}
	for i := range list.Items {
		p := &list.Items[i]
		if p.Status.Memory == nil || p.Status.Memory.Phase != "Ready" {
			continue
		}
		counts, err := r.fetchLightragDocCounts(ctx, httpc, p.Name)
		if err != nil {
			log.FromContext(ctx).V(1).Info("lightrag doc-count read failed", "project", p.Name, "error", err.Error())
			r.Metrics.LightragQueryError()
			continue
		}
		for _, status := range lightragDocStatuses {
			r.Metrics.SetLightragDocuments(p.Name, status, counts[status])
		}
	}
}

// lightragBaseURL returns the in-cluster base URL of a project's lightrag
// Service, or the test override when set.
func (r *ProjectReconciler) lightragBaseURL(project string) string {
	if r.LightragBaseURL != nil {
		return r.LightragBaseURL(project)
	}
	return fmt.Sprintf("http://%s.%s.svc:9621", memory.NamesFor(project).Lightrag, r.MemoryConfig.Namespace)
}

// fetchLightragDocCounts GETs /documents/status_counts from a project's lightrag
// and returns the per-status document counts. lightrag runs in no-auth mode (no
// AUTH_ACCOUNTS configured), so no Authorization header is needed - matching the
// tatara-memory client.
func (r *ProjectReconciler) fetchLightragDocCounts(ctx context.Context, httpc *http.Client, project string) (map[string]int, error) {
	url := r.lightragBaseURL(project) + "/documents/status_counts"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("lightrag status_counts: HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return nil, err
	}
	return parseLightragStatusCounts(body)
}

// parseLightragStatusCounts decodes a lightrag StatusCountsResponse
// ({"status_counts":{"PROCESSED":130,...}}) into a status->count map.
func parseLightragStatusCounts(body []byte) (map[string]int, error) {
	var payload struct {
		StatusCounts map[string]int `json:"status_counts"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	return payload.StatusCounts, nil
}

// issueStateFor returns the tatara_issue_state "state" label for a live Task.
// A Task whose work is over (done/rejected) returns "" and drops out of the
// gauge, and so does a PARKED one: the gauge is "what is being worked on right
// now", and a parked Task is by definition not. #521 needed that second clause
// explicitly, because park stopped being folded into TaskDone - without it a
// parked Task's raw state leaks into the gauge and reads as live work.
// It is a pure helper so it can be unit-tested independently of the reconciler.
func issueStateFor(t *tataradevv1alpha1.Task) string {
	if tataradevv1alpha1.TaskDone(t) || tataradevv1alpha1.Parked(t) {
		return ""
	}
	return t.Status.State
}

// updateIssueStateCounts recomputes tatara_issue_state from authoritative cluster
// state by listing all non-terminal, issue-scoped Tasks and setting one gauge
// series per live issue. A Reset() before each pass ensures stale (closed or
// terminal) issues are not retained, enabling the dashboard to list every open
// issue with its current stage, token usage, and turn count.
func (r *ProjectReconciler) updateIssueStateCounts(ctx context.Context) {
	if r.Metrics == nil {
		return
	}
	var list tataradevv1alpha1.TaskList
	if err := r.List(ctx, &list); err != nil {
		return
	}
	r.Metrics.ResetIssueState()
	for i := range list.Items {
		t := &list.Items[i]
		project, repo, kind, issue, _ := taskTokenLabels(t)
		if issue == "" {
			continue
		}
		state := issueStateFor(t)
		if state == "" {
			continue
		}
		incident := "false"
		if t.Labels[labelIncident] == "true" {
			incident = "true"
		}
		r.Metrics.SetIssueState(project, repo, issue, kind, state, incident)
	}
}

// taskStageBucket keys the operator_task_state COUNT aggregation (contract
// K.1): the metric is low-cardinality by design, one series per (stage,kind),
// not per-task.
type taskStageBucket struct {
	stage, kind string
}

// updateTaskStageGauges recomputes operator_task_state (a COUNT per
// (stage,kind) bucket) and operator_task_state_age_seconds (per-task, seconds
// since Status.StageEnteredAt) from authoritative cluster state (contract
// K.1). A Reset() before each pass ensures a Task that left its stage or was
// deleted does not leave a stale series behind (contract M22).
func (r *ProjectReconciler) updateTaskStageGauges(ctx context.Context) {
	if r.Metrics == nil {
		return
	}
	var list tataradevv1alpha1.TaskList
	if err := r.List(ctx, &list); err != nil {
		return
	}
	r.Metrics.ResetTaskStageGauges()
	now := time.Now()
	counts := make(map[taskStageBucket]int)
	for i := range list.Items {
		t := &list.Items[i]
		stg := t.Status.State
		if stg == "" {
			continue
		}
		kind := t.Spec.Kind
		counts[taskStageBucket{stage: stg, kind: kind}]++
		if t.Status.StateEnteredAt != nil {
			// Carry-adjusted (issue #480): a merge-timeout/deploy-timeout un-park
			// re-stamps StageEnteredAt, so a bare now.Sub would under-report true
			// stage residency by up to a full budget per re-entry.
			age := stage.StateElapsedSeconds(t, now)
			r.Metrics.SetTaskStageAge(t.Name, stg, kind, age)
		}
	}
	for b, n := range counts {
		r.Metrics.SetTaskStage(b.stage, b.kind, float64(n))
	}
}

// queueAgeBucket keys the operator_queue_age_seconds aggregation (contract
// K.1): the OLDEST QueuedEvent's age per (project,class,priority,state) bucket.
type queueAgeBucket struct {
	project, class, priority, state string
}

// updateQueueAgeGauge recomputes operator_queue_age_seconds from authoritative
// cluster state (contract K.1): the age of the OLDEST QueuedEvent in each
// (project,class,priority,state) bucket. A Reset() before each pass ensures an
// emptied bucket does not retain its last value, and also drops the series of a
// project that no longer has any QueuedEvents.
func (r *ProjectReconciler) updateQueueAgeGauge(ctx context.Context) {
	if r.Metrics == nil {
		return
	}
	var list tataradevv1alpha1.QueuedEventList
	if err := r.List(ctx, &list); err != nil {
		return
	}
	r.Metrics.ResetQueueAge()
	oldest := make(map[queueAgeBucket]time.Time)
	for i := range list.Items {
		q := &list.Items[i]
		// EffectiveQueuePriority applies the CRD default (2) for a nil priority,
		// matching how the dispatcher reads it - so a nil-priority event does NOT
		// share a bucket with a genuine explicit-priority-0 (incident) event.
		priority := strconv.Itoa(tataradevv1alpha1.EffectiveQueuePriority(q.Spec))
		state := q.Status.State
		if state == "" {
			state = tataradevv1alpha1.QueueStateQueued
		}
		b := queueAgeBucket{project: q.Spec.ProjectRef, class: q.Spec.Class, priority: priority, state: state}
		created := q.CreationTimestamp.Time
		if existing, ok := oldest[b]; !ok || created.Before(existing) {
			oldest[b] = created
		}
	}
	now := time.Now()
	for b, ts := range oldest {
		r.Metrics.SetQueueAge(b.project, b.class, b.priority, b.state, now.Sub(ts).Seconds())
	}
}

// validateSecret returns the condition (reason, message, ready) for the
// Project's scmSecretRef. ready is true only when the secret exists and has
// both required keys.
func (r *ProjectReconciler) validateSecret(ctx context.Context, project *tataradevv1alpha1.Project) (reason, message string, ready bool) {
	if project.Spec.ScmSecretRef == "" {
		return "SecretRefEmpty", "spec.scmSecretRef is empty", false
	}
	var secret corev1.Secret
	key := types.NamespacedName{Namespace: project.Namespace, Name: project.Spec.ScmSecretRef}
	if err := r.Get(ctx, key, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return "SecretNotFound", fmt.Sprintf("secret %q not found", project.Spec.ScmSecretRef), false
		}
		return "SecretError", err.Error(), false
	}
	for _, k := range []string{"token", "webhookSecret"} {
		if len(secret.Data[k]) == 0 {
			return "SecretMissingKeys", fmt.Sprintf("secret %q missing key %q", project.Spec.ScmSecretRef, k), false
		}
	}
	return "Validated", "scm secret present with token and webhookSecret", true
}

// SetupWithManager registers the reconciler with the manager, watching
// Projects and the full per-project memory stack kinds.
func (r *ProjectReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Self-wire the uncached reader (same idiom as DispatcherReconciler,
	// queue_controller.go): a wire.go that forgets to set APIReader must not
	// silently fall back to the cached Client for driveUnparks' ApplyUnpark
	// call with no compile error and no test failure.
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	return projectControllerBuilder(mgr).Complete(r)
}

// projectControllerBuilder assembles the Project controller's ENTIRE watch set.
// It is a separate function so the envtest (project_brainstorm_chain_test.go's
// sibling coverage) registers the SAME builder production registers, instead of
// a hand-copied duplicate that would go on passing after someone deleted the
// real edge - deleting any Owns/Watches here now turns that test RED.
func projectControllerBuilder(mgr ctrl.Manager) *builder.Builder {
	bld := ctrl.NewControllerManagedBy(mgr).
		For(&tataradevv1alpha1.Project{}).
		Owns(&corev1.Secret{}).
		Owns(&cnpgv1.Cluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&networkingv1.Ingress{}).
		// The EVENT-DRIVEN REFILL trigger (design section 4, task O6). Issue is
		// not owned by Project (it is owned by the Task working it), so this is a
		// plain Watches edge, not Owns. proposalVerdictPredicate is what keeps a
		// project with a large Issue backlog from turning every mirror resync
		// into a Project reconcile.
		Watches(&tataradevv1alpha1.Issue{},
			handler.EnqueueRequestsFromMapFunc(issueToProject),
			builder.WithPredicates(proposalVerdictPredicate()))
	// THE BRAINSTORM CHAIN. A cycle that SKIPS writes no Issue, so the edge above
	// never fires for it. See brainstormChainEdge for the real rationale (an
	// immediate wake plus bypassing the workqueue's rate limiter under error
	// backoff, NOT a 15m wait avoided - Project already self-requeues every 30s
	// via defaultUnparkDriveInterval). The skip breaker this edge used to route
	// around is GONE (retired for a pure level law); what now governs a project
	// that skips repeatedly is brainstormRefillDecision's cooldown gate
	// (proposalcount.go, C2 fix round) - a durable per-project minimum interval
	// between sessions, not a consecutive-skip counter. In particular, do NOT
	// add GenerationChangedPredicate to it.
	bld = brainstormChainEdge(bld)
	// MaxConcurrentReconciles: 1 is explicit here; scan dedup/cap logic assumes
	// serialised reconciles per kind.
	return bld.WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: 1})
}
