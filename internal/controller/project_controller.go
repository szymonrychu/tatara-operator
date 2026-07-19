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
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/grafanamcp"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/scm"
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
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
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
	// Seq provides durable per-project sequence numbers for QueuedEvents created
	// by cron scans. Wired in wire.go; tests create via &queue.SeqSource{Client, Namespace}.
	Seq *queue.SeqSource

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

	// lastComputeProjectCounts / lastResumeNoReentryParks / lastReapTerminal
	// pace computeProjectCounts / resumeNoReentryParks / ReapTerminal, one map
	// each, keyed per project (like lastDriveUnparks): three more full-
	// namespace-List blocks that used to re-run on every single Reconcile pass
	// regardless of cadence (tatara-operator#367). Read/written only on the
	// serialised reconcile path (MaxConcurrentReconciles=1); no mutex required.
	lastComputeProjectCounts map[string]time.Time
	lastResumeNoReentryParks map[string]time.Time
	lastReapTerminal         map[string]time.Time
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
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors;prometheusrules;podmonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch

// Reconcile validates spec.scmSecretRef and sets status.webhookURL plus the
// Ready condition. A missing or malformed secret is reported via the Ready
// condition (status False), not returned as an error.
func (r *ProjectReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var project tataradevv1alpha1.Project
	if err := r.Get(ctx, req.NamespacedName, &project); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		r.Metrics.ReconcileResult("Project", "error")
		return ctrl.Result{}, fmt.Errorf("get project: %w", err)
	}

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

	r.ensureLabelColors(ctx, &project)

	scanRequeue, scanErr := r.runScans(ctx, &project)
	if scanErr != nil {
		r.Metrics.ReconcileResult("Project", "error")
		return ctrl.Result{}, scanErr
	}
	requeueAfter = soonestRequeue(requeueAfter, scanRequeue)

	// THE F.6 RE-ENTRY DRIVER (fix W3). Applies stage.Unpark to every parked Task
	// whose reason has a re-entry rule (time-based merge/deploy/no-outcome plus the
	// comment-driven awaiting-human/backlog-sweep backstop); identity-unverified is
	// webhook+grammar-driven and skipped. Paced independently of Reconcile()'s other
	// drivers (tatara-operator#368: an unrelated 10s-or-faster forced cadence was
	// hammering this path across the full parked backlog every single pass). A
	// persist failure requeues.
	unparkRequeue, unparkErr := r.driveUnparksPaced(ctx, &project, time.Now())
	if unparkErr != nil {
		r.Metrics.ReconcileResult("Project", "error")
		return ctrl.Result{}, fmt.Errorf("drive unparks: %w", unparkErr)
	}
	requeueAfter = soonestRequeue(requeueAfter, unparkRequeue)

	// WS3-I4: a human reply to a Task parked with a NO-RE-ENTRY reason triggers a
	// fresh gated re-mint (sever(Orphan) + MintForItem), never a smuggled re-entry.
	// Leader-only; the reaper is the backstop. Runs after driveUnparks so a
	// re-entryable park has already been resumed and is no longer parked here.
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
			case "clarify":
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
	r.lastGaugeRecompute = time.Now()
}

// updateMemoryStackCounts lists all Projects and sets the operator_memory_stacks
// gauge to the current cluster-wide count per phase. Projects without
// status.memory are not counted.
func (r *ProjectReconciler) updateMemoryStackCounts(ctx context.Context) {
	var list tataradevv1alpha1.ProjectList
	if err := r.List(ctx, &list); err != nil {
		return
	}
	var provisioning, ready, failed, degraded int
	for i := range list.Items {
		mem := list.Items[i].Status.Memory
		if mem == nil {
			continue
		}
		switch mem.Phase {
		case "Provisioning":
			provisioning++
		case "Ready":
			ready++
		case "Failed":
			failed++
		case "Degraded":
			degraded++
		}
	}
	r.Metrics.SetMemoryStackCounts(provisioning, ready, failed, degraded)
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

// issueStateFor returns the tatara_issue_state "state" label for a live Task:
// its STAGE. A Task whose work is over (a closed-set terminal, or delivered)
// returns "" and drops out of the gauge. It is a pure helper so it can be
// unit-tested independently of the reconciler.
func issueStateFor(t *tataradevv1alpha1.Task) string {
	if tataradevv1alpha1.TaskDone(t) {
		return ""
	}
	return t.Status.Stage
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

// taskStageBucket keys the operator_task_stage COUNT aggregation (contract
// K.1): the metric is low-cardinality by design, one series per (stage,kind),
// not per-task.
type taskStageBucket struct {
	stage, kind string
}

// updateTaskStageGauges recomputes operator_task_stage (a COUNT per
// (stage,kind) bucket) and operator_task_stage_age_seconds (per-task, seconds
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
		stg := t.Status.Stage
		if stg == "" {
			continue
		}
		kind := t.Spec.Kind
		counts[taskStageBucket{stage: stg, kind: kind}]++
		if t.Status.StageEnteredAt != nil {
			age := now.Sub(t.Status.StageEnteredAt.Time).Seconds()
			r.Metrics.SetTaskStageAge(t.Name, stg, kind, age)
		}
	}
	for b, n := range counts {
		r.Metrics.SetTaskStage(b.stage, b.kind, float64(n))
	}
}

// queueAgeBucket keys the operator_queue_age_seconds aggregation (contract
// K.1): the OLDEST QueuedEvent's age per (class,priority,state) bucket.
type queueAgeBucket struct {
	class, priority, state string
}

// updateQueueAgeGauge recomputes operator_queue_age_seconds from authoritative
// cluster state (contract K.1): the age of the OLDEST QueuedEvent in each
// (class,priority,state) bucket. A Reset() before each pass ensures an emptied
// bucket does not retain its last value.
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
		b := queueAgeBucket{class: q.Spec.Class, priority: priority, state: state}
		created := q.CreationTimestamp.Time
		if existing, ok := oldest[b]; !ok || created.Before(existing) {
			oldest[b] = created
		}
	}
	now := time.Now()
	for b, ts := range oldest {
		r.Metrics.SetQueueAge(b.class, b.priority, b.state, now.Sub(ts).Seconds())
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
	return ctrl.NewControllerManagedBy(mgr).
		For(&tataradevv1alpha1.Project{}).
		Owns(&corev1.Secret{}).
		Owns(&cnpgv1.Cluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&networkingv1.Ingress{}).
		// MaxConcurrentReconciles: 1 is explicit here; scan dedup/cap logic
		// assumes serialised reconciles per kind.
		WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}
