package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/ingest"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ReingestAnnotation aliases the canonical constant from api/v1alpha1 so
// controller code keeps using the same short name internally.
const ReingestAnnotation = tataradevv1alpha1.ReingestRequestedAnnotation

// maxScheduleRequeue bounds the cron requeue so clock skew or long sleeps still
// re-evaluate the schedule reasonably soon.
const maxScheduleRequeue = 6 * time.Hour

// repoPhaseMemoryDisabled / repoReasonMemoryDisabled mark a Repository whose
// owning Project has spec.memory.enabled=false. Ingest writes to the memory
// stack and there is no stack, so ingest is NOT APPLICABLE - which is a
// terminal, non-alarming state, distinct both from "gated, waiting for memory"
// and from "ingest failed".
const (
	repoPhaseMemoryDisabled  = "MemoryDisabled"
	repoReasonMemoryDisabled = "MemoryDisabled"
)

// ingestBackoff constants for exponential back-off between failed Job re-creations.
const (
	baseIngestBackoff = 30 * time.Second
	maxIngestBackoff  = 30 * time.Minute

	// incrementalFallbackThreshold is the number of consecutive incremental-ingest
	// failures after which the controller falls back to a full ingest. This
	// self-heals repos whose LastIngestedCommit no longer exists in history (e.g.
	// after a force-push / branch rewrite).
	incrementalFallbackThreshold = 3
)

// memoryStuckAlertFor mirrors the `for:` of tatara-observability's CRITICAL
// "Memory stack stuck not ready" rule, which reads the same evidence
// (operator_memory_stacks{phase=~"Provisioning|Failed|Degraded"}) per project.
const memoryStuckAlertFor = 15 * time.Minute

// ingestGateMaskDelay is how long the project memory stack must have been
// continuously non-Ready before a gated repo's operator_repository_ingest_failing
// gauge is masked (issue #525, see publishIngestHealth).
//
// It is a QUALIFICATION on entering the mask, deliberately not a dwell on
// leaving it. A CHATTERING gate - a memory pod that restarts every few minutes,
// each restart clearing Project.status.memory.readySince and buying a fresh
// MemoryReadyStabilizationWindow - must NOT mask anything: the repo really is
// stuck failing and nothing about a 3m blip changes that. Masking on a blip
// would punch holes in the gauge that keep the 1h "Repository stuck in failing
// ingest state" rule from ever reaching its `for`, which is strictly worse than
// the deadlock this masking exists to fix.
//
// The delay is derived, not chosen: memoryStuckAlertFor is the window the
// per-project critical memory alert needs on the same evidence, plus one
// MemoryReadyStabilizationWindow of margin for scrape and evaluation latency.
// The invariant it buys is that the mask can never engage on a repo whose
// project is not already eligible to page in its own right.
const ingestGateMaskDelay = memoryStuckAlertFor + tataradevv1alpha1.MemoryReadyStabilizationWindow

// ingestBackoff returns the back-off duration for the given consecutive failure
// count: base * 2^(failures-1), capped at maxIngestBackoff.
func ingestBackoff(failures int) time.Duration {
	if failures <= 0 {
		return baseIngestBackoff
	}
	// Cap the shift to avoid int overflow (30 shifts exceeds 30m anyway).
	shift := failures - 1
	if shift > 30 {
		shift = 30
	}
	d := baseIngestBackoff * (1 << uint(shift))
	if d > maxIngestBackoff || d < 0 {
		return maxIngestBackoff
	}
	return d
}

// RepositoryReconciler drives ingest Jobs for Repositories.
type RepositoryReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	Metrics      *obs.OperatorMetrics
	IngestConfig ingest.Config
	// Recorder emits Kubernetes Events on the Repository (e.g. why an ingest
	// failed) so the cause survives the short-lived Job pod's GC. May be nil in
	// tests, in which case Event emission is skipped.
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=tatara.dev,resources=repositories,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tatara.dev,resources=repositories/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=tatara.dev,resources=projects,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile launches and tracks the ingest Job for a Repository per the
// re-ingest trigger contract.
// The body is doReconcile; this wrapper is the #538 shutdown-cancellation
// boundary (see classifyReconcileShutdown).
func (r *RepositoryReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	res, err := r.doReconcile(ctx, req)
	return classifyReconcileShutdown(ctx, "repository", req.Name, res, err)
}

func (r *RepositoryReconciler) doReconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var repo tataradevv1alpha1.Repository
	if err := r.Get(ctx, req.NamespacedName, &repo); err != nil {
		if apierrors.IsNotFound(err) {
			// The Repository is gone: retire its gauges so no series outlives it
			// (issue #457). Now that they carry a project label, a leftover series
			// would keep naming a repo/project pair that no longer exists.
			r.Metrics.ForgetRepository(req.Name)
			return ctrl.Result{}, nil
		}
		r.Metrics.ReconcileResult("Repository", "error")
		return ctrl.Result{}, fmt.Errorf("get repository: %w", err)
	}

	// Repair the desynchronised phase BEFORE publishing health, so the gauge
	// reports the repaired state in the same pass rather than one reconcile late.
	if err := r.repairStaleFailedPhase(ctx, &repo); err != nil {
		r.Metrics.ReconcileResult("Repository", "error")
		return ctrl.Result{}, fmt.Errorf("repair stale Failed phase: %w", err)
	}

	// Read the owning Project here rather than at the gate below: the gate state
	// is an INPUT to publishIngestHealth (issue #525), and the gauges must be
	// published on every reconcile - including the ones that return early at the
	// Job concurrency guard. A lookup failure is not surfaced here but at its
	// original position further down, so error handling and ordering are
	// unchanged; a repo whose Project cannot be read is reported as not gated.
	var project tataradevv1alpha1.Project
	projectErr := r.Get(ctx, types.NamespacedName{Namespace: repo.Namespace, Name: repo.Spec.ProjectRef}, &project)
	memoryGated := projectErr == nil && !tataradevv1alpha1.MemoryStablyReady(&project, time.Now())

	// Publish the live per-repo ingest-health gauges every reconcile so alerting
	// can key on the CURRENT condition (recovery-aware) instead of the monotonic
	// operator_ingest_job_total counter, which kept TataraIngestJobFailing firing
	// for an hour after a self-healed incremental burst (issue #138).
	//
	// The gauges report whether the GATE is what is holding this repo's NEXT
	// ingest, which is why an in-flight Job disqualifies it: the concurrency
	// guard below returns before the gate is ever consulted, so a memory blip
	// mid-ingest would otherwise report a running repo as gated and mask its
	// failing gauge while it is genuinely retrying. This is a REPORTING
	// predicate only - the gate itself keys on memoryGated alone, so a stale
	// status.jobName can never let an ingest past it.
	r.publishIngestHealth(&repo, memoryGated && repo.Status.JobName == "", memoryNotReadyFor(&project, time.Now()))

	// item 7 (FIX-2): keep the printcolumn-backed open issue/incident counts
	// fresh on every reconcile, independent of ingest state/gating - this MUST
	// run before the IngestEnabled early-return below, otherwise a non-ingested
	// repo never gets its counts computed, contradicting this comment.
	if err := r.patchStatus(ctx, &repo, func(fresh *tataradevv1alpha1.Repository) bool {
		return r.computeRepoCounts(ctx, fresh)
	}); err != nil {
		r.Metrics.ReconcileResult("Repository", "error")
		return ctrl.Result{}, fmt.Errorf("compute repo counts: %w", err)
	}

	if !tataradevv1alpha1.BoolVal(repo.Spec.IngestEnabled, true) {
		// This early-return sits BEFORE the memory-disabled short-circuit below, so
		// a repo that has ingest switched off never reaches it. Retire the two
		// ingest series here too, or a memory-disabled project whose repos also
		// have ingestEnabled=false keeps ageing a last-ingest timestamp nothing
		// will ever refresh, straight into TataraIngestStale. For a repo with
		// ingest off the honest value of both gauges is "no series", the same
		// stance publishIngestHealth already takes on the failing gauge.
		r.Metrics.SetRepositoryIngestGated(repo.Spec.ProjectRef, repo.Name, false)
		r.Metrics.ClearRepositoryLastIngestTimestamp(repo.Spec.ProjectRef, repo.Name)
		return ctrl.Result{}, nil
	}

	// Concurrency guard: a named Job that still exists blocks new launches.
	if repo.Status.JobName != "" {
		var job batchv1.Job
		err := r.Get(ctx, types.NamespacedName{Namespace: repo.Namespace, Name: repo.Status.JobName}, &job)
		switch {
		case err == nil && jobActive(&job):
			l.Info("ingest job still active, requeueing",
				"action", "ingest_guard", "resource_id", repo.Name, "job", repo.Status.JobName)
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		case err == nil:
			// terminal job handled by Task 5 result-apply path
			return r.handleFinishedJob(ctx, &repo, &job)
		case apierrors.IsNotFound(err):
			// Job vanished (TTL/manual delete); clear and re-evaluate.
			if err := r.patchStatus(ctx, &repo, func(fresh *tataradevv1alpha1.Repository) bool {
				if fresh.Status.JobName == "" {
					return false
				}
				fresh.Status.JobName = ""
				return true
			}); err != nil {
				r.Metrics.ReconcileResult("Repository", "error")
				return ctrl.Result{}, fmt.Errorf("clear stale jobName: %w", err)
			}
		default:
			r.Metrics.ReconcileResult("Repository", "error")
			return ctrl.Result{}, fmt.Errorf("get ingest job: %w", err)
		}
	}

	// The Project was read at the top of the reconcile (for the gate state the
	// gauges need); this is where a failed read has always aborted.
	if projectErr != nil {
		r.Metrics.ReconcileResult("Repository", "error")
		return ctrl.Result{}, fmt.Errorf("get owning project %q: %w", repo.Spec.ProjectRef, projectErr)
	}

	// Memory DISABLED is not the same as memory not-ready, and must not be
	// handled by the gate below. The gate is an unbounded 15s poll with no
	// terminal state: it is correct for a stack that is going to become ready and
	// catastrophic for one that never will. A disabled project would sit in it
	// forever, holding operator_repository_ingest_gated at 1 (TataraIngestGated
	// fires at 1h) and ageing operator_repository_last_ingest_timestamp_seconds
	// past the staleness budget (TataraIngestStale). Short-circuit cleanly
	// instead: ingest is not applicable here, and there is nothing to retry.
	if tataradevv1alpha1.MemoryDisabled(&project) {
		if err := r.patchStatus(ctx, &repo, func(fresh *tataradevv1alpha1.Repository) bool {
			fresh.Status.Phase = repoPhaseMemoryDisabled
			meta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
				Type:               "MemoryNotReady",
				Status:             metav1.ConditionFalse,
				Reason:             repoReasonMemoryDisabled,
				Message:            "project " + project.Name + " has memory disabled; ingest does not apply",
				ObservedGeneration: fresh.Generation,
			})
			return true
		}); err != nil {
			r.Metrics.ReconcileResult("Repository", "error")
			return ctrl.Result{}, fmt.Errorf("set memory-disabled ingest state: %w", err)
		}
		r.Metrics.SetRepositoryIngestGated(repo.Spec.ProjectRef, repo.Name, false)
		// Retire the staleness series: publishIngestHealth republished it at the
		// top of this pass from the last successful ingest, and nothing will ever
		// refresh it again, so leaving it would age straight into TataraIngestStale.
		r.Metrics.ClearRepositoryLastIngestTimestamp(repo.Spec.ProjectRef, repo.Name)
		l.Info("ingest skipped: project memory is disabled",
			"action", "ingest_memory_disabled", "resource_id", repo.Name, "project", project.Name)
		r.Metrics.ReconcileResult("Repository", "success")
		return ctrl.Result{}, nil
	}

	// THE ONE REMAINING MEMORY GATE. Ingest is the only path that WRITES to the
	// memory stack, so a not-ready backend here means a partial corpus, not just
	// reduced recall. Agent spawn and turn submission are deliberately NOT gated
	// (see v1alpha1.MemoryStablyReady).
	if memoryGated {
		if err := r.patchStatus(ctx, &repo, func(fresh *tataradevv1alpha1.Repository) bool {
			meta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
				Type:               "MemoryNotReady",
				Status:             metav1.ConditionTrue,
				Reason:             "MemoryProvisioning",
				Message:            "waiting for project " + project.Name + " memory stack to become stably Ready",
				ObservedGeneration: fresh.Generation,
			})
			return true
		}); err != nil {
			r.Metrics.ReconcileResult("Repository", "error")
			return ctrl.Result{}, fmt.Errorf("set MemoryNotReady condition: %w", err)
		}
		// Issue #434: the gate is a SILENT hold - nothing fails, nothing retries, and
		// the reconcile records "success" - so operator_repository_ingest_failing
		// reads 0 for a gated repo indefinitely. operator_repository_ingest_gated is
		// the only live signal that the gate is what is holding ingest; it is
		// published (with the masked failing gauge) by publishIngestHealth above.
		l.Info("ingest gated: project memory not stably ready",
			"action", "ingest_gate", "resource_id", repo.Name, "project", project.Name)
		r.Metrics.ReconcileResult("Repository", "success")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// Memory is Ready: clear the provisioning condition if it lingers from an
	// earlier not-ready reconcile. Persist immediately when it flips, so it clears
	// even on reconciles that launch no ingest (already-ingested repos).
	if err := r.patchStatus(ctx, &repo, func(fresh *tataradevv1alpha1.Repository) bool {
		changed := false
		// Drop a phase left behind by a memory-disabled episode so a re-enabled
		// project does not read MemoryDisabled until its next ingest completes.
		if fresh.Status.Phase == repoPhaseMemoryDisabled {
			fresh.Status.Phase = ""
			changed = true
		}
		return meta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               "MemoryNotReady",
			Status:             metav1.ConditionFalse,
			Reason:             "MemoryReady",
			Message:            "project memory stack is Ready",
			ObservedGeneration: fresh.Generation,
		}) || changed
	}); err != nil {
		r.Metrics.ReconcileResult("Repository", "error")
		return ctrl.Result{}, fmt.Errorf("clear MemoryNotReady condition: %w", err)
	}

	since, want := r.ingestDecision(&repo)
	if !want {
		// Finding 5: when there is nothing to ingest and the repo is no longer
		// in a failing state, clear any stale IngestBackoff condition so it does
		// not misreport health.
		if repo.Status.IngestFailureCount == 0 {
			if err := r.patchStatus(ctx, &repo, func(fresh *tataradevv1alpha1.Repository) bool {
				// Re-check on the fresh object: a concurrent failure write may have
				// raised the count, in which case the backoff condition is not stale.
				if fresh.Status.IngestFailureCount != 0 {
					return false
				}
				return meta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
					Type:               "IngestBackoff",
					Status:             metav1.ConditionFalse,
					Reason:             "IngestIdle",
					Message:            "no ingest pending and no recent failures",
					ObservedGeneration: fresh.Generation,
				})
			}); err != nil {
				r.Metrics.ReconcileResult("Repository", "error")
				return ctrl.Result{}, fmt.Errorf("clear stale IngestBackoff condition: %w", err)
			}
		}
		res, err := r.scheduleNextReingest(ctx, &repo)
		if err != nil {
			r.Metrics.ReconcileResult("Repository", "error")
			return ctrl.Result{}, err
		}
		r.Metrics.ReconcileResult("Repository", "success")
		return res, nil
	}

	// Exponential back-off gate: if there have been recent failures and the
	// back-off window has not yet elapsed, hold off and requeue.
	if repo.Status.IngestFailureCount > 0 && repo.Status.LastIngestFailureTime != nil {
		backoff := ingestBackoff(repo.Status.IngestFailureCount)
		retryAt := repo.Status.LastIngestFailureTime.Add(backoff)
		if time.Now().Before(retryAt) {
			remaining := time.Until(retryAt)
			failCount := repo.Status.IngestFailureCount
			if err := r.patchStatus(ctx, &repo, func(fresh *tataradevv1alpha1.Repository) bool {
				meta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
					Type:   "IngestBackoff",
					Status: metav1.ConditionTrue,
					Reason: "IngestFailing",
					Message: fmt.Sprintf("ingest has failed %d time(s); next retry in %s",
						failCount, remaining.Round(time.Second)),
					ObservedGeneration: fresh.Generation,
				})
				return true
			}); err != nil {
				r.Metrics.ReconcileResult("Repository", "error")
				return ctrl.Result{}, fmt.Errorf("set IngestBackoff condition: %w", err)
			}
			l.Info("ingest backoff active",
				"action", "ingest_backoff",
				"resource_id", repo.Name,
				"failure_count", repo.Status.IngestFailureCount,
				"retry_after", retryAt.Format(time.RFC3339))
			r.Metrics.ReconcileResult("Repository", "success")
			return ctrl.Result{RequeueAfter: remaining}, nil
		}
	}

	if err := r.ensureResultConfigMap(ctx, &repo); err != nil {
		r.Metrics.ReconcileResult("Repository", "error")
		return ctrl.Result{}, fmt.Errorf("ensure result configmap: %w", err)
	}

	// The Job name is deterministic per ingest attempt (ingest.JobName), which is
	// what makes this launch idempotent: a second reconcile that decided on the
	// same attempt - because it read the Repository before status.jobName landed
	// in its cache - gets AlreadyExists and ADOPTS that Job below instead of
	// launching a duplicate. Two duplicate ingest Jobs were observed for one repo
	// 16ms and 21ms apart from the same pod; only the last was adopted into
	// status, and the orphan ran to completion with its outcome never reconciled
	// (issue #457). The mirror of the Create/AlreadyExists mint in queue_controller.
	job := ingest.BuildJob(&project, &repo, since, project.Status.Memory.Endpoint, r.IngestConfig)
	if err := r.Create(ctx, job); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			r.Metrics.ReconcileResult("Repository", "error")
			return ctrl.Result{}, fmt.Errorf("create ingest job: %w", err)
		}
		r.Metrics.IngestJobDeduplicated(repo.Spec.ProjectRef, repo.Name)
		l.Info("ingest job already exists, adopting instead of duplicating",
			"action", "ingest_dedup", "resource_id", repo.Name, "job", job.Name)
	}

	if err := r.patchStatus(ctx, &repo, func(fresh *tataradevv1alpha1.Repository) bool {
		// Clear any lingering IngestBackoff condition before recording the launch.
		meta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               "IngestBackoff",
			Status:             metav1.ConditionFalse,
			Reason:             "IngestRetrying",
			Message:            "backoff elapsed, launching ingest job",
			ObservedGeneration: fresh.Generation,
		})
		fresh.Status.JobName = job.Name
		fresh.Status.Phase = "Ingesting"
		meta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               "Ingested",
			Status:             metav1.ConditionFalse,
			Reason:             "IngestStarted",
			Message:            "ingest job " + job.Name + " launched",
			ObservedGeneration: fresh.Generation,
		})
		return true
	}); err != nil {
		r.Metrics.ReconcileResult("Repository", "error")
		return ctrl.Result{}, fmt.Errorf("update repository status: %w", err)
	}

	l.Info("launched ingest job",
		"action", "ingest_start", "resource_id", repo.Name, "job", job.Name,
		"incremental", since != "")
	r.Metrics.ReconcileResult("Repository", "success")
	return ctrl.Result{}, nil
}

// publishIngestHealth exports the live per-repo ingest-health gauges from the
// Repository status. operator_repository_ingest_failing is the current-state,
// recovery-aware signal alerting keys on instead of the monotonic
// operator_ingest_job_total counter; it is 1 while the repo is Failed or has
// unresolved consecutive failures and clears the moment a re-ingest succeeds
// (issue #138). A disabled repo never reports failing. The last-ingest timestamp
// lets PromQL compute staleness as time() - the gauge.
//
// It is the SINGLE writer of both ingest gauges, and a SUSTAINED "gated" wins
// over "failing" (issue #525). The two used to be independent, which deadlocked:
// a repo that was already failing when the memory-readiness gate closed launches
// no new ingest Job, and only a SUCCESSFUL ingest resets Phase/IngestFailureCount,
// so the failing gauge stayed pinned at 1 for as long as the project memory
// stack was down. mtg-decks held "Repository stuck in failing ingest state" for
// 18.6h with zero ingest attempts in 19.6h, and no action on the repo could
// have cleared it. Masking the report is deliberate and the CR state is left
// alone: IngestFailureCount also drives the exponential back-off and the
// incremental-to-full escalation, so resetting it would restart back-off at 30s
// and drop the self-heal the instant a just-recovered memory stack came back.
// While the gate holds, operator_repository_ingest_gated is the accurate signal.
//
// notReadyFor is how long the project memory stack has been continuously
// non-Ready, and the mask engages only past ingestGateMaskDelay. A gate that has
// only just closed does NOT mask: see that constant for why a chattering gate
// must keep reporting the truth. The mask is not latched in either direction -
// it is recomputed every reconcile, so the failing gauge un-masks on the first
// pass after the gate opens, with no successful ingest in between.
//
// The caller decides what "gated" means - see Reconcile: only a repo with no Job
// in flight counts, or the mask would swallow a repo that is actively retrying.
func (r *RepositoryReconciler) publishIngestHealth(repo *tataradevv1alpha1.Repository, gated bool, notReadyFor time.Duration) {
	enabled := tataradevv1alpha1.BoolVal(repo.Spec.IngestEnabled, true)
	gated = enabled && gated
	r.Metrics.SetRepositoryIngestGated(repo.Spec.ProjectRef, repo.Name, gated)
	masked := gated && notReadyFor >= ingestGateMaskDelay
	failing := enabled && !masked && (repo.Status.Phase == "Failed" || repo.Status.IngestFailureCount > 0)
	r.Metrics.SetRepositoryIngestFailing(repo.Spec.ProjectRef, repo.Name, failing)
	if repo.Status.LastIngestTime != nil {
		r.Metrics.SetRepositoryLastIngestTimestamp(repo.Spec.ProjectRef, repo.Name,
			float64(repo.Status.LastIngestTime.Unix()))
	}
}

// memoryNotReadyFor reports how long p's memory stack has been continuously in a
// non-Ready phase, and 0 when it is Ready or when no clock has been recorded.
//
// The clock is Project.status.memory.provisioningSince deliberately, not the
// Repository's own MemoryNotReady condition: several Repository reconcile paths
// (ingestEnabled toggled off, an unreadable Project, a failed count patch)
// return before the condition is cleared, so that condition can stay True across
// a recovery and hand a LATER gate close an hours-old transition time. This
// field is maintained unconditionally by the Project reconciler, is cleared the
// moment the stack reaches Ready, and is the exact quantity the per-project
// operator_memory_stacks{phase=~"Provisioning|Failed|Degraded"} alert measures -
// which is what makes ingestGateMaskDelay comparable to that alert's `for`.
//
// 0 for a Ready stack is not just a guard: publishIngestHealth only consults
// this while the gate is closed, and a stack that is Ready-but-inside the
// stabilization window is a healthy stack, not an outage.
func memoryNotReadyFor(p *tataradevv1alpha1.Project, now time.Time) time.Duration {
	if p == nil || p.Status.Memory == nil || p.Status.Memory.Phase == "Ready" {
		return 0
	}
	if p.Status.Memory.ProvisioningSince == nil {
		return 0
	}
	return now.Sub(p.Status.Memory.ProvisioningSince.Time)
}

// repairStaleFailedPhase enforces the invariant that Status.Phase=="Failed"
// implies an unresolved ingest failure (IngestFailureCount>0).
//
// (Phase="Failed", IngestFailureCount=0) was reachable and NOTHING repaired it
// (issue #457): Status.Phase is written only on Job completion, so once the
// failure counter was back at 0 while the phase still read Failed, the pair
// could not resynchronise. publishIngestHealth derives
// operator_repository_ingest_failing from (Phase=="Failed" || count>0), so the
// gauge latched at 1 - one repo held the alert for 20.8h with zero ingest
// failures in 7 days of logs, and escaped only on an unrelated cron tick.
//
// The repair also carries the re-ingest trigger below: because Failed now
// implies count>0, making Phase=="Failed" a reason to re-ingest can never
// bypass the exponential back-off gate, which keys on exactly that counter.
//
// The repaired phase is derived from what the status actually records: a repo
// with a LastIngestedCommit has a completed ingest, so it is Ingested; a repo
// that never ingested has no phase to claim and is cleared, which lets the
// ordinary first-full-ingest path take over.
func (r *RepositoryReconciler) repairStaleFailedPhase(ctx context.Context, repo *tataradevv1alpha1.Repository) error {
	if repo.Status.Phase != "Failed" || repo.Status.IngestFailureCount != 0 {
		return nil
	}
	repaired := false
	if err := r.patchStatus(ctx, repo, func(fresh *tataradevv1alpha1.Repository) bool {
		// Re-check on the fresh object (and on every conflict retry): a concurrent
		// failure write may have raised the count, in which case Failed is honest.
		repaired = false
		if fresh.Status.Phase != "Failed" || fresh.Status.IngestFailureCount != 0 {
			return false
		}
		fresh.Status.Phase = ""
		if fresh.Status.LastIngestedCommit != "" {
			fresh.Status.Phase = "Ingested"
		}
		repaired = true
		return true
	}); err != nil {
		return err
	}
	if !repaired {
		return nil
	}
	r.Metrics.RepositoryPhaseRepaired(repo.Spec.ProjectRef, repo.Name)
	log.FromContext(ctx).Info("repaired desynchronised ingest phase",
		"action", "ingest_phase_repair", "resource_id", repo.Name,
		"project", repo.Spec.ProjectRef, "phase", repo.Status.Phase)
	return nil
}

// ingestDecision returns (sinceSHA, wantIngest). Full ingest (empty since)
// when lastIngestedCommit is empty. Incremental (since=lastIngestedCommit)
// when the reingest-requested annotation is newer than lastIngestTime, or when
// the repo is left in a Failed phase.
// Finding 4: when IngestFailureCount has reached incrementalFallbackThreshold,
// the since SHA is cleared so the Job performs a full ingest; this self-heals
// repos whose LastIngestedCommit was removed from history (force-push/rewrite).
//
// Phase=="Failed" is a first-class trigger (issue #457). Before that, an
// already-ingested repo wanted an ingest only while a newer re-ingest
// annotation was pending, so a Failed repo that lost its annotation - the
// Repository CR is helm-managed and an apply strips the annotation - waited for
// the next ReingestSchedule cron tick while its recall corpus went stale. It
// cannot hot-loop: repairStaleFailedPhase guarantees Phase=="Failed" implies
// IngestFailureCount>0, which is exactly what the exponential back-off gate in
// Reconcile keys on, so a repeatedly failing repo backs off to maxIngestBackoff
// rather than launching a Job per reconcile.
func (r *RepositoryReconciler) ingestDecision(repo *tataradevv1alpha1.Repository) (string, bool) {
	if repo.Status.LastIngestedCommit == "" {
		return "", true
	}
	if !reingestRequested(repo) && repo.Status.Phase != "Failed" {
		return "", false
	}
	// Fall back to a full ingest after repeated incremental failures so a
	// force-pushed branch (where the since-SHA no longer exists in history)
	// can self-heal rather than looping forever.
	if repo.Status.IngestFailureCount >= incrementalFallbackThreshold {
		return "", true
	}
	return repo.Status.LastIngestedCommit, true
}

// reingestRequested reports whether the reingest-requested annotation carries a
// parseable timestamp newer than the last successful ingest.
func reingestRequested(repo *tataradevv1alpha1.Repository) bool {
	raw := repo.Annotations[ReingestAnnotation]
	if raw == "" {
		return false
	}
	requested, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return false
	}
	// LastIngestTime is *metav1.Time; treat nil as zero time (always older).
	var lastIngestTime time.Time
	if repo.Status.LastIngestTime != nil {
		lastIngestTime = repo.Status.LastIngestTime.Time
	}
	return requested.After(lastIngestTime)
}

// scheduleNextReingest applies the per-Repository cron schedule for an
// already-ingested repo. It parses spec.reingestSchedule and computes the next
// fire from base = lastScheduledReingest | lastIngestTime | creationTimestamp.
// When the fire is due (and strictly after lastIngestTime, so an in-flight
// ingest from another trigger is not double-stamped), it stamps the existing
// reingest-requested annotation and records lastScheduledReingest; the
// annotation change re-triggers reconcile, which launches the Job via the
// existing path. Otherwise it requeues at the next fire (clamped). A bad cron
// expression is logged at ERROR and skipped (no requeue, no error).
func (r *RepositoryReconciler) scheduleNextReingest(ctx context.Context, repo *tataradevv1alpha1.Repository) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	// Only schedule once a repo has been ingested at least once; a first full
	// ingest is driven by ingestDecision, not the cron.
	// Only schedule once a repo has been ingested at least once. ReingestSchedule
	// is a Required+MinLength field so the empty-string branch is unreachable for
	// any object that passed admission; the parse below handles malformed values.
	if repo.Status.LastIngestedCommit == "" {
		return ctrl.Result{}, nil
	}

	schedule, err := cron.ParseStandard(repo.Spec.ReingestSchedule)
	if err != nil {
		l.Error(err, "invalid reingestSchedule, skipping cron",
			"action", "ingest_schedule_invalid", "resource_id", repo.Name,
			"schedule", repo.Spec.ReingestSchedule)
		return ctrl.Result{}, nil
	}

	var lastIngestTime time.Time
	if repo.Status.LastIngestTime != nil {
		lastIngestTime = repo.Status.LastIngestTime.Time
	}

	base := repo.CreationTimestamp.Time
	if repo.Status.LastIngestTime != nil {
		base = repo.Status.LastIngestTime.Time
	}
	if repo.Status.LastScheduledReingest != nil {
		base = repo.Status.LastScheduledReingest.Time
	}

	now := time.Now()
	next := schedule.Next(base)

	if now.Before(next) {
		requeue := next.Sub(now)
		if requeue > maxScheduleRequeue {
			requeue = maxScheduleRequeue
		}
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	// Due. Guard against firing while an ingest from another trigger is still
	// in flight or just finished: only stamp when now is strictly after the
	// last successful ingest.
	if !now.After(lastIngestTime) {
		return ctrl.Result{RequeueAfter: maxScheduleRequeue}, nil
	}

	// Stamp the annotation trigger first. LastScheduledReingest advances only
	// after the annotation write succeeds so a failed trigger never advances
	// the dedup base (which would cause the due-but-unstamped fire to be skipped
	// entirely on the next reconcile). Wrapped in RetryOnConflict to match the
	// hardening already applied to the LastScheduledReingest status write below.
	stamp := now.UTC().Format(time.RFC3339)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tataradevv1alpha1.Repository{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(repo), fresh); err != nil {
			return err
		}
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations[ReingestAnnotation] = stamp
		if err := r.Update(ctx, fresh); err != nil {
			return err
		}
		// Propagate annotation update back to caller so the guard below works.
		*repo = *fresh
		return nil
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("stamp scheduled reingest annotation: %w", err)
	}

	// Persist the dedup guard only after the trigger is safely written.
	scheduled := metav1.NewTime(now)
	if err := r.patchStatus(ctx, repo, func(fresh *tataradevv1alpha1.Repository) bool {
		fresh.Status.LastScheduledReingest = &scheduled
		return true
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("update lastScheduledReingest: %w", err)
	}

	l.Info("scheduled re-ingest requested",
		"action", "ingest_schedule_fire", "resource_id", repo.Name,
		"schedule", repo.Spec.ReingestSchedule)
	return ctrl.Result{}, nil
}

// ensureResultConfigMap creates (or resets) the <repo>-ingest-result ConfigMap
// (owner-ref Repository) so the Job can patch it and the reconciler can read
// it back. data["sha"] is always reset to "" before each launch so a stale
// value from a prior ingest does not slip through the cache race window where
// the Job-Complete watch fires before the CM-patch watch.
func (r *RepositoryReconciler) ensureResultConfigMap(ctx context.Context, repo *tataradevv1alpha1.Repository) error {
	cm := &corev1.ConfigMap{}
	key := types.NamespacedName{Namespace: repo.Namespace, Name: ingest.ResultConfigMapName(repo)}
	if err := r.Get(ctx, key, cm); err == nil {
		// CM already exists: reset sha so readResultSHA rejects a stale value.
		if cm.Data["sha"] != "" {
			cm.Data["sha"] = ""
			if updateErr := r.Update(ctx, cm); updateErr != nil {
				return fmt.Errorf("reset result configmap sha: %w", updateErr)
			}
		}
		return nil
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get result configmap: %w", err)
	}
	cm = &corev1.ConfigMap{}
	cm.Name = ingest.ResultConfigMapName(repo)
	cm.Namespace = repo.Namespace
	cm.Data = map[string]string{"sha": ""}
	if err := controllerutil.SetControllerReference(repo, cm, r.Scheme); err != nil {
		return fmt.Errorf("set ownerref on result configmap: %w", err)
	}
	if err := r.Create(ctx, cm); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create result configmap: %w", err)
	}
	return nil
}

// patchStatus applies mutate to a freshly fetched copy of repo and writes the
// status subresource, retrying on conflict. mutate reports whether it changed
// anything; when it returns false the write is skipped. On success the persisted
// object is copied back into repo so later reads in the same Reconcile observe
// the applied change. This is the conflict-safe analogue of a bare
// r.Status().Update on the object fetched at the top of Reconcile: Repository
// status is also written by handleFinishedJob, the REST API handlers, and the
// webhook server, any of which can advance the resourceVersion between that Get
// and the write (the source of the IngestBackoff 409-conflict reconcile-error
// storm). It matches the retry.RetryOnConflict + fresh-Get convention used by the
// rest of this file.
func (r *RepositoryReconciler) patchStatus(ctx context.Context, repo *tataradevv1alpha1.Repository, mutate func(*tataradevv1alpha1.Repository) bool) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tataradevv1alpha1.Repository{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(repo), fresh); err != nil {
			return err
		}
		if !mutate(fresh) {
			*repo = *fresh
			return nil
		}
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*repo = *fresh
		return nil
	})
}

// computeRepoCounts fills OpenIssuesCount/OpenIncidentsCount on repo.Status
// from a namespace-scoped Task List + Go filter (item 7), scoped to this repo
// via Spec.RepositoryRef. Returns whether status changed, matching
// patchStatus's mutate signature.
func (r *RepositoryReconciler) computeRepoCounts(ctx context.Context, repo *tataradevv1alpha1.Repository) bool {
	var tasks tataradevv1alpha1.TaskList
	if err := r.List(ctx, &tasks, client.InNamespace(repo.Namespace)); err != nil {
		return false
	}
	issues, incidents := 0, 0
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if t.Spec.RepositoryRef != repo.Name || tataradevv1alpha1.TaskDone(t) {
			continue
		}
		switch t.Spec.Kind {
		case "incident":
			incidents++
		case SweepIssueKind:
			issues++
		}
	}
	if repo.Status.OpenIssuesCount == issues && repo.Status.OpenIncidentsCount == incidents {
		return false
	}
	repo.Status.OpenIssuesCount = issues
	repo.Status.OpenIncidentsCount = incidents
	return true
}

// handleFinishedJob applies a terminal ingest Job's outcome to the Repository
// status: on success it reads the resolved HEAD SHA from the result ConfigMap
// and records lastIngestedCommit/lastIngestTime/phase=Ingested; on failure it
// records phase=Failed. It always clears status.jobName and observes the Job
// duration.
func (r *RepositoryReconciler) handleFinishedJob(ctx context.Context, repo *tataradevv1alpha1.Repository, job *batchv1.Job) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	// Attribute the ingest-job metric by mode so alerting pages only on terminal
	// full-ingest failures. A Job created before this label existed (in-flight at
	// rollout) reads as full: an unclassifiable ingest failure is treated as the
	// alerting case rather than silently dropped.
	mode := job.Labels[ingest.LabelIngestMode]
	if mode == "" {
		mode = ingest.IngestModeFull
	}

	// Record duration for all finished jobs (success and failure). Failed jobs
	// do not have CompletionTime set by Kubernetes; prefer the LastTransitionTime
	// of the JobFailed condition (set by K8s when it marks the job failed) to
	// avoid inflating the histogram with reconcile-observation lag. Fall back to
	// time.Now() only when that condition timestamp is also absent.
	if job.Status.StartTime != nil {
		end := job.Status.CompletionTime
		if end == nil {
			// Try the JobFailed condition timestamp first.
			for i := range job.Status.Conditions {
				c := &job.Status.Conditions[i]
				if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue && !c.LastTransitionTime.IsZero() {
					end = &c.LastTransitionTime
					break
				}
			}
			if end == nil {
				now := metav1.Now()
				end = &now
			}
		}
		r.Metrics.ObserveIngestJobDuration(end.Sub(job.Status.StartTime.Time).Seconds())
	}

	if jobSucceeded(job) {
		r.Metrics.IngestJobResult("success", mode)
		sha, err := r.readResultSHA(ctx, repo)
		if err != nil {
			r.Metrics.ReconcileResult("Repository", "error")
			return ctrl.Result{}, fmt.Errorf("read ingest result sha: %w", err)
		}
		ingestTime := metav1.Now()
		if err := r.patchStatus(ctx, repo, func(fresh *tataradevv1alpha1.Repository) bool {
			fresh.Status.LastIngestedCommit = sha
			fresh.Status.LastIngestTime = &ingestTime
			fresh.Status.Phase = "Ingested"
			fresh.Status.JobName = ""
			fresh.Status.IngestFailureCount = 0
			fresh.Status.LastIngestFailureTime = nil
			meta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
				Type:               "Ingested",
				Status:             metav1.ConditionTrue,
				Reason:             "IngestSucceeded",
				Message:            "ingested at " + sha,
				ObservedGeneration: fresh.Generation,
			})
			return true
		}); err != nil {
			r.Metrics.ReconcileResult("Repository", "error")
			return ctrl.Result{}, fmt.Errorf("update repository status: %w", err)
		}
		// Consume the reingest-requested annotation so the trigger is
		// self-extinguishing instead of relying on timestamp ordering to suppress
		// re-fires. Done after the status write so the status always reflects the
		// completed ingest even if the metadata patch is retried.
		if _, ok := repo.Annotations[ReingestAnnotation]; ok {
			delete(repo.Annotations, ReingestAnnotation)
			if err := r.Update(ctx, repo); err != nil {
				// Non-fatal: the timestamp ordering in ingestDecision still prevents
				// a spurious re-trigger; log and continue.
				l.Error(err, "clear reingest annotation after success",
					"action", "ingest_annotation_clear", "resource_id", repo.Name)
			}
		}
		l.Info("ingest succeeded",
			"action", "ingest_succeeded", "resource_id", repo.Name, "sha", sha, "job", job.Name)
		r.Metrics.ReconcileResult("Repository", "success")
		return ctrl.Result{}, nil
	}

	r.Metrics.IngestJobResult("failure", mode)
	failTime := metav1.NewTime(time.Now())
	// Capture WHY the ingest failed from the failed pod's terminated-container
	// state (the FallbackToLogsOnError termination message) before it is GC'd, so
	// the cause lands in the status condition, the log line, and an Event rather
	// than only "Job failed".
	reason := r.failedPodReason(ctx, job)
	condMsg := "ingest job " + job.Name + " failed"
	if reason != "" {
		condMsg += ": " + reason
	}
	var newFailureCount int
	if err := r.patchStatus(ctx, repo, func(fresh *tataradevv1alpha1.Repository) bool {
		fresh.Status.Phase = "Failed"
		fresh.Status.JobName = ""
		fresh.Status.IngestFailureCount++
		fresh.Status.LastIngestFailureTime = &failTime
		meta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               "Ingested",
			Status:             metav1.ConditionFalse,
			Reason:             "IngestFailed",
			Message:            condMsg,
			ObservedGeneration: fresh.Generation,
		})
		newFailureCount = fresh.Status.IngestFailureCount
		return true
	}); err != nil {
		r.Metrics.ReconcileResult("Repository", "error")
		return ctrl.Result{}, fmt.Errorf("update repository status: %w", err)
	}
	l.Info("ingest failed",
		"action", "ingest_failed", "resource_id", repo.Name, "job", job.Name,
		"failure_count", newFailureCount, "reason", reason)
	if r.Recorder != nil {
		r.Recorder.Eventf(repo, nil, corev1.EventTypeWarning, "IngestFailed", "Ingest", "%s", condMsg)
	}
	r.Metrics.ReconcileResult("Repository", "error")
	return ctrl.Result{RequeueAfter: ingestBackoff(newFailureCount)}, nil
}

// failedPodReason returns a short, human-readable reason for why the most recent
// pod of a failed ingest Job terminated: the Kubernetes termination reason, the
// exit code, and the captured termination message (the tail of the container log,
// surfaced because the ingest containers run with
// TerminationMessagePolicy=FallbackToLogsOnError). It scans the init container
// (clone) first, then the ingest container, returning the first non-zero exit.
// Returns "" when no failed pod or terminated container is found - the short-lived
// Job pods are GC'd with the Job (TTL 600s), after which the in-pod cause is no
// longer observable and the caller falls back to a generic message.
func (r *RepositoryReconciler) failedPodReason(ctx context.Context, job *batchv1.Job) string {
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{"batch.kubernetes.io/job-name": job.Name}); err != nil {
		return ""
	}
	var pod *corev1.Pod
	for i := range pods.Items {
		if p := &pods.Items[i]; pod == nil || podStartedAfter(p, pod) {
			pod = p
		}
	}
	if pod == nil {
		return ""
	}
	statuses := append(append([]corev1.ContainerStatus{}, pod.Status.InitContainerStatuses...), pod.Status.ContainerStatuses...)
	for i := range statuses {
		t := statuses[i].State.Terminated
		if t == nil || t.ExitCode == 0 {
			continue
		}
		return formatTermination(t)
	}
	return ""
}

// podStartedAfter reports whether pod a started later than pod b, so the most
// recent attempt of a multi-pod Job (full ingest, BackoffLimit=2) is chosen.
func podStartedAfter(a, b *corev1.Pod) bool {
	if a.Status.StartTime == nil {
		return false
	}
	if b.Status.StartTime == nil {
		return true
	}
	return a.Status.StartTime.After(b.Status.StartTime.Time)
}

// formatTermination renders a terminated container state into a single line,
// truncating the captured log tail so it stays bounded in the status condition
// and Event.
func formatTermination(t *corev1.ContainerStateTerminated) string {
	const maxMsg = 512
	msg := strings.TrimSpace(t.Message)
	if len(msg) > maxMsg {
		msg = msg[:maxMsg] + "..."
	}
	switch {
	case t.Reason != "" && msg != "":
		return fmt.Sprintf("%s (exit %d): %s", t.Reason, t.ExitCode, msg)
	case msg != "":
		return fmt.Sprintf("exit %d: %s", t.ExitCode, msg)
	case t.Reason != "":
		return fmt.Sprintf("%s (exit %d)", t.Reason, t.ExitCode)
	default:
		return fmt.Sprintf("exit %d", t.ExitCode)
	}
}

// jobSucceeded reports whether the Job has a Complete=True condition.
func jobSucceeded(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// readResultSHA reads data["sha"] from the repo's result ConfigMap.
func (r *RepositoryReconciler) readResultSHA(ctx context.Context, repo *tataradevv1alpha1.Repository) (string, error) {
	var cm corev1.ConfigMap
	key := types.NamespacedName{Namespace: repo.Namespace, Name: ingest.ResultConfigMapName(repo)}
	if err := r.Get(ctx, key, &cm); err != nil {
		return "", fmt.Errorf("get result configmap: %w", err)
	}
	sha := cm.Data["sha"]
	if sha == "" {
		return "", fmt.Errorf("result configmap %s has empty sha", cm.Name)
	}
	return sha, nil
}

// jobActive reports whether a Job has neither completed nor failed.
func jobActive(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return false
		}
	}
	return true
}

// SetupWithManager registers the reconciler, watching Repositories and the
// Jobs they own.
func (r *RepositoryReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("repository-controller")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&tataradevv1alpha1.Repository{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.ConfigMap{}).
		// MaxConcurrentReconciles: 1 serialises Repository reconciles to avoid
		// races in read-then-write sequences; the admission queue seq accounting
		// assumes a single active reconcile per controller kind.
		WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}
