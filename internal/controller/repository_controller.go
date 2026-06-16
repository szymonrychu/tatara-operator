package controller

import (
	"context"
	"fmt"
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

// backlogRequeue is the short requeue used while a scan still has open items with
// no Task (lanes full), so freed lanes refill without waiting for the next cron fire.
const backlogRequeue = 60 * time.Second

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
}

// +kubebuilder:rbac:groups=tatara.dev,resources=repositories,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tatara.dev,resources=repositories/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=tatara.dev,resources=projects,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch

// Reconcile launches and tracks the ingest Job for a Repository per the
// re-ingest trigger contract.
func (r *RepositoryReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var repo tataradevv1alpha1.Repository
	if err := r.Get(ctx, req.NamespacedName, &repo); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		r.Metrics.ReconcileResult("Repository", "error")
		return ctrl.Result{}, fmt.Errorf("get repository: %w", err)
	}

	if !tataradevv1alpha1.BoolVal(repo.Spec.IngestEnabled, true) {
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
			repo.Status.JobName = ""
			if err := r.Status().Update(ctx, &repo); err != nil {
				r.Metrics.ReconcileResult("Repository", "error")
				return ctrl.Result{}, fmt.Errorf("clear stale jobName: %w", err)
			}
		default:
			r.Metrics.ReconcileResult("Repository", "error")
			return ctrl.Result{}, fmt.Errorf("get ingest job: %w", err)
		}
	}

	var project tataradevv1alpha1.Project
	if err := r.Get(ctx, types.NamespacedName{Namespace: repo.Namespace, Name: repo.Spec.ProjectRef}, &project); err != nil {
		r.Metrics.ReconcileResult("Repository", "error")
		return ctrl.Result{}, fmt.Errorf("get owning project %q: %w", repo.Spec.ProjectRef, err)
	}

	if project.Status.Memory == nil || project.Status.Memory.Phase != "Ready" {
		meta.SetStatusCondition(&repo.Status.Conditions, metav1.Condition{
			Type:               "MemoryNotReady",
			Status:             metav1.ConditionTrue,
			Reason:             "MemoryProvisioning",
			Message:            "waiting for project " + project.Name + " memory stack to become Ready",
			ObservedGeneration: repo.Generation,
		})
		if err := r.Status().Update(ctx, &repo); err != nil {
			r.Metrics.ReconcileResult("Repository", "error")
			return ctrl.Result{}, fmt.Errorf("set MemoryNotReady condition: %w", err)
		}
		l.Info("ingest gated: project memory not ready",
			"action", "ingest_gate", "resource_id", repo.Name, "project", project.Name)
		r.Metrics.ReconcileResult("Repository", "success")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// Memory is Ready: clear the provisioning condition if it lingers from an
	// earlier not-ready reconcile. Persist immediately when it flips, so it clears
	// even on reconciles that launch no ingest (already-ingested repos).
	if meta.SetStatusCondition(&repo.Status.Conditions, metav1.Condition{
		Type:               "MemoryNotReady",
		Status:             metav1.ConditionFalse,
		Reason:             "MemoryReady",
		Message:            "project memory stack is Ready",
		ObservedGeneration: repo.Generation,
	}) {
		if err := r.Status().Update(ctx, &repo); err != nil {
			r.Metrics.ReconcileResult("Repository", "error")
			return ctrl.Result{}, fmt.Errorf("clear MemoryNotReady condition: %w", err)
		}
	}

	since, want := r.ingestDecision(&repo)
	if !want {
		// Finding 5: when there is nothing to ingest and the repo is no longer
		// in a failing state, clear any stale IngestBackoff condition so it does
		// not misreport health.
		if repo.Status.IngestFailureCount == 0 {
			if meta.SetStatusCondition(&repo.Status.Conditions, metav1.Condition{
				Type:               "IngestBackoff",
				Status:             metav1.ConditionFalse,
				Reason:             "IngestIdle",
				Message:            "no ingest pending and no recent failures",
				ObservedGeneration: repo.Generation,
			}) {
				if err := r.Status().Update(ctx, &repo); err != nil {
					r.Metrics.ReconcileResult("Repository", "error")
					return ctrl.Result{}, fmt.Errorf("clear stale IngestBackoff condition: %w", err)
				}
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
			meta.SetStatusCondition(&repo.Status.Conditions, metav1.Condition{
				Type:   "IngestBackoff",
				Status: metav1.ConditionTrue,
				Reason: "IngestFailing",
				Message: fmt.Sprintf("ingest has failed %d time(s); next retry in %s",
					repo.Status.IngestFailureCount, remaining.Round(time.Second)),
				ObservedGeneration: repo.Generation,
			})
			if err := r.Status().Update(ctx, &repo); err != nil {
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

	// Clear any lingering IngestBackoff condition before launching.
	meta.SetStatusCondition(&repo.Status.Conditions, metav1.Condition{
		Type:               "IngestBackoff",
		Status:             metav1.ConditionFalse,
		Reason:             "IngestRetrying",
		Message:            "backoff elapsed, launching ingest job",
		ObservedGeneration: repo.Generation,
	})

	if err := r.ensureResultConfigMap(ctx, &repo); err != nil {
		r.Metrics.ReconcileResult("Repository", "error")
		return ctrl.Result{}, fmt.Errorf("ensure result configmap: %w", err)
	}

	job := ingest.BuildJob(&project, &repo, since, project.Status.Memory.Endpoint, r.IngestConfig)
	if err := r.Create(ctx, job); err != nil {
		r.Metrics.ReconcileResult("Repository", "error")
		return ctrl.Result{}, fmt.Errorf("create ingest job: %w", err)
	}

	repo.Status.JobName = job.Name
	repo.Status.Phase = "Ingesting"
	meta.SetStatusCondition(&repo.Status.Conditions, metav1.Condition{
		Type:               "Ingested",
		Status:             metav1.ConditionFalse,
		Reason:             "IngestStarted",
		Message:            "ingest job " + job.Name + " launched",
		ObservedGeneration: repo.Generation,
	})
	if err := r.Status().Update(ctx, &repo); err != nil {
		r.Metrics.ReconcileResult("Repository", "error")
		return ctrl.Result{}, fmt.Errorf("update repository status: %w", err)
	}

	l.Info("launched ingest job",
		"action", "ingest_start", "resource_id", repo.Name, "job", job.Name,
		"incremental", since != "")
	r.Metrics.ReconcileResult("Repository", "success")
	return ctrl.Result{}, nil
}

// ingestDecision returns (sinceSHA, wantIngest). Full ingest (empty since)
// when lastIngestedCommit is empty. Incremental (since=lastIngestedCommit)
// when the reingest-requested annotation is newer than lastIngestTime.
// Finding 4: when IngestFailureCount has reached incrementalFallbackThreshold,
// the since SHA is cleared so the Job performs a full ingest; this self-heals
// repos whose LastIngestedCommit was removed from history (force-push/rewrite).
func (r *RepositoryReconciler) ingestDecision(repo *tataradevv1alpha1.Repository) (string, bool) {
	if repo.Status.LastIngestedCommit == "" {
		return "", true
	}
	raw := repo.Annotations[ReingestAnnotation]
	if raw == "" {
		return "", false
	}
	requested, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return "", false
	}
	// LastIngestTime is *metav1.Time; treat nil as zero time (always older).
	var lastIngestTime time.Time
	if repo.Status.LastIngestTime != nil {
		lastIngestTime = repo.Status.LastIngestTime.Time
	}
	if requested.After(lastIngestTime) {
		// Fall back to a full ingest after repeated incremental failures so a
		// force-pushed branch (where the since-SHA no longer exists in history)
		// can self-heal rather than looping forever.
		if repo.Status.IngestFailureCount >= incrementalFallbackThreshold {
			return "", true
		}
		return repo.Status.LastIngestedCommit, true
	}
	return "", false
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
	repo.Status.LastScheduledReingest = &scheduled
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tataradevv1alpha1.Repository{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(repo), fresh); err != nil {
			return err
		}
		fresh.Status.LastScheduledReingest = &scheduled
		return r.Status().Update(ctx, fresh)
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

// handleFinishedJob applies a terminal ingest Job's outcome to the Repository
// status: on success it reads the resolved HEAD SHA from the result ConfigMap
// and records lastIngestedCommit/lastIngestTime/phase=Ingested; on failure it
// records phase=Failed. It always clears status.jobName and observes the Job
// duration.
func (r *RepositoryReconciler) handleFinishedJob(ctx context.Context, repo *tataradevv1alpha1.Repository, job *batchv1.Job) (ctrl.Result, error) {
	l := log.FromContext(ctx)

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
		r.Metrics.IngestJobResult("success")
		sha, err := r.readResultSHA(ctx, repo)
		if err != nil {
			r.Metrics.ReconcileResult("Repository", "error")
			return ctrl.Result{}, fmt.Errorf("read ingest result sha: %w", err)
		}
		ingestTime := metav1.Now()
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh := &tataradevv1alpha1.Repository{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(repo), fresh); err != nil {
				return err
			}
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
			if updateErr := r.Status().Update(ctx, fresh); updateErr != nil {
				return updateErr
			}
			// Refresh repo for the annotation clear below.
			*repo = *fresh
			return nil
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

	r.Metrics.IngestJobResult("failure")
	failTime := metav1.NewTime(time.Now())
	var newFailureCount int
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tataradevv1alpha1.Repository{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(repo), fresh); err != nil {
			return err
		}
		fresh.Status.Phase = "Failed"
		fresh.Status.JobName = ""
		fresh.Status.IngestFailureCount++
		fresh.Status.LastIngestFailureTime = &failTime
		meta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               "Ingested",
			Status:             metav1.ConditionFalse,
			Reason:             "IngestFailed",
			Message:            "ingest job " + job.Name + " failed",
			ObservedGeneration: fresh.Generation,
		})
		if updateErr := r.Status().Update(ctx, fresh); updateErr != nil {
			return updateErr
		}
		newFailureCount = fresh.Status.IngestFailureCount
		return nil
	}); err != nil {
		r.Metrics.ReconcileResult("Repository", "error")
		return ctrl.Result{}, fmt.Errorf("update repository status: %w", err)
	}
	l.Info("ingest failed",
		"action", "ingest_failed", "resource_id", repo.Name, "job", job.Name,
		"failure_count", newFailureCount)
	r.Metrics.ReconcileResult("Repository", "error")
	return ctrl.Result{RequeueAfter: ingestBackoff(newFailureCount)}, nil
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
	return ctrl.NewControllerManagedBy(mgr).
		For(&tataradevv1alpha1.Repository{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.ConfigMap{}).
		// MaxConcurrentReconciles: 1 is explicit here because laneOccupancy gating
		// assumes serialised reconciles per kind; raising this without revisiting
		// that invariant would cause correctness bugs.
		WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}
