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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ReingestAnnotation aliases the canonical constant from api/v1alpha1 so
// controller code keeps using the same short name internally.
const ReingestAnnotation = tataradevv1alpha1.ReingestRequestedAnnotation

// maxScheduleRequeue bounds the cron requeue so clock skew or long sleeps still
// re-evaluate the schedule reasonably soon.
const maxScheduleRequeue = 6 * time.Hour

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

	if !repo.Spec.IngestEnabled {
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
		res, err := r.scheduleNextReingest(ctx, &repo)
		if err != nil {
			r.Metrics.ReconcileResult("Repository", "error")
			return ctrl.Result{}, err
		}
		r.Metrics.ReconcileResult("Repository", "success")
		return res, nil
	}

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
	if repo.Status.LastIngestedCommit == "" || repo.Spec.ReingestSchedule == "" {
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

	if repo.Annotations == nil {
		repo.Annotations = map[string]string{}
	}
	repo.Annotations[ReingestAnnotation] = now.UTC().Format(time.RFC3339)
	if err := r.Update(ctx, repo); err != nil {
		return ctrl.Result{}, fmt.Errorf("stamp scheduled reingest annotation: %w", err)
	}

	scheduled := metav1.NewTime(now)
	repo.Status.LastScheduledReingest = &scheduled
	if err := r.Status().Update(ctx, repo); err != nil {
		return ctrl.Result{}, fmt.Errorf("update lastScheduledReingest: %w", err)
	}

	l.Info("scheduled re-ingest requested",
		"action", "ingest_schedule_fire", "resource_id", repo.Name,
		"schedule", repo.Spec.ReingestSchedule)
	return ctrl.Result{}, nil
}

// ensureResultConfigMap creates the empty <repo>-ingest-result ConfigMap
// (owner-ref Repository) if absent so the Job can patch it and the reconciler
// can read it back.
func (r *RepositoryReconciler) ensureResultConfigMap(ctx context.Context, repo *tataradevv1alpha1.Repository) error {
	cm := &corev1.ConfigMap{}
	cm.Name = ingest.ResultConfigMapName(repo)
	cm.Namespace = repo.Namespace
	if err := r.Get(ctx, types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}, cm); err == nil {
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

	if job.Status.StartTime != nil && job.Status.CompletionTime != nil {
		r.Metrics.ObserveIngestJobDuration(job.Status.CompletionTime.Sub(job.Status.StartTime.Time).Seconds())
	}

	if jobSucceeded(job) {
		sha, err := r.readResultSHA(ctx, repo)
		if err != nil {
			r.Metrics.ReconcileResult("Repository", "error")
			return ctrl.Result{}, fmt.Errorf("read ingest result sha: %w", err)
		}
		now := metav1.Now()
		repo.Status.LastIngestedCommit = sha
		repo.Status.LastIngestTime = &now
		repo.Status.Phase = "Ingested"
		repo.Status.JobName = ""
		meta.SetStatusCondition(&repo.Status.Conditions, metav1.Condition{
			Type:               "Ingested",
			Status:             metav1.ConditionTrue,
			Reason:             "IngestSucceeded",
			Message:            "ingested at " + sha,
			ObservedGeneration: repo.Generation,
		})
		if err := r.Status().Update(ctx, repo); err != nil {
			r.Metrics.ReconcileResult("Repository", "error")
			return ctrl.Result{}, fmt.Errorf("update repository status: %w", err)
		}
		l.Info("ingest succeeded",
			"action", "ingest_succeeded", "resource_id", repo.Name, "sha", sha, "job", job.Name)
		r.Metrics.ReconcileResult("Repository", "success")
		return ctrl.Result{}, nil
	}

	repo.Status.Phase = "Failed"
	repo.Status.JobName = ""
	meta.SetStatusCondition(&repo.Status.Conditions, metav1.Condition{
		Type:               "Ingested",
		Status:             metav1.ConditionFalse,
		Reason:             "IngestFailed",
		Message:            "ingest job " + job.Name + " failed",
		ObservedGeneration: repo.Generation,
	})
	if err := r.Status().Update(ctx, repo); err != nil {
		r.Metrics.ReconcileResult("Repository", "error")
		return ctrl.Result{}, fmt.Errorf("update repository status: %w", err)
	}
	l.Info("ingest failed",
		"action", "ingest_failed", "resource_id", repo.Name, "job", job.Name)
	r.Metrics.ReconcileResult("Repository", "error")
	return ctrl.Result{}, nil
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
		Complete(r)
}
