// Package controller holds the tatara-operator reconcilers.
package controller

import (
	"context"
	"fmt"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/grafanamcp"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// defaultGaugeRecomputeInterval is how often updateMemoryStackCounts and
// updateLifecycleStateCounts run. Both do a full ProjectList / TaskList scan;
// running them on every per-Project reconcile is O(N) per cycle per project.
// 60 s is coarse-grained enough to avoid list pressure while still converging
// the gauges quickly after any phase/state change.
const defaultGaugeRecomputeInterval = 60 * time.Second

// ProjectReconciler validates a Project's SCM secret and publishes its
// webhook URL.
type ProjectReconciler struct {
	client.Client
	Scheme              *runtime.Scheme
	Metrics             *obs.OperatorMetrics
	LifecycleMetrics    *obs.LifecycleMetrics
	ExternalWebhookBase string
	MemoryConfig        memory.Config
	GrafanaConfig       grafanamcp.Config
	// ReaderFor returns a token-bound scm.SCMReader for a provider name and token.
	// Nil in tests that do not exercise scanning; wired in wire.go at runtime.
	ReaderFor func(provider, token string) (scm.SCMReader, error)
	// SCMFor returns the SCMWriter for a provider name (token passed per call).
	SCMFor func(provider string) (scm.SCMWriter, error)

	// GaugeRecomputeInterval controls how often the cluster-wide gauge scans
	// (updateMemoryStackCounts + updateLifecycleStateCounts) run. Defaults to
	// defaultGaugeRecomputeInterval when zero. MaxConcurrentReconciles=1 means
	// this field is read/written under the controller's serialised call path;
	// no mutex required.
	GaugeRecomputeInterval time.Duration
	lastGaugeRecompute     time.Time
}

// +kubebuilder:rbac:groups=tatara.dev,resources=projects,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tatara.dev,resources=projects/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete

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
	if grafanaRequeueAfter > 0 && (requeueAfter == 0 || grafanaRequeueAfter < requeueAfter) {
		requeueAfter = grafanaRequeueAfter
	}

	if err := r.Status().Update(ctx, &project); err != nil {
		r.Metrics.ReconcileResult("Project", "error")
		return ctrl.Result{}, fmt.Errorf("update project status: %w", err)
	}

	if memErr != nil {
		r.Metrics.ReconcileResult("Project", "error")
		return ctrl.Result{}, memErr
	}

	r.maybeRecomputeGauges(ctx)

	scanRequeue, scanErr := r.runScans(ctx, &project)
	if scanErr != nil {
		r.Metrics.ReconcileResult("Project", "error")
		return ctrl.Result{}, scanErr
	}
	requeueAfter = soonestRequeue(requeueAfter, scanRequeue)

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

// maybeRecomputeGauges runs updateMemoryStackCounts and updateLifecycleStateCounts
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
	r.updateLifecycleStateCounts(ctx)
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
	var provisioning, ready, failed int
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
		}
	}
	r.Metrics.SetMemoryStackCounts(provisioning, ready, failed)
}

// lifecycleStates is the full set of issueLifecycle states the
// tatara_lifecycle_state gauge tracks. updateLifecycleStateCounts Sets every one
// each pass (including 0 for drained states) so a state that empties out reads 0
// rather than retaining its last value.
var lifecycleStates = []string{
	"Triage", "Implement", "Conversation", "MRCI", "Merge", "MainCI",
	"Done", "Stopped", "Parked",
}

// updateLifecycleStateCounts recomputes tatara_lifecycle_state from authoritative
// cluster state: it lists every issueLifecycle Task, counts them by
// Status.LifecycleState, and Sets the gauge for all known states (zeros
// included). This is the sole writer of the gauge; it is restart-safe and
// terminal-safe, unlike the per-transition deltas it replaced, and mirrors
// updateMemoryStackCounts. Tasks of other Kinds carry an empty LifecycleState and
// are naturally excluded; the explicit Kind filter guards against ever emitting a
// state="" series.
func (r *ProjectReconciler) updateLifecycleStateCounts(ctx context.Context) {
	if r.LifecycleMetrics == nil {
		return
	}
	var list tataradevv1alpha1.TaskList
	if err := r.List(ctx, &list); err != nil {
		return
	}
	counts := make(map[string]int, len(lifecycleStates))
	for i := range list.Items {
		t := &list.Items[i]
		if t.Spec.Kind != "issueLifecycle" || t.Status.LifecycleState == "" {
			continue
		}
		counts[t.Status.LifecycleState]++
	}
	for _, state := range lifecycleStates {
		r.LifecycleMetrics.SetLifecycleState(state, float64(counts[state]))
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
		// MaxConcurrentReconciles: 1 is explicit here because laneOccupancy gating
		// assumes serialised reconciles per kind; raising this without revisiting
		// that invariant would cause correctness bugs.
		WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}
