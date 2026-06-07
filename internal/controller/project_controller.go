// Package controller holds the tatara-operator reconcilers.
package controller

import (
	"context"
	"fmt"

	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ProjectReconciler validates a Project's SCM secret and publishes its
// webhook URL.
type ProjectReconciler struct {
	client.Client
	Scheme              *runtime.Scheme
	Metrics             *obs.OperatorMetrics
	ExternalWebhookBase string
	MemoryConfig        memory.Config
}

// +kubebuilder:rbac:groups=tatara.dev,resources=projects,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tatara.dev,resources=projects/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

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

	if err := r.Status().Update(ctx, &project); err != nil {
		r.Metrics.ReconcileResult("Project", "error")
		return ctrl.Result{}, fmt.Errorf("update project status: %w", err)
	}

	l.Info("reconciled project",
		"action", "reconcile_project",
		"resource_id", project.Name,
		"ready", ready,
		"reason", reason)
	r.Metrics.ReconcileResult("Project", "success")
	return ctrl.Result{}, nil
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
// Projects.
func (r *ProjectReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tataradevv1alpha1.Project{}).
		Owns(&corev1.Secret{}).
		Complete(r)
}
