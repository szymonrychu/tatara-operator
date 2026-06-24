package controller

import (
	"context"
	"fmt"
	"time"

	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/grafanamcp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const grafanaFieldOwner = "tatara-operator"
const grafanaRequeue = 10 * time.Second

// reconcileGrafanaMCP provisions (or tears down) the per-Project read-only
// grafana-mcp workload, gated on Spec.Grafana.Enabled. It mirrors
// reconcileMemory: SSA-apply when enabled, status roll-up, teardown when off.
// Failure is isolated and does not block other reconciles.
func (r *ProjectReconciler) reconcileGrafanaMCP(ctx context.Context, p *tataradevv1alpha1.Project) (time.Duration, error) {
	l := log.FromContext(ctx)
	ns := r.GrafanaConfig.Namespace
	name := grafanamcp.Name(p.Name)

	enabled := p.Spec.Grafana != nil && p.Spec.Grafana.Enabled
	if !enabled {
		// Teardown: best-effort delete of a previously-applied workload.
		_ = r.Delete(ctx, &appsv1.Deployment{ObjectMeta: objMeta(ns, name)})
		_ = r.Delete(ctx, &corev1.Service{ObjectMeta: objMeta(ns, name)})
		p.Status.Grafana = nil
		return 0, nil
	}

	if err := grafanamcp.ValidateImage(r.GrafanaConfig.Image); err != nil {
		p.Status.Grafana = &tataradevv1alpha1.GrafanaStatus{Phase: "Failed", Endpoint: grafanamcp.Endpoint(p.Name, ns)}
		l.Error(err, "grafana-mcp image rejected", "action", "grafana_image_validate", "resource_id", p.Name, "image", r.GrafanaConfig.Image)
		return 0, err
	}

	objs := []client.Object{
		grafanamcp.Deployment(p, r.GrafanaConfig),
		grafanamcp.Service(p, r.GrafanaConfig),
	}
	for _, obj := range objs {
		if err := r.Patch(ctx, obj, client.Apply, //nolint:staticcheck
			client.FieldOwner(grafanaFieldOwner), client.ForceOwnership); err != nil {
			p.Status.Grafana = &tataradevv1alpha1.GrafanaStatus{Phase: "Failed", Endpoint: grafanamcp.Endpoint(p.Name, ns)}
			l.Error(err, "grafana-mcp apply failed", "action", "grafana_apply", "resource_id", p.Name)
			return 0, fmt.Errorf("apply %T %s: %w", obj, obj.GetName(), err)
		}
	}

	phase := "Provisioning"
	var d appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &d); err == nil {
		if d.Status.AvailableReplicas >= 1 {
			phase = "Ready"
		}
	} else if !apierrors.IsNotFound(err) {
		return grafanaRequeue, nil // transient cache blip; do not flap
	}
	p.Status.Grafana = &tataradevv1alpha1.GrafanaStatus{Phase: phase, Endpoint: grafanamcp.Endpoint(p.Name, ns)}
	if phase != "Ready" {
		return grafanaRequeue, nil
	}
	return 0, nil
}

func objMeta(ns, name string) metav1.ObjectMeta { return metav1.ObjectMeta{Name: name, Namespace: ns} }
