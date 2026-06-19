package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

func TestReconcileGrafanaMCP_AppliesWhenEnabled(t *testing.T) {
	ctx := context.Background()
	p := &tatarav1alpha1.Project{}
	p.Name = "gmcp-on"
	p.Namespace = testNS
	p.Spec.ScmSecretRef = "gmcp-on-scm"
	p.Spec.Grafana = &tatarav1alpha1.GrafanaSpec{Enabled: true, URL: "http://grafana:3000", SecretRef: "gmcp-on-grafana"}
	if err := k8sClient.Create(ctx, p); err != nil {
		t.Fatalf("create project: %v", err)
	}

	r := &ProjectReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	r.GrafanaConfig.Namespace = testNS
	r.GrafanaConfig.Image = "grafana/mcp-grafana:test"

	if _, err := r.reconcileGrafanaMCP(ctx, p); err != nil {
		t.Fatalf("reconcileGrafanaMCP: %v", err)
	}

	var d appsv1.Deployment
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "grafana-mcp-gmcp-on"}, &d); err != nil {
		t.Fatalf("expected grafana-mcp deployment: %v", err)
	}
	var svc corev1.Service
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "grafana-mcp-gmcp-on"}, &svc); err != nil {
		t.Fatalf("expected grafana-mcp service: %v", err)
	}
	if p.Status.Grafana == nil || p.Status.Grafana.Endpoint == "" {
		t.Fatalf("status.grafana not set: %+v", p.Status.Grafana)
	}
}

func TestReconcileGrafanaMCP_TeardownWhenDisabled(t *testing.T) {
	ctx := context.Background()
	p := &tatarav1alpha1.Project{}
	p.Name = "gmcp-off"
	p.Namespace = testNS
	p.Spec.ScmSecretRef = "gmcp-off-scm"
	// No Grafana spec -> feature off.
	if err := k8sClient.Create(ctx, p); err != nil {
		t.Fatalf("create project: %v", err)
	}
	r := &ProjectReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	r.GrafanaConfig.Namespace = testNS
	r.GrafanaConfig.Image = "grafana/mcp-grafana:test"

	if _, err := r.reconcileGrafanaMCP(ctx, p); err != nil {
		t.Fatalf("reconcile (disabled): %v", err)
	}
	var d appsv1.Deployment
	err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "grafana-mcp-gmcp-off"}, &d)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Fatalf("disabled project must have NO grafana-mcp deployment; got err=%v", err)
	}
}
