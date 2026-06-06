package controller

import (
	"context"
	"testing"
	"time"

	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func newProjectReconciler() *ProjectReconciler {
	return &ProjectReconciler{
		Client:              k8sClient,
		Scheme:              k8sClient.Scheme(),
		Metrics:             obs.NewOperatorMetrics(prometheus.NewRegistry()),
		ExternalWebhookBase: "https://tatara.example/operator/webhooks",
	}
}

func reconcileProject(t *testing.T, name string) (ctrl.Result, error) {
	t.Helper()
	r := newProjectReconciler()
	return r.Reconcile(logf.IntoContext(context.Background(), logf.Log), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: name},
	})
}

func mkSecret(t *testing.T, name string, data map[string][]byte) {
	t.Helper()
	s := &corev1.Secret{}
	s.Name = name
	s.Namespace = testNS
	s.Data = data
	if err := k8sClient.Create(context.Background(), s); err != nil {
		t.Fatalf("create secret %s: %v", name, err)
	}
}

func getProject(t *testing.T, name string) *tataradevv1alpha1.Project {
	t.Helper()
	p := &tataradevv1alpha1.Project{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, p); err != nil {
		t.Fatalf("get project %s: %v", name, err)
	}
	return p
}

func waitProjectReady(t *testing.T, name string, want metav1.ConditionStatus) *tataradevv1alpha1.Project {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		p := getProject(t, name)
		c := apierrors.FindStatusCondition(p.Status.Conditions, "Ready")
		if c != nil && c.Status == want {
			return p
		}
		time.Sleep(interval)
	}
	t.Fatalf("project %s Ready never reached %s", name, want)
	return nil
}

func TestProjectReconcile_ValidSecret(t *testing.T) {
	ctx := context.Background()
	mkSecret(t, "valid-scm", map[string][]byte{
		"token":         []byte("ghp_x"),
		"webhookSecret": []byte("hmac"),
	})
	p := &tataradevv1alpha1.Project{}
	p.Name = "proj-valid"
	p.Namespace = testNS
	p.Spec.ScmSecretRef = "valid-scm"
	if err := k8sClient.Create(ctx, p); err != nil {
		t.Fatalf("create project: %v", err)
	}

	if _, err := reconcileProject(t, "proj-valid"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := waitProjectReady(t, "proj-valid", metav1.ConditionTrue)
	want := "https://tatara.example/operator/webhooks/proj-valid"
	if got.Status.WebhookURL != want {
		t.Errorf("webhookURL = %q, want %q", got.Status.WebhookURL, want)
	}
}

func TestProjectReconcile_MissingSecret(t *testing.T) {
	ctx := context.Background()
	p := &tataradevv1alpha1.Project{}
	p.Name = "proj-nosecret"
	p.Namespace = testNS
	p.Spec.ScmSecretRef = "does-not-exist"
	if err := k8sClient.Create(ctx, p); err != nil {
		t.Fatalf("create project: %v", err)
	}

	if _, err := reconcileProject(t, "proj-nosecret"); err != nil {
		t.Fatalf("reconcile returned error, want nil (status carries failure): %v", err)
	}
	got := waitProjectReady(t, "proj-nosecret", metav1.ConditionFalse)
	c := apierrors.FindStatusCondition(got.Status.Conditions, "Ready")
	if c.Reason != "SecretNotFound" {
		t.Errorf("reason = %q, want SecretNotFound", c.Reason)
	}
}

func TestProjectReconcile_MissingKeys(t *testing.T) {
	ctx := context.Background()
	mkSecret(t, "partial-scm", map[string][]byte{"token": []byte("ghp_x")})
	p := &tataradevv1alpha1.Project{}
	p.Name = "proj-partialkeys"
	p.Namespace = testNS
	p.Spec.ScmSecretRef = "partial-scm"
	if err := k8sClient.Create(ctx, p); err != nil {
		t.Fatalf("create project: %v", err)
	}

	if _, err := reconcileProject(t, "proj-partialkeys"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := waitProjectReady(t, "proj-partialkeys", metav1.ConditionFalse)
	c := apierrors.FindStatusCondition(got.Status.Conditions, "Ready")
	if c.Reason != "SecretMissingKeys" {
		t.Errorf("reason = %q, want SecretMissingKeys", c.Reason)
	}
}

var _ = client.IgnoreNotFound
