package controller

import (
	"context"
	"testing"

	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

func newMemoryReconciler() *ProjectReconciler {
	r := newProjectReconciler()
	r.MemoryConfig = memory.Config{
		Namespace:        testNS,
		MemoryImage:      "harbor.example/tatara-memory:test",
		LightragImage:    "harbor.example/lightrag:test",
		Neo4jImage:       "neo4j:5-community",
		OpenAISecretName: "openai-shared",
		OIDCIssuer:       "https://keycloak.example/realms/tatara",
		OIDCAudience:     "tatara-memory",
	}
	return r
}

func mkMemoryProject(t *testing.T, name string) *tataradevv1alpha1.Project {
	t.Helper()
	mkSecret(t, name+"-scm", map[string][]byte{
		"token":         []byte("ghp_x"),
		"webhookSecret": []byte("hmac"),
	})
	p := &tataradevv1alpha1.Project{}
	p.Name = name
	p.Namespace = testNS
	p.Spec.ScmSecretRef = name + "-scm"
	if err := k8sClient.Create(context.Background(), p); err != nil {
		t.Fatalf("create project %s: %v", name, err)
	}
	return getProject(t, name)
}

func TestEnsureNeo4jPassword_GeneratesOnceAndIsStable(t *testing.T) {
	ctx := context.Background()
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "pw-once")

	pw1, err := r.ensureNeo4jPassword(ctx, p)
	if err != nil {
		t.Fatalf("ensureNeo4jPassword first call: %v", err)
	}
	if len(pw1) < 24 {
		t.Fatalf("password too short: %d chars", len(pw1))
	}

	names := memory.NamesFor(p.Name)
	var sec corev1.Secret
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: names.Neo4jSecret}, &sec); err != nil {
		t.Fatalf("neo4j secret not persisted: %v", err)
	}

	pw2, err := r.ensureNeo4jPassword(ctx, p)
	if err != nil {
		t.Fatalf("ensureNeo4jPassword second call: %v", err)
	}
	if pw2 != pw1 {
		t.Fatalf("password rotated on second reconcile: %q != %q", pw2, pw1)
	}
}
