package controller

import (
	"context"
	"testing"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func TestApplyMemoryStack_CreatesStackWithOwnerRefs(t *testing.T) {
	ctx := context.Background()
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "stack-create")

	pw, err := r.ensureNeo4jPassword(ctx, p)
	if err != nil {
		t.Fatalf("password: %v", err)
	}
	if err := r.applyMemoryStack(ctx, p, pw); err != nil {
		t.Fatalf("applyMemoryStack: %v", err)
	}

	names := memory.NamesFor(p.Name)

	// cnpg Cluster present, owner-ref'd to the Project, instances from spec default.
	var cluster cnpgv1.Cluster
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: names.PGCluster}, &cluster); err != nil {
		t.Fatalf("get cnpg cluster: %v", err)
	}
	assertOwnedByProject(t, cluster.GetOwnerReferences(), p.Name)

	// memory Deployment present and owner-ref'd.
	var dep appsv1.Deployment
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: names.Memory}, &dep); err != nil {
		t.Fatalf("get memory deployment: %v", err)
	}
	assertOwnedByProject(t, dep.GetOwnerReferences(), p.Name)

	// neo4j StatefulSet present and owner-ref'd.
	var sts appsv1.StatefulSet
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: names.Neo4j}, &sts); err != nil {
		t.Fatalf("get neo4j statefulset: %v", err)
	}
	assertOwnedByProject(t, sts.GetOwnerReferences(), p.Name)

	// Idempotent: a second apply must not error.
	if err := r.applyMemoryStack(ctx, p, pw); err != nil {
		t.Fatalf("second applyMemoryStack: %v", err)
	}
}

func assertOwnedByProject(t *testing.T, refs []metav1.OwnerReference, project string) {
	t.Helper()
	for _, ref := range refs {
		if ref.Kind == "Project" && ref.Name == project && ref.Controller != nil && *ref.Controller {
			return
		}
	}
	t.Fatalf("no controller ownerRef to Project %q in %+v", project, refs)
}

func TestMemoryPhase_Transitions(t *testing.T) {
	cases := []struct {
		name           string
		readyInstances int
		wantInstances  int
		neo4jReady     int32
		lightragAvail  int32
		memoryAvail    int32
		want           string
	}{
		{"all-down", 0, 1, 0, 0, 0, "Provisioning"},
		{"pg-only", 1, 1, 0, 0, 0, "Provisioning"},
		{"all-but-memory", 1, 1, 1, 1, 0, "Provisioning"},
		{"all-ready", 1, 1, 1, 1, 1, "Ready"},
		{"ha-pg-partial", 1, 3, 1, 1, 1, "Provisioning"},
		{"ha-pg-ready", 3, 3, 1, 1, 1, "Ready"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := memoryPhase(tc.readyInstances, tc.wantInstances, tc.neo4jReady, tc.lightragAvail, tc.memoryAvail)
			if got != tc.want {
				t.Fatalf("memoryPhase = %q, want %q", got, tc.want)
			}
		})
	}
}
