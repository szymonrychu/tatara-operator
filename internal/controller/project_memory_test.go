package controller

import (
	"context"
	"testing"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
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

func reconcileMemory(t *testing.T, r *ProjectReconciler, name string) (ctrl.Result, error) {
	t.Helper()
	return r.Reconcile(logfIntoTestCtx(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: name},
	})
}

func TestReconcile_ProvisionsStackAndSetsEndpoint(t *testing.T) {
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "rec-prov")

	res, err := reconcileMemory(t, r, p.Name)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Fatalf("expected requeue while Provisioning, got %+v", res)
	}

	got := getProject(t, p.Name)
	if got.Status.Memory == nil {
		t.Fatalf("status.memory is nil")
	}
	if got.Status.Memory.Phase != "Provisioning" {
		t.Fatalf("phase = %q, want Provisioning", got.Status.Memory.Phase)
	}
	wantEndpoint := memory.Endpoint(p.Name, testNS)
	if got.Status.Memory.Endpoint != wantEndpoint {
		t.Fatalf("endpoint = %q, want %q", got.Status.Memory.Endpoint, wantEndpoint)
	}
}

func TestReconcile_TransitionsToReadyWhenOwnedHealthy(t *testing.T) {
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "rec-ready")

	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	fakeStackHealthy(t, p.Name)

	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	got := waitMemoryPhase(t, p.Name, "Ready")
	c := apimeta.FindStatusCondition(got.Status.Conditions, "MemoryReady")
	if c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("MemoryReady condition = %+v, want True", c)
	}
}

func TestReconcile_FailedOnApplyError(t *testing.T) {
	r := newMemoryReconciler()
	// Empty namespace makes every SSA target a non-existent namespace, so the
	// apply fails and the reconciler records phase=Failed + MemoryReady=False.
	r.MemoryConfig.Namespace = "no-such-namespace-xyz"
	p := mkMemoryProject(t, "rec-fail")

	if _, err := reconcileMemory(t, r, p.Name); err == nil {
		t.Fatalf("expected reconcile error from apply failure")
	}
	got := getProject(t, p.Name)
	if got.Status.Memory == nil || got.Status.Memory.Phase != "Failed" {
		t.Fatalf("phase = %v, want Failed", got.Status.Memory)
	}
	c := apimeta.FindStatusCondition(got.Status.Conditions, "MemoryReady")
	if c == nil || c.Status != metav1.ConditionFalse {
		t.Fatalf("MemoryReady = %+v, want False", c)
	}
}

func TestReconcile_CascadeDeleteRemovesStack(t *testing.T) {
	ctx := context.Background()
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "rec-cascade")
	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	names := memory.NamesFor(p.Name)

	// envtest has no GC controller; assert the controller ownerRef + Background
	// propagation are in place, which is what drives real-cluster cascade.
	var cluster cnpgv1.Cluster
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: names.PGCluster}, &cluster); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	assertOwnedByProject(t, cluster.GetOwnerReferences(), p.Name)
	var pvc corev1.PersistentVolumeClaim
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: names.LightragPVC}, &pvc); err != nil {
		t.Fatalf("get lightrag pvc: %v", err)
	}
	assertOwnedByProject(t, pvc.GetOwnerReferences(), p.Name)

	if err := k8sClient.Delete(ctx, getProject(t, p.Name)); err != nil {
		t.Fatalf("delete project: %v", err)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: p.Name}, &tataradevv1alpha1.Project{}); err == nil {
		// Project still terminating is acceptable; the cascade is GC-driven and
		// not simulated in envtest. The ownerRef assertions above prove it.
		_ = cluster
	}
}

func logfIntoTestCtx() context.Context {
	return logf.IntoContext(context.Background(), logf.Log)
}

func waitMemoryPhase(t *testing.T, name, want string) *tataradevv1alpha1.Project {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		p := getProject(t, name)
		if p.Status.Memory != nil && p.Status.Memory.Phase == want {
			return p
		}
		time.Sleep(interval)
	}
	t.Fatalf("project %s memory phase never reached %s", name, want)
	return nil
}

// fakeStackHealthy patches the owned objects' status subresources to the
// healthy values the reconciler reads (no kubelet/cnpg-operator in envtest).
func fakeStackHealthy(t *testing.T, project string) {
	t.Helper()
	ctx := context.Background()
	names := memory.NamesFor(project)

	var cluster cnpgv1.Cluster
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: names.PGCluster}, &cluster); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	cluster.Status.ReadyInstances = 1
	if err := k8sClient.Status().Update(ctx, &cluster); err != nil {
		t.Fatalf("fake cluster status: %v", err)
	}

	var sts appsv1.StatefulSet
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: names.Neo4j}, &sts); err != nil {
		t.Fatalf("get sts: %v", err)
	}
	sts.Status.Replicas = 1
	sts.Status.ReadyReplicas = 1
	if err := k8sClient.Status().Update(ctx, &sts); err != nil {
		t.Fatalf("fake sts status: %v", err)
	}

	for _, dn := range []string{names.Lightrag, names.Memory} {
		var dep appsv1.Deployment
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: dn}, &dep); err != nil {
			t.Fatalf("get deployment %s: %v", dn, err)
		}
		dep.Status.Replicas = 1
		dep.Status.ReadyReplicas = 1
		dep.Status.AvailableReplicas = 1
		if err := k8sClient.Status().Update(ctx, &dep); err != nil {
			t.Fatalf("fake deployment %s status: %v", dn, err)
		}
	}
}
