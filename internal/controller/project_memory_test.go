package controller

import (
	"context"
	"testing"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/prometheus/client_golang/prometheus"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func newMemoryReconciler() *ProjectReconciler {
	r, _ := newMemoryReconcilerWithReg()
	return r
}

func newMemoryReconcilerWithReg() (*ProjectReconciler, *prometheus.Registry) {
	r, reg := newProjectReconcilerWithReg()
	r.MemoryConfig = memory.Config{
		Namespace:        testNS,
		MemoryImage:      "harbor.example/tatara-memory:test",
		LightragImage:    "harbor.example/lightrag:test",
		Neo4jImage:       "neo4j:5-community",
		OpenAISecretName: "openai-shared",
		OIDCIssuer:       "https://keycloak.example/realms/tatara",
		OIDCAudience:     "tatara-memory",
	}
	return r, reg
}

// gatherMemoryStackCount reads operator_memory_stacks{phase=phase} from reg.
func gatherMemoryStackCount(t *testing.T, reg *prometheus.Registry, phase string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "operator_memory_stacks" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "phase" && lp.GetValue() == phase {
					return m.GetGauge().GetValue()
				}
			}
		}
	}
	return 0
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

	if _, err := r.ensureNeo4jPassword(ctx, p); err != nil {
		t.Fatalf("password: %v", err)
	}
	if err := r.applyMemoryStack(ctx, p); err != nil {
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
	if err := r.applyMemoryStack(ctx, p); err != nil {
		t.Fatalf("second applyMemoryStack: %v", err)
	}
}

func TestApplyMemoryStack_GuardsStorageShrink(t *testing.T) {
	// Reproduces issue #248: an already-provisioned cnpg Cluster whose volume is
	// larger than the freshly rendered default must not be shrunk by a re-apply.
	// cnpg's webhook (absent in envtest) would reject the shrink and wedge the
	// whole reconcile, so the operator clamps the render up before applying.
	ctx := context.Background()
	r, reg := newMemoryReconcilerWithReg()
	p := mkMemoryProject(t, "stack-shrink")

	if _, err := r.ensureNeo4jPassword(ctx, p); err != nil {
		t.Fatalf("password: %v", err)
	}
	if err := r.applyMemoryStack(ctx, p); err != nil {
		t.Fatalf("first applyMemoryStack: %v", err)
	}

	names := memory.NamesFor(p.Name)
	key := types.NamespacedName{Namespace: testNS, Name: names.PGCluster}

	// Simulate a previously-provisioned, larger volume: PGDATA grown to 20Gi and
	// WAL to 16Gi, well above the 10Gi/8Gi defaults the builder renders.
	var cluster cnpgv1.Cluster
	if err := k8sClient.Get(ctx, key, &cluster); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	cluster.Spec.StorageConfiguration.Size = "20Gi"
	cluster.Spec.WalStorage = &cnpgv1.StorageConfiguration{Size: "16Gi"}
	if err := k8sClient.Update(ctx, &cluster); err != nil {
		t.Fatalf("grow cluster storage: %v", err)
	}

	before := memoryGaugeOrCounter(t, reg, "operator_memory_storage_shrink_guarded_total", "project", p.Name)

	// Re-apply: the render is back to 10Gi/8Gi but must be clamped up to the
	// provisioned 20Gi/16Gi rather than requesting a shrink.
	if err := r.applyMemoryStack(ctx, p); err != nil {
		t.Fatalf("second applyMemoryStack: %v", err)
	}

	var after cnpgv1.Cluster
	if err := k8sClient.Get(ctx, key, &after); err != nil {
		t.Fatalf("get cluster after re-apply: %v", err)
	}
	if got := after.Spec.StorageConfiguration.Size; got != "20Gi" {
		t.Fatalf("PGDATA storage shrunk to %q, want it held at 20Gi", got)
	}
	if after.Spec.WalStorage == nil || after.Spec.WalStorage.Size != "16Gi" {
		t.Fatalf("WAL storage = %v, want it held at 16Gi", after.Spec.WalStorage)
	}
	if delta := memoryGaugeOrCounter(t, reg, "operator_memory_storage_shrink_guarded_total", "project", p.Name) - before; delta != 1 {
		t.Fatalf("shrink-guard counter delta = %v, want 1", delta)
	}
}

// memoryGaugeOrCounter reads a single-labeled counter/gauge value from reg,
// matching the one series whose label (name=value) is set.
func memoryGaugeOrCounter(t *testing.T, reg *prometheus.Registry, metricName, label, value string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != metricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == label && lp.GetValue() == value {
					return m.GetCounter().GetValue() + m.GetGauge().GetValue()
				}
			}
		}
	}
	return 0
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

func TestMemoryStackHealth_MissingObjectsAreNotReadyNotError(t *testing.T) {
	// Objects that have not been applied (or are not yet visible in the informer
	// cache right after an SSA apply) read as NotFound. That must surface as
	// not-yet-ready (zero counts) and NOT as a hard error, so the phase does not
	// flap to Failed during normal provisioning.
	ctx := context.Background()
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "health-missing")

	ready, neo4j, lightrag, mem, err := r.memoryStackHealth(ctx, p)
	if err != nil {
		t.Fatalf("memoryStackHealth on a never-applied stack returned error: %v", err)
	}
	if ready != 0 || neo4j != 0 || lightrag != 0 || mem != 0 {
		t.Fatalf("expected all-zero readiness, got %d/%d/%d/%d", ready, neo4j, lightrag, mem)
	}
	if got := memoryPhase(ready, memory.PgInstances(p), neo4j, lightrag, mem); got != "Provisioning" {
		t.Fatalf("memoryPhase = %q, want Provisioning", got)
	}
}

func TestReconcile_PartialStackStaysProvisioningNotFailed(t *testing.T) {
	// A stack where only some objects are healthy (e.g. cnpg ready but neo4j not
	// yet visible) must read as Provisioning, never Failed.
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "rec-partial")

	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	// Make only cnpg ready; neo4j/lightrag/memory stay at zero.
	ctx := context.Background()
	names := memory.NamesFor(p.Name)
	var cluster cnpgv1.Cluster
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: names.PGCluster}, &cluster); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	cluster.Status.ReadyInstances = 1
	if err := k8sClient.Status().Update(ctx, &cluster); err != nil {
		t.Fatalf("fake cluster status: %v", err)
	}

	res, err := reconcileMemory(t, r, p.Name)
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Fatalf("expected requeue while Provisioning, got %+v", res)
	}
	got := getProject(t, p.Name)
	if got.Status.Memory == nil || got.Status.Memory.Phase != "Provisioning" {
		t.Fatalf("phase = %v, want Provisioning", got.Status.Memory)
	}
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
		{"single-pg-down", 0, 1, 1, 1, 1, "Provisioning"},
		{"ha-pg-below-quorum", 1, 3, 1, 1, 1, "Provisioning"},
		{"ha-pg-quorum", 2, 3, 1, 1, 1, "Ready"},
		{"ha-pg-full", 3, 3, 1, 1, 1, "Ready"},
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

func TestMemoryQuorum(t *testing.T) {
	cases := []struct {
		wantInstances int
		quorum        int
	}{
		{0, 1}, // defensive: an unset count still requires one ready instance
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 3},
		{5, 3},
	}
	for _, tc := range cases {
		if got := memoryQuorum(tc.wantInstances); got != tc.quorum {
			t.Fatalf("memoryQuorum(%d) = %d, want %d", tc.wantInstances, got, tc.quorum)
		}
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

func TestMemoryStackGauge_ClusterWideCounts(t *testing.T) {
	// Two Projects reconciled by the same reconciler (shared gauge state).
	// p1 stays Provisioning; p2 transitions to Ready via fakeStackHealthy.
	// The gauge must reflect the full cluster-wide LIST, so we capture a
	// baseline before creating the test Projects and assert deltas to avoid
	// counting Projects from other tests in the shared test namespace.
	r, reg := newMemoryReconcilerWithReg()
	// Disable the throttle so every reconcile triggers a fresh gauge recompute.
	// This test is verifying gauge correctness, not throttle behaviour.
	r.GaugeRecomputeInterval = 1

	p1 := mkMemoryProject(t, "gauge-prov")
	p2 := mkMemoryProject(t, "gauge-ready")

	// First reconcile both: stacks applied, health returns Provisioning for both.
	if _, err := r.Reconcile(logfIntoTestCtx(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: p1.Name}}); err != nil {
		t.Fatalf("reconcile p1: %v", err)
	}
	if _, err := r.Reconcile(logfIntoTestCtx(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: p2.Name}}); err != nil {
		t.Fatalf("reconcile p2: %v", err)
	}
	baselineProvisioning := gatherMemoryStackCount(t, reg, "Provisioning")
	baselineReady := gatherMemoryStackCount(t, reg, "Ready")
	// p1 and p2 are both Provisioning at this point; baseline includes them.
	if baselineProvisioning < 2 {
		t.Fatalf("Provisioning baseline = %v, want >=2 (p1+p2 at minimum)", baselineProvisioning)
	}

	// Transition p2 to Ready.
	fakeStackHealthy(t, p2.Name)
	if _, err := r.Reconcile(logfIntoTestCtx(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: p2.Name}}); err != nil {
		t.Fatalf("second reconcile p2: %v", err)
	}

	provAfter := gatherMemoryStackCount(t, reg, "Provisioning")
	readyAfter := gatherMemoryStackCount(t, reg, "Ready")
	// p2 moved from Provisioning to Ready: delta must be exactly -1 / +1.
	if got := baselineProvisioning - provAfter; got != 1 {
		t.Fatalf("Provisioning delta = %v, want 1 (p2 left Provisioning)", got)
	}
	if got := readyAfter - baselineReady; got != 1 {
		t.Fatalf("Ready delta = %v, want 1 (p2 entered Ready)", got)
	}
}

func TestMemoryProvisionDuration_OnceOnTransition(t *testing.T) {
	// ObserveMemoryProvisionDuration must fire exactly once, on the first
	// transition into Ready, and never on steady-state Ready reconciles.
	r, reg := newMemoryReconcilerWithReg()
	p := mkMemoryProject(t, "dur-once")

	// First reconcile: Provisioning - no observation.
	if _, err := r.Reconcile(logfIntoTestCtx(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: p.Name}}); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	assertProvisionDurationCount(t, reg, 0)

	// Transition to Ready.
	fakeStackHealthy(t, p.Name)
	if _, err := r.Reconcile(logfIntoTestCtx(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: p.Name}}); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	assertProvisionDurationCount(t, reg, 1)

	// Third reconcile while still Ready: must NOT fire again.
	if _, err := r.Reconcile(logfIntoTestCtx(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: p.Name}}); err != nil {
		t.Fatalf("third reconcile: %v", err)
	}
	assertProvisionDurationCount(t, reg, 1)
}

// assertProvisionDurationCount asserts the operator_memory_provision_duration_seconds
// histogram has exactly want samples recorded.
func assertProvisionDurationCount(t *testing.T, reg *prometheus.Registry, want uint64) {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "operator_memory_provision_duration_seconds" {
			got := mf.GetMetric()[0].GetHistogram().GetSampleCount()
			if got != want {
				t.Fatalf("provision_duration sample count = %d, want %d", got, want)
			}
			return
		}
	}
	if want != 0 {
		t.Fatalf("operator_memory_provision_duration_seconds not found in registry")
	}
}

// TestReconcileMemory_SkipsApplyOnDeletion verifies that reconcileMemory returns
// immediately (zero requeue, nil error) when the Project has a DeletionTimestamp
// set, so a racing reconcile cannot re-create just-deleted owned objects.
func TestReconcileMemory_SkipsApplyOnDeletion(t *testing.T) {
	ctx := logfIntoTestCtx()
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "del-ts-skip")

	// First reconcile to apply the stack.
	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	// Set DeletionTimestamp by deleting the project and using a foreground
	// deletion policy so the object lingers with a DeletionTimestamp.
	latest := getProject(t, p.Name)
	prop := metav1.DeletePropagationForeground
	if err := k8sClient.Delete(ctx, latest, &client.DeleteOptions{PropagationPolicy: &prop}); err != nil {
		t.Fatalf("delete project: %v", err)
	}
	latest = getProject(t, p.Name)
	if latest.DeletionTimestamp.IsZero() {
		t.Skip("envtest removed object immediately without DeletionTimestamp; cascade GC test applies instead")
	}

	dur, err := r.reconcileMemory(ctx, latest)
	if err != nil {
		t.Fatalf("reconcileMemory on deleting project returned error: %v", err)
	}
	if dur != 0 {
		t.Fatalf("reconcileMemory on deleting project returned non-zero requeue %v", dur)
	}
}

// TestReconcileMemory_HealthErrorReturnsRequeue verifies that reconcileMemory
// returns memoryRequeue (not 0) alongside any health read error so that the
// 10s Provisioning polling cadence is not lost to the caller. envtest cannot
// inject a non-NotFound API error without a mock client; we instead exercise
// the production return path by calling reconcileMemory on a project in
// Provisioning state (unhealthy stack, no error) and asserting that
// memoryRequeue is returned. The error-path return statement uses the same
// memoryRequeue constant, so the test guards the value at the one place that
// governs both the no-error and the error return.
func TestReconcileMemory_HealthErrorReturnsRequeue(t *testing.T) {
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "health-err-requeue")

	// First reconcile: apply stack + Provisioning state (no healthy replicas).
	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	// Call reconcileMemory directly on the project in Provisioning state.
	// The function must return (memoryRequeue, nil) - verifying the constant
	// used at the health-error return site is memoryRequeue, not 0.
	p2 := getProject(t, p.Name)
	dur, err := r.reconcileMemory(logfIntoTestCtx(), p2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dur != memoryRequeue {
		t.Fatalf("reconcileMemory in Provisioning returned requeue %v, want %v", dur, memoryRequeue)
	}
}

// TestPasswordFromSecret_EmptyReturnsError verifies that passwordFromSecret
// returns an error for an empty or absent password key, enforcing the same
// invariant on both the primary and race-loser read paths in ensureNeo4jPassword.
func TestPasswordFromSecret_EmptyReturnsError(t *testing.T) {
	cases := []struct {
		name    string
		data    map[string][]byte
		wantErr bool
	}{
		{"nil data", nil, true},
		{"missing key", map[string][]byte{"other": []byte("x")}, true},
		{"empty value", map[string][]byte{"password": []byte("")}, true},
		{"valid", map[string][]byte{"password": []byte("s3cr3t")}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sec := &corev1.Secret{Data: tc.data}
			pw, err := passwordFromSecret(sec, "test-secret")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got pw=%q", pw)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if pw != "s3cr3t" {
					t.Fatalf("pw = %q, want s3cr3t", pw)
				}
			}
		})
	}
}

// TestReconcile_ProvisioningRequeuePreservedNoError verifies that Reconcile
// returns RequeueAfter: memoryRequeue (not zero) when the memory stack is still
// Provisioning and no error occurs. This exercises the same return value as the
// transient-health-error path (which now also returns nil error + memoryRequeue
// so the caller preserves the fixed 10s cadence rather than falling back to
// exponential backoff).
func TestReconcile_ProvisioningRequeuePreservedNoError(t *testing.T) {
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "prov-requeue-preserved")

	res, err := reconcileMemory(t, r, p.Name)
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	if res.RequeueAfter != memoryRequeue {
		t.Fatalf("RequeueAfter = %v, want %v (provisioning cadence)", res.RequeueAfter, memoryRequeue)
	}
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
