package controller

import (
	"context"
	"testing"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func boolPtrMem(v bool) *bool { return &v }

// setProjectMemoryEnabled flips spec.memory.enabled on a live Project, which
// bumps its generation - the idempotence marker the disable path keys on.
func setProjectMemoryEnabled(t *testing.T, name string, enabled bool) {
	t.Helper()
	p := &tataradevv1alpha1.Project{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: name}, p); err != nil {
		t.Fatalf("get project %s: %v", name, err)
	}
	if p.Spec.Memory == nil {
		p.Spec.Memory = &tataradevv1alpha1.MemorySpec{}
	}
	p.Spec.Memory.Enabled = boolPtrMem(enabled)
	if err := k8sClient.Update(context.Background(), p); err != nil {
		t.Fatalf("set spec.memory.enabled=%v on %s: %v", enabled, name, err)
	}
}

// mkStackPVC creates one PVC of the memory stack with the labels and owner
// references it carries in a live cluster. ownedByCluster reproduces the cnpg
// PVCs, which carry an ownerRef to the cnpg Cluster (not to the Project), so
// deleting the Cluster is what would cascade them away.
func mkStackPVC(t *testing.T, p *tataradevv1alpha1.Project, name string, labels map[string]string, ownedByCluster string) {
	t.Helper()
	tr := true
	// Only ONE reference may be the controller. A cnpg PVC's controller is the
	// cnpg Cluster; the Project reference (when present) is a plain one.
	projectIsController := ownedByCluster == ""
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNS,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: tataradevv1alpha1.GroupVersion.String(),
				Kind:       "Project",
				Name:       p.Name,
				UID:        p.UID,
				Controller: &projectIsController,
			}},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
	if ownedByCluster != "" {
		pvc.OwnerReferences = append(pvc.OwnerReferences, metav1.OwnerReference{
			APIVersion: cnpgv1.SchemeGroupVersion.String(),
			Kind:       "Cluster",
			Name:       ownedByCluster,
			UID:        types.UID("fake-cluster-uid-" + name),
			Controller: &tr,
		})
	}
	if err := k8sClient.Create(context.Background(), pvc); err != nil {
		t.Fatalf("create pvc %s: %v", name, err)
	}
}

// seedStackPVCs creates the PVCs the memory stack owns that no controller
// creates under envtest: the two cnpg volumes and the neo4j StatefulSet volume.
// The lightrag PVC is applied by applyMemoryStack itself.
func seedStackPVCs(t *testing.T, p *tataradevv1alpha1.Project) []string {
	t.Helper()
	n := memory.NamesFor(p.Name)
	pgData := n.PGCluster + "-1"
	pgWAL := n.PGCluster + "-1-wal"
	neo4j := "data-" + n.Neo4j + "-0"
	mkStackPVC(t, p, pgData, map[string]string{
		cnpgClusterLabel: n.PGCluster, cnpgPVCRoleLabel: cnpgPVCRolePGData,
	}, n.PGCluster)
	mkStackPVC(t, p, pgWAL, map[string]string{
		cnpgClusterLabel: n.PGCluster, cnpgPVCRoleLabel: cnpgPVCRolePGWAL,
	}, n.PGCluster)
	// The neo4j PVC comes from a StatefulSet volumeClaimTemplate, which carries
	// NO tatara.dev/project label (the template's metadata is immutable on a live
	// StatefulSet, so it can never be added). Name-prefix matching is the only
	// way to find it.
	mkStackPVC(t, p, neo4j, nil, "")
	return []string{pgData, pgWAL, neo4j, n.LightragPVC}
}

func getPVC(t *testing.T, name string) *corev1.PersistentVolumeClaim {
	t.Helper()
	pvc := &corev1.PersistentVolumeClaim{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: name}, pvc); err != nil {
		t.Fatalf("get pvc %s: %v", name, err)
	}
	return pvc
}

func objAbsent(t *testing.T, name string, obj client.Object) bool {
	t.Helper()
	err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, obj)
	if err == nil {
		return false
	}
	if apierrors.IsNotFound(err) {
		return true
	}
	t.Fatalf("get %T %s: %v", obj, name, err)
	return false
}

// countingDeleteClient records every Delete so the idempotence assertion can
// prove the second disabled pass issues NO deletes at all, rather than issuing
// them again and swallowing the NotFounds.
type countingDeleteClient struct {
	client.Client
	deletes int
}

func (c *countingDeleteClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	c.deletes++
	return c.Client.Delete(ctx, obj, opts...)
}

// A Project with no spec.memory at all keeps its memory stack: the feature is
// opt-OUT, and a silent teardown of every pre-existing Project would be the
// worst possible regression.
func TestReconcileMemory_EnabledByDefaultWhenUnset(t *testing.T) {
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "mem-default-on")

	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := getProject(t, p.Name)
	if got.Status.Memory == nil || got.Status.Memory.Phase == tataradevv1alpha1.MemoryPhaseDisabled {
		t.Fatalf("phase = %v, want a provisioning stack for a Project with no spec.memory", got.Status.Memory)
	}
	n := memory.NamesFor(p.Name)
	if objAbsent(t, n.Memory, &appsv1.Deployment{}) {
		t.Fatal("memory Deployment absent: an unset spec.memory.enabled must not disable the stack")
	}
}

// Disabling must DELETE the compute objects, not merely stop reconciling them:
// a leftover Deployment keeps running (and a leftover monitor keeps alerting)
// forever, the point the PGScheduledBackup comment already makes.
func TestReconcileMemory_DisabledTearsDownComputeObjects(t *testing.T) {
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "mem-teardown")

	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("provision reconcile: %v", err)
	}
	n := memory.NamesFor(p.Name)
	if objAbsent(t, n.Memory, &appsv1.Deployment{}) {
		t.Fatal("provision reconcile did not create the memory Deployment; the test proves nothing")
	}

	setProjectMemoryEnabled(t, p.Name, false)
	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("disable reconcile: %v", err)
	}
	// Disabled is TERMINAL: the memory reconcile asks for no poll at all. (The
	// full Reconcile still carries the unrelated counts-pacing requeue, so this
	// is asserted on reconcileMemory itself.)
	requeue, err := r.reconcileMemory(logfIntoTestCtx(), getProject(t, p.Name))
	if err != nil {
		t.Fatalf("second disable reconcile: %v", err)
	}
	if requeue != 0 {
		t.Fatalf("reconcileMemory requeue = %s, want 0: Disabled is terminal, not a poll", requeue)
	}

	for _, tc := range []struct {
		name string
		obj  client.Object
	}{
		{n.Memory, &appsv1.Deployment{}},
		{n.Memory, &corev1.Service{}},
		{n.Memory, &corev1.ConfigMap{}},
		{n.Lightrag, &appsv1.Deployment{}},
		{n.Lightrag, &corev1.Service{}},
		{n.Neo4j, &appsv1.StatefulSet{}},
		{n.Neo4j, &corev1.Service{}},
		{n.PGCluster, &cnpgv1.Cluster{}},
	} {
		if !objAbsent(t, tc.name, tc.obj) {
			t.Errorf("%T %s survived the disable teardown", tc.obj, tc.name)
		}
	}

	got := getProject(t, p.Name)
	if got.Status.Memory.Phase != tataradevv1alpha1.MemoryPhaseDisabled {
		t.Fatalf("phase = %q, want %q", got.Status.Memory.Phase, tataradevv1alpha1.MemoryPhaseDisabled)
	}
	if got.Status.Memory.Endpoint != "" {
		t.Errorf("endpoint = %q, want empty on a disabled stack", got.Status.Memory.Endpoint)
	}
	if got.Status.Memory.ReadySince != nil || got.Status.Memory.ProvisioningSince != nil {
		t.Errorf("stale readiness clocks left on a disabled stack: %+v", got.Status.Memory)
	}
	cond := apimeta.FindStatusCondition(got.Status.Conditions, "MemoryReady")
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "Disabled" {
		t.Fatalf("MemoryReady = %+v, want False/Disabled", cond)
	}
	// The neo4j password Secret must SURVIVE: it is what a re-enable reuses, and
	// regenerating it would leave the retained neo4j volume unopenable.
	if objAbsent(t, n.Neo4jSecret, &corev1.Secret{}) {
		t.Error("neo4j password Secret was deleted; a re-enabled stack could not open its retained volume")
	}
}

// THE data-safety assertion. spec.memory.enabled=false is a config change, not
// a delete request: every PVC in the stack must survive it, with the owner
// references that would cascade it away stripped and a label that finds it
// again. memoryBackup.enabled is off by default and nothing calls the restore
// path automatically, so a cascade here is unrecoverable.
func TestReconcileMemory_DisabledRetainsPVCsWithOwnerRefsStripped(t *testing.T) {
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "mem-retain")

	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("provision reconcile: %v", err)
	}
	pvcs := seedStackPVCs(t, p)

	setProjectMemoryEnabled(t, p.Name, false)
	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("disable reconcile: %v", err)
	}

	for _, name := range pvcs {
		pvc := getPVC(t, name) // fatals if the PVC is gone
		if len(pvc.OwnerReferences) != 0 {
			t.Errorf("pvc %s still carries ownerReferences %+v: a cascade would take the data with it",
				name, pvc.OwnerReferences)
		}
		if got := pvc.Labels[memory.RetainedForProjectLabel]; got != p.Name {
			t.Errorf("pvc %s label %s = %q, want %q so the volume can be found again",
				name, memory.RetainedForProjectLabel, got, p.Name)
		}
	}
}

// A disabled stack is reconciled on every Project watch event forever. The
// second pass must be a cheap no-op, not a delete storm.
func TestReconcileMemory_DisabledSecondPassIssuesNoDeletes(t *testing.T) {
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "mem-idem")

	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("provision reconcile: %v", err)
	}
	seedStackPVCs(t, p)
	setProjectMemoryEnabled(t, p.Name, false)

	// reconcileMemory is driven directly rather than through Reconcile: the
	// unrelated grafana-mcp teardown issues two unconditional deletes on every
	// pass, which would drown the signal this test is about.
	counting := &countingDeleteClient{Client: k8sClient}
	r.Client = counting
	ctx := logfIntoTestCtx()
	live := getProject(t, p.Name)
	if _, err := r.reconcileMemory(ctx, live); err != nil {
		t.Fatalf("first disabled reconcile: %v", err)
	}
	if counting.deletes == 0 {
		t.Fatal("the first disabled reconcile issued no deletes; the test proves nothing")
	}

	counting.deletes = 0
	if _, err := r.reconcileMemory(ctx, live); err != nil {
		t.Fatalf("second disabled reconcile: %v", err)
	}
	if counting.deletes != 0 {
		t.Fatalf("second disabled reconcile issued %d deletes, want 0 (teardown must be generation-guarded)",
			counting.deletes)
	}
}

// Re-enabling must re-adopt the retained volumes BY NAME - same PVC, Project
// ownerRef restored, retention label cleared - never orphan them into a
// duplicate set alongside freshly provisioned ones.
func TestReconcileMemory_ReEnableReadoptsRetainedPVCs(t *testing.T) {
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "mem-reenable")

	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("provision reconcile: %v", err)
	}
	pvcs := seedStackPVCs(t, p)
	uidBefore := map[string]types.UID{}
	setProjectMemoryEnabled(t, p.Name, false)
	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("disable reconcile: %v", err)
	}
	for _, name := range pvcs {
		uidBefore[name] = getPVC(t, name).UID
	}

	setProjectMemoryEnabled(t, p.Name, true)
	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("re-enable reconcile: %v", err)
	}

	var list corev1.PersistentVolumeClaimList
	if err := k8sClient.List(context.Background(), &list, client.InNamespace(testNS)); err != nil {
		t.Fatalf("list pvcs: %v", err)
	}
	for _, name := range pvcs {
		pvc := getPVC(t, name)
		if pvc.UID != uidBefore[name] {
			t.Errorf("pvc %s was recreated (uid %s -> %s), not re-adopted", name, uidBefore[name], pvc.UID)
		}
		if _, still := pvc.Labels[memory.RetainedForProjectLabel]; still {
			t.Errorf("pvc %s still carries the retention label after re-enable", name)
		}
		if len(pvc.OwnerReferences) == 0 {
			t.Errorf("pvc %s was not re-adopted: no ownerReferences restored", name)
		}
	}

	got := getProject(t, p.Name)
	if got.Status.Memory.Phase == tataradevv1alpha1.MemoryPhaseDisabled {
		t.Fatal("phase still Disabled after re-enable")
	}
	if got.Status.Memory.DisabledGeneration != 0 {
		t.Fatalf("DisabledGeneration = %d, want 0 after re-enable", got.Status.Memory.DisabledGeneration)
	}
	if objAbsent(t, memory.NamesFor(p.Name).Memory, &appsv1.Deployment{}) {
		t.Fatal("memory Deployment was not re-created on re-enable")
	}
}

// The gauge the critical memory alerts read must report a disabled project as
// Disabled, not as Failed/Degraded and not as an anonymous gap.
func TestUpdateMemoryStackCounts_DisabledIsNotBroken(t *testing.T) {
	r, reg := newMemoryReconcilerWithReg()
	p := mkMemoryProject(t, "mem-gauge-disabled")
	setProjectMemoryEnabled(t, p.Name, false)
	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("disable reconcile: %v", err)
	}

	r.updateMemoryStackCounts(context.Background())
	if v := gatherMemoryStackProjectPhase(t, reg, p.Name, tataradevv1alpha1.MemoryPhaseDisabled); v != 1 {
		t.Errorf("operator_memory_stacks{project=%q,phase=Disabled} = %v, want 1", p.Name, v)
	}
	for _, broken := range []string{"Failed", "Degraded", "Provisioning"} {
		if v := gatherMemoryStackProjectPhase(t, reg, p.Name, broken); v != 0 {
			t.Errorf("operator_memory_stacks{project=%q,phase=%s} = %v, want 0", p.Name, broken, v)
		}
	}
}
