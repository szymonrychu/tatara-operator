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
//
// It returns the volumes that must SURVIVE a disable. The lightrag PVC is
// deliberately NOT among them - see tearDownMemoryStack: its removal on disable
// is an explicit, approved decision, unlike the postgres corpus and the graph.
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
	return []string{pgData, pgWAL, neo4j}
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

// pvcDeleted reports whether a Delete has been issued against the PVC.
//
// It accepts "gone" OR "terminating", because envtest cannot produce the former
// for a PVC: the apiserver's StorageObjectInUseProtection admission plugin
// stamps kubernetes.io/pvc-protection at CREATE, and the controller that
// removes that finalizer lives in kube-controller-manager, which envtest does
// not run. So a deleted PVC keeps its object with a deletionTimestamp forever
// here, while on a real cluster it is collected as soon as nothing mounts it.
func pvcDeleted(t *testing.T, name string) bool {
	t.Helper()
	pvc := &corev1.PersistentVolumeClaim{}
	err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, pvc)
	if apierrors.IsNotFound(err) {
		return true
	}
	if err != nil {
		t.Fatalf("get pvc %s: %v", name, err)
	}
	return pvc.DeletionTimestamp != nil
}

// releasePVCFinalizers does by hand what kube-controller-manager's PVC
// protection controller does on a real cluster, so a test can observe the
// object actually disappear. See pvcDeleted for why this is necessary.
func releasePVCFinalizers(t *testing.T, name string) {
	t.Helper()
	ctx := context.Background()
	pvc := &corev1.PersistentVolumeClaim{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		t.Fatalf("get pvc %s: %v", name, err)
	}
	pvc.Finalizers = nil
	if err := k8sClient.Update(ctx, pvc); err != nil {
		t.Fatalf("release finalizers on pvc %s: %v", name, err)
	}
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

// THE data-safety assertion, and the asymmetry that goes with it.
//
// spec.memory.enabled=false is a config change, not a delete request - for the
// postgres corpus and the neo4j graph. Those must survive it, with the owner
// references that would cascade them away stripped and a label that finds them
// again; memoryBackup.enabled is off by default and nothing calls the restore
// path automatically, so a cascade there is unrecoverable.
//
// LightRAG is the ONE exception, by explicit owner decision: its PVC is DELETED
// on disable. This test asserts BOTH halves precisely so the split cannot be
// "tidied up" later in either direction - not into deleting all four, and not
// into retaining all four.
func TestReconcileMemory_DisabledRetainsPgAndNeo4jButDeletesLightrag(t *testing.T) {
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "mem-retain")

	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("provision reconcile: %v", err)
	}
	retained := seedStackPVCs(t, p)
	lightragPVC := memory.NamesFor(p.Name).LightragPVC
	if objAbsent(t, lightragPVC, &corev1.PersistentVolumeClaim{}) {
		t.Fatal("provision reconcile did not create the lightrag PVC; the deletion half proves nothing")
	}

	setProjectMemoryEnabled(t, p.Name, false)
	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("disable reconcile: %v", err)
	}

	// Half one: postgres + neo4j survive, unreachable by any cascade, findable,
	// and with no delete even ATTEMPTED against them.
	for _, name := range retained {
		pvc := getPVC(t, name) // fatals if the PVC is gone
		if pvc.DeletionTimestamp != nil {
			t.Errorf("pvc %s is terminating: the postgres corpus and the graph must be retained, "+
				"only lightrag is approved for removal", name)
		}
		if len(pvc.OwnerReferences) != 0 {
			t.Errorf("pvc %s still carries ownerReferences %+v: a cascade would take the data with it",
				name, pvc.OwnerReferences)
		}
		if got := pvc.Labels[memory.RetainedForProjectLabel]; got != p.Name {
			t.Errorf("pvc %s label %s = %q, want %q so the volume can be found again",
				name, memory.RetainedForProjectLabel, got, p.Name)
		}
	}

	// Half two: lightrag is REMOVED. Approved and intended, not an oversight.
	if !pvcDeleted(t, lightragPVC) {
		got := getPVC(t, lightragPVC)
		t.Errorf("lightrag PVC %s survived the disable: its removal is approved and intended "+
			"(labels=%v ownerRefs=%d)", lightragPVC, got.Labels, len(got.OwnerReferences))
	}
	// And it must not have been retained on the way out: no stripped-ownerRef
	// orphan left behind under the retention label.
	var list corev1.PersistentVolumeClaimList
	if err := k8sClient.List(context.Background(), &list, client.InNamespace(testNS),
		client.MatchingLabels{memory.RetainedForProjectLabel: p.Name}); err != nil {
		t.Fatalf("list retained pvcs: %v", err)
	}
	for i := range list.Items {
		if list.Items[i].Name == lightragPVC {
			t.Errorf("lightrag PVC %s was retained; it must be deleted, not kept", lightragPVC)
		}
	}
	if len(list.Items) != len(retained) {
		t.Errorf("retained PVC count = %d, want %d (exactly pg + neo4j)", len(list.Items), len(retained))
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

	// LightRAG was DELETED on disable, so re-enable must PROVISION IT FRESH
	// rather than re-adopt it. Let the deleted object actually go first - envtest
	// pins it on the pvc-protection finalizer with no controller to release it
	// (see pvcDeleted) - so "recreated" is observable rather than assumed.
	lightragPVC := memory.NamesFor(p.Name).LightragPVC
	if !pvcDeleted(t, lightragPVC) {
		t.Fatalf("lightrag PVC %s was not deleted on disable; the re-provision half proves nothing",
			lightragPVC)
	}
	lightragUIDBefore := getPVC(t, lightragPVC).UID
	releasePVCFinalizers(t, lightragPVC)
	if !objAbsent(t, lightragPVC, &corev1.PersistentVolumeClaim{}) {
		t.Fatalf("lightrag PVC %s still present after its finalizer was released", lightragPVC)
	}

	setProjectMemoryEnabled(t, p.Name, true)
	if _, err := reconcileMemory(t, r, p.Name); err != nil {
		t.Fatalf("re-enable reconcile: %v", err)
	}

	if objAbsent(t, lightragPVC, &corev1.PersistentVolumeClaim{}) {
		t.Errorf("lightrag PVC %s was not re-provisioned on re-enable", lightragPVC)
	} else {
		fresh := getPVC(t, lightragPVC)
		if fresh.UID == lightragUIDBefore {
			t.Errorf("lightrag PVC %s kept its old identity (uid %s): it must be a FRESH, empty volume",
				lightragPVC, fresh.UID)
		}
		if _, still := fresh.Labels[memory.RetainedForProjectLabel]; still {
			t.Errorf("re-provisioned lightrag PVC %s carries a retention label", lightragPVC)
		}
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
