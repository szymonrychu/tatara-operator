package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// TestPDBEditWakesProjectReconcile pins the watch edge for the memory-stack
// PodDisruptionBudgets.
//
// applyMemoryStack now emits two PDBs per Project, so they are part of the
// desired state the reconciler owns. Without an Owns edge, a hand-edited or
// deleted PDB is not corrected until the next 30s self-requeue, and every other
// object in the same apply set (Deployment, Service, ConfigMap, Ingress, ...)
// already has one - so the gap is silent asymmetry rather than a design choice.
//
// It registers the SAME builder production registers, for the same reason
// TestBrainstormChainWakesProjectReconcile does: a hand-copied watch set would
// keep passing after someone deleted the real edge.
func TestPDBEditWakesProjectReconcile(t *testing.T) {
	mgr := newTestManager(t)
	wakes := make(chan reconcile.Request, 32)

	err := projectControllerBuilder(mgr).
		Named("pdb-watch-envtest").
		Complete(reconcile.Func(func(_ context.Context, req reconcile.Request) (reconcile.Result, error) {
			select {
			case wakes <- req:
			default:
			}
			return reconcile.Result{}, nil
		}))
	if err != nil {
		t.Fatalf("register the test controller: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan error, 1)
	go func() { started <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-started
	})
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("manager cache did not sync")
	}

	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "pdb-watch-proj", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "pdb-scm",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"},
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	key := types.NamespacedName{Namespace: testNS, Name: proj.Name}
	if !awaitWake(wakes, key, timeout) {
		t.Fatal("the Project create never reached the reconciler; the manager is not actually running")
	}

	maxUnavailable := intstr.FromInt32(1)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "mem-pdb-watch-proj", Namespace: testNS},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &maxUnavailable,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
				"app.kubernetes.io/instance": "mem-pdb-watch-proj",
			}},
		},
	}
	if err := controllerutil.SetControllerReference(proj, pdb, scheme.Scheme); err != nil {
		t.Fatalf("set controller reference: %v", err)
	}
	if err := k8sClient.Create(ctx, pdb); err != nil {
		t.Fatalf("create pdb: %v", err)
	}
	if !awaitWake(wakes, key, timeout) {
		t.Fatalf("creating an owned PodDisruptionBudget did not wake the Project reconcile within %s; "+
			"projectControllerBuilder is missing Owns(&policyv1.PodDisruptionBudget{}), so drift on the "+
			"memory-stack PDBs is only corrected on the next %s self-requeue",
			timeout, defaultUnparkDriveInterval)
	}

	// A drift edit must wake it too, not just the create.
	twoUnavailable := intstr.FromInt32(2)
	pdb.Spec.MaxUnavailable = &twoUnavailable
	if err := k8sClient.Update(ctx, pdb); err != nil {
		t.Fatalf("update pdb: %v", err)
	}
	if !awaitWake(wakes, key, timeout) {
		t.Fatal("hand-editing an owned PodDisruptionBudget did not wake the Project reconcile")
	}
}
