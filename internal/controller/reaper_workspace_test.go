package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// mkWorkspacePVC creates a workspace PVC carrying the wrapper selector plus the
// task label, exactly as BuildWorkspacePVC does.
func mkWorkspacePVC(t *testing.T, name, taskName string) {
	t.Helper()
	sc := tatarav1alpha1.DefaultWorkspaceStorageClass
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNS,
			Labels: map[string]string{
				agent.LabelManagedBy: agent.ManagedByValue,
				agent.LabelComponent: agent.ComponentAgent,
				agent.LabelTask:      taskName,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			StorageClassName: &sc,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
	if err := k8sClient.Create(context.Background(), pvc); err != nil {
		t.Fatalf("create pvc %s: %v", name, err)
	}
}

// pvcExists reports whether a PVC is still live. A PVC pending deletion counts
// as GONE: the API server's StorageObjectInUseProtection admission plugin stamps
// every PVC with the kubernetes.io/pvc-protection finalizer, and envtest runs no
// pvc-protection controller to remove it, so a deleted claim lingers here with a
// deletionTimestamp instead of disappearing.
func pvcExists(t *testing.T, name string) bool {
	t.Helper()
	var pvc corev1.PersistentVolumeClaim
	err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: name}, &pvc)
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil {
		t.Fatalf("get pvc %s: %v", name, err)
	}
	return pvc.DeletionTimestamp.IsZero()
}

// A workspace volume whose Task no longer exists is dead storage. Nothing else
// will ever delete it: the Task ownerRef cascade only fires while the Task
// object is still around to be deleted, and a Task reaped before this feature
// shipped (or one whose terminal delete failed transiently) leaves the claim
// standing.
func TestReapOrphans_WorkspacePVCWithNoTaskIsReaped(t *testing.T) {
	mkWorkspacePVC(t, "reap-ws-orphan", "no-such-task-ws-orphan")

	reaperServer().ReapOrphans(context.Background())

	if pvcExists(t, "reap-ws-orphan") {
		t.Error("expected a workspace PVC whose Task is absent to be reaped")
	}
}

// A Task that reached a terminal outcome will never run another pod, so its
// workspace is retired here too - the independent retry path for a terminal
// delete that failed transiently at the transition.
func TestReapOrphans_WorkspacePVCOfADoneTaskIsReaped(t *testing.T) {
	mkTask(t, "ws-reap-done", "proj-x", "")
	tk := getTask(t, "ws-reap-done")
	tk.Status.State = tatarav1alpha1.StateDone
	if err := k8sClient.Status().Update(context.Background(), tk); err != nil {
		t.Fatalf("set state: %v", err)
	}
	mkWorkspacePVC(t, "reap-ws-done", "ws-reap-done")

	reaperServer().ReapOrphans(context.Background())

	if pvcExists(t, "reap-ws-done") {
		t.Error("expected the workspace PVC of a done Task to be reaped")
	}
}

// THE CREATION GRACE. ensureWorkspacePVC creates the claim and the Task's own
// cache entry can lag it by a beat; reaping a PVC microseconds after its create
// would delete the volume out from under the pod about to mount it. This is the
// same guard the Pod and Service passes carry.
func TestReapOrphans_FreshWorkspacePVCIsSparedByTheGrace(t *testing.T) {
	mkWorkspacePVC(t, "reap-ws-fresh", "no-such-task-ws-fresh")

	srv := &CallbackServer{
		Client:      k8sClient,
		Metrics:     obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Namespace:   testNS,
		ReaperGrace: time.Hour,
	}
	srv.ReapOrphans(context.Background())

	if !pvcExists(t, "reap-ws-fresh") {
		t.Error("a PVC younger than the grace window must never be reaped")
	}
}

// A live, non-terminal Task keeps its workspace. This is the case the whole
// feature exists for.
func TestReapOrphans_WorkspacePVCOfALiveTaskIsKept(t *testing.T) {
	mkTask(t, "ws-reap-live", "proj-x", "")
	tk := getTask(t, "ws-reap-live")
	tk.Status.State = tatarav1alpha1.StateUnderImplementation
	if err := k8sClient.Status().Update(context.Background(), tk); err != nil {
		t.Fatalf("set state: %v", err)
	}
	mkWorkspacePVC(t, "reap-ws-live", "ws-reap-live")

	reaperServer().ReapOrphans(context.Background())

	if !pvcExists(t, "reap-ws-live") {
		t.Error("a live Task's workspace PVC must not be reaped")
	}
}

// The per-PROJECT cache PVC carries a DIFFERENT component label precisely so it
// can never be a candidate here: it has no Task, so a pass that saw it would
// read it as an orphan and delete the shared cache on the first sweep.
func TestReapOrphans_ProjectCachePVCIsNeverACandidate(t *testing.T) {
	sc := tatarav1alpha1.DefaultWorkspaceStorageClass
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      agent.CachePVCName("reapproj"),
			Namespace: testNS,
			Labels: map[string]string{
				agent.LabelManagedBy:  agent.ManagedByValue,
				agent.LabelComponent:  agent.ComponentAgentCache,
				agent.LabelProjectKey: "reapproj",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			StorageClassName: &sc,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
	if err := k8sClient.Create(context.Background(), pvc); err != nil {
		t.Fatalf("create cache pvc: %v", err)
	}

	reaperServer().ReapOrphans(context.Background())

	if !pvcExists(t, agent.CachePVCName("reapproj")) {
		t.Error("the project build-cache PVC must never be reaped by the wrapper sweep")
	}
}
