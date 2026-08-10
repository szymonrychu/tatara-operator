package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// wsPodConfig is tsPodConfig with the operator-wide workspace switch ON.
func wsPodConfig() agent.PodConfig {
	cfg := tsPodConfig()
	cfg.WorkspacePVCEnabled = true
	return cfg
}

func wsReconciler(c client.Client) *TaskReconciler {
	r := tsReconciler(c)
	r.PodConfig = wsPodConfig()
	return r
}

func wsGetPVC(t *testing.T, c client.Client, name string) (*corev1.PersistentVolumeClaim, bool) {
	t.Helper()
	var pvc corev1.PersistentVolumeClaim
	err := c.Get(context.Background(), types.NamespacedName{Namespace: mdNS, Name: name}, &pvc)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	require.NoError(t, err)
	return &pvc, true
}

func wsPodExists(t *testing.T, c client.Client, task *tatarav1alpha1.Task) bool {
	t.Helper()
	var pod corev1.Pod
	err := c.Get(context.Background(), types.NamespacedName{Namespace: mdNS, Name: agent.PodName(task)}, &pod)
	if apierrors.IsNotFound(err) {
		return false
	}
	require.NoError(t, err)
	return true
}

// wsBind marks a PVC Bound, which is what rook-ceph-rwx (volumeBindingMode
// Immediate) does within seconds of the create.
func wsBind(t *testing.T, c client.Client, name string) {
	t.Helper()
	pvc, ok := wsGetPVC(t, c, name)
	require.True(t, ok)
	pvc.Status.Phase = corev1.ClaimBound
	require.NoError(t, c.Status().Update(context.Background(), pvc))
}

func wsCachePVC(t *testing.T, c client.Client, project string) {
	t.Helper()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: agent.CachePVCName(project), Namespace: mdNS},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	require.NoError(t, c.Create(context.Background(), pvc))
	require.NoError(t, c.Status().Update(context.Background(), pvc))
}

// THE BOUND GATE, and it is the single most important safety property in this
// change. handleNotReady respawns any pod that is not Ready inside the 5 minute
// boot deadline, and the pod-recreation ceiling was DELETED, so a pod left
// Pending on an unbound PVC is roughly 288 pod creations per Task per day. The
// pod must therefore not exist at all until the claim is Bound.
func TestEnsureStagePod_DoesNotSpawnAPodUntilTheWorkspacePVCIsBound(t *testing.T) {
	task := tsTask("ws-gate", "implement", tatarav1alpha1.StateUnderImplementation, time.Now())
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := wsReconciler(c)

	skipped, err := r.ensureStagePod(context.Background(), proj, task)
	require.NoError(t, err)
	require.True(t, skipped, "an unbound workspace PVC must SKIP the spawn, not create a Pending pod")
	require.False(t, wsPodExists(t, c, task), "no pod may exist while the PVC is unbound")

	pvc, ok := wsGetPVC(t, c, agent.WorkspacePVCName(task))
	require.True(t, ok, "the workspace PVC must have been created")
	require.Equal(t, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}, pvc.Spec.AccessModes)
	require.NotNil(t, pvc.Spec.StorageClassName)
	require.Equal(t, tatarav1alpha1.DefaultWorkspaceStorageClass, *pvc.Spec.StorageClassName)
	require.Equal(t, tatarav1alpha1.DefaultWorkspaceSize,
		pvc.Spec.Resources.Requests.Storage().String())

	// Same ownerRef the Pod and Service carry, so it cascade-deletes with the Task.
	require.Len(t, pvc.OwnerReferences, 1)
	require.Equal(t, "Task", pvc.OwnerReferences[0].Kind)
	require.Equal(t, task.Name, pvc.OwnerReferences[0].Name)

	// podLabels MINUS the agent-kind label: that one is per-stage, and this
	// volume deliberately outlives a stage.
	require.Equal(t, task.Name, pvc.Labels[agent.LabelTask])
	require.Equal(t, agent.ManagedByValue, pvc.Labels[agent.LabelManagedBy])
	require.NotContains(t, pvc.Labels, agent.LabelAgentKind)

	// Still no pod on the next pass, and no duplicate PVC either.
	skipped, err = r.ensureStagePod(context.Background(), proj, task)
	require.NoError(t, err)
	require.True(t, skipped)
	require.False(t, wsPodExists(t, c, task))
}

// Idempotence on re-entry: a second ensure over an already-Bound claim creates
// nothing new and lets the pod through.
func TestEnsureStagePod_SpawnsOnceTheWorkspacePVCIsBound(t *testing.T) {
	task := tsTask("ws-bound", "implement", tatarav1alpha1.StateUnderImplementation, time.Now())
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := wsReconciler(c)

	_, err := r.ensureStagePod(context.Background(), proj, task)
	require.NoError(t, err)
	wsBind(t, c, agent.WorkspacePVCName(task))
	wsCachePVC(t, c, proj.Name)

	skipped, err := r.ensureStagePod(context.Background(), proj, task)
	require.NoError(t, err)
	require.False(t, skipped)
	require.True(t, wsPodExists(t, c, task))

	pvc, ok := wsGetPVC(t, c, agent.WorkspacePVCName(task))
	require.True(t, ok)
	require.Equal(t, corev1.ClaimBound, pvc.Status.Phase, "the ensure must not have recreated it")
}

// Bounded wait. rook-ceph-rwx is volumeBindingMode Immediate, so binding does
// not wait for a pod: it either happens in seconds or something is genuinely
// wrong. Past the deadline the Task PARKS rather than requeueing forever.
func TestEnsureStagePod_ParksWhenTheWorkspacePVCNeverBinds(t *testing.T) {
	task := tsTask("ws-stuck", "implement", tatarav1alpha1.StateUnderImplementation, time.Now())
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := wsReconciler(c)

	_, err := r.ensureStagePod(context.Background(), proj, task)
	require.NoError(t, err)

	// Backdate the claim past the bind deadline; it stays Pending.
	pvc, ok := wsGetPVC(t, c, agent.WorkspacePVCName(task))
	require.True(t, ok)
	pvc.CreationTimestamp = metav1.NewTime(time.Now().Add(-workspaceBindDeadline - time.Minute))
	require.NoError(t, c.Update(context.Background(), pvc))

	skipped, err := r.ensureStagePod(context.Background(), proj, task)
	require.NoError(t, err)
	require.True(t, skipped)
	require.False(t, wsPodExists(t, c, task), "an unprovisionable PVC must cost ZERO pods")

	got := mdGetTask(t, c, task.Name)
	require.True(t, tatarav1alpha1.Parked(got), "the Task must park rather than requeue forever")
	require.Equal(t, stage.ReasonOperatorError, got.Status.ParkReason)
}

// The escape hatch really is one: with spec.workspace.enabled=false the Task
// spawns exactly as it did before this feature existed, with no PVC at all.
func TestEnsureStagePod_NoPVCWhenTheProjectDisablesTheWorkspace(t *testing.T) {
	fa := false
	task := tsTask("ws-off", "implement", tatarav1alpha1.StateUnderImplementation, time.Now())
	proj := tsProject(3)
	proj.Spec.Workspace = &tatarav1alpha1.WorkspaceSpec{Enabled: &fa}
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := wsReconciler(c)

	skipped, err := r.ensureStagePod(context.Background(), proj, task)
	require.NoError(t, err)
	require.False(t, skipped)
	require.True(t, wsPodExists(t, c, task))
	_, ok := wsGetPVC(t, c, agent.WorkspacePVCName(task))
	require.False(t, ok, "a disabled workspace must create no PVC")
}

// The operator-wide switch ships OFF, so nothing changes for a cluster that has
// not opted in.
func TestEnsureStagePod_NoPVCWhenTheOperatorWideSwitchIsOff(t *testing.T) {
	task := tsTask("ws-global-off", "implement", tatarav1alpha1.StateUnderImplementation, time.Now())
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := tsReconciler(c) // tsPodConfig: WorkspacePVCEnabled false

	skipped, err := r.ensureStagePod(context.Background(), proj, task)
	require.NoError(t, err)
	require.False(t, skipped)
	require.True(t, wsPodExists(t, c, task))
	_, ok := wsGetPVC(t, c, agent.WorkspacePVCName(task))
	require.False(t, ok)
}

// A malformed per-project quantity must fail the ensure with a readable error,
// never reach resource.MustParse, and never leave a PVC behind.
func TestEnsureStagePod_RejectsAMalformedWorkspaceSize(t *testing.T) {
	task := tsTask("ws-bad-size", "implement", tatarav1alpha1.StateUnderImplementation, time.Now())
	proj := tsProject(3)
	proj.Spec.Workspace = &tatarav1alpha1.WorkspaceSpec{Size: "ten gigs"}
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := wsReconciler(c)

	_, err := r.ensureStagePod(context.Background(), proj, task)
	require.Error(t, err)
	require.Contains(t, err.Error(), "workspace.size")
	require.False(t, wsPodExists(t, c, task))
}

// TERMINAL deletes the workspace volume. A Task that reached done/rejected will
// never run another pod, so its per-Task volume is dead weight in the pool.
func TestEnterStage_TerminalOutcomeDeletesTheWorkspacePVC(t *testing.T) {
	task := tsTask("ws-terminal", "implement", tatarav1alpha1.StateUnderImplementation, time.Now())
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := wsReconciler(c)

	_, err := r.ensureStagePod(context.Background(), proj, task)
	require.NoError(t, err)
	_, ok := wsGetPVC(t, c, agent.WorkspacePVCName(task))
	require.True(t, ok, "precondition: the PVC exists")

	require.NoError(t, r.enter(context.Background(), proj, task, nil,
		tatarav1alpha1.StateRejected, stage.ReasonFalsePositive, time.Now()))

	_, ok = wsGetPVC(t, c, agent.WorkspacePVCName(task))
	require.False(t, ok, "a terminal outcome must delete the workspace PVC")
}

// PARK MUST NOT DELETE IT, and this is the assertion that matters most in the
// whole teardown story.
//
// ParkTask tears the POD down too, so folding the PVC delete into that same
// block would look harmless and be catastrophic: parked Tasks are roughly two
// thirds of the live population, and a parked Task can UNPARK and resume. The
// volume it would resume onto is exactly the committed work this feature exists
// to stop losing. The delete therefore lives in its own `if`, keyed on
// TaskIsTerminalOutcome and nothing else.
func TestParkDoesNotDeleteTheWorkspacePVC(t *testing.T) {
	task := tsTask("ws-park", "implement", tatarav1alpha1.StateUnderImplementation, time.Now())
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := wsReconciler(c)

	_, err := r.ensureStagePod(context.Background(), proj, task)
	require.NoError(t, err)
	_, ok := wsGetPVC(t, c, agent.WorkspacePVCName(task))
	require.True(t, ok, "precondition: the PVC exists")

	require.NoError(t, r.park(context.Background(), proj, task, stage.ReasonAwaitingHuman, time.Now()))

	got := mdGetTask(t, c, task.Name)
	require.True(t, tatarav1alpha1.Parked(got), "precondition: the Task really parked")
	_, ok = wsGetPVC(t, c, agent.WorkspacePVCName(task))
	require.True(t, ok, "a PARK must NEVER delete the workspace PVC: the Task can unpark and resume onto it")
}

// The same guarantee via the transition path: enterStage tears the POD down on
// every stage exit, and the PVC delete must not ride along on a NON-terminal
// one. The Task is walking on, and its work-in-progress walks with it.
func TestEnterStage_NonTerminalTransitionKeepsTheWorkspacePVC(t *testing.T) {
	task := tsTask("ws-nonterm", "implement", tatarav1alpha1.StateUnderImplementation, time.Now())
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := wsReconciler(c)

	_, err := r.ensureStagePod(context.Background(), proj, task)
	require.NoError(t, err)

	require.NoError(t, r.enter(context.Background(), proj, task, nil,
		tatarav1alpha1.StateAwaitingReview, "", time.Now()))

	_, ok := wsGetPVC(t, c, agent.WorkspacePVCName(task))
	require.True(t, ok, "the review stage INHERITS the implement stage's workspace; it must still exist")
}

// The CACHE volume is project-scoped and must survive every Task terminal: it is
// shared, and where the entire performance win of this feature lives.
func TestEnterStage_TerminalOutcomeKeepsTheProjectCachePVC(t *testing.T) {
	task := tsTask("ws-cache-keep", "implement", tatarav1alpha1.StateUnderImplementation, time.Now())
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := wsReconciler(c)
	wsCachePVC(t, c, proj.Name)

	_, err := r.ensureStagePod(context.Background(), proj, task)
	require.NoError(t, err)
	require.NoError(t, r.enter(context.Background(), proj, task, nil,
		tatarav1alpha1.StateRejected, stage.ReasonFalsePositive, time.Now()))

	_, ok := wsGetPVC(t, c, agent.CachePVCName(proj.Name))
	require.True(t, ok, "the per-project build cache must outlive every Task")
}
