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
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// seedStagedTask creates a Task on the NEW stage model plus its wrapper Pod, and
// returns the PodWatchReconciler under test. stg == "" seeds a LEGACY
// phase-driven Task (no stage), which the watch must leave entirely alone.
func seedStagedTask(t *testing.T, name, stg, agentKind string, sess *fakeSession) (*PodWatchReconciler, *tatarav1alpha1.Task, *corev1.Pod) {
	t.Helper()
	ctx := context.Background()

	task := &tatarav1alpha1.Task{}
	task.Name = name
	task.Namespace = testNS
	task.Spec.ProjectRef = name + "-proj"
	task.Spec.Goal = "g"
	task.Spec.Kind = "clarify"
	require.NoError(t, k8sClient.Create(ctx, task))

	if stg != "" {
		task.Status.Stage = stg
		task.Status.AgentKind = agentKind
		entered := metav1.NewTime(time.Now().Add(-30 * time.Minute))
		task.Status.StageEnteredAt = &entered
		require.NoError(t, k8sClient.Status().Update(ctx, task))
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      agent.PodName(task),
			Namespace: testNS,
			Labels: map[string]string{
				agent.LabelManagedBy: agent.ManagedByValue,
				agent.LabelComponent: agent.ComponentAgent,
				agent.LabelTask:      task.Name,
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "wrapper", Image: "wrapper:1"}}},
	}
	require.NoError(t, k8sClient.Create(ctx, pod))

	r := &PodWatchReconciler{Client: k8sClient, Session: sess, Namespace: testNS}
	return r, getTask(t, name), pod
}

func setPodStatus(t *testing.T, pod *corev1.Pod, startedAgo time.Duration, ready bool) {
	t.Helper()
	start := metav1.NewTime(time.Now().Add(-startedAgo))
	pod.Status.Phase = corev1.PodRunning
	pod.Status.StartTime = &start
	cond := corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionFalse}
	if ready {
		cond.Status = corev1.ConditionTrue
		cond.LastTransitionTime = metav1.NewTime(time.Now())
	} else {
		pod.Status.Phase = corev1.PodPending
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name:  "wrapper",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
		}}
	}
	pod.Status.Conditions = []corev1.PodCondition{cond}
	require.NoError(t, k8sClient.Status().Update(context.Background(), pod))
}

func reconcilePod(t *testing.T, r *PodWatchReconciler, pod *corev1.Pod) ctrl.Result {
	t.Helper()
	ctx := logf.IntoContext(context.Background(), logf.Log)
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}})
	require.NoError(t, err)
	return res
}

// TestPodWatchStampsPodStartedAt: RESIDUE 9. On pod CREATE, status.podStartedAt
// is stamped - that is what arms CLOCK 2. Nothing else in the operator sets it,
// and the entire three-clock model is load-bearing on it.
func TestPodWatchStampsPodStartedAt(t *testing.T) {
	r, task, pod := seedStagedTask(t, "podclock-create", tatarav1alpha1.StageImplementing, "implement", newFakeSession())
	require.Nil(t, task.Status.PodStartedAt, "no pod-create stamp yet")

	// Before the stamp the Task is on CLOCK 1: it is queued, not running.
	clock, _, _, _ := stage.ArmedClock(task, false)
	require.Equal(t, stage.ClockAdmission, clock)

	setPodStatus(t, pod, 10*time.Second, false)
	reconcilePod(t, r, pod)

	got := getTask(t, "podclock-create")
	require.NotNil(t, got.Status.PodStartedAt, "pod CREATE must stamp podStartedAt")
	require.Nil(t, got.Status.StageWorkStartedAt, "the pod is not Ready: CLOCK 3 stays disarmed")
	require.Equal(t, 1, got.Status.Stats.PodRuns)

	clock, since, budget, _ := stage.ArmedClock(got, false)
	require.Equal(t, stage.ClockReadiness, clock, "podStartedAt arms CLOCK 2")
	require.Equal(t, got.Status.PodStartedAt.Time, since, "CLOCK 2 measures from podStartedAt, never from stageEnteredAt")
	require.Equal(t, tatarav1alpha1.PodReadyTimeout, budget)

	// Idempotent: re-reconciling the same pod does not double-count podRuns.
	reconcilePod(t, r, pod)
	require.Equal(t, 1, getTask(t, "podclock-create").Status.Stats.PodRuns)
}

// TestPodWatchStampsStageWorkStartedAtOnReady: on pod READY, status
// .stageWorkStartedAt is stamped and CLOCK 3 (the stage work budget) takes over.
func TestPodWatchStampsStageWorkStartedAtOnReady(t *testing.T) {
	r, _, pod := seedStagedTask(t, "podclock-ready", tatarav1alpha1.StageImplementing, "implement", newFakeSession())
	setPodStatus(t, pod, 20*time.Second, true)
	reconcilePod(t, r, pod)

	got := getTask(t, "podclock-ready")
	require.NotNil(t, got.Status.PodStartedAt)
	require.NotNil(t, got.Status.StageWorkStartedAt, "pod READY must stamp stageWorkStartedAt")

	clock, since, _, _ := stage.ArmedClock(got, false)
	require.Equal(t, stage.ClockWork, clock, "stageWorkStartedAt arms CLOCK 3")
	require.Equal(t, got.Status.StageWorkStartedAt.Time, since)
}

// TestPodWatchIgnoresLegacyTasks: the greenness rule. A phase-driven Task carries
// no stage, and the watch must not touch it - the old TaskReconciler owns it whole.
func TestPodWatchIgnoresLegacyTasks(t *testing.T) {
	r, _, pod := seedStagedTask(t, "podclock-legacy", "", "", newFakeSession())
	setPodStatus(t, pod, 20*time.Second, true)
	reconcilePod(t, r, pod)

	got := getTask(t, "podclock-legacy")
	require.Nil(t, got.Status.PodStartedAt)
	require.Nil(t, got.Status.StageWorkStartedAt)
	require.Zero(t, got.Status.Stats.PodRuns)
}

// TestPodWatchImagePullBackOffRespawns: a pod stuck Pending (ImagePullBackOff)
// never becomes Ready, so stageWorkStartedAt stays nil and CLOCK 2 stays armed.
// At PodReadyTimeout the pod RESPAWNS (+1 podRecreations). It does NOT fail the
// Task, and there is no such reason as pod-not-ready.
func TestPodWatchImagePullBackOffRespawns(t *testing.T) {
	r, _, pod := seedStagedTask(t, "podclock-imagepull", tatarav1alpha1.StageImplementing, "implement", newFakeSession())

	// Under the deadline: still booting. No respawn.
	setPodStatus(t, pod, 1*time.Minute, false)
	res := reconcilePod(t, r, pod)
	require.Equal(t, agentBootRequeue, res.RequeueAfter)
	require.Zero(t, getTask(t, "podclock-imagepull").Status.Stats.PodRecreations)
	require.Nil(t, getTask(t, "podclock-imagepull").Status.StageWorkStartedAt)

	// Past PodReadyTimeout (5m): CLOCK 2 breaches -> RESPAWN.
	setPodStatus(t, pod, 6*time.Minute, false)
	reconcilePod(t, r, pod)

	got := getTask(t, "podclock-imagepull")
	require.Equal(t, 1, got.Status.Stats.PodRecreations, "a never-Ready pod RESPAWNS")
	require.Equal(t, tatarav1alpha1.StageImplementing, got.Status.Stage, "a never-Ready pod does NOT fail the Task")
	require.Nil(t, got.Status.PodStartedAt, "the replaced pod's clock is cleared so the replacement re-stamps a FRESH one")

	fresh := &corev1.Pod{}
	err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: pod.Name}, fresh)
	require.True(t, apierrors.IsNotFound(err) || fresh.DeletionTimestamp != nil, "the never-Ready pod is deleted")
}

// TestPodWatchRecreationBudgetExhaustedFailsTask: once maxPodRecreations is spent
// the terminal is failed(pod-recreation-exhausted) - NEVER pod-not-ready, which
// is not a member of the F.5 closed reason set.
func TestPodWatchRecreationBudgetExhaustedFailsTask(t *testing.T) {
	r, task, pod := seedStagedTask(t, "podclock-exhausted", tatarav1alpha1.StageImplementing, "implement", newFakeSession())
	task.Status.Stats.PodRecreations = maxPodRecreations
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	setPodStatus(t, pod, 6*time.Minute, false)
	reconcilePod(t, r, pod)

	got := getTask(t, "podclock-exhausted")
	require.Equal(t, tatarav1alpha1.StageFailed, got.Status.Stage)
	require.Equal(t, stage.ReasonPodRecreationExhausted, got.Status.StageReason)
	require.True(t, stage.ValidReason(got.Status.StageReason))
	require.NotEqual(t, "pod-not-ready", got.Status.StageReason)
}

// TestPodWatchUnadmittedTaskSitsOnClock1: a Task that has not been admitted has
// no pod and no podStartedAt. It sits on CLOCK 1 (24h admission-starved) and must
// not fail on the 5m readiness clock.
func TestPodWatchUnadmittedTaskSitsOnClock1(t *testing.T) {
	entered := metav1.NewTime(time.Now().Add(-6 * time.Minute))
	task := &tatarav1alpha1.Task{
		Status: tatarav1alpha1.TaskStatus{
			Stage:          tatarav1alpha1.StageImplementing,
			StageEnteredAt: &entered,
		},
	}
	clock, _, budget, edge := stage.ArmedClock(task, false)
	require.Equal(t, stage.ClockAdmission, clock)
	require.Equal(t, tatarav1alpha1.AdmissionStarvedBudget, budget)
	require.Equal(t, stage.ReasonAdmissionStarved, edge.Reason)
	_, elapsed := stage.Elapsed(task, false, time.Now())
	require.False(t, elapsed, "6 minutes without a pod must not trip anything: the readiness clock is not armed")
}

// TestPodWatchRestampsPodStartedAtOnRespawn: a Task re-entering a stage, or a pod
// respawning inside one, MUST get a fresh podStartedAt. A stale one makes
// G.7's t0 = podStartedAt + agentPodTTLSeconds ALREADY PAST, so the operator
// TTL-stops the next pod before its first turn.
func TestPodWatchRestampsPodStartedAtOnRespawn(t *testing.T) {
	r, task, pod := seedStagedTask(t, "podclock-restamp", tatarav1alpha1.StageImplementing, "implement", newFakeSession())

	// A stale stamp from the pod this one replaced, an hour ago.
	stale := metav1.NewTime(time.Now().Add(-1 * time.Hour))
	staleWork := metav1.NewTime(time.Now().Add(-55 * time.Minute))
	task.Status.PodStartedAt = &stale
	task.Status.StageWorkStartedAt = &staleWork
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	proj := &tatarav1alpha1.Project{Spec: tatarav1alpha1.ProjectSpec{AgentPodTTLSeconds: 1800}}
	require.True(t, agent.TTLExpired(proj, getTask(t, "podclock-restamp"), time.Now()),
		"precondition: the STALE stamp puts t0 in the past")

	setPodStatus(t, pod, 5*time.Second, false)
	reconcilePod(t, r, pod)

	got := getTask(t, "podclock-restamp")
	require.NotNil(t, got.Status.PodStartedAt)
	require.True(t, got.Status.PodStartedAt.After(stale.Time), "the fresh pod must RE-stamp podStartedAt")
	require.Nil(t, got.Status.StageWorkStartedAt, "the replaced pod's work clock must not survive into the new pod")

	t0, ok := agent.TTLDeadline(proj, got)
	require.True(t, ok)
	require.True(t, t0.After(time.Now()), "the fresh pod gets a FRESH t0, comfortably in the future")
	require.False(t, agent.TTLExpired(proj, got, time.Now()))
}

// TestPodWatchContractMismatchFailsBeforeTurnZero is the G.10 handshake. A
// wrapper reporting contractVersion 1 (or none at all) fails the Task INSTANTLY,
// with ZERO turns submitted. The wrapper image is pinned in a DIFFERENT helm
// release than the operator and helmfile applies releases concurrently, so this
// state is reachable and WILL happen; without the instant fail every such pod
// burns 40 Opus turns producing nothing, silently.
func TestPodWatchContractMismatchFailsBeforeTurnZero(t *testing.T) {
	for _, tc := range []struct {
		name string
		info agent.SessionInfo
	}{
		{name: "old wrapper reports v1", info: agent.SessionInfo{State: agent.SessionStateReady, ContractVersion: ptrInt(1)}},
		{name: "old wrapper reports no contractVersion at all", info: agent.SessionInfo{State: agent.SessionStateReady}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newFakeSession()
			sess.sessionInfo = tc.info
			name := "podclock-mismatch-" + sanitizeName(tc.name)
			r, _, pod := seedStagedTask(t, name, tatarav1alpha1.StageImplementing, "implement", sess)
			setPodStatus(t, pod, 20*time.Second, true)
			reconcilePod(t, r, pod)

			got := getTask(t, name)
			require.Equal(t, tatarav1alpha1.StageFailed, got.Status.Stage)
			require.Equal(t, stage.ReasonAgentContractMismatch, got.Status.StageReason)
			require.Nil(t, got.Status.StageWorkStartedAt, "a mismatched pod never starts the work clock")

			// The property that makes this defence worth having: ZERO tokens burned.
			require.Empty(t, sess.submits, "a contract-mismatched pod must submit ZERO turns")
			require.Empty(t, sess.handoffs, "a contract-mismatched pod must submit ZERO turns")
		})
	}
}

// TestPodWatchContractMatchProceeds: the happy path must not be broken by the
// handshake. A wrapper on the right contract stamps the work clock and no Task
// is failed.
func TestPodWatchContractMatchProceeds(t *testing.T) {
	sess := newFakeSession()
	r, _, pod := seedStagedTask(t, "podclock-contract-ok", tatarav1alpha1.StageReviewing, "review", sess)
	setPodStatus(t, pod, 20*time.Second, true)
	reconcilePod(t, r, pod)

	got := getTask(t, "podclock-contract-ok")
	require.Equal(t, tatarav1alpha1.StageReviewing, got.Status.Stage)
	require.Empty(t, got.Status.StageReason)
	require.NotNil(t, got.Status.StageWorkStartedAt)
}

func ptrInt(v int) *int { return &v }

func sanitizeName(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out = append(out, r)
		case r == ' ':
			out = append(out, '-')
		}
	}
	return string(out)
}
