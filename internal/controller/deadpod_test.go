package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// THE DEAD-POD CONTRACT.
//
// An agent Pod runs with RestartPolicy: Never, so a wrapper that dies leaves the
// Pod OBJECT standing in phase Failed with its container terminated. The
// operator read only NotFound, so an OOMKilled pod was indistinguishable from a
// healthy one: the Task sat at its stage holding a live-pod slot until the
// turn-inactivity clock fired ~59 minutes later.
//
// podUnusable is the predicate that closes that. It answers "can this pod still
// serve a turn", not "does this pod object exist".

// dpPod builds a wrapper Pod for a Task with the given phase and container
// states, annotated for the Task's current stage so ensureStagePod treats it as
// this stage's pod.
func dpPod(task *tatarav1alpha1.Task, phase corev1.PodPhase, statuses ...corev1.ContainerStatus) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        agent.PodName(task),
			Namespace:   mdNS,
			Annotations: map[string]string{annPodStage: task.Status.State},
		},
		Status: corev1.PodStatus{Phase: phase, ContainerStatuses: statuses},
	}
}

func dpTerminated(exitCode int32, reason string) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name: "wrapper",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode:   exitCode,
			Reason:     reason,
			FinishedAt: metav1.NewTime(time.Date(2026, 8, 10, 7, 51, 51, 0, time.UTC)),
		}},
	}
}

func dpRunning() corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:  "wrapper",
		Ready: true,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
	}
}

// dpRanTask is a Task whose pod RAN and became Ready: both pod clocks stamped.
func dpRanTask(name string, now time.Time) *tatarav1alpha1.Task {
	task := tsTask(name, "implement", tatarav1alpha1.StateUnderImplementation, now.Add(-time.Hour))
	pod := metav1.NewTime(now.Add(-30 * time.Minute))
	work := metav1.NewTime(now.Add(-29 * time.Minute))
	task.Status.PodStartedAt = &pod
	task.Status.StateWorkStartedAt = &work
	return task
}

// TestPodUnusable_Classification is the whole predicate in one table.
//
// The exit-code clause is the load-bearing one: a container that terminated
// SUCCESSFULLY is a wrapper that shut itself down after the agent said its piece,
// and the agent-handoff stop owns that pod. Calling it unusable would route a
// clean finish into respawnLostPod, which counts a recreation into the churn
// alert and KEEPS the last-turn payload the handoff stop is supposed to spend.
func TestPodUnusable_Classification(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name       string
		pod        func(*tatarav1alpha1.Task) *corev1.Pod
		wantGone   bool
		wantReason string
	}{
		{"pod object absent", nil, true, obs.RecreationReasonPodGone},
		{
			"running and serving",
			func(task *tatarav1alpha1.Task) *corev1.Pod { return dpPod(task, corev1.PodRunning, dpRunning()) },
			false, "",
		},
		{
			"OOMKilled",
			func(task *tatarav1alpha1.Task) *corev1.Pod {
				return dpPod(task, corev1.PodFailed, dpTerminated(137, "OOMKilled"))
			},
			true, obs.RecreationReasonOOMKilled,
		},
		{
			"phase Failed with no container detail",
			func(task *tatarav1alpha1.Task) *corev1.Pod { return dpPod(task, corev1.PodFailed) },
			true, obs.RecreationReasonPodFailed,
		},
		{
			"container exited non-zero while the pod still reads Running",
			func(task *tatarav1alpha1.Task) *corev1.Pod {
				return dpPod(task, corev1.PodRunning, dpTerminated(1, "Error"))
			},
			true, obs.RecreationReasonContainerExited,
		},
		{
			"container exited ZERO: a clean shutdown is not a dead pod",
			func(task *tatarav1alpha1.Task) *corev1.Pod {
				return dpPod(task, corev1.PodSucceeded, dpTerminated(0, "Completed"))
			},
			false, "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := dpRanTask("unusable", now)
			objs := []client.Object{tsProject(3), mdSecret(), task}
			if tc.pod != nil {
				objs = append(objs, tc.pod(task))
			}
			r := tsReconciler(newMirrorClient(t, objs...))

			gone, reason, err := r.podUnusable(context.Background(), task)
			require.NoError(t, err)
			require.Equal(t, tc.wantGone, gone)
			require.Equal(t, tc.wantReason, reason)
		})
	}
}

// A FAILED pod on a Task whose pod became Ready is exactly the incident shape.
// reconcileCaps owns it: podStoppedNoOutcome is true, so BudgetExit parks
// no-outcome (an UnparkTimer stall, not a terminal) and ParkTask deletes the
// corpse - freeing the live-pod slot the dead pod was holding.
func TestReconcileCaps_AnOOMKilledCorpseIsTreatedAsAStoppedPod(t *testing.T) {
	now := time.Now()
	task := dpRanTask("oom-caps", now)
	proj := tsProject(3)
	r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task,
		dpPod(task, corev1.PodFailed, dpTerminated(137, "OOMKilled"))))

	got := tsReconcile(t, r, proj, task, now)

	require.True(t, tatarav1alpha1.Parked(got), "an OOMKilled corpse must not be mistaken for a working pod")
	require.Equal(t, stage.ReasonNoOutcome, got.Status.ParkReason)

	corpse := &corev1.Pod{}
	err := r.Get(context.Background(), types.NamespacedName{Namespace: mdNS, Name: agent.PodName(task)}, corpse)
	require.True(t, apierrors.IsNotFound(err), "the park must take the corpse with it, or it holds a live-pod slot forever")

	// The warning has to land HERE. podUnusable catches the OOM long before any
	// TTL-stop sequence would run, so if this path stayed silent the next pod
	// would read only the surviving pre-OOM note and go hunting for a commit that
	// never left the dead pod's ephemeral workspace.
	require.Len(t, got.Status.Notes, 1)
	require.Equal(t, agent.NoteAgentOperator, got.Status.Notes[0].Agent)
	require.Contains(t, got.Status.Notes[0].Body, agent.OOMKilledNoteMarker)
	require.Contains(t, got.Status.Notes[0].Body, "ephemeral")
}

// The OOM note is written ONCE. A reconcile that re-reads a corpse it has already
// noted must not stack a second copy onto the journal.
func TestNoteOOMKilledPod_IsIdempotent(t *testing.T) {
	now := time.Now()
	task := dpRanTask("oom-note-twice", now)
	proj := tsProject(3)
	r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task,
		dpPod(task, corev1.PodFailed, dpTerminated(137, "OOMKilled"))))

	require.NoError(t, r.noteOOMKilledPod(context.Background(), proj, task))
	first := mdGetTask(t, r.Client, task.Name)
	require.Len(t, first.Status.Notes, 1)

	require.NoError(t, r.noteOOMKilledPod(context.Background(), proj, first))
	require.Len(t, mdGetTask(t, r.Client, task.Name).Status.Notes, 1)
}

// A NON-OOM death writes no note at all. The claim the OOM note makes - that the
// workspace is gone and earlier commits may not be on the remote - is specific to
// a kill, and stamping it on every dead wrapper would train agents to ignore it.
func TestNoteOOMKilledPod_SaysNothingAboutAnOrdinaryExit(t *testing.T) {
	now := time.Now()
	task := dpRanTask("plain-exit", now)
	proj := tsProject(3)
	r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task,
		dpPod(task, corev1.PodFailed, dpTerminated(1, "Error"))))

	require.NoError(t, r.noteOOMKilledPod(context.Background(), proj, task))
	require.Empty(t, mdGetTask(t, r.Client, task.Name).Status.Notes)
}

// respawnLostPod was written for a pod that had already VANISHED. It is now
// reached with the corpse still standing, and pod names are stable per Task - so
// unless it deletes the corpse first, ensureStagePod sees a pod already annotated
// for this stage, returns early, and the Task never gets its replacement.
func TestRespawnLostPod_DeletesTheCorpseSoTheReplacementCanBeCreated(t *testing.T) {
	now := time.Now()
	task := dpRanTask("oom-respawn", now)
	task.Status.StateWorkStartedAt = nil // never became Ready: the respawn call site
	proj := tsProject(3)
	corpsePod := dpPod(task, corev1.PodFailed, dpTerminated(137, "OOMKilled"))
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: agent.PodName(task), Namespace: mdNS}}
	r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task, corpsePod, svc))

	oomCount := obs.PodRecreationCounter(task.Spec.ProjectRef, task.Spec.Kind, obs.RecreationReasonOOMKilled)
	before := testutil.ToFloat64(oomCount)
	_, err := r.respawnLostPod(context.Background(), proj, task, obs.RecreationReasonOOMKilled, now)
	require.NoError(t, err)
	require.Equal(t, float64(1), testutil.ToFloat64(oomCount)-before,
		"the OOM kill is the whole point of the reason label: the recreation-loop runbook splits on it")

	corpse := &corev1.Pod{}
	gerr := r.Get(context.Background(), types.NamespacedName{Namespace: mdNS, Name: agent.PodName(task)}, corpse)
	require.True(t, apierrors.IsNotFound(gerr), "the corpse must be gone: the replacement collides on its name")

	got := mdGetTask(t, r.Client, task.Name)
	require.False(t, tatarav1alpha1.Parked(got), "a respawn is not a park")
	require.Nil(t, got.Status.PodStartedAt, "the replacement must stamp a fresh pod clock")

	// The deliberate KEEP: this path writes no handoff note, so the last-turn
	// payload is the only surviving trace of the dead pod's work (#527).
	require.Equal(t, task.Status.LastTurnFinalText, got.Status.LastTurnFinalText)
}

// A Task whose pod never became Ready and whose container then exited routes to
// the respawn call site, not to the caps park.
func TestReconcilePodStage_ATerminatedContainerRespawns(t *testing.T) {
	now := time.Now()
	task := dpRanTask("exited-respawn", now)
	task.Status.StateWorkStartedAt = nil
	proj := tsProject(3)
	r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task,
		dpPod(task, corev1.PodRunning, dpTerminated(1, "Error"))))

	before := testutil.ToFloat64(obs.PodRecreationCounter(task.Spec.ProjectRef, task.Spec.Kind,
		obs.RecreationReasonContainerExited))
	got := tsReconcile(t, r, proj, task, now)
	after := testutil.ToFloat64(obs.PodRecreationCounter(task.Spec.ProjectRef, task.Spec.Kind,
		obs.RecreationReasonContainerExited))

	require.Equal(t, float64(1), after-before, "the recreation must be counted under the reason that drove it")
	require.Equal(t, 1, got.Status.Stats.PodRecreations)
	require.False(t, tatarav1alpha1.Parked(got))
}

// THE HAZARD THIS CHANGE MUST NOT CREATE.
//
// An agent that writes task_note(kind=handoff) and stops leaves a wrapper that
// exits ZERO. agentAskedToBeStopped owns that pod: it tears it down and SPENDS
// the last-turn payload. podUnusable is checked FIRST, so a predicate that
// counted every terminated container would divert the clean finish into a
// respawn - a spurious recreation on the churn alert, and a spent payload kept
// alive for a later synthetic note to replay.
func TestPodUnusable_ACleanlyFinishedPodGoesToTheHandoffStopNotARespawn(t *testing.T) {
	now := time.Now()
	task := dpRanTask("clean-finish", now)
	task.Status.Notes = []tatarav1alpha1.Note{{
		At:    metav1.NewTime(now.Add(-time.Minute)),
		Agent: "implement",
		Kind:  agent.NoteKindHandoff,
		Body:  "everything the next pod needs",
	}}
	task.Status.LastTurnFinalText = "done"
	proj := tsProject(3)
	r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task,
		dpPod(task, corev1.PodSucceeded, dpTerminated(0, "Completed"))))

	got := tsReconcile(t, r, proj, task, now)

	require.Equal(t, 0, got.Status.Stats.PodRecreations, "a clean shutdown is not a pod recreation")
	require.False(t, tatarav1alpha1.Parked(got))
	require.Empty(t, got.Status.LastTurnFinalText,
		"the agent-handoff stop must have spent the last-turn payload, not respawnLostPod kept it")
}
