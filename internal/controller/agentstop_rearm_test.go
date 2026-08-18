package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// newAgentStopFixture is a Task at under-implementation whose pod has ALREADY
// been taken down by an agent-requested stop: podStartedAt and
// stateWorkStartedAt nil, which is exactly the shape stopAfterAgentHandoff
// leaves behind and exactly the shape reconcilePodStage answers by minting a
// replacement pod.
func newAgentStopFixture(t *testing.T, stops int) (*v1alpha1.Project, *v1alpha1.Task, *TaskReconciler) {
	t.Helper()
	now := time.Now()
	proj := tsStablyReadyProject(3)
	task := tsTask("as-1", "upgrade", v1alpha1.StateUnderImplementation, now.Add(-4*time.Hour))
	task.Status.AgentKind = stage.AgentKindFor(v1alpha1.StateUnderImplementation, "upgrade")
	task.Status.Stats.AgentStops = stops
	lastEvent := metav1.NewTime(now.Add(-time.Minute))
	task.Status.ConversationLastEventAt = &lastEvent

	c := newMirrorClient(t, proj, mdSecret(), task)
	r := tsReconciler(&liveTaskClient{Client: c, task: task})
	reg := prometheus.NewRegistry()
	r.BundleMetrics = obs.NewBundleMetrics(reg)
	r.Metrics = obs.NewOperatorMetrics(reg)
	r.Session = &countingSession{}
	return proj, task, r
}

// THE LATCH. An agent-requested stop that has already been answered with
// AgentStopReArmCap replacement pods, with nothing new queued for any of them,
// must cost NO further pod. Before this the re-arm was unconditional:
// stopAfterAgentHandoff nils podStartedAt, reconcilePodStage sees an absent pod
// in a live state and mints another, ~80 seconds apart, 127 times.
//
// The park is no-outcome and not a new reason: "the pod ran and is gone and the
// Task never left the state" is exactly what reconcileCaps already writes for
// the un-graceful version of the same fact, and a second spelling of one fact is
// how a park vocabulary rots.
func TestAgentStopReArmCapParksInsteadOfMintingAnotherPod(t *testing.T) {
	proj, task, r := newAgentStopFixture(t, v1alpha1.AgentStopReArmCap)

	_, err := r.reconcilePodStage(context.Background(), proj, task, "upgrade", time.Now())
	require.NoError(t, err)

	fresh := &v1alpha1.Task{}
	require.NoError(t, r.Get(context.Background(), objectKeyOf(task), fresh))
	require.Equal(t, stage.ReasonNoOutcome, fresh.Status.ParkReason,
		"the identical question, answered identically %d times, must stop being asked",
		v1alpha1.AgentStopReArmCap)
	require.Nil(t, fresh.Status.PodStartedAt, "no replacement pod may be minted at the cap")
}

// THE LEGITIMATE CASE IS UNTOUCHED. An agent that hands over after real work -
// it ran out of context mid-implementation and the next pod carries on - is
// inside the continuation budget and gets its replacement pod. The operator
// cannot tell that apart from "I have nothing to do" without a wire-contract
// field the wrapper does not send, so the budget is what buys the difference.
func TestAgentStopInsideTheBudgetStillReArms(t *testing.T) {
	proj, task, r := newAgentStopFixture(t, v1alpha1.AgentStopReArmCap-1)

	_, err := r.reconcilePodStage(context.Background(), proj, task, "upgrade", time.Now())
	require.NoError(t, err)

	fresh := &v1alpha1.Task{}
	require.NoError(t, r.Get(context.Background(), objectKeyOf(task), fresh))
	require.Empty(t, fresh.Status.ParkReason,
		"a continuation handoff inside the budget must never park the Task")
}

// A PENDING EVENT IS A CHANGE OF CIRCUMSTANCE. A human commenting on a Task
// whose agent has stopped repeatedly is putting something NEW in front of it,
// which is a different question and is entitled to a pod however deep the
// streak. The event is spent when the turn is submitted, so one comment buys
// exactly one pod.
func TestAgentStopCapYieldsToAPendingEvent(t *testing.T) {
	proj, task, r := newAgentStopFixture(t, v1alpha1.AgentStopReArmCap+5)
	task.Status.PendingEvents = []v1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Repo: "mtg-decks", Number: 78,
		Author: "szymonrychu", Body: "please also bump the sideboard list",
	}}

	_, err := r.reconcilePodStage(context.Background(), proj, task, "upgrade", time.Now())
	require.NoError(t, err)

	fresh := &v1alpha1.Task{}
	require.NoError(t, r.Get(context.Background(), objectKeyOf(task), fresh))
	require.Empty(t, fresh.Status.ParkReason,
		"a queued human comment must never be swallowed by the re-arm cap")
}

// THE COUNTER AND THE METRIC. The stop has to record that it happened, or the
// bound has nothing to count and Prometheus has nothing to see: for the whole of
// the upgrade-qe-e4016501fd9107d9 incident operator_pod_recreations_total had
// ZERO series, because a stop-and-respawn through normal admission was not a
// "recreation" on any of the three paths that counted one.
func TestStopAfterAgentHandoffCountsTheStopAndTheChurn(t *testing.T) {
	ctx := context.Background()
	mkTaskProject(t, "p-ah-cnt", 3)
	mkTaskRepository(t, "r-ah-cnt", "p-ah-cnt")
	mkTask(t, "t-ah-cnt", "p-ah-cnt", "r-ah-cnt")

	task := getTask(t, "t-ah-cnt")
	task.Status.State = v1alpha1.StateUnderImplementation
	task.Status.PodStartedAt = &metav1.Time{Time: time.Now().Add(-30 * time.Minute)}
	task.Status.Notes = []v1alpha1.Note{{
		At: metav1.NewTime(time.Now().Add(-time.Minute)), Agent: "upgrade",
		Kind: agent.NoteKindHandoff, Body: "TURN 41 (close-out). No code, no forge write.",
	}}
	require.NoError(t, k8sClient.Status().Update(ctx, task))

	proj := getProject(t, "p-ah-cnt")
	kind := getTask(t, "t-ah-cnt").Spec.Kind
	before := testutil.ToFloat64(obs.PodRecreationCounter(proj.Name, kind, obs.RecreationReasonAgentStop))
	beforeStop := testutil.ToFloat64(obs.AgentRequestedStopCounter(proj.Name, kind, obs.AgentStopReArmed))

	r := newTaskReconciler(newFakeSession())
	_, err := r.stopAfterAgentHandoff(ctx, proj, getTask(t, "t-ah-cnt"), "upgrade", time.Now())
	require.NoError(t, err)

	got := getTask(t, "t-ah-cnt")
	require.Equal(t, 1, got.Status.Stats.AgentStops,
		"nothing recorded that the agent asked to stop, so nothing could bound the repeat")
	require.Equal(t, before+1,
		testutil.ToFloat64(obs.PodRecreationCounter(proj.Name, kind, obs.RecreationReasonAgentStop)),
		"the churn alert reads operator_pod_recreations_total; this path has to appear in it")
	require.Equal(t, beforeStop+1,
		testutil.ToFloat64(obs.AgentRequestedStopCounter(proj.Name, kind, obs.AgentStopReArmed)))

	// A SECOND consecutive stop in the same state accumulates.
	second := getTask(t, "t-ah-cnt")
	second.Status.PodStartedAt = &metav1.Time{Time: time.Now().Add(-10 * time.Minute)}
	second.Status.Notes = append(second.Status.Notes, v1alpha1.Note{
		At: metav1.NewTime(time.Now()), Agent: "upgrade",
		Kind: agent.NoteKindHandoff, Body: "TURN 42 (close-out). No code, no forge write.",
	})
	require.NoError(t, k8sClient.Status().Update(ctx, second))
	_, err = r.stopAfterAgentHandoff(ctx, proj, getTask(t, "t-ah-cnt"), "upgrade", time.Now())
	require.NoError(t, err)
	require.Equal(t, 2, getTask(t, "t-ah-cnt").Status.Stats.AgentStops)
}
