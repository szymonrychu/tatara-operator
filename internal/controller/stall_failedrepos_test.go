package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// THE AGENT-REQUESTED STOP IS THE DOMINANT HEALTHY STOP, AND IT SPENDS THE
// LAST-TURN PAYLOAD WITHOUT READING THE ONE FIELD THE AGENT COULD NOT KNOW.
//
// stopAfterAgentHandoff deliberately skips the G.7 sequence, so the
// failed-repos note StopWithHandoff writes on its TTLHandoffAgent return never
// runs here - and clearLastTurn then retires the field unread. The agent's own
// note cannot cover for it: HandoffTurnText is a fixed string and the wrapper
// reports the push failure to pod stdout only.
func TestStopAfterAgentHandoff_RecordsTheReposWhosePushFailed(t *testing.T) {
	ctx := context.Background()
	mkTaskProject(t, "p-ah-fr", 3)
	mkTaskRepository(t, "r-ah-fr", "p-ah-fr")
	mkTask(t, "t-ah-fr", "p-ah-fr", "r-ah-fr")

	task := getTask(t, "t-ah-fr")
	task.Status.PodStartedAt = &metav1.Time{Time: time.Now().Add(-time.Hour)}
	task.Status.LastTurnFinalText = "eight of nine landed"
	task.Status.LastTurnPushedRepos = []string{"tatara-operator"}
	task.Status.LastTurnFailedRepos = []string{"tatara-cli"}
	task.Status.Notes = []tatarav1alpha1.Note{{
		At: metav1.NewTime(time.Now()), Agent: "implement",
		Kind: agent.NoteKindHandoff, Body: "PR is open, CI green",
	}}
	require.NoError(t, k8sClient.Status().Update(ctx, task))

	r := newTaskReconciler(newFakeSession())
	_, err := r.stopAfterAgentHandoff(ctx, getProject(t, "p-ah-fr"),
		getTask(t, "t-ah-fr"), "implement", time.Now())
	require.NoError(t, err)

	got := getTask(t, "t-ah-fr")
	require.Empty(t, got.Status.LastTurnFailedRepos, "the payload is still spent, not kept")

	var operatorNotes int
	var named bool
	var opAt time.Time
	for _, n := range got.Status.Notes {
		if n.Agent != agent.NoteAgentOperator {
			continue
		}
		operatorNotes++
		opAt = n.At.Time
		if strings.Contains(n.Body, "tatara-cli") {
			named = true
		}
	}
	require.True(t, named, "the next pod is never told repo tatara-cli's commits did not reach origin")
	require.Equal(t, 1, operatorNotes)

	// The REAL predicate, not a re-implementation of it: the operator's addendum
	// is kind=handoff, so if it were mistaken for an agent handoff every caller of
	// this - handoffNoteCount, the agentAskedSomething sites, agentAskedToBeStopped
	// itself - would see one handoff too many.
	require.True(t, agent.HasAgentHandoffNote(got))
	require.False(t, agent.HasAgentHandoffNoteSince(got, opAt.Add(time.Millisecond)),
		"nothing written at or after the operator's addendum may read as an agent handoff")
}

// A stop with nothing lost writes nothing extra: the overwhelming majority of
// agent-requested stops must be byte-identical to what they were.
func TestStopAfterAgentHandoff_WritesNoNoteWhenNothingFailed(t *testing.T) {
	ctx := context.Background()
	mkTaskProject(t, "p-ah-fr2", 3)
	mkTaskRepository(t, "r-ah-fr2", "p-ah-fr2")
	mkTask(t, "t-ah-fr2", "p-ah-fr2", "r-ah-fr2")

	task := getTask(t, "t-ah-fr2")
	task.Status.PodStartedAt = &metav1.Time{Time: time.Now().Add(-time.Hour)}
	task.Status.LastTurnPushedRepos = []string{"tatara-operator"}
	task.Status.Notes = []tatarav1alpha1.Note{{
		At: metav1.NewTime(time.Now()), Agent: "implement",
		Kind: agent.NoteKindHandoff, Body: "all nine pushed",
	}}
	require.NoError(t, k8sClient.Status().Update(ctx, task))

	r := newTaskReconciler(newFakeSession())
	_, err := r.stopAfterAgentHandoff(ctx, getProject(t, "p-ah-fr2"),
		getTask(t, "t-ah-fr2"), "implement", time.Now())
	require.NoError(t, err)

	require.Len(t, getTask(t, "t-ah-fr2").Status.Notes, 1)
}

// The payload survives a respawn on purpose (clearLastTurn is deliberately not
// run there), so the same repo list can reach a second stop. The note is a
// standing statement of fact, not an event: saying it twice adds nothing and
// costs journal budget the agent's own notes need.
func TestStopAfterAgentHandoff_DoesNotRepeatANoteAlreadyInTheJournal(t *testing.T) {
	ctx := context.Background()
	mkTaskProject(t, "p-ah-fr3", 3)
	mkTaskRepository(t, "r-ah-fr3", "p-ah-fr3")
	mkTask(t, "t-ah-fr3", "p-ah-fr3", "r-ah-fr3")

	arm := func() {
		task := getTask(t, "t-ah-fr3")
		task.Status.PodStartedAt = &metav1.Time{Time: time.Now().Add(-time.Hour)}
		task.Status.LastTurnFailedRepos = []string{"tatara-cli"}
		task.Status.Notes = append(task.Status.Notes, tatarav1alpha1.Note{
			At: metav1.NewTime(time.Now()), Agent: "implement",
			Kind: agent.NoteKindHandoff, Body: "PR is open, CI green",
		})
		require.NoError(t, k8sClient.Status().Update(ctx, task))
	}

	r := newTaskReconciler(newFakeSession())
	for range 2 {
		arm()
		_, err := r.stopAfterAgentHandoff(ctx, getProject(t, "p-ah-fr3"),
			getTask(t, "t-ah-fr3"), "implement", time.Now())
		require.NoError(t, err)
	}

	got := getTask(t, "t-ah-fr3")
	operatorNotes := 0
	for _, n := range got.Status.Notes {
		if n.Agent == agent.NoteAgentOperator {
			operatorNotes++
		}
	}
	require.Equal(t, 1, operatorNotes)
}
