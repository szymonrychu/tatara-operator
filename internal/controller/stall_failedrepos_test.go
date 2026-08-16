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

	// The agent's note is BACKDATED so that `after` below sits strictly between it
	// and the operator's addendum. metav1.Time is second-granular: written at
	// time.Now() the two notes land in the same second, no threshold can separate
	// them, and HasAgentHandoffNoteSince(opAt+delta) is then vacuously false
	// whatever Agent the addendum carries.
	agentNoteAt := time.Now().Add(-time.Hour)
	after := agentNoteAt.Add(time.Minute)

	task := getTask(t, "t-ah-fr")
	task.Status.PodStartedAt = &metav1.Time{Time: time.Now().Add(-2 * time.Hour)}
	task.Status.LastTurnFinalText = "eight of nine landed"
	task.Status.LastTurnPushedRepos = []string{"tatara-operator"}
	task.Status.LastTurnFailedRepos = []string{"tatara-cli"}
	task.Status.LastTurnReposTurnID = "turn-12"
	task.Status.Notes = []tatarav1alpha1.Note{{
		At: metav1.NewTime(agentNoteAt), Agent: "implement",
		Kind: agent.NoteKindHandoff, Body: "PR is open, CI green",
	}}
	require.NoError(t, k8sClient.Status().Update(ctx, task))

	r := newTaskReconciler(newFakeSession())
	_, err := r.stopAfterAgentHandoff(ctx, getProject(t, "p-ah-fr"),
		getTask(t, "t-ah-fr"), "implement", time.Now())
	require.NoError(t, err)

	got := getTask(t, "t-ah-fr")
	require.Empty(t, got.Status.LastTurnFailedRepos, "the payload is still spent, not kept")

	var named bool
	for _, n := range got.Status.Notes {
		if n.Agent == agent.NoteAgentOperator && strings.Contains(n.Body, "tatara-cli") {
			named = true
		}
	}
	require.True(t, named, "the next pod is never told repo tatara-cli's commits did not reach origin")
	require.Equal(t, 1, countOperatorNotes(got))

	// The REAL predicate, not a re-implementation of it: the operator's addendum
	// is kind=handoff, so if it were mistaken for an agent handoff every caller of
	// this - handoffNoteCount, the agentAskedSomething sites, agentAskedToBeStopped
	// itself - would see one handoff too many. The addendum is the ONLY note at or
	// after `after`, so this is false exactly when its Agent is NoteAgentOperator.
	require.True(t, agent.HasAgentHandoffNote(got))
	require.False(t, agent.HasAgentHandoffNoteSince(got, after),
		"the operator's own addendum must not read as an agent handoff")
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
		task.Status.LastTurnReposTurnID = "turn-5"
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

	require.Equal(t, 1, countOperatorNotes(getTask(t, "t-ah-fr3")))
}

// THE SAME REPOS FAILING ON A LATER TURN IS A NEW LOSS, NOT A REPLAY.
//
// failedReposSentence is a pure function of the repo list, so a persistent
// rejection - branch protection, auth, a diverged branch - renders a
// byte-identical body every time it recurs. Deduping on the prose alone would
// match the note written many turns ago, skip the write, and let the
// unconditional clearLastTurn below it destroy the report it just suppressed:
// the surviving note would sit behind several agent notes saying the work
// landed, while its own text claims "the LAST turn's commit/push FAILED".
func TestStopAfterAgentHandoff_RecordsTheSameReposFailingOnALaterTurn(t *testing.T) {
	ctx := context.Background()
	mkTaskProject(t, "p-ah-fr4", 3)
	mkTaskRepository(t, "r-ah-fr4", "p-ah-fr4")
	mkTask(t, "t-ah-fr4", "p-ah-fr4", "r-ah-fr4")

	arm := func(turnID string) {
		task := getTask(t, "t-ah-fr4")
		task.Status.PodStartedAt = &metav1.Time{Time: time.Now().Add(-time.Hour)}
		task.Status.LastTurnFailedRepos = []string{"tatara-cli"}
		task.Status.LastTurnReposTurnID = turnID
		task.Status.Notes = append(task.Status.Notes, tatarav1alpha1.Note{
			At: metav1.NewTime(time.Now()), Agent: "implement",
			Kind: agent.NoteKindHandoff, Body: "PR is open, CI green",
		})
		require.NoError(t, k8sClient.Status().Update(ctx, task))
	}

	r := newTaskReconciler(newFakeSession())
	for _, turnID := range []string{"turn-5", "turn-12"} {
		arm(turnID)
		_, err := r.stopAfterAgentHandoff(ctx, getProject(t, "p-ah-fr4"),
			getTask(t, "t-ah-fr4"), "implement", time.Now())
		require.NoError(t, err)
	}

	require.Equal(t, 2, countOperatorNotes(getTask(t, "t-ah-fr4")),
		"turn 12's loss is recorded nowhere if turn 5's note suppresses it")
}

// THE DEDUPE MUST DECIDE AGAINST THE OBJECT VERSION IT MUTATES.
//
// The caller's Task snapshot is captured long before the append on the TTL path
// (waitIdle + SubmitHandoffTurn + waitHandoffNote all run in between), so a scan
// over that snapshot can miss a note that is already stored and write a
// duplicate. Passing a snapshot that predates the stored note is the
// deterministic form of that race.
func TestAppendFailedReposNote_DedupesAgainstTheStoredTaskNotTheCallersSnapshot(t *testing.T) {
	ctx := context.Background()
	mkTaskProject(t, "p-ah-fr5", 3)
	mkTaskRepository(t, "r-ah-fr5", "p-ah-fr5")
	mkTask(t, "t-ah-fr5", "p-ah-fr5", "r-ah-fr5")

	stale := getTask(t, "t-ah-fr5")
	stale.Status.LastTurnFailedRepos = []string{"tatara-cli"}
	stale.Status.LastTurnReposTurnID = "turn-5"

	notes := &agent.FitNoteAppender{
		Client: k8sClient, Spiller: nil, Namespace: stale.Namespace,
	}
	// First write lands on the stored object; `stale` never learns about it.
	agent.AppendFailedReposNote(ctx, notes, stale, stale.Status.LastTurnFailedRepos,
		stale.Status.LastTurnReposTurnID, time.Now())
	require.Equal(t, 1, countOperatorNotes(getTask(t, "t-ah-fr5")))

	agent.AppendFailedReposNote(ctx, notes, stale, stale.Status.LastTurnFailedRepos,
		stale.Status.LastTurnReposTurnID, time.Now())
	require.Equal(t, 1, countOperatorNotes(getTask(t, "t-ah-fr5")))
}

func countOperatorNotes(t *tatarav1alpha1.Task) int {
	n := 0
	for _, note := range t.Status.Notes {
		if note.Agent == agent.NoteAgentOperator {
			n++
		}
	}
	return n
}
