package agent_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// A REPO THAT FAILED TO PUSH IS THE ONE THING THE SYNTHETIC NOTE MUST NOT DROP.
//
// tatara-claude-code-wrapper#167: the wrapper's turn-end loop no longer aborts on
// the first failing repo, so a turn can end having pushed most repos and lost
// one. The pod's workspace is ephemeral, so that repo's commits exist nowhere but
// on a disk that is about to disappear. The synthetic handoff note is the only
// place the next pod can learn it - a note that lists only what DID push reads as
// a complete turn.
func TestTTLStop_SyntheticNoteNamesTheReposThatFailedToPush(t *testing.T) {
	sess := &stopSession{
		states:     []string{agent.SessionStateReady},
		handoffErr: &agent.HTTPError{Status: http.StatusGone},
	}
	h := newTTLHarness(t, sess)
	in := h.input()
	in.PushedRepos = []string{"tatara-operator"}
	in.FailedRepos = []string{"tatara-cli"}

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
	require.NoError(t, err)
	require.Equal(t, agent.TTLHandoffSynthetic, res.Handoff)

	body := h.notes(t)[0].Body
	require.Contains(t, body, "tatara-cli",
		"the note must name the repo whose work never reached origin")
	require.Contains(t, body, "commit/push FAILED",
		"naming the repo is not enough; the note must say what happened to it")
}

// A turn whose ONLY content is a failed push is not content-free. It is the most
// consequential thing a turn can report, and rendering it as the placeholder
// would throw away the one fact worth carrying.
func TestTTLStop_FailedReposAloneIsNotAContentFreeNote(t *testing.T) {
	sess := &stopSession{
		states:     []string{agent.SessionStateReady},
		handoffErr: &agent.HTTPError{Status: http.StatusGone},
	}
	h := newTTLHarness(t, sess)
	in := h.input()
	in.LastFinalText, in.PushedRepos = "", nil
	in.FailedRepos = []string{"tatara-cli"}

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
	require.NoError(t, err)
	require.Equal(t, agent.TTLHandoffSynthetic, res.Handoff,
		"a lost push is continuation state, not the absence of it")

	body := h.notes(t)[0].Body
	require.NotContains(t, body, agent.SyntheticNoteLostMarker,
		"the placeholder marker trains the next agent to distrust the note; this one has something to say")
	require.Contains(t, body, "tatara-cli")
}

// THE MOST URGENT SENTENCE MUST NOT BE THE FIRST ONE TRUNCATED.
//
// status.lastTurnFinalText is capped at exactly NoteBodyMaxBytes and Note.Body
// is capped at the same number, so a maximal final text alone fills the entire
// note budget. A failed-push warning appended after it would be cut off in full
// - and it is the one line in the note describing work that no longer exists
// anywhere.
func TestTTLStop_FailedReposWarningSurvivesAMaximalFinalText(t *testing.T) {
	sess := &stopSession{
		states:     []string{agent.SessionStateReady},
		handoffErr: &agent.HTTPError{Status: http.StatusGone},
	}
	h := newTTLHarness(t, sess)
	in := h.input()
	in.LastFinalText = strings.Repeat("x", tatarav1alpha1.NoteBodyMaxBytes)
	in.FailedRepos = []string{"tatara-cli"}

	_, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
	require.NoError(t, err)

	body := h.notes(t)[0].Body
	require.LessOrEqual(t, len(body), tatarav1alpha1.NoteBodyMaxBytes, "Note.Body CRD MaxLength")
	require.Contains(t, body, "tatara-cli",
		"a long final text must lose ITS tail, not the warning about work that no longer exists")
}

// THE HEALTHY STOP PATH DISCARDED THE FAILED SET UNSPENT.
//
// When the agent answers the handoff turn, StopWithHandoff returns before
// writeSyntheticNote ever runs, and every caller then clears the last-turn state.
// The agent's own note cannot cover for it: HandoffTurnText is a fixed string and
// the wrapper reports the push failure to pod stdout only, which is not in the
// agent's context. Left alone, the one field that names LOST WORK would surface
// only on the paths that were already loud.
func TestTTLStop_AgentHandoffStillReportsTheFailedRepos(t *testing.T) {
	sess := &stopSession{states: []string{agent.SessionStateBusy, agent.SessionStateReady}}
	h := newTTLHarness(t, sess)
	sess.onHandoff = func() { appendAgentHandoffNote(t, h) }

	in := h.input()
	in.FailedRepos = []string{"tatara-cli"}

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
	require.NoError(t, err)
	require.Equal(t, agent.TTLHandoffAgent, res.Handoff,
		"the agent answered: the operator's addendum reports lost work, it does not claim the handoff")

	notes := h.notes(t)
	require.Len(t, notes, 2, "the agent's handoff plus the operator's failed-push addendum")
	require.Equal(t, "implement", notes[0].Agent, "the agent's own note must survive untouched")
	require.Equal(t, agent.NoteAgentOperator, notes[1].Agent)
	require.Contains(t, notes[1].Body, "tatara-cli",
		"the next pod learns about a lost repo here or nowhere")
}

// The addendum exists for the failed set and nothing else: an ordinary stop must
// read exactly as it did before, one note, written by the agent.
func TestTTLStop_AgentHandoffWritesNoAddendumWhenNothingFailed(t *testing.T) {
	sess := &stopSession{states: []string{agent.SessionStateBusy, agent.SessionStateReady}}
	h := newTTLHarness(t, sess)
	sess.onHandoff = func() { appendAgentHandoffNote(t, h) }

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, h.input())
	require.NoError(t, err)
	require.Equal(t, agent.TTLHandoffAgent, res.Handoff)
	require.Len(t, h.notes(t), 1)
}

// THE SAFETY PUSHER CAN LAND THOSE COMMITS AFTER THE TURN-END FAILURE.
//
// It pushes every repo each interval regardless of tree state, so a rejection at
// 12:00 that succeeds at 12:05 leaves the status field saying lost while the
// commits are on origin. An unconditional "redo it" then sends the next agent to
// redo work that already exists. The repo's convention for the same class of
// uncertainty is the OOM note: name it and tell the agent to VERIFY.
func TestTTLStop_FailedReposWarningTellsTheNextAgentToCheckOrigin(t *testing.T) {
	sess := &stopSession{
		states:     []string{agent.SessionStateReady},
		handoffErr: &agent.HTTPError{Status: http.StatusGone},
	}
	h := newTTLHarness(t, sess)
	in := h.input()
	in.FailedRepos = []string{"tatara-cli"}

	_, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
	require.NoError(t, err)

	body := h.notes(t)[0].Body
	require.Contains(t, body, "origin",
		"the mid-turn safety pusher may have landed the commits after the turn-end failure")
	require.NotContains(t, body, "Treat that work as LOST and redo it",
		"an unconditional loss claim sends the next agent to redo work that may be on origin")
}

// A LONG REPO LIST MUST COST NAMES, NEVER THE DIRECTIVE.
//
// The warning is capped in its own right so the reservation cannot starve the
// note it is reserved out of. Cutting the RENDERED SENTENCE truncates inside the
// joined repo list and takes the trailing directive with it, leaving a note that
// names some lost repos and silently omits the rest. clampPushedRepos admits 20
// names with no per-name cap, so this is reachable.
func TestTTLStop_ALongFailedReposListNeverEatsTheDirective(t *testing.T) {
	sess := &stopSession{
		states:     []string{agent.SessionStateReady},
		handoffErr: &agent.HTTPError{Status: http.StatusGone},
	}
	h := newTTLHarness(t, sess)
	in := h.input()
	for i := range 20 {
		in.FailedRepos = append(in.FailedRepos, strings.Repeat(string(rune('a'+i)), 200))
	}

	_, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
	require.NoError(t, err)

	body := h.notes(t)[0].Body
	require.LessOrEqual(t, len(body), tatarav1alpha1.NoteBodyMaxBytes, "Note.Body CRD MaxLength")
	require.Contains(t, body, "origin", "the directive is the part of the warning that must never be cut")
	require.Contains(t, body, in.FailedRepos[0], "the first name must be rendered whole")
	require.Contains(t, body, "more)",
		"names that did not fit must be reported as elided, not silently dropped")
	require.True(t, strings.HasSuffix(body, "."),
		"a note ending mid-list is a note that lies about how many repos were lost")
}

// THE ADDENDUM MUST NEVER READ AS THE AGENT ASKING A QUESTION.
//
// It is a handoff-kind note the OPERATOR wrote, and every "did the agent say
// something" predicate keys on Agent != operator. If that ever regressed, a Task
// whose only note is this warning would park awaiting-human on a question nobody
// asked, waiting forever for a reply nobody owes.
func TestFailedReposAddendumIsNotAnAgentHandoff(t *testing.T) {
	sess := &stopSession{states: []string{agent.SessionStateBusy, agent.SessionStateReady}}
	h := newTTLHarness(t, sess)
	sess.onHandoff = func() { appendAgentHandoffNote(t, h) }

	in := h.input()
	in.FailedRepos = []string{"tatara-cli"}
	_, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
	require.NoError(t, err)

	addendum := h.notes(t)[1]
	only := &tatarav1alpha1.Task{Status: tatarav1alpha1.TaskStatus{Notes: []tatarav1alpha1.Note{addendum}}}
	require.False(t, agent.HasAgentHandoffNote(only),
		"the operator talking to the next pod is not the agent talking to a human")
}

// THE ADDENDUM NAMES ITS TURN, WITHIN THE SAME BUDGET.
//
// The turn id is what stops a persistent rejection - the same repos failing again
// many turns later - from rendering a body byte-identical to the older note and
// being swallowed by the idempotency scan. Unlike the synthetic note's copy, this
// body spends the WHOLE note budget on names, so the suffix has to be reserved
// out of that budget rather than appended after it: the apiserver rejects a
// Note.Body over MaxLength outright, and the write that is lost is the one naming
// work that exists nowhere else.
func TestFailedReposAddendumNamesItsTurnWithinTheNoteBudget(t *testing.T) {
	sess := &stopSession{states: []string{agent.SessionStateBusy, agent.SessionStateReady}}
	h := newTTLHarness(t, sess)
	sess.onHandoff = func() { appendAgentHandoffNote(t, h) }

	in := h.input()
	// Deliberately NOT a repeat of a letter the repo names below also use: the
	// Contains assertion has to be satisfiable by the suffix alone, or a later
	// change to the name length would let a repo name answer for it and the
	// mutation-kill would quietly stop working.
	in.ReposTurnID = "turn-" + strings.Repeat("Z", tatarav1alpha1.LastTurnReposTurnIDMaxBytes-5)
	for i := range 20 {
		in.FailedRepos = append(in.FailedRepos, strings.Repeat(string(rune('a'+i)), 200))
	}

	_, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
	require.NoError(t, err)

	body := h.notes(t)[1].Body
	require.LessOrEqual(t, len(body), tatarav1alpha1.NoteBodyMaxBytes, "Note.Body CRD MaxLength")
	require.Contains(t, body, in.ReposTurnID, "the turn id is what makes a recurrence distinguishable")
}

// An UNKNOWN turn - lists written by a binary that predates the field - renders
// no suffix rather than an empty one, so the body stays exactly what it was.
func TestFailedReposAddendumOmitsAnUnknownTurn(t *testing.T) {
	sess := &stopSession{states: []string{agent.SessionStateBusy, agent.SessionStateReady}}
	h := newTTLHarness(t, sess)
	sess.onHandoff = func() { appendAgentHandoffNote(t, h) }

	in := h.input()
	in.FailedRepos = []string{"tatara-cli"}
	_, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
	require.NoError(t, err)

	require.True(t, strings.HasSuffix(h.notes(t)[1].Body, "Repos: tatara-cli."),
		"an unknown turn must not leave a dangling turn clause")
}

// A name longer than the whole names budget renders as the count alone. Half a
// repo name is not a repo, so there is nothing else the note can honestly say -
// but it must still say the number, and it must not leave the gap where the
// names would have been.
func TestTTLStop_ASingleOversizedRepoNameStillReportsTheCount(t *testing.T) {
	sess := &stopSession{
		states:     []string{agent.SessionStateReady},
		handoffErr: &agent.HTTPError{Status: http.StatusGone},
	}
	h := newTTLHarness(t, sess)
	in := h.input()
	in.FailedRepos = []string{strings.Repeat("z", tatarav1alpha1.NoteBodyMaxBytes)}

	_, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
	require.NoError(t, err)

	body := h.notes(t)[0].Body
	require.LessOrEqual(t, len(body), tatarav1alpha1.NoteBodyMaxBytes)
	require.Contains(t, body, "Repos: (+1 more).",
		"the count is all that fits, and it must read as a sentence rather than a gap")
}

// appendAgentHandoffNote is the agent answering the G.7 step-3 handoff turn.
func appendAgentHandoffNote(t *testing.T, h *ttlHarness) {
	t.Helper()
	fresh := &tatarav1alpha1.Task{}
	require.NoError(t, h.c.Get(context.Background(),
		types.NamespacedName{Namespace: ttlNS, Name: "task-ttl"}, fresh))
	fresh.Status.Notes = append(fresh.Status.Notes, tatarav1alpha1.Note{
		At: metav1.NewTime(h.now), Agent: "implement", Kind: "handoff",
		Body: "PR #12 is open; rebase onto main and re-run the merge gate.",
	})
	require.NoError(t, h.c.Status().Update(context.Background(), fresh))
}
