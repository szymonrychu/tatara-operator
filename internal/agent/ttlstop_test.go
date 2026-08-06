package agent_test

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

const ttlNS = "tatara"

func ttlScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(s))
	require.NoError(t, tatarav1alpha1.AddToScheme(s))
	return s
}

// stopSession is a scriptable Session for the G.7 stop sequence. states is
// consumed one entry per GetSession call (the last entry repeats); handoffErr is
// what SubmitHandoffTurn returns.
type stopSession struct {
	mu             sync.Mutex
	states         []string
	getCalls       int
	handoffErr     error
	handoffs       int
	normalTurns    int
	deleteErr      error
	deleteSessions int
	// onHandoff runs after a successful SubmitHandoffTurn: the agent's side of
	// the turn (it writes its own handoff note).
	onHandoff func()
	// onDeleteSession runs at the start of DeleteSession, before deleteErr is
	// returned: the wrapper's side of a teardown that races the pod going away.
	onDeleteSession func()
}

func (s *stopSession) SubmitTurn(context.Context, string, string, string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.normalTurns++
	return "normal", nil
}

func (s *stopSession) SubmitHandoffTurn(_ context.Context, _, text, _ string) (string, error) {
	s.mu.Lock()
	s.handoffs++
	err := s.handoffErr
	cb := s.onHandoff
	s.mu.Unlock()
	if err != nil {
		return "", err
	}
	if text != agent.HandoffTurnText {
		return "", nil
	}
	if cb != nil {
		cb()
	}
	return "handoff-1", nil
}

func (s *stopSession) Interject(context.Context, string, string) error { return nil }

func (s *stopSession) GetTurn(context.Context, string, string) (agent.TurnResult, error) {
	return agent.TurnResult{}, nil
}

func (s *stopSession) GetSession(context.Context, string) (agent.SessionInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := agent.SessionStateReady
	if len(s.states) > 0 {
		if s.getCalls < len(s.states) {
			st = s.states[s.getCalls]
		} else {
			st = s.states[len(s.states)-1]
		}
	}
	s.getCalls++
	v := agent.ContractVersion
	return agent.SessionInfo{State: st, ContractVersion: &v}, nil
}

func (s *stopSession) DeleteSession(context.Context, string) error {
	s.mu.Lock()
	s.deleteSessions++
	err := s.deleteErr
	cb := s.onDeleteSession
	s.mu.Unlock()
	if cb != nil {
		cb()
	}
	return err
}

// directNoteAppender writes notes straight onto the Task status. The production
// appender routes through objbudget.FitTask; this one keeps the TTL tests free of
// a Spiller.
type directNoteAppender struct {
	c  client.Client
	ns string
}

func (a *directNoteAppender) AppendNote(ctx context.Context, taskName string, n tatarav1alpha1.Note) error {
	fresh := &tatarav1alpha1.Task{}
	if err := a.c.Get(ctx, types.NamespacedName{Namespace: a.ns, Name: taskName}, fresh); err != nil {
		return err
	}
	fresh.Status.Notes = append(fresh.Status.Notes, n)
	return a.c.Status().Update(ctx, fresh)
}

type ttlHarness struct {
	c       client.Client
	sess    *stopSession
	stopper *agent.TTLStopper
	task    *tatarav1alpha1.Task
	now     time.Time
	outcome string
	handoff string
}

func newTTLHarness(t *testing.T, sess *stopSession) *ttlHarness {
	t.Helper()
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	podStart := metav1.NewTime(now.Add(-1 * time.Hour))
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-ttl", Namespace: ttlNS, UID: "uid-1"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "demo", Kind: "implement"},
		Status: tatarav1alpha1.TaskStatus{
			Stage:        tatarav1alpha1.StageImplementing,
			AgentKind:    "implement",
			PodStartedAt: &podStart,
		},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: agent.PodName(task), Namespace: ttlNS}}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: agent.PodName(task), Namespace: ttlNS}}

	c := fake.NewClientBuilder().
		WithScheme(ttlScheme(t)).
		WithStatusSubresource(&tatarav1alpha1.Task{}).
		WithObjects(task.DeepCopy(), pod, svc).
		Build()

	h := &ttlHarness{c: c, sess: sess, task: task, now: now}
	h.stopper = &agent.TTLStopper{
		Client:    c,
		Session:   sess,
		Notes:     &directNoteAppender{c: c, ns: ttlNS},
		Namespace: ttlNS,
		Poll:      10 * time.Millisecond,
		Now:       func() time.Time { return h.now },
		Sleep: func(context.Context, time.Duration) error {
			h.now = h.now.Add(10 * time.Millisecond)
			return nil
		},
		Record: func(_, outcome, handoff string) { h.outcome, h.handoff = outcome, handoff },
	}
	return h
}

func (h *ttlHarness) input() agent.TTLStopInput {
	return agent.TTLStopInput{
		BaseURL:       "http://wrapper",
		CallbackURL:   "http://cb",
		AgentKind:     "implement",
		Deadline:      h.now.Add(-1 * time.Minute), // t0 has already passed
		TurnTimeout:   30 * time.Second,
		LastFinalText: "wired the reconciler, tests still red",
		PushedRepos:   []string{"tatara-operator", "tatara-cli"},
	}
}

func (h *ttlHarness) notes(t *testing.T) []tatarav1alpha1.Note {
	t.Helper()
	fresh := &tatarav1alpha1.Task{}
	require.NoError(t, h.c.Get(context.Background(), types.NamespacedName{Namespace: ttlNS, Name: "task-ttl"}, fresh))
	return fresh.Status.Notes
}

func (h *ttlHarness) podExists(t *testing.T) bool {
	t.Helper()
	pod := &corev1.Pod{}
	err := h.c.Get(context.Background(), types.NamespacedName{Namespace: ttlNS, Name: agent.PodName(h.task)}, pod)
	return err == nil
}

// TestTTLDeadline: t0 = podStartedAt + agentPodTTLSeconds, and it is armed ONLY
// when podStartedAt is set. A Task that has not been admitted has no pod and
// therefore no pod clock.
func TestTTLDeadline(t *testing.T) {
	start := metav1.NewTime(time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC))
	proj := &tatarav1alpha1.Project{Spec: tatarav1alpha1.ProjectSpec{AgentPodTTLSeconds: 3600}}
	task := &tatarav1alpha1.Task{Status: tatarav1alpha1.TaskStatus{PodStartedAt: &start}}

	t0, ok := agent.TTLDeadline(proj, task)
	require.True(t, ok)
	require.Equal(t, start.Add(time.Hour), t0)
	require.False(t, agent.TTLExpired(proj, task, start.Add(59*time.Minute)))
	require.True(t, agent.TTLExpired(proj, task, start.Add(61*time.Minute)))

	// No podStartedAt: the Task is on CLOCK 1, not the TTL. It must never TTL-stop.
	unadmitted := &tatarav1alpha1.Task{}
	_, ok = agent.TTLDeadline(proj, unadmitted)
	require.False(t, ok)
	require.False(t, agent.TTLExpired(proj, unadmitted, start.Add(100*time.Hour)))

	// No TTL configured: never expires.
	noTTL := &tatarav1alpha1.Project{}
	_, ok = agent.TTLDeadline(noTTL, task)
	require.False(t, ok)
}

// TestTTLStop_AgentHandoff is outcome 1: the wrapper admits the ONE handoff turn
// past t0, the agent answers it, and its own note is the continuation state.
func TestTTLStop_AgentHandoff(t *testing.T) {
	sess := &stopSession{states: []string{agent.SessionStateBusy, agent.SessionStateReady}}
	h := newTTLHarness(t, sess)
	sess.onHandoff = func() {
		fresh := &tatarav1alpha1.Task{}
		require.NoError(t, h.c.Get(context.Background(), types.NamespacedName{Namespace: ttlNS, Name: "task-ttl"}, fresh))
		fresh.Status.Notes = append(fresh.Status.Notes, tatarav1alpha1.Note{
			At: metav1.NewTime(h.now), Agent: "implement", Kind: "handoff",
			Body: "PR #12 is open; rebase onto main and re-run the merge gate.",
		})
		require.NoError(t, h.c.Status().Update(context.Background(), fresh))
	}

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, h.input())
	require.NoError(t, err)
	require.Equal(t, agent.TTLOutcomeGraceful, res.Outcome)
	require.Equal(t, agent.TTLHandoffAgent, res.Handoff)
	require.Equal(t, agent.TTLOutcomeGraceful, h.outcome, "the TTL metric must be recorded")
	require.Equal(t, agent.TTLHandoffAgent, h.handoff)

	require.Equal(t, 1, sess.handoffs, "EXACTLY ONE handoff turn is admitted past t0")
	require.Zero(t, sess.normalTurns, "no NORMAL turn may be submitted past t0")

	notes := h.notes(t)
	require.NotEmpty(t, notes, "Task.status.notes is NEVER empty after a TTL stop")
	require.Equal(t, "handoff", notes[0].Kind)
	require.Equal(t, "implement", notes[0].Agent)
	require.False(t, h.podExists(t))
}

// TestTTLStop_SyntheticHandoff_On410 is outcome 2 and the sharp edge: a wrapper
// that 410s the handoff turn must STILL produce a note. v5's G.7 was
// self-refuting - step 1 refused turns past t0 and step 3 submitted one - and
// taken literally it left notes EMPTY after every TTL stop, which is the
// continuation state gone.
func TestTTLStop_SyntheticHandoff_On410(t *testing.T) {
	sess := &stopSession{
		states:     []string{agent.SessionStateReady},
		handoffErr: &agent.HTTPError{Status: http.StatusGone, Body: "past ttl"},
	}
	h := newTTLHarness(t, sess)

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, h.input())
	require.NoError(t, err)
	require.Equal(t, agent.TTLHandoffSynthetic, res.Handoff)
	require.Equal(t, agent.TTLHandoffSynthetic, h.handoff)

	notes := h.notes(t)
	require.Len(t, notes, 1, "a 410'd handoff turn must STILL produce the synthetic note, not an empty notes")
	require.Equal(t, "operator", notes[0].Agent)
	require.Equal(t, "handoff", notes[0].Kind)
	require.Contains(t, notes[0].Body, "TTL stop.")
	require.Contains(t, notes[0].Body, "wired the reconciler, tests still red")
	require.Contains(t, notes[0].Body, "tatara-operator, tatara-cli", "the note is BUILT from pushedRepos")
	require.Contains(t, notes[0].Body, "No agent handoff was captured.")
	require.False(t, h.podExists(t))
}

// TestTTLStop_SyntheticHandoff_OnStuckTurn: the in-flight turn never completes
// within turnTimeoutSeconds, so the handoff turn would 409. The operator does not
// even try it, and writes the synthetic note.
func TestTTLStop_SyntheticHandoff_OnStuckTurn(t *testing.T) {
	sess := &stopSession{states: []string{agent.SessionStateBusy}} // busy forever
	h := newTTLHarness(t, sess)

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, h.input())
	require.NoError(t, err)
	require.Equal(t, agent.TTLHandoffSynthetic, res.Handoff)
	require.Zero(t, sess.handoffs, "no handoff turn while a turn is in flight: POST /v1/messages 409s")
	require.NotEmpty(t, h.notes(t), "Task.status.notes is NEVER empty after a TTL stop")
}

// TestTTLStop_ForceDeleted is outcome 3: the wrapper will not stop cleanly, so
// the pod is force-deleted. The synthetic note has ALREADY landed, so the notes
// are non-empty here too.
func TestTTLStop_ForceDeleted(t *testing.T) {
	sess := &stopSession{
		states:     []string{agent.SessionStateReady},
		handoffErr: &agent.HTTPError{Status: http.StatusInternalServerError},
		deleteErr:  &agent.UnreachableError{},
	}
	h := newTTLHarness(t, sess)

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, h.input())
	require.NoError(t, err)
	require.Equal(t, agent.TTLOutcomeForceDeleted, res.Outcome)
	require.Equal(t, agent.TTLOutcomeForceDeleted, h.outcome)

	notes := h.notes(t)
	require.NotEmpty(t, notes, "Task.status.notes is NEVER empty after a TTL stop")
	require.Equal(t, "operator", notes[0].Agent)
	require.Equal(t, "handoff", notes[0].Kind)
	require.False(t, h.podExists(t))
}

// TestTTLStop_NotesNeverEmpty_EveryDimensionPair is the MANDATORY regression
// test. Notes ARE the continuation state; a TTL stop that leaves them empty
// makes the next pod start from nothing, redo the work, and burn maxTurns.
// Assert the property DIRECTLY, over the full cross-product of the two
// dimensions the stop now reports, not by proxy.
//
// The pairing matters as much as the property: `handoff` and `outcome` vary
// INDEPENDENTLY, and the whole point of #527 is that the old single label could
// not express that.
func TestTTLStop_NotesNeverEmpty_EveryDimensionPair(t *testing.T) {
	cases := []struct {
		name        string
		handoffErr  error
		deleteErr   error
		agentWrote  bool
		emptyInput  bool
		wantOutcome string
		wantHandoff string
	}{
		{
			name: "agent handoff, graceful stop", agentWrote: true,
			wantOutcome: agent.TTLOutcomeGraceful, wantHandoff: agent.TTLHandoffAgent,
		},
		{
			name: "agent handoff, forced stop", agentWrote: true,
			deleteErr:   &agent.UnreachableError{},
			wantOutcome: agent.TTLOutcomeForceDeleted, wantHandoff: agent.TTLHandoffAgent,
		},
		{
			name:        "synthetic handoff, graceful stop",
			handoffErr:  &agent.HTTPError{Status: http.StatusGone},
			wantOutcome: agent.TTLOutcomeGraceful, wantHandoff: agent.TTLHandoffSynthetic,
		},
		{
			name:        "synthetic handoff, forced stop",
			handoffErr:  &agent.HTTPError{Status: http.StatusBadGateway},
			deleteErr:   &agent.UnreachableError{},
			wantOutcome: agent.TTLOutcomeForceDeleted, wantHandoff: agent.TTLHandoffSynthetic,
		},
		{
			name:       "no handoff captured, graceful stop",
			handoffErr: &agent.HTTPError{Status: http.StatusGone}, emptyInput: true,
			wantOutcome: agent.TTLOutcomeGraceful, wantHandoff: agent.TTLHandoffNone,
		},
		{
			name:       "no handoff captured, forced stop",
			handoffErr: &agent.HTTPError{Status: http.StatusGone}, emptyInput: true,
			deleteErr:   &agent.UnreachableError{},
			wantOutcome: agent.TTLOutcomeForceDeleted, wantHandoff: agent.TTLHandoffNone,
		},
	}
	require.Len(t, cases, 6, "both stop outcomes x all three handoff kinds must be covered")

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sess := &stopSession{
				states:     []string{agent.SessionStateReady},
				handoffErr: tc.handoffErr,
				deleteErr:  tc.deleteErr,
			}
			h := newTTLHarness(t, sess)
			if tc.agentWrote {
				sess.onHandoff = func() {
					fresh := &tatarav1alpha1.Task{}
					require.NoError(t, h.c.Get(context.Background(), types.NamespacedName{Namespace: ttlNS, Name: "task-ttl"}, fresh))
					fresh.Status.Notes = append(fresh.Status.Notes, tatarav1alpha1.Note{
						At: metav1.NewTime(h.now), Agent: "implement", Kind: "handoff", Body: "next pod: rebase onto main",
					})
					require.NoError(t, h.c.Status().Update(context.Background(), fresh))
				}
			}
			in := h.input()
			if tc.emptyInput {
				in.LastFinalText, in.PushedRepos = "", nil
			}

			res, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
			require.NoError(t, err)
			require.Equal(t, tc.wantOutcome, res.Outcome)
			require.Equal(t, tc.wantHandoff, res.Handoff)

			notes := h.notes(t)
			require.NotEmpty(t, notes, "%s left Task.status.notes EMPTY: the continuation state is gone", tc.name)
			require.Equal(t, "handoff", notes[len(notes)-1].Kind)
			require.NotEmpty(t, notes[len(notes)-1].Body)
		})
	}
}

// TestTTLStop_SyntheticNoteIsTruncated: a long finalText must not make the note
// unwritable. A note the API server rejects on MaxLength is an EMPTY notes
// journal, the exact failure this path exists to prevent.
func TestTTLStop_SyntheticNoteIsTruncated(t *testing.T) {
	sess := &stopSession{
		states:     []string{agent.SessionStateReady},
		handoffErr: &agent.HTTPError{Status: http.StatusGone},
	}
	h := newTTLHarness(t, sess)
	in := h.input()
	in.LastFinalText = strings.Repeat("x", 9000)

	res, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
	require.NoError(t, err)
	require.Equal(t, agent.TTLHandoffSynthetic, res.Handoff)

	notes := h.notes(t)
	require.Len(t, notes, 1)
	require.LessOrEqual(t, len(notes[0].Body), 4096, "Note.Body has a CRD MaxLength of 4096")
}

// TestTTLStop_NoPushedRepos: "no diff" and "forgot to push" must be
// distinguishable in the handoff the next pod reads.
func TestTTLStop_NoPushedRepos(t *testing.T) {
	sess := &stopSession{
		states:     []string{agent.SessionStateReady},
		handoffErr: &agent.HTTPError{Status: http.StatusGone},
	}
	h := newTTLHarness(t, sess)
	in := h.input()
	in.PushedRepos = nil

	_, err := h.stopper.StopWithHandoff(context.Background(), h.task, in)
	require.NoError(t, err)
	require.Contains(t, h.notes(t)[0].Body, "Repos pushed: none.")
}
