package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
)

// The three TTL stop outcomes (contract G.7), the values of the outcome label
// on operator_agent_pod_ttl_expired_total. Task.status.notes is NEVER empty
// after ANY of them - either the agent wrote its handoff, or the operator wrote
// a synthetic one from the last turn's finalText + pushedRepos.
const (
	// TTLOutcomeAgentHandoff: the agent answered the handoff turn and wrote a
	// kind=handoff note of its own.
	TTLOutcomeAgentHandoff = "agent_handoff"
	// TTLOutcomeSyntheticHandoff: the handoff turn was refused (410/409/5xx) or
	// never landed; the operator wrote the synthetic note and the pod stopped
	// cleanly.
	TTLOutcomeSyntheticHandoff = "synthetic_handoff"
	// TTLOutcomeForceDeleted: as synthetic_handoff, and the pod had to be
	// force-deleted (the graceful session stop failed, or the hard cap fired).
	TTLOutcomeForceDeleted = "force_deleted"
)

// NoteKindHandoff is the Note.Kind the handoff turn asks the agent to write and
// the operator writes for it when the agent cannot. Notes ARE the continuation
// state: a TTL stop that leaves notes empty makes the next pod start from
// nothing, redo the work, and burn maxTurns.
const NoteKindHandoff = "handoff"

// NoteAgentOperator is the ONE Note.Agent value an agent can never produce. The
// synthetic handoff is the operator's own note, and it says so.
const NoteAgentOperator = "operator"

// HandoffTurnText is the G.7 step-3 turn, verbatim.
const HandoffTurnText = "Your pod is being stopped. Call task_note(kind=handoff) with everything the next pod needs, then stop."

// TTLGrace is the slack added on top of 2*turnTimeoutSeconds when computing the
// G.7 step-4 hard cap.
const TTLGrace = 60 * time.Second

// TTLPollInterval is how often the stop sequence re-reads the wrapper session
// and the Task's notes while waiting.
const TTLPollInterval = 5 * time.Second

// NoteAppender appends one Note to a Task's status journal. Production wires
// FitNoteAppender, which routes the write through the A.7 byte-budget guard.
type NoteAppender interface {
	AppendNote(ctx context.Context, taskName string, n tatarav1alpha1.Note) error
}

// FitNoteAppender is the production NoteAppender: every note lands via
// objbudget.FitTask, so an over-budget Task spills its oldest notes to
// tatara-memory instead of blowing the etcd object limit.
type FitNoteAppender struct {
	Client    client.Client
	Spiller   objbudget.Spiller
	Namespace string
}

func (a *FitNoteAppender) AppendNote(ctx context.Context, taskName string, n tatarav1alpha1.Note) error {
	return objbudget.FitTask(ctx, a.Client, a.Spiller,
		types.NamespacedName{Namespace: a.Namespace, Name: taskName},
		func(t *tatarav1alpha1.Task) {
			t.Status.Notes = append(t.Status.Notes, n)
		})
}

var _ NoteAppender = (*FitNoteAppender)(nil)

// TTLDeadline is G.7's t0 = podStartedAt + agentPodTTLSeconds. ok is false when
// the project sets no TTL, or when the Task has no podStartedAt - a Task that
// has not been admitted has no pod, and therefore no pod clock.
//
// t0 is only correct if podStartedAt is FRESH. A stale podStartedAt carried
// across a stage transition puts t0 in the past for a pod that has just started,
// and the operator TTL-stops it before its first turn. stage.Enter nils both pod
// timestamps on every transition; the pod-CREATE stamp re-arms this one.
func TTLDeadline(project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (time.Time, bool) {
	ttl := project.Spec.AgentPodTTLSeconds
	if ttl <= 0 || task.Status.PodStartedAt == nil {
		return time.Time{}, false
	}
	return task.Status.PodStartedAt.Add(time.Duration(ttl) * time.Second), true
}

// TTLExpired reports whether the Task's pod is past t0.
func TTLExpired(project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, now time.Time) bool {
	t0, ok := TTLDeadline(project, task)
	if !ok {
		return false
	}
	return now.After(t0)
}

// TTLStopInput is the per-stop context the operator supplies. LastFinalText and
// PushedRepos come off the most recent turn-complete payload; they are what the
// synthetic handoff note is BUILT from, which is why pushedRepos is retained on
// the wire (G.2): without it the operator cannot tell "no diff" from "forgot to
// push" on a multi-repo Task.
type TTLStopInput struct {
	BaseURL     string
	CallbackURL string
	// AgentKind labels the TTL metric.
	AgentKind string
	// Deadline is t0.
	Deadline time.Time
	// TurnTimeout is Project.spec.agent.turnTimeoutSeconds.
	TurnTimeout   time.Duration
	LastFinalText string
	PushedRepos   []string
}

// TTLStopper drives the G.7 stop sequence for one pod.
type TTLStopper struct {
	Client    client.Client
	Session   Session
	Notes     NoteAppender
	Namespace string
	// Record is the operator_agent_pod_ttl_expired_total hook
	// (obs.AgentPodTTLExpired). Optional.
	Record func(agentKind, outcome string)
	// Now and Sleep are injectable so the sequence is testable without wall time.
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
	// Poll overrides TTLPollInterval.
	Poll time.Duration
}

func (s *TTLStopper) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *TTLStopper) sleep(ctx context.Context, d time.Duration) error {
	if s.Sleep != nil {
		return s.Sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (s *TTLStopper) poll() time.Duration {
	if s.Poll > 0 {
		return s.Poll
	}
	return TTLPollInterval
}

// StopWithHandoff runs the G.7 stop sequence and returns the outcome
// (agent_handoff | synthetic_handoff | force_deleted).
//
//	t0 = podStartedAt + agentPodTTLSeconds
//
//	1. The wrapper has already stopped admitting NORMAL turns (410 Gone past t0).
//	   It still admits EXACTLY ONE turn with handoff=true.
//	2. Wait for the in-flight turn's completion, bounded by turnTimeoutSeconds.
//	   (A pod is mid-turn at TTL expiry essentially always, and POST /v1/messages
//	   409s while a turn is in flight - which is why the handoff cannot simply be
//	   submitted.)
//	3. Submit the handoff turn, handoff=true, bounded by turnTimeoutSeconds.
//	4. Hard cap at t0 + 2*turnTimeoutSeconds + 60s. On the cap, or on any
//	   410/409/5xx from step 3, write the SYNTHETIC note IN-PROCESS and
//	   force-delete the pod.
//	5. The pod is stopped; the caller frees the slot and rolls up the stats.
//
// The Task's notes are non-empty on return in ALL THREE outcomes. That is the
// property the whole mechanism exists for.
func (s *TTLStopper) StopWithHandoff(ctx context.Context, task *tatarav1alpha1.Task, in TTLStopInput) (string, error) {
	hardCap := in.Deadline.Add(2*in.TurnTimeout + TTLGrace)

	before, err := s.handoffNoteCount(ctx, task.Name)
	if err != nil {
		return "", err
	}

	// Step 2: wait out the in-flight turn, bounded by turnTimeoutSeconds and by
	// the hard cap.
	waitUntil := earliest(s.now().Add(in.TurnTimeout), hardCap)
	turnCleared := s.waitIdle(ctx, in.BaseURL, waitUntil)

	// Step 3: submit THE handoff turn - the one turn the wrapper still admits past
	// t0. A refusal (410/409/5xx) or a hard-cap breach goes straight to the
	// synthetic note; the notes are never left empty.
	if turnCleared && s.now().Before(hardCap) {
		_, serr := s.Session.SubmitHandoffTurn(ctx, in.BaseURL, HandoffTurnText, in.CallbackURL)
		if serr == nil {
			deadline := earliest(s.now().Add(in.TurnTimeout), hardCap)
			if s.waitHandoffNote(ctx, task.Name, before, deadline) {
				return s.finish(ctx, task, in, TTLOutcomeAgentHandoff)
			}
		}
	}

	// Step 4: the operator writes the handoff the agent could not.
	if err := s.writeSyntheticNote(ctx, task, in); err != nil {
		return "", err
	}
	return s.finish(ctx, task, in, TTLOutcomeSyntheticHandoff)
}

// finish stops the pod and records the outcome. A graceful stop that fails - or
// an outcome reached past the hard cap - escalates to a force-delete, which is
// its own outcome. The synthetic note has already landed by then, so notes are
// non-empty in that case too.
func (s *TTLStopper) finish(ctx context.Context, task *tatarav1alpha1.Task, in TTLStopInput, outcome string) (string, error) {
	if err := s.Session.DeleteSession(ctx, in.BaseURL); err != nil {
		if ferr := s.forceDeletePod(ctx, task); ferr != nil {
			return "", ferr
		}
		outcome = TTLOutcomeForceDeleted
	} else if err := DeleteWrapper(ctx, s.Client, s.Namespace, task); err != nil {
		if ferr := s.forceDeletePod(ctx, task); ferr != nil {
			return "", ferr
		}
		outcome = TTLOutcomeForceDeleted
	}
	if s.Record != nil {
		s.Record(in.AgentKind, outcome)
	}
	return outcome, nil
}

// forceDeletePod deletes the wrapper Pod with a zero grace period. A pod whose
// wrapper is wedged mid-turn will not honour SIGTERM; the TTL is a hard bound.
func (s *TTLStopper) forceDeletePod(ctx context.Context, task *tatarav1alpha1.Task) error {
	grace := int64(0)
	pod := &corev1.Pod{}
	pod.Name = PodName(task)
	pod.Namespace = s.Namespace
	if err := s.Client.Delete(ctx, pod, &client.DeleteOptions{GracePeriodSeconds: &grace}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("agent: force-delete wrapper pod: %w", err)
	}
	svc := &corev1.Service{}
	svc.Name = PodName(task)
	svc.Namespace = s.Namespace
	if err := s.Client.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("agent: force-delete wrapper service: %w", err)
	}
	return nil
}

// waitIdle polls GET /v1/session until the wrapper reports no turn in flight, or
// until deadline. It returns false when the turn never cleared: the operator
// then skips the handoff turn (POST /v1/messages would 409 anyway) and goes
// straight to the synthetic note.
func (s *TTLStopper) waitIdle(ctx context.Context, baseURL string, deadline time.Time) bool {
	for {
		info, err := s.Session.GetSession(ctx, baseURL)
		// An unreachable or dead wrapper is not going to finish its turn, and it is
		// certainly not going to take the handoff turn.
		if err != nil {
			return false
		}
		if info.State == SessionStateDead {
			return false
		}
		if !info.TurnInFlight() {
			return true
		}
		if !s.now().Add(s.poll()).Before(deadline) {
			return false
		}
		if err := s.sleep(ctx, s.poll()); err != nil {
			return false
		}
	}
}

// waitHandoffNote polls the Task until a NEW kind=handoff note appears (the
// agent answered the handoff turn), or until deadline.
func (s *TTLStopper) waitHandoffNote(ctx context.Context, taskName string, before int, deadline time.Time) bool {
	for {
		n, err := s.handoffNoteCount(ctx, taskName)
		if err == nil && n > before {
			return true
		}
		if !s.now().Add(s.poll()).Before(deadline) {
			return false
		}
		if err := s.sleep(ctx, s.poll()); err != nil {
			return false
		}
	}
}

func (s *TTLStopper) handoffNoteCount(ctx context.Context, taskName string) (int, error) {
	fresh := &tatarav1alpha1.Task{}
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: taskName}, fresh); err != nil {
		return 0, fmt.Errorf("agent: reload task %s for handoff notes: %w", taskName, err)
	}
	n := 0
	for _, note := range fresh.Status.Notes {
		if note.Kind == NoteKindHandoff && note.Agent != NoteAgentOperator {
			n++
		}
	}
	return n, nil
}

// writeSyntheticNote is G.7 step 4: the operator's own handoff note, built from
// the last turn's final text and the repos the agent pushed. It is the ONLY
// thing standing between a TTL stop and an empty notes journal.
func (s *TTLStopper) writeSyntheticNote(ctx context.Context, task *tatarav1alpha1.Task, in TTLStopInput) error {
	pushed := "none"
	if len(in.PushedRepos) > 0 {
		pushed = strings.Join(in.PushedRepos, ", ")
	}
	final := strings.TrimSpace(in.LastFinalText)
	if final == "" {
		final = "(none)"
	}
	body := fmt.Sprintf("TTL stop. Last turn's final text: %s. Repos pushed: %s. No agent handoff was captured.", final, pushed)
	n := tatarav1alpha1.Note{
		At:    metav1.NewTime(s.now()),
		Agent: NoteAgentOperator,
		Kind:  NoteKindHandoff,
		Body:  truncateNoteBody(body),
	}
	if err := s.Notes.AppendNote(ctx, task.Name, n); err != nil {
		return fmt.Errorf("agent: write synthetic handoff note: %w", err)
	}
	return nil
}

// maxNoteBody is the Note.Body CRD MaxLength. A long finalText must not make the
// synthetic note unwritable - an over-long note that the API server rejects is
// an EMPTY notes journal, the exact failure this whole path exists to prevent.
const maxNoteBody = 4096

func truncateNoteBody(s string) string {
	if len(s) <= maxNoteBody {
		return s
	}
	const ellipsis = "...(truncated)"
	return s[:maxNoteBody-len(ellipsis)] + ellipsis
}

func earliest(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// IsSessionGone reports whether err means the wrapper session is finished for
// good: 410 Gone (past t0) or an already-deleted session (404).
func IsSessionGone(err error) bool {
	if IsTTLGone(err) {
		return true
	}
	var he *HTTPError
	if errors.As(err, &he) {
		return he.Status == 404
	}
	return false
}
