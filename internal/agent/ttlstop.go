package agent

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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

// A G.7 TTL stop reports TWO INDEPENDENT FACTS, on two labels of
// operator_agent_pod_ttl_expired_total. They used to share one label, and the
// conflation cost ~19 days of silent work loss (issue #527): finish() overwrote
// the capture fact with the stop fact whenever teardown failed, so
// synthetic_handoff was structurally unreachable and never once recorded, while
// force_deleted - the value the alert fires on - could equally mean "the agent
// handed off perfectly and the wrapper was slow to die".
//
// TTLOutcome* answers: HOW WAS THE POD STOPPED?
const (
	// TTLOutcomeGraceful: the wrapper session closed and the pod came down on
	// its own. Says NOTHING about whether a handoff was captured.
	TTLOutcomeGraceful = "graceful"
	// TTLOutcomeForceDeleted: the graceful stop failed against a pod that was
	// still there, so it was deleted with a zero grace period. Says NOTHING about
	// whether a handoff was captured.
	TTLOutcomeForceDeleted = "force_deleted"
)

// TTLHandoff* answers: HOW WAS THE CONTINUATION STATE CAPTURED? This is the
// dimension that decides whether work was lost.
const (
	// TTLHandoffAgent: the agent answered the handoff turn and wrote a
	// kind=handoff note of its own. Continuity intact.
	TTLHandoffAgent = "agent"
	// TTLHandoffSynthetic: the handoff turn was refused (410/409/5xx), never
	// landed, or the wrapper was already gone; the operator wrote the note itself
	// from the last turn's finalText + pushedRepos. Continuity degraded but real.
	TTLHandoffSynthetic = "synthetic"
	// TTLHandoffNone: as synthetic, and the operator held NOTHING to write - no
	// finalText, no pushedRepos. The note that lands is a placeholder saying so.
	// THIS is silent work loss, and it is the bucket the alert belongs on.
	TTLHandoffNone = "none"
)

// TTLStopResult is one stop, on both dimensions.
type TTLStopResult struct {
	// Outcome is graceful | force_deleted.
	Outcome string
	// Handoff is agent | synthetic | none.
	Handoff string
}

// EnvAgentPodTTLSeconds is the wrapper env carrying this pod's own copy of the
// resolved TTL. The wrapper computes its 410-Gone cutoff from it, and the reaper
// reads it back off the pod spec (PodTTLDeadlineFromSpec).
const EnvAgentPodTTLSeconds = "AGENT_POD_TTL_SECONDS"

// EnvTurnTimeoutSeconds is the wrapper env carrying Project.spec.agent
// .turnTimeoutSeconds. The stop sequence's step-4 hard cap is derived from it,
// so the reaper reads it back off the pod spec to learn how long the stop can
// legitimately still hold this pod (PodTTLStopWindowFromSpec).
const EnvTurnTimeoutSeconds = "TURN_TIMEOUT_SECONDS"

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

// TTLDeadline is G.7's t0 = anchor + agentPodTTLSeconds. ok is false when the
// project sets no TTL, or when the Task has no podStartedAt - a Task that has
// not been admitted has no pod, and therefore no pod clock.
//
// anchor is podStartedAt, EXCEPT on a conversing Task with a
// conversationLastEventAt that postdates it: a maintainer's reply is exactly
// what stage.ArmedClock's idle clock resets on (see conversationLastEventAt's
// doc comment), and before this fix the pod-level TTL never learned about
// that reset - a reply landing at minute 50 of a 60-minute idle budget still
// rotated the live pod at the ORIGINAL podStartedAt+60m, discarding the reply
// window the maintainer was just promised (issue #508). Anchoring on
// whichever is LATER keeps this clock in lockstep with the one that already
// governs when the Task itself leaves conversing.
//
// t0 is only correct if podStartedAt is FRESH. A stale podStartedAt carried
// across a stage transition puts t0 in the past for a pod that has just started,
// and the operator TTL-stops it before its first turn. stage.Enter nils both pod
// timestamps on every transition; the pod-CREATE stamp re-arms this one.
func TTLDeadline(project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (time.Time, bool) {
	// PodTTLSeconds, never project.Spec.AgentPodTTLSeconds directly: the pod's
	// AGENT_POD_TTL_SECONDS env is stamped from the same resolver, and if these
	// two ever diverge the wrapper 410s turns the operator still believes are
	// admissible (or the operator stops a pod the wrapper is happily serving).
	ttl := PodTTLSeconds(project, task)
	anchor, ok := ttlAnchor(task)
	if ttl <= 0 || !ok {
		return time.Time{}, false
	}
	return anchor.Add(time.Duration(ttl) * time.Second), true
}

// ttlAnchor is the instant t0 is measured FROM. See TTLDeadline for why a
// conversing Task anchors on the later of podStartedAt and
// conversationLastEventAt.
func ttlAnchor(task *tatarav1alpha1.Task) (time.Time, bool) {
	if task.Status.PodStartedAt == nil {
		return time.Time{}, false
	}
	anchor := task.Status.PodStartedAt.Time
	if task.Status.Stage == tatarav1alpha1.StageConversing && task.Status.ConversationLastEventAt != nil &&
		task.Status.ConversationLastEventAt.After(anchor) {
		anchor = task.Status.ConversationLastEventAt.Time
	}
	return anchor, true
}

// PodTTLDeadlineFromSpec is TTLDeadline computed WITHOUT a Project, by reading
// the AGENT_POD_TTL_SECONDS the operator stamped on the pod itself (PodSpec
// sets it from the same PodTTLSeconds resolver). It exists for the reaper, which
// sweeps pods and Tasks and resolves no Projects, and which must be able to tell
// whether the G.7 stop already owns a pod's teardown (issue #527).
//
// ok is false when the pod carries no readable TTL: no TTL means no t0, no TTL
// stop is coming, and the caller must NOT treat the pod as spoken for.
func PodTTLDeadlineFromSpec(pod *corev1.Pod, task *tatarav1alpha1.Task) (time.Time, bool) {
	anchor, ok := ttlAnchor(task)
	if !ok {
		return time.Time{}, false
	}
	ttl, ok := podEnvSeconds(pod, EnvAgentPodTTLSeconds)
	if !ok || ttl <= 0 {
		return time.Time{}, false
	}
	return anchor.Add(ttl), true
}

// PodTTLStopWindowFromSpec is the interval [start, end) during which the G.7
// stop sequence OWNS this pod's teardown, computed from the pod's own env for a
// caller that resolves no Projects.
//
//	start = t0                                (PodTTLDeadlineFromSpec)
//	end   = t0 + 2*turnTimeout + TTLGrace     (StopWithHandoff's step-4 hard cap)
//
// The far end is the point of this function. The idle reaper must stand down
// inside the window - the stop needs the wrapper ALIVE to offer the agent its
// one handoff turn (issue #527) - but standing down from t0 with no far end
// trades a backstop for an ASSUMPTION, namely that some reconcile actually
// reaches the TTL gate. reconcilePodStage can early-return before it (a
// committed handoff, an unadmitted ticket) and any persistent error upstream of
// it never gets there either, which is exactly the class issue #237's backstop
// exists for. Past the hard cap the stop has either finished or is not coming,
// and #237 re-arms with its full reach.
//
// The cap, not the reaper's own IdlePodReapAfter: at the stock
// turnTimeoutSeconds=900 the cap is 31m, LONGER than the 30m backstop, so
// borrowing that constant would re-arm the reaper mid-sequence and re-open the
// race. An unreadable turnTimeout degrades to zero, matching the stopper, whose
// TurnTimeout comes from the same Project field and whose waits then collapse.
//
// ok is false when the pod carries no readable TTL: no t0, no stop, not spoken
// for.
func PodTTLStopWindowFromSpec(pod *corev1.Pod, task *tatarav1alpha1.Task) (start, end time.Time, ok bool) {
	t0, ok := PodTTLDeadlineFromSpec(pod, task)
	if !ok {
		return time.Time{}, time.Time{}, false
	}
	turnTimeout, _ := podEnvSeconds(pod, EnvTurnTimeoutSeconds)
	if turnTimeout < 0 {
		turnTimeout = 0
	}
	return t0, t0.Add(2*turnTimeout + TTLGrace), true
}

// podEnvSeconds reads an integer-seconds env off the pod's containers. ok is
// false when the var is absent or unparseable; the value is then zero.
func podEnvSeconds(pod *corev1.Pod, name string) (time.Duration, bool) {
	for i := range pod.Spec.Containers {
		for _, e := range pod.Spec.Containers[i].Env {
			if e.Name != name {
				continue
			}
			n, err := strconv.Atoi(e.Value)
			if err != nil {
				return 0, false
			}
			return time.Duration(n) * time.Second, true
		}
	}
	return 0, false
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
	// LastTurnAt is when that payload was captured. It goes in the note because
	// status.lastTurn can OUTLIVE the pod it describes: respawnLostPod leaves it
	// standing deliberately (see task_stage.go), so "the last turn" is not
	// necessarily this pod's. A reader who cannot date it cannot tell fresh
	// continuation from a carry-over across a crash. Zero means unknown and is
	// simply not rendered.
	LastTurnAt time.Time
}

// TTLStopper drives the G.7 stop sequence for one pod.
type TTLStopper struct {
	Client    client.Client
	Session   Session
	Notes     NoteAppender
	Namespace string
	// Record is the operator_agent_pod_ttl_expired_total hook
	// (obs.AgentPodTTLExpired). Optional.
	Record func(agentKind, outcome, handoff string)
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

// StopWithHandoff runs the G.7 stop sequence and returns how the pod was
// stopped and how the continuation state was captured.
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
// The Task's notes are non-empty on return in EVERY case. That is the property
// the whole mechanism exists for - but see TTLHandoffNone: non-empty is not the
// same as useful, and the two are now told apart.
func (s *TTLStopper) StopWithHandoff(ctx context.Context, task *tatarav1alpha1.Task, in TTLStopInput) (TTLStopResult, error) {
	// A wrapper that is ALREADY GONE - evicted, or deleted out from under this
	// sequence - has no turn to offer and nothing to force-delete. Probing up
	// front stops the sequence spending 2*turnTimeout talking to a corpse and
	// then reporting force_deleted for a pod nobody forced.
	//
	// NOT the guard that closes issue #527's 12:37:58Z reap-then-stop race: on
	// the ttlStop path reconcilePodStage has just run podGone - the same cached
	// Get on the same key - and routes an absent pod to respawnLostPod, so this
	// probe cannot fire there. The reaper's stand-down window is what closes that
	// race. What reaches here is conversingHandoffAndPark, which has no podGone
	// ahead of it, and a genuine delete landing between the two reads.
	if s.wrapperGone(ctx, task) {
		handoff, err := s.writeSyntheticNote(ctx, task, in)
		if err != nil {
			return TTLStopResult{}, err
		}
		return s.record(in, TTLOutcomeGraceful, handoff), nil
	}

	hardCap := in.Deadline.Add(2*in.TurnTimeout + TTLGrace)

	before, err := s.handoffNoteCount(ctx, task.Name)
	if err != nil {
		return TTLStopResult{}, err
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
				return s.finish(ctx, task, in, TTLHandoffAgent)
			}
		}
	}

	// Step 4: the operator writes the handoff the agent could not.
	handoff, err := s.writeSyntheticNote(ctx, task, in)
	if err != nil {
		return TTLStopResult{}, err
	}
	return s.finish(ctx, task, in, handoff)
}

// finish stops the pod and records the result. It decides the STOP dimension and
// ONLY the stop dimension: handoff is passed through untouched. It used to
// overwrite it, which is exactly how the metric lost the ability to say whether
// any work survived.
//
// A graceful stop that fails escalates to a force-delete - but only counts as
// one if the pod was actually still there. Force-deleting an absent pod is a
// no-op, and reporting force_deleted for it is a fabrication that fires the
// alert on a Task nothing went wrong for.
func (s *TTLStopper) finish(ctx context.Context, task *tatarav1alpha1.Task, in TTLStopInput, handoff string) (TTLStopResult, error) {
	stopErr := s.Session.DeleteSession(ctx, in.BaseURL)
	if stopErr == nil {
		stopErr = DeleteWrapper(ctx, s.Client, s.Namespace, task)
	}
	if stopErr == nil {
		return s.record(in, TTLOutcomeGraceful, handoff), nil
	}
	gone := s.wrapperGone(ctx, task)
	if ferr := s.forceDeletePod(ctx, task); ferr != nil {
		return TTLStopResult{}, ferr
	}
	if gone {
		return s.record(in, TTLOutcomeGraceful, handoff), nil
	}
	return s.record(in, TTLOutcomeForceDeleted, handoff), nil
}

func (s *TTLStopper) record(in TTLStopInput, outcome, handoff string) TTLStopResult {
	if s.Record != nil {
		s.Record(in.AgentKind, outcome, handoff)
	}
	return TTLStopResult{Outcome: outcome, Handoff: handoff}
}

// wrapperGone reports whether the wrapper Pod backing this Task is already
// absent. Only a definite NotFound counts: a read error means "unknown", and the
// conservative answer to unknown is to run the full stop sequence.
func (s *TTLStopper) wrapperGone(ctx context.Context, task *tatarav1alpha1.Task) bool {
	pod := &corev1.Pod{}
	err := s.Client.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: PodName(task)}, pod)
	return apierrors.IsNotFound(err)
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
// thing standing between a TTL stop and an empty notes journal. It returns the
// HANDOFF dimension it achieved.
//
// The empty case is the whole of issue #527. Until status.lastTurn existed the
// operator held neither field on ANY stop, so this rendered the same constant
// "(none)"/"none" string every time - a note that satisfied the non-empty-notes
// invariant syntactically and told the next pod nothing. A placeholder must read
// as a placeholder, to the next agent AND to the metric.
func (s *TTLStopper) writeSyntheticNote(ctx context.Context, task *tatarav1alpha1.Task, in TTLStopInput) (string, error) {
	final := strings.TrimSpace(in.LastFinalText)
	body, handoff := syntheticNoteBody(final, in.PushedRepos, in.LastTurnAt)
	n := tatarav1alpha1.Note{
		At:    metav1.NewTime(s.now()),
		Agent: NoteAgentOperator,
		Kind:  NoteKindHandoff,
		Body:  truncateNoteBody(body),
	}
	if err := s.Notes.AppendNote(ctx, task.Name, n); err != nil {
		return "", fmt.Errorf("agent: write synthetic handoff note: %w", err)
	}
	return handoff, nil
}

// SyntheticNoteLostMarker is the phrase a content-free synthetic handoff note
// leads with. It is the grep the runbook points at, so it is a constant rather
// than a string literal buried in a format directive.
const SyntheticNoteLostMarker = "NO CONTINUATION STATE WAS CAPTURED"

// syntheticNoteBody renders the operator's handoff note and says which handoff
// dimension it represents. EITHER field alone is real continuation state; only
// an entirely empty payload is TTLHandoffNone.
//
// at DATES the payload, and is load-bearing rather than decorative: status
// .lastTurn survives a respawnLostPod on purpose, so the turn this note is built
// from may predate the pod being stopped. Zero means unknown and renders
// nothing - a zero time printed as an instant would be worse than silence.
func syntheticNoteBody(final string, pushedRepos []string, at time.Time) (string, string) {
	if final == "" && len(pushedRepos) == 0 {
		return "TTL stop. " + SyntheticNoteLostMarker + ": the agent did not answer the handoff turn " +
			"and the operator holds no final text or pushed repos for the last turn. This note is a " +
			"PLACEHOLDER, not a handoff - the work done on the previous pod is not recorded anywhere. " +
			"Re-derive state from the issue thread and the repos; do not read this note as continuity.", TTLHandoffNone
	}
	pushed := "none"
	if len(pushedRepos) > 0 {
		pushed = strings.Join(pushedRepos, ", ")
	}
	if final == "" {
		final = "(not recorded)"
	}
	when := "an unrecorded time"
	if !at.IsZero() {
		when = at.UTC().Format(time.RFC3339)
	}
	return fmt.Sprintf("TTL stop. Last COMPLETED turn (%s) final text: %s. Repos pushed: %s. "+
		"No agent handoff was captured, so this is the operator's reconstruction, not the agent's own "+
		"handoff. If that turn predates this pod, the work since is unrecorded.",
		when, final, pushed), TTLHandoffSynthetic
}

// maxNoteBody is the Note.Body CRD MaxLength. A long finalText must not make the
// synthetic note unwritable - an over-long note that the API server rejects is
// an EMPTY notes journal, the exact failure this whole path exists to prevent.
const maxNoteBody = tatarav1alpha1.NoteBodyMaxBytes

func truncateNoteBody(s string) string {
	if len(s) <= maxNoteBody {
		return s
	}
	const ellipsis = "...(truncated)"
	return tatarav1alpha1.TruncateUTF8(s, maxNoteBody-len(ellipsis)) + ellipsis
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
