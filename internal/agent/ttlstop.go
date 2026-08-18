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
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// A G.7 TTL stop reports TWO INDEPENDENT FACTS, on two labels of
// operator_agent_pod_ttl_expired_total. They shared one label until #527, and
// the conflation is why ~19 days of silent work loss went unnoticed: finish()
// overwrote the capture fact with the stop fact on any teardown error, so
// synthetic_handoff was structurally unreachable and never once recorded over
// 30 days, while force_deleted - the value the alert fired on - could equally
// mean "the agent handed off perfectly and the wrapper was slow to die".
//
// TTLOutcome* answers: HOW WAS THE POD STOPPED?
const (
	// TTLOutcomeGraceful: the wrapper session closed and the pod came down on its
	// own (or was already gone). Says NOTHING about whether a handoff was
	// captured.
	TTLOutcomeGraceful = "graceful"
	// TTLOutcomeForceDeleted: the graceful stop failed against a pod that was
	// still there, so it was deleted with a zero grace period. Says NOTHING about
	// whether a handoff was captured.
	TTLOutcomeForceDeleted = "force_deleted"
)

// TTLHandoff* answers: HOW WAS THE CONTINUATION STATE CAPTURED? This is the
// dimension that decides whether work was lost, and the only one an alert on
// work loss can be built from.
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
	// THIS is the silent work loss, and it is the bucket to alert on.
	TTLHandoffNone = "none"
)

// TTLCause* answers the THIRD question: WHY DID THE STOP RUN AT ALL?
//
// All four ran through this one sequence and reported one undifferentiated
// counter, which made a routine hourly TTL rotation and a hung agent being killed
// indistinguishable on the metric. They are not the same event and they do not
// have the same fix.
const (
	// TTLCauseTTL: the pod reached t0. The routine, healthy rotation, and the only
	// cause this sequence was originally written for.
	TTLCauseTTL = "ttl"
	// TTLCauseStall: a turn went silent past turnTimeoutSeconds and the operator
	// stopped it. NOT routine.
	TTLCauseStall = "stall"
	// TTLCauseEviction: the live-pod ceiling reclaimed this pod's slot for another
	// Task. The pod was healthy; the fleet was full.
	TTLCauseEviction = "eviction"
	// TTLCauseIdle: a live conversation exhausted its idle budget with no human
	// reply. Also healthy; the human simply went away.
	TTLCauseIdle = "idle"
)

// TTLStopResult is one stop, reported on both dimensions.
type TTLStopResult struct {
	// Outcome is graceful | force_deleted: how the POD was stopped.
	Outcome string
	// Handoff is agent | synthetic | none: how the CONTINUATION STATE was
	// captured.
	Handoff string
}

// SyntheticNoteLostMarker is the phrase a CONTENT-FREE synthetic handoff note
// leads with. It is what the next agent reads instead of a handoff, and it is
// the string the runbook's diagnosis step greps for, so it is a constant rather
// than a literal buried in a format directive.
const SyntheticNoteLostMarker = "NO CONTINUATION STATE WAS CAPTURED"

// EnvAgentPodTTLSeconds is the wrapper env carrying this pod's own copy of the
// resolved TTL. The wrapper computes its 410-Gone cutoff from it, and the reaper
// reads it back off the pod spec (PodTTLDeadlineFromSpec) because it resolves no
// Projects.
const EnvAgentPodTTLSeconds = "AGENT_POD_TTL_SECONDS"

// EnvTurnTimeoutSeconds is the wrapper env carrying
// Project.spec.agent.turnTimeoutSeconds. The stop sequence's step-4 hard cap is
// derived from it, so the reaper reads it back off the pod spec to learn how
// long the stop can legitimately still hold this pod
// (PodTTLStopWindowFromSpec).
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
	// AppendNoteOnce is AppendNote skipped when a note with the same Agent and
	// Body is already in the journal.
	//
	// It exists as its own method rather than as a scan in the caller because the
	// decision has to be made against the object version the append MUTATES. A
	// caller holding a Task snapshot cannot do that: on the TTL path the snapshot
	// is captured at StopWithHandoff entry and the append happens after waitIdle,
	// SubmitHandoffTurn and waitHandoffNote, so it can be many minutes stale. The
	// sibling helper appendOperatorNoteTo does it the same way, inside the closure.
	//
	// "Once" is about the NOTE, not about the write: a suppressed append still
	// costs the same round trip the unconditional one does.
	AppendNoteOnce(ctx context.Context, taskName string, n tatarav1alpha1.Note) error
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
	return a.append(ctx, taskName, n, false)
}

func (a *FitNoteAppender) AppendNoteOnce(ctx context.Context, taskName string, n tatarav1alpha1.Note) error {
	return a.append(ctx, taskName, n, true)
}

// WithNoteCap because this IS a note appender: the C.2.6 cap is a property of
// the journal, not of the agent-facing REST path that used to be its only
// enforcer (#616).
func (a *FitNoteAppender) append(ctx context.Context, taskName string, n tatarav1alpha1.Note, once bool) error {
	return objbudget.FitTask(ctx, a.Client, a.Spiller,
		types.NamespacedName{Namespace: a.Namespace, Name: taskName},
		func(t *tatarav1alpha1.Task) {
			if once {
				for _, have := range t.Status.Notes {
					if have.Agent == n.Agent && have.Body == n.Body {
						return
					}
				}
			}
			t.Status.Notes = append(t.Status.Notes, n)
		}, objbudget.WithNoteCap())
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

// ttlAnchor is the instant t0 is measured FROM. See TTLDeadline for why a live
// Task anchors on the later of podStartedAt and conversationLastEventAt.
func ttlAnchor(task *tatarav1alpha1.Task) (time.Time, bool) {
	if task.Status.PodStartedAt == nil {
		return time.Time{}, false
	}
	anchor := task.Status.PodStartedAt.Time
	if stage.Live(task.Status.State) && task.Status.ConversationLastEventAt != nil &&
		task.Status.ConversationLastEventAt.After(anchor) {
		anchor = task.Status.ConversationLastEventAt.Time
	}
	return anchor, true
}

// PodTTLDeadlineFromSpec is TTLDeadline computed WITHOUT a Project, by reading
// back the AGENT_POD_TTL_SECONDS the operator stamped on the pod itself (PodSpec
// sets it from the same PodTTLSeconds resolver, so the two cannot diverge).
//
// It exists for the reaper, which sweeps pods and Tasks and resolves no
// Projects, and which has to be able to tell whether the G.7 stop already owns a
// pod's teardown (#527).
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
//	start = t0                            (PodTTLDeadlineFromSpec)
//	end   = t0 + 2*turnTimeout + TTLGrace (StopWithHandoff's step-4 hard cap)
//
// The FAR END is the point of this function. The idle reaper must stand down
// inside the window - the stop needs the wrapper ALIVE to offer the agent its
// one handoff turn (#527) - but standing down from t0 with no far end would
// trade a backstop for an ASSUMPTION: that some reconcile actually reaches the
// TTL gate. reconcilePodStage can early-return before it, and any persistent
// error upstream of it never gets there either, which is exactly the class of
// wedged reconcile #237's backstop exists for. Past the hard cap the stop has
// either finished or is not coming, and #237 re-arms with its full reach.
//
// The cap, NOT the reaper's own IdlePodReapAfter: at the stock
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

// TTLStopInput is the per-stop context the operator supplies. LastFinalText,
// PushedRepos and FailedRepos come off the most recent turn-complete payload;
// they are what the synthetic handoff note is BUILT from, which is why
// pushedRepos is retained on the wire (G.2): without it the operator cannot tell
// "no diff" from "forgot to push" on a multi-repo Task.
type TTLStopInput struct {
	BaseURL     string
	CallbackURL string
	// AgentKind labels the TTL metric.
	AgentKind string
	// Deadline is t0.
	Deadline time.Time
	// TurnTimeout is Project.spec.agent.turnTimeoutSeconds.
	TurnTimeout time.Duration
	// MaxWait REPLACES the TurnTimeout-derived bounds when non-zero: the hard cap
	// becomes Deadline+MaxWait and each wait is bounded by MaxWait rather than by
	// TurnTimeout.
	//
	// It exists for the STALLED-TURN caller, where deriving the bound from
	// TurnTimeout is not conservative but absurd: that caller only runs BECAUSE a
	// turn already burned its entire TurnTimeout with no activity, so waiting
	// another 2*TurnTimeout would be waiting hours for the very turn we just
	// declared dead. The only thing worth waiting for there is the seconds-wide
	// race where the turn completes between the stall check and this call, and
	// MaxWait is sized for exactly that.
	//
	// Zero keeps the G.7 TTL behaviour (2*TurnTimeout + TTLGrace) untouched.
	MaxWait       time.Duration
	LastFinalText string
	PushedRepos   []string
	// FailedRepos are the repos the last turn tried to push and could not
	// (tatara-claude-code-wrapper#167). It is the one field here that reports lost
	// WORK rather than lost context: the pod's workspace is ephemeral, so those
	// commits survive nowhere, and this note is the only place the next pod can
	// be told to go and redo them.
	FailedRepos []string
	// ReposTurnID is status.lastTurnReposTurnID: which turn the two lists above
	// describe. It travels with them so the failed-repos note can name its turn
	// and a recurrence cannot be mistaken for a replay. Empty means unknown.
	ReposTurnID string
	// Cause is the ttl|stall|eviction|idle label on
	// operator_agent_pod_ttl_expired_total: WHY this stop ran. Empty defaults to
	// TTLCauseTTL, which is both the historic meaning of the counter and the only
	// value a caller can omit without lying.
	Cause string
}

// cause is the metric label, defaulted. An empty string is never emitted: a blank
// label value is a hole in the series that no query can select for, and the
// default is the one value that was already true of every increment before the
// label existed.
func (in TTLStopInput) cause() string {
	if in.Cause == "" {
		return TTLCauseTTL
	}
	return in.Cause
}

// waitBound is the per-step wait: MaxWait when the caller set one, else the
// project's turn timeout.
func (in TTLStopInput) waitBound() time.Duration {
	if in.MaxWait > 0 {
		return in.MaxWait
	}
	return in.TurnTimeout
}

// hardCap is the absolute end of the stop sequence.
func (in TTLStopInput) hardCap() time.Time {
	if in.MaxWait > 0 {
		return in.Deadline.Add(in.MaxWait)
	}
	return in.Deadline.Add(2*in.TurnTimeout + TTLGrace)
}

// TTLStopper drives the G.7 stop sequence for one pod.
type TTLStopper struct {
	Client    client.Client
	Session   Session
	Notes     NoteAppender
	Namespace string
	// Record is the operator_agent_pod_ttl_expired_total hook
	// (obs.AgentPodTTLExpired). Optional.
	Record func(agentKind, outcome, handoff, cause string)
	// RecordEmptySynthetic is the operator_agent_synthetic_handoff_empty_total
	// hook (obs.AgentSyntheticHandoffEmpty): a synthetic note was written with NO
	// continuation state to put in it, which is the #527 failure. Optional.
	RecordEmptySynthetic func(agentKind string)
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
// stopped and, independently, how the continuation state was captured.
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
	// A wrapper that is ALREADY GONE - evicted, reaped, or deleted out from under
	// this sequence - has no turn to offer and nothing to force-delete. Probing up
	// front is what stops the sequence spending the full 2*turnTimeout talking to
	// a corpse and then reporting force_deleted for a pod nobody forced. That is
	// the 12:37:58Z event in #527 exactly: the reaper deleted the pod at 12:10:19Z
	// and the stop ran 27 minutes later against nothing.
	//
	// It does not close the reap-then-stop RACE - the reaper's stand-down window
	// does that. It bounds the damage when the race is already lost, and it is the
	// only guard on the callers that have no podGone check ahead of them
	// (stalledTurnStop, liveHandoffAndPark).
	if s.wrapperGone(ctx, task) {
		handoff, err := s.writeSyntheticNote(ctx, task, in)
		if err != nil {
			return TTLStopResult{}, err
		}
		return s.record(in, TTLOutcomeGraceful, handoff), nil
	}

	hardCap := in.hardCap()

	before, err := s.handoffNoteCount(ctx, task.Name)
	if err != nil {
		return TTLStopResult{}, err
	}

	// Step 2: wait out the in-flight turn, bounded by turnTimeoutSeconds and by
	// the hard cap.
	waitUntil := earliest(s.now().Add(in.waitBound()), hardCap)
	turnCleared := s.waitIdle(ctx, in.BaseURL, waitUntil)

	// Step 3: submit THE handoff turn - the one turn the wrapper still admits past
	// t0. A refusal (410/409/5xx) or a hard-cap breach goes straight to the
	// synthetic note; the notes are never left empty.
	if turnCleared && s.now().Before(hardCap) {
		_, serr := s.Session.SubmitHandoffTurn(ctx, in.BaseURL, HandoffTurnText, in.CallbackURL)
		if serr == nil {
			deadline := earliest(s.now().Add(in.waitBound()), hardCap)
			if s.waitHandoffNote(ctx, task.Name, before, deadline) {
				s.noteFailedRepos(ctx, task, in)
				return s.finish(ctx, task, in, TTLHandoffAgent)
			}
		}
	}

	// Step 4: the operator writes the handoff the agent could not. It reports
	// which capture dimension it actually achieved - a note built from nothing is
	// not a synthetic handoff, it is a placeholder.
	handoff, err := s.writeSyntheticNote(ctx, task, in)
	if err != nil {
		return TTLStopResult{}, err
	}
	return s.finish(ctx, task, in, handoff)
}

// finish stops the pod and records the result. It decides the STOP dimension and
// ONLY the stop dimension: handoff is passed through untouched.
//
// It used to overwrite it, and that is precisely how the metric lost the ability
// to say whether any work survived: a Task whose agent wrote a perfect handoff
// and whose wrapper then failed to tear down cleanly was reported as
// force_deleted, indistinguishable from total loss (#527).
//
// A graceful stop that fails escalates to a force-delete, but only COUNTS as one
// if the pod was actually still there. Force-deleting an absent pod is a no-op,
// and reporting force_deleted for it is a fabrication that fires the alert on a
// Task nothing went wrong for - the session call fails precisely BECAUSE the
// wrapper is gone, so this is the common case, not an exotic one.
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

// wrapperGone reports whether the wrapper Pod backing this Task is already
// absent. Only a definite NotFound counts: a read error means "unknown", and the
// conservative answer to unknown is to run the full stop sequence.
func (s *TTLStopper) wrapperGone(ctx context.Context, task *tatarav1alpha1.Task) bool {
	pod := &corev1.Pod{}
	err := s.Client.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: PodName(task)}, pod)
	return apierrors.IsNotFound(err)
}

// record emits the metric (when wired) and returns the result the caller logs.
func (s *TTLStopper) record(in TTLStopInput, outcome, handoff string) TTLStopResult {
	if s.Record != nil {
		s.Record(in.AgentKind, outcome, handoff, in.cause())
	}
	return TTLStopResult{Outcome: outcome, Handoff: handoff}
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
		if isAgentHandoffNote(note) {
			n++
		}
	}
	return n, nil
}

// isAgentHandoffNote reports whether a note is a handoff the AGENT wrote. The
// operator's synthetic note is deliberately excluded: it is the operator talking
// to the next pod, never the agent talking to a human.
func isAgentHandoffNote(n tatarav1alpha1.Note) bool {
	return n.Kind == NoteKindHandoff && n.Agent != NoteAgentOperator
}

// HasAgentHandoffNote reports whether the Task's notes journal carries a handoff
// note the AGENT authored - i.e. whether the agent was ever actually given the
// chance to say something, and said it.
//
// It is the predicate that separates a genuine human gate from wreckage: a Task
// whose pod ended without one was never asked a question, so parking it
// awaiting-human (which only a human comment un-parks) waits forever on a reply
// nobody owes. The operator's own synthetic note does NOT count, however rich its
// contents - it is continuation state for the next pod, not a question for a
// person.
func HasAgentHandoffNote(t *tatarav1alpha1.Task) bool {
	return HasAgentHandoffNoteSince(t, time.Time{})
}

// HasAgentHandoffNoteSince is HasAgentHandoffNote scoped to notes written at or
// after `since`, and the scoping is not a refinement - it is what makes the
// predicate usable as "THIS pod's agent asked to be stopped".
//
// The notes journal is per-TASK and outlives every pod that writes to it. Every
// graceful stop ends by putting a handoff note in it, so a CONTINUATION pod is
// born into a Task that already carries one; a bare HasAgentHandoffNote as a
// stop trigger would therefore kill each replacement pod the instant it started,
// forever. Passing the current pod's start time is what confines the question to
// the pod actually running.
//
// A zero `since` matches everything, which is the whole-journal question
// HasAgentHandoffNote asks.
func HasAgentHandoffNoteSince(t *tatarav1alpha1.Task, since time.Time) bool {
	for _, n := range t.Status.Notes {
		if isAgentHandoffNote(n) && !n.At.Time.Before(since) {
			return true
		}
	}
	return false
}

// writeSyntheticNote is G.7 step 4: the operator's own handoff note, built from
// the last turn's final text and the repos the agent pushed. It is the ONLY
// thing standing between a TTL stop and an empty notes journal.
//
// It returns the HANDOFF dimension it achieved. EITHER field alone is real
// continuation state - a push with no closing message still tells the next pod
// where the work went - so only an entirely empty payload is TTLHandoffNone.
func (s *TTLStopper) writeSyntheticNote(ctx context.Context, task *tatarav1alpha1.Task, in TTLStopInput) (string, error) {
	pushed := "none"
	if len(in.PushedRepos) > 0 {
		pushed = strings.Join(in.PushedRepos, ", ")
	}
	final := strings.TrimSpace(in.LastFinalText)
	handoff := TTLHandoffSynthetic
	// A failed push is content, and of the three inputs it is the one carrying
	// the most urgency: the other two describe work that survived, this one names
	// work that did not. A turn whose only report is "repo X never reached origin"
	// must get a real note - degrading it to the placeholder would discard the one
	// fact the next pod cannot recover any other way
	// (tatara-claude-code-wrapper#167).
	contentFree := final == "" && len(in.PushedRepos) == 0 && len(in.FailedRepos) == 0
	if final == "" {
		final = "(none)"
	}
	if contentFree {
		handoff = TTLHandoffNone
		// LOUD, because for ~19 days this was the ONLY thing this function ever
		// wrote and nothing said so: the note satisfied the non-empty-notes
		// invariant while carrying zero continuation state, the next pod resumed
		// from nothing, re-ran turn-0 and re-charged maxTurnsPerTask, and the Task
		// looked healthy throughout (#527).
		//
		// The counter is KEPT alongside handoff="none", which is now the primary
		// signal: it predates the label split, tatara-observability may still be
		// selecting it, and retiring an emitted series is a separate decision from
		// adding a better one.
		if s.RecordEmptySynthetic != nil {
			s.RecordEmptySynthetic(in.AgentKind)
		}
		log.FromContext(ctx).Info("synthetic handoff note has no continuation state to carry",
			"action", "synthetic_handoff_empty", "resource_id", task.Name,
			"agent_kind", in.AgentKind, "pushed_repos", len(in.PushedRepos))
	}
	body := fmt.Sprintf("TTL stop. Last turn's final text: %s. Repos pushed: %s. No agent handoff was captured.", final, pushed)
	if contentFree {
		body = syntheticNoteLostBody()
	}
	// AN OOMKILLED POD GETS A DIFFERENT NOTE, because the generic one is not merely
	// unhelpful there - it is misleading. An OOM kill can never produce an agent
	// handoff (waitIdle errors on the dead session endpoint, so turnCleared is
	// false and SubmitHandoffTurn is never reached), and the pod's workspace was
	// EPHEMERAL: it died with the container. The next agent reads the surviving
	// earlier note, is told a branch and a commit exist, goes looking for them on
	// the remote, and finds nothing - which is exactly what happened on
	// imp-mtg-mtg-decks-i37. Name the kill and tell it to verify.
	if at, ok := s.oomKilledAt(ctx, task); ok {
		body = OOMKilledNoteBody(at) +
			fmt.Sprintf(" Last turn's final text: %s. Repos pushed: %s.", final, pushed)
	}
	// LAST, so it survives every branch above - including the OOM rewrite, which
	// is the branch that needs it most: an OOM kill and a failed push lose the
	// same commits for the same reason. Appended rather than woven in so the note
	// reads exactly as before when nothing failed, which is almost every stop.
	//
	// It is RESERVED out of the budget rather than appended after truncation:
	// lastTurnFinalText is capped at exactly NoteBodyMaxBytes and Note.Body is
	// capped at the same number, so a maximal final text alone fills the note and
	// a plain append would drop this sentence in full. The final text can afford
	// to lose its tail; this cannot, because it names work that no longer exists
	// anywhere.
	// Budgeted in turn, so the reservation can never starve the note it is
	// reserved out of: the two bound each other rather than trusting the repo list
	// to be short. The budget is spent on the repo NAMES; the sentence's fixed
	// prose is a few hundred bytes and comfortably inside a quarter of the note.
	warning := failedReposSentence(in.FailedRepos, maxNoteBody/4)
	n := tatarav1alpha1.Note{
		At:    metav1.NewTime(s.now()),
		Agent: NoteAgentOperator,
		Kind:  NoteKindHandoff,
		Body:  truncateNoteBodyTo(body, maxNoteBody-len(warning)) + warning,
	}
	if err := s.Notes.AppendNote(ctx, task.Name, n); err != nil {
		return "", fmt.Errorf("agent: write synthetic handoff note: %w", err)
	}
	return handoff, nil
}

func (s *TTLStopper) noteFailedRepos(ctx context.Context, task *tatarav1alpha1.Task, in TTLStopInput) {
	AppendFailedReposNote(ctx, s.Notes, task, in.FailedRepos, in.ReposTurnID, s.now())
}

// AppendFailedReposNote appends the operator's own note naming the repos whose
// push failed, on the stop paths that SPEND the last-turn payload without ever
// reaching the G.7 synthetic note.
//
// Those paths are the HEALTHY ones, which is why the field was inert without
// this. StopWithHandoff returns TTLHandoffAgent as soon as the agent answers the
// handoff turn; stopAfterAgentHandoff skips the sequence entirely because the
// agent wrote the note unprompted. Both then run clearLastTurn. Left to
// writeSyntheticNote alone, the field surfaced only when the pod was ALSO
// wedged - the case that was already loud.
//
// It does NOT cover the stage-transition teardown: stage.stampEnter clears the
// payload on every edge and ensureStagePod then deletes the pod with no stop
// sequence of any kind. That path renders no handoff note at all, synthetic or
// otherwise, so giving it one is a strictly larger change than this field. What
// it costs here is the already-documented "failedRepos describes THIS turn"
// limitation seen from the other side: a failure reported in turn N is retired
// by an edge taken in turn N+1.
//
// The agent's own note cannot substitute on any of them: HandoffTurnText is a
// fixed string, and the wrapper reports the push failure to pod stdout, which is
// neither in the agent's context nor Loki-scraped. So the agent writes a note
// that cannot mention it, and the one fact describing LOST WORK dies with the
// pod.
//
// Agent is NoteAgentOperator, so isAgentHandoffNote does not match it and
// handoffNoteCount, HasAgentHandoffNote{,Since} and the agentAskedSomething call
// sites are all unmoved by an addendum landing beside the agent's own note.
//
// IDEMPOTENT ON THE RENDERED BODY, which is why the body has to NAME ITS TURN.
// The payload survives a respawn (clearLastTurn is deliberately not run there),
// so the same repo list can reach a second stop and say the same thing twice.
// But failedReposSentence is a pure function of the list, and a persistent
// rejection - branch protection, auth, a diverged branch - is exactly the shape
// that recurs with an IDENTICAL list many turns later. Deduping on the prose
// alone would match the older note, skip the write, and let the caller's
// unconditional clearLastTurn destroy the report it just suppressed: the
// survivor then sits behind agent notes saying the work landed, while its own
// text claims "the LAST turn's". So the turn id joins the body, the way
// OOMKilledNoteBody carries the kill timestamp - the discriminator is IN the
// fact, and exact-body dedupe distinguishes turns by construction.
//
// turnID is status.lastTurnReposTurnID, which exists to say which turn the repo
// lists describe, and it travels with `failed` from the same snapshot so the two
// cannot disagree. Empty - an in-place upgrade whose lists were written by a
// binary that had no such field - degrades to the old body-only behaviour, which
// is the right way for an UNKNOWN turn to fail: a note suppressed is recoverable
// on the next distinct failure, a note doubled on every reconcile is not.
//
// Exact body rather than a marker prefix, so it never suppresses a note about a
// DIFFERENT repo list, and so it does not collide with the copy of the same
// sentence writeSyntheticNote embeds in a larger body.
//
// The dedupe runs INSIDE the appender's own read-modify-write (AppendNoteOnce),
// not over the caller's Task snapshot: `task` here is the object identity, and
// its status is deliberately not read.
//
// Best-effort, and deliberately so on both counts: a note that will not write
// must never keep a pod alive, and returning an error here would send the
// TTL caller back through a stop sequence whose agent handoff note now predates
// its own baseline - the retry would time out waiting for a second one and
// overwrite a perfectly good agent handoff with the synthetic note.
func AppendFailedReposNote(ctx context.Context, notes NoteAppender, task *tatarav1alpha1.Task,
	failed []string, turnID string, at time.Time) {

	// Budgeted against the suffix rather than clamped after it: failedReposSentence
	// spends its whole budget on repo names, so a plain append would push a maximal
	// list past Note.Body's MaxLength and the apiserver would reject the write.
	suffix := failedReposTurnSuffix(turnID)
	body := strings.TrimSpace(failedReposSentence(failed, maxNoteBody-len(suffix)))
	if body == "" {
		return
	}
	n := tatarav1alpha1.Note{
		At:    metav1.NewTime(at),
		Agent: NoteAgentOperator,
		Kind:  NoteKindHandoff,
		Body:  body + suffix,
	}
	if err := notes.AppendNoteOnce(ctx, task.Name, n); err != nil {
		log.FromContext(ctx).Error(err, "could not record the repos whose push failed",
			"action", "failed_repos_note", "resource_id", task.Name,
			"failed_repos", failed, "repos_turn_id", turnID)
	}
}

// failedReposTurnSuffix names the turn the repo list describes, so two distinct
// losses of the SAME repos render two distinct bodies.
//
// A full clause rather than a trailing "(turn X)" parenthetical, for the reason
// OOMKilledNoteBody puts its timestamp inside one: a bare parenthetical after a
// closed sentence does not say what it qualifies, and this note is read by an
// LLM. The turn id itself is operator-facing - the next pod holds a fresh
// wrapper session and no way to look one up - so the clause has to carry its own
// meaning rather than lean on the reader recognising the id.
//
// The id is already clamped to LastTurnReposTurnIDMaxBytes on the write path,
// and bounded again by the CRD marker, so this is bounded without a third clamp.
func failedReposTurnSuffix(turnID string) string {
	if turnID == "" {
		return ""
	}
	return " This describes turn " + turnID + "."
}

// syntheticNoteLostBody is what the operator writes when it holds nothing to
// hand off with.
//
// The note that used to land here read "TTL stop. Last turn's final text:
// (none). Repos pushed: none. No agent handoff was captured." - which is
// indistinguishable, to the agent reading it, from a turn that genuinely
// produced nothing worth saying. #557 added a counter for this case but left the
// note itself unchanged, so the operator could SEE the loss while the next agent
// still could not: it read "(none)" as continuity, resumed from it, re-ran
// turn-0 and re-charged maxTurnsPerTask.
//
// A placeholder must read as a placeholder. This one names the loss, says
// explicitly that the previous pod's work is not recorded anywhere, and tells
// the next agent to re-derive rather than to continue.
func syntheticNoteLostBody() string {
	return "TTL stop. " + SyntheticNoteLostMarker + ": the agent did not answer the handoff turn " +
		"and the operator holds no final text and no pushed repos for the last turn. This note is a " +
		"PLACEHOLDER, not a handoff - the work done on the previous pod is not recorded anywhere. " +
		"Re-derive the state from the issue thread and the repos; do not read this note as continuity."
}

// failedReposSentence renders the repos whose commit/push failed on the last
// turn, or "" when none did.
//
// It is a separate sentence, appended to whichever body was chosen, because it
// is a separate instruction: everything else in the note describes state the
// next agent can READ, and this describes work it has to REDO. The wrapper
// attempts every repo and reports the ones that failed rather than aborting the
// loop (tatara-claude-code-wrapper#167), so this is the only signal that a
// pushed-repos list is short because work was lost rather than because there was
// nothing to push.
// It does NOT declare the work lost outright. The mid-turn safety pusher pushes
// every repo each interval regardless of tree state, so a rejection at 12:00 that
// succeeds at 12:05 leaves this field naming repos whose commits ARE on origin,
// and an unconditional "redo it" sends the next agent to redo work that already
// exists. This is the same uncertainty the OOM note handles the same way: name
// it, and tell the agent to verify.
//
// The repo names come LAST and inside a budget of their own, because they are the
// only part that can be long: clampPushedRepos admits 20 names with no per-name
// cap. Truncating the rendered sentence would cut inside the join and take the
// directive with it, leaving a note that names some lost repos and silently omits
// the others. Names are dropped whole and counted instead.
//
// budget bounds the NAMES. It is a precondition, not a clamp: both call sites
// pass at least maxNoteBody/4, several times the fixed prose, and a budget under
// that renders the prose anyway rather than mutilating the directive it exists to
// protect.
func failedReposSentence(failed []string, budget int) string {
	if len(failed) == 0 {
		return ""
	}
	const format = " WARNING: the last turn's commit/push FAILED for %d repo(s). Those commits were only ever " +
		"on the previous pod's workspace, which is gone - unless the mid-turn safety pusher landed them after " +
		"the turn-end failure. CHECK origin for the task branch in each, and redo only what is missing. " +
		"Repos: %s."
	room := budget - len(fmt.Sprintf(format, len(failed), ""))
	return fmt.Sprintf(format, len(failed), joinWithinBudget(failed, room))
}

// joinWithinBudget renders as many names as fit and reports how many it dropped.
// A name is rendered whole or not at all: half a repo name is not a repo, and a
// list that ends mid-name reads as a complete list. A single name too long for
// the whole budget therefore renders as the count alone, which is all the note
// can honestly say about it.
func joinWithinBudget(names []string, budget int) string {
	elided := func(from int) string {
		sep := " "
		if from == 0 {
			sep = ""
		}
		return fmt.Sprintf("%s(+%d more)", sep, len(names)-from)
	}
	// Reserved against the WHOLE count, and against the separated form, so the
	// reservation can never be too small for the suffix it is reserving for.
	reserve := len(fmt.Sprintf(" (+%d more)", len(names)))
	var b strings.Builder
	for i, n := range names {
		sep := ""
		if i > 0 {
			sep = ", "
		}
		need := b.Len() + len(sep) + len(n)
		if i < len(names)-1 {
			need += reserve
		}
		if need > budget {
			return b.String() + elided(i)
		}
		b.WriteString(sep)
		b.WriteString(n)
	}
	return b.String()
}

// OOMKilledNoteMarker is the phrase an OOM-kill handoff note leads with, for the
// same reason SyntheticNoteLostMarker is a constant: it is what the next agent
// reads instead of a handoff, and the runbook greps for it.
const OOMKilledNoteMarker = "PREVIOUS POD WAS OOMKILLED"

// OOMKilledNoteBody is the handoff note for a pod the kernel took out for
// exceeding its memory limit.
//
// It says the two things the generic placeholder cannot. First, WHY there is no
// agent handoff: an OOMKilled wrapper is unreachable, so the handoff turn was
// never even offered. Second, and this is the one that costs real work: the
// pod's workspace was ephemeral and died with it, so a branch or commit named in
// an EARLIER note may never have reached the remote. Without that, the next agent
// treats the surviving note as fact and hunts for a commit that does not exist.
//
// Exported because the respawn path in internal/controller writes it too - the
// pod-unusable detection now catches most OOM kills before any stop sequence gets
// to run, and the warning has to land on whichever path gets there first.
func OOMKilledNoteBody(at time.Time) string {
	return OOMKilledNoteMarker + ": the previous pod was killed at " + at.UTC().Format(time.RFC3339) +
		" for exceeding its memory limit, so no agent handoff could be written. Its workspace was " +
		"ephemeral and died with it: any branch or commit named in an earlier note may never have " +
		"reached the remote. Verify every branch and commit against the remote before assuming the " +
		"prior work exists, and do not attempt to recover local state from the dead pod."
}

// oomKilledAt reports when the Task's wrapper Pod was OOMKilled, reading the
// kubelet's terminated-container state off the Pod object. RestartPolicy: Never
// means the corpse is still there to be read. A missing pod, an unreadable one,
// or any other termination reason reports false.
func (s *TTLStopper) oomKilledAt(ctx context.Context, task *tatarav1alpha1.Task) (time.Time, bool) {
	pod := &corev1.Pod{}
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: PodName(task)}, pod); err != nil {
		return time.Time{}, false
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if t := cs.State.Terminated; t != nil && t.Reason == ReasonOOMKilled {
			return t.FinishedAt.Time, true
		}
	}
	return time.Time{}, false
}

// ReasonOOMKilled is the kubelet's ContainerStateTerminated.Reason for a
// container the kernel OOM killer took out. k8s.io/api exports no symbol for it.
const ReasonOOMKilled = "OOMKilled"

// maxNoteBody is the Note.Body CRD MaxLength. A long finalText must not make the
// synthetic note unwritable - an over-long note that the API server rejects is
// an EMPTY notes journal, the exact failure this whole path exists to prevent.
const maxNoteBody = tatarav1alpha1.NoteBodyMaxBytes

// truncateNoteBodyTo cuts s to budget bytes, on a rune boundary, marking the
// cut. The budget is explicit because the caller reserves room out of
// maxNoteBody for a suffix it refuses to let the truncation eat.
func truncateNoteBodyTo(s string, budget int) string {
	if len(s) <= budget {
		return s
	}
	const ellipsis = "...(truncated)"
	if budget <= len(ellipsis) {
		return tatarav1alpha1.TruncateUTF8(s, max(budget, 0))
	}
	return tatarav1alpha1.TruncateUTF8(s, budget-len(ellipsis)) + ellipsis
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
