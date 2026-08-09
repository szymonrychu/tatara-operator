package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// newStallFixture is newConversingExitFixture wound forward to the moment the
// old code declared a stall: an implement Task with a live pod, a turn submitted
// two hours ago and not one activity tick since.
//
// The project carries an explicit grace and attempt count rather than relying on
// the CRD defaults, because the whole ladder is a function of those two numbers
// and a test that reads them from a default cannot tell "the default changed"
// from "the ladder broke".
func newStallFixture(t *testing.T, graceSeconds, maxAttempts int) (
	*tatarav1alpha1.Project, *tatarav1alpha1.Task, *TaskReconciler, *exitSession) {

	t.Helper()
	proj, task, r, sess := newConversingExitFixture(t)
	proj.Spec.Agent.TurnTimeoutSeconds = 600
	proj.Spec.Agent.StallProbeGraceSeconds = graceSeconds
	proj.Spec.Agent.StallProbeMaxAttempts = maxAttempts

	task.Status.State = tatarav1alpha1.StateUnderImplementation
	task.Status.AgentKind = stage.AgentKindFor(tatarav1alpha1.StateUnderImplementation, "implement")
	task.Annotations = map[string]string{
		annCurrentTurn:   "turn-quiet",
		annTurnStartedAt: time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339),
	}
	if err := r.Update(context.Background(), task); err != nil {
		t.Fatalf("seed annotations: %v", err)
	}
	return proj, task, r, sess
}

// seedProbeInFlight puts an episode mid-ladder: probe `id` sent `sentAgo` ago,
// `attempts` probes spent so far.
func seedProbeInFlight(t *testing.T, r *TaskReconciler, task *tatarav1alpha1.Task,
	id string, sentAgo time.Duration, attempts int) {

	t.Helper()
	if err := r.patchTaskAnnotations(context.Background(), task, func(fresh *tatarav1alpha1.Task) bool {
		fresh.Annotations[annStallProbeID] = id
		fresh.Annotations[annStallProbeAt] = time.Now().Add(-sentAgo).UTC().Format(time.RFC3339)
		fresh.Annotations[annStallProbeAttempts] = fmt.Sprint(attempts)
		return true
	}); err != nil {
		t.Fatalf("seed probe: %v", err)
	}
}

func stallProbeCount(t *testing.T, outcome string) float64 {
	t.Helper()
	return testutil.ToFloat64(obs.StallProbeCounter(outcome))
}

// THE LADDER, END TO END.
//
// One table over the three axes that decide everything: what the probe came back
// as, how many attempts the episode has already spent, and whether the wrapper
// serves the probe endpoints at all. Each row asserts the three things that can
// differ - was a NEW probe written, was the turn INTERRUPTED, was the pod
// STOPPED - because those three are the whole behavioural surface of this phase.
func TestReconcileStall_TheProbeLadder(t *testing.T) {
	const grace = 300

	for _, tc := range []struct {
		name string
		// probeState seeds ProbeStatus for the outstanding probe. Empty means no
		// probe is outstanding yet (the episode has not started).
		probeState string
		// attempts already spent; sentAgo is how long ago the outstanding probe
		// went out, which decides whether the grace window is over.
		attempts    int
		sentAgo     time.Duration
		maxAttempts int
		// probeErr overrides every probe call - this is the wrapper-support axis.
		probeErr error

		wantProbes     int
		wantInterrupts int
		wantStopped    bool
		wantOutcome    string
	}{
		{
			name: "inactivity opens the ladder: ask, do not kill",
			// No probe outstanding, the turn is past its window. The old code went
			// straight to stalledTurnStop here; this is the branch that replaced it.
			maxAttempts: 2,
			wantProbes:  1,
			wantOutcome: obs.StallProbeSent,
		},
		{
			name:        "answered: the turn is alive and nothing is stopped",
			probeState:  agent.ProbeStateAnswered,
			attempts:    1,
			sentAgo:     10 * time.Second,
			maxAttempts: 2,
			wantOutcome: obs.StallProbeAnswered,
		},
		{
			name: "inside the grace window: no verdict yet",
			// delivered-not-yet-answered 10s into a 300s grace is what a HEALTHY
			// agent inside a long tool call looks like. Escalating here would make
			// the grace window decorative.
			probeState:  agent.ProbeStateDelivered,
			attempts:    1,
			sentAgo:     10 * time.Second,
			maxAttempts: 2,
		},
		{
			name:        "delivered but unanswered, attempts left: ask again",
			probeState:  agent.ProbeStateDelivered,
			attempts:    1,
			sentAgo:     (grace + 10) * time.Second,
			maxAttempts: 2,
			wantProbes:  1,
			wantOutcome: obs.StallProbeUnanswered,
		},
		{
			name:        "delivered but unanswered, attempts spent: ESC then stop",
			probeState:  agent.ProbeStateDelivered,
			attempts:    2,
			sentAgo:     (grace + 10) * time.Second,
			maxAttempts: 2,
			// The agent consumed the message and would not answer it. Nothing left
			// to ask.
			wantInterrupts: 1,
			wantStopped:    true,
			wantOutcome:    obs.StallProbeUnanswered,
		},
		{
			name: "never delivered, attempts left: ask again",
			// pending = the `enqueue` line has no matching `remove`: the agent has
			// not reached a tool-call boundary, i.e. it is blocked INSIDE one tool
			// call. Counted apart from unanswered because it is a different fault.
			probeState:  agent.ProbeStatePending,
			attempts:    1,
			sentAgo:     (grace + 10) * time.Second,
			maxAttempts: 2,
			wantProbes:  1,
			wantOutcome: obs.StallProbeNeverDelivered,
		},
		{
			name:           "never delivered, attempts spent: ESC then stop",
			probeState:     agent.ProbeStatePending,
			attempts:       2,
			sentAgo:        (grace + 10) * time.Second,
			maxAttempts:    2,
			wantInterrupts: 1,
			wantStopped:    true,
			wantOutcome:    obs.StallProbeNeverDelivered,
		},
		{
			name:        "maxAttempts=1: one unanswered probe is the whole ladder",
			probeState:  agent.ProbeStatePending,
			attempts:    1,
			sentAgo:     (grace + 10) * time.Second,
			maxAttempts: 1,
			// The knob is a real knob, not a shape the code assumes is 2.
			wantInterrupts: 1,
			wantStopped:    true,
			wantOutcome:    obs.StallProbeNeverDelivered,
		},
		{
			name: "the wrapper has no probe endpoints: the pre-probe stop, verbatim",
			// THE SAFETY PROPERTY. An old wrapper under a new operator behaves
			// exactly as it did under an old operator - no probe, no ESC, straight
			// to the stop. It is what lets this phase deploy ahead of the wrapper.
			maxAttempts: 2,
			probeErr:    agent.ErrProbeUnsupported,
			wantStopped: true,
			wantOutcome: obs.StallProbeUnsupported,
		},
		{
			name:        "the wrapper forgot the probe id: also the pre-probe stop",
			probeState:  agent.ProbeStateDelivered,
			attempts:    1,
			sentAgo:     10 * time.Second,
			maxAttempts: 2,
			probeErr:    agent.ErrProbeUnsupported,
			wantStopped: true,
			wantOutcome: obs.StallProbeUnsupported,
		},
		{
			name: "the probe call itself failed: the pre-probe stop, counted as error",
			// A transport error is not a verdict about the agent - but it leaves the
			// operator with exactly the evidence the pre-probe operator had, which
			// is none, so it does what that operator did. Never worse than today.
			maxAttempts: 2,
			probeErr:    errors.New("connection refused"),
			wantStopped: true,
			wantOutcome: obs.StallProbeError,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proj, task, r, sess := newStallFixture(t, grace, tc.maxAttempts)
			if tc.probeState != "" {
				seedProbeInFlight(t, r, task, "probe-seed", tc.sentAgo, tc.attempts)
				sess.probeStatus["probe-seed"] = agent.ProbeResult{
					ProbeID: "probe-seed", State: tc.probeState, Answer: "TATARA-ALIVE compiling",
				}
			}
			sess.probeErr = tc.probeErr

			var before float64
			if tc.wantOutcome != "" {
				before = stallProbeCount(t, tc.wantOutcome)
			}

			_, handled, err := r.reconcileStall(context.Background(), proj, task, stage.AgentImplement, time.Now())
			if err != nil {
				t.Fatalf("reconcileStall: %v", err)
			}
			if !handled {
				t.Fatal("handled = false: a turn past its inactivity window is always the stall machinery's business")
			}

			if got := len(sess.probes); got != tc.wantProbes {
				t.Errorf("probes written = %d, want %d", got, tc.wantProbes)
			}
			if got := len(sess.interrupts); got != tc.wantInterrupts {
				t.Errorf("interrupts = %d, want %d", got, tc.wantInterrupts)
			}
			if got := sess.handoffTurns > 0; got != tc.wantStopped {
				t.Errorf("pod stopped = %v, want %v (handoff turns = %d)", got, tc.wantStopped, sess.handoffTurns)
			}
			if tc.wantOutcome != "" {
				if delta := stallProbeCount(t, tc.wantOutcome) - before; delta != 1 {
					t.Errorf("operator_stall_probe_total{outcome=%q} delta = %v, want 1", tc.wantOutcome, delta)
				}
			}
		})
	}
}

// AN ANSWERED PROBE EXTENDS THE TURN INDEFINITELY, and that is the entire point
// of the phase.
//
// The answer is evidence, and evidence beats a clock: the operator re-bases the
// inactivity anchor on it, so the next verdict is a full turnTimeoutSeconds
// away - and the probe after that re-bases it again. There is no residual cap
// anywhere on the path, so an agent that keeps answering keeps working, for as
// long as it keeps answering.
func TestReconcileStall_AnsweredExtendsTheTurnWithNoBound(t *testing.T) {
	proj, task, r, sess := newStallFixture(t, 300, 2)

	// Three consecutive stall windows, each answered. If ANY cap survived
	// anywhere on this path, one of these rounds would stop the pod.
	for round := 1; round <= 3; round++ {
		now := time.Now()
		// Wind the turn back past its window again, exactly as real time would.
		if err := r.patchTaskAnnotations(context.Background(), task, func(fresh *tatarav1alpha1.Task) bool {
			fresh.Annotations[annTurnStartedAt] = now.Add(-2 * time.Hour).UTC().Format(time.RFC3339)
			fresh.Annotations[annTurnLastActivity] = now.Add(-2 * time.Hour).UTC().Format(time.RFC3339)
			return true
		}); err != nil {
			t.Fatalf("round %d rewind: %v", round, err)
		}

		if _, handled, err := r.reconcileStall(context.Background(), proj, task, stage.AgentImplement, now); err != nil || !handled {
			t.Fatalf("round %d probe send: handled=%v err=%v", round, handled, err)
		}
		probeID := mdGetTask(t, r.Client, task.Name).Annotations[annStallProbeID]
		if probeID == "" {
			t.Fatalf("round %d: no probe was written", round)
		}
		sess.probeStatus[probeID] = agent.ProbeResult{
			ProbeID: probeID, State: agent.ProbeStateAnswered, Answer: "TATARA-ALIVE still running the test suite",
		}

		answeredAt := now.Add(time.Minute)
		if _, handled, err := r.reconcileStall(context.Background(), proj, task, stage.AgentImplement, answeredAt); err != nil || !handled {
			t.Fatalf("round %d answer: handled=%v err=%v", round, handled, err)
		}

		got := mdGetTask(t, r.Client, task.Name)
		// The episode is retired, so the NEXT stall starts a fresh ladder rather
		// than inheriting a spent one.
		for _, ann := range []string{annStallProbeID, annStallProbeAt, annStallProbeAttempts} {
			if got.Annotations[ann] != "" {
				t.Errorf("round %d: annotation %s = %q, want cleared", round, ann, got.Annotations[ann])
			}
		}
		// And the anchor moved, which is what buys the next full window.
		if got.Annotations[annTurnLastActivity] != answeredAt.UTC().Format(time.RFC3339) {
			t.Errorf("round %d: turn-last-activity-at = %q, want the answer's timestamp %q",
				round, got.Annotations[annTurnLastActivity], answeredAt.UTC().Format(time.RFC3339))
		}
		// Nothing was stopped, interrupted, or torn down.
		if sess.handoffTurns != 0 || len(sess.interrupts) != 0 {
			t.Fatalf("round %d: handoffTurns=%d interrupts=%d, want 0/0 - an agent that answers is never escalated",
				round, sess.handoffTurns, len(sess.interrupts))
		}
		if tatarav1alpha1.TaskDone(got) || tatarav1alpha1.Parked(got) {
			t.Fatalf("round %d: task done=%v parked=%v", round, tatarav1alpha1.TaskDone(got), tatarav1alpha1.Parked(got))
		}

		// The immediately following pass finds nothing to do: not stalled any more.
		if _, handled, err := r.reconcileStall(context.Background(), proj, task, stage.AgentImplement, answeredAt); err != nil || handled {
			t.Fatalf("round %d follow-up: handled=%v err=%v, want handled=false - the answer ended the episode", round, handled, err)
		}
	}
}

// THE ESC PRECEDES THE STOP, AND THE ORDER IS THE WHOLE POINT.
//
// stalledTurnStop opens by waiting for the wrapper to go idle and only then
// submits the handoff turn - and a wedged turn never goes idle, so today that
// wait times out, POST /v1/messages would 409 anyway, and the sequence falls
// through to the operator's synthetic note. That is what makes "graceful" a
// misnomer in production. The interrupt cancels the in-flight turn (measured
// ~40ms, mid-tool, with session/sessionId/JSONL/workspace/context all surviving),
// so the wrapper reaches idle for real and the AGENT writes its own handoff.
func TestReconcileStall_EscalationInterruptsBeforeStopping(t *testing.T) {
	proj, task, r, sess := newStallFixture(t, 300, 2)
	seedProbeInFlight(t, r, task, "probe-wedged", 310*time.Second, 2)
	sess.probeStatus["probe-wedged"] = agent.ProbeResult{ProbeID: "probe-wedged", State: agent.ProbeStatePending}

	if _, handled, err := r.reconcileStall(context.Background(), proj, task, stage.AgentImplement, time.Now()); err != nil || !handled {
		t.Fatalf("reconcileStall: handled=%v err=%v", handled, err)
	}

	if len(sess.interrupts) != 1 {
		t.Fatalf("interrupts = %d, want 1: without the ESC the handoff turn is not submittable", len(sess.interrupts))
	}
	// The ESC bought a real handoff turn rather than the synthetic-note fallback.
	if sess.handoffTurns != 1 {
		t.Fatalf("handoff turns = %d, want 1", sess.handoffTurns)
	}
	got := mdGetTask(t, r.Client, task.Name)
	var handoff bool
	for _, n := range got.Status.Notes {
		if n.Kind == agent.NoteKindHandoff {
			handoff = true
		}
	}
	if !handoff {
		t.Fatalf("no kind=handoff note: notes=%d", len(got.Status.Notes))
	}
	// The turn AND the probe episode are cleared together - a probeId that
	// outlives its pod names a probe no wrapper knows, and a new wrapper 404s an
	// unknown id exactly as an old wrapper 404s the route.
	for _, ann := range []string{
		annCurrentTurn, annTurnStartedAt, annTurnLastActivity, annTurnComplete,
		annStallProbeID, annStallProbeAt, annStallProbeAttempts,
	} {
		if got.Annotations[ann] != "" {
			t.Errorf("annotation %s = %q, want cleared", ann, got.Annotations[ann])
		}
	}
	// A stalled TURN is still not a dead TASK.
	if got.Status.PodStartedAt != nil || got.Status.StateWorkStartedAt != nil {
		t.Errorf("pod clocks not re-armed: pod=%v work=%v", got.Status.PodStartedAt, got.Status.StateWorkStartedAt)
	}
	if tatarav1alpha1.TaskDone(got) {
		t.Error("stopping a stalled TURN must never terminate the TASK")
	}
}

// An interrupt that fails must not become a new way to lose a stop. The stop
// below it is the same stop this Task got before the phase, and it copes with a
// turn that never clears by construction - that is the path it has been taking
// in production all along.
func TestReconcileStall_InterruptFailureStillStops(t *testing.T) {
	proj, task, r, sess := newStallFixture(t, 300, 1)
	seedProbeInFlight(t, r, task, "probe-x", 310*time.Second, 1)
	sess.probeStatus["probe-x"] = agent.ProbeResult{ProbeID: "probe-x", State: agent.ProbeStatePending}
	sess.interruptErr = agent.ErrProbeUnsupported

	if _, handled, err := r.reconcileStall(context.Background(), proj, task, stage.AgentImplement, time.Now()); err != nil || !handled {
		t.Fatalf("reconcileStall: handled=%v err=%v", handled, err)
	}
	if sess.handoffTurns != 1 {
		t.Fatalf("handoff turns = %d, want 1: a refused ESC must not swallow the stop", sess.handoffTurns)
	}
}

// A turn that resumed reporting activity while a probe was outstanding retires
// the episode instead of carrying a spent ladder into the next stall.
func TestReconcileStall_RecoveredTurnRetiresTheEpisode(t *testing.T) {
	proj, task, r, sess := newStallFixture(t, 300, 2)
	seedProbeInFlight(t, r, task, "probe-old", 10*time.Second, 2)
	if err := r.patchTaskAnnotations(context.Background(), task, func(fresh *tatarav1alpha1.Task) bool {
		fresh.Annotations[annTurnLastActivity] = time.Now().Add(-time.Second).UTC().Format(time.RFC3339)
		return true
	}); err != nil {
		t.Fatalf("seed activity: %v", err)
	}

	_, handled, err := r.reconcileStall(context.Background(), proj, task, stage.AgentImplement, time.Now())
	if err != nil {
		t.Fatalf("reconcileStall: %v", err)
	}
	if handled {
		t.Fatal("handled = true: a turn inside its window must fall through to ordinary stage work")
	}
	got := mdGetTask(t, r.Client, task.Name)
	for _, ann := range []string{annStallProbeID, annStallProbeAt, annStallProbeAttempts} {
		if got.Annotations[ann] != "" {
			t.Errorf("annotation %s = %q, want cleared", ann, got.Annotations[ann])
		}
	}
	if sess.handoffTurns != 0 || len(sess.interrupts) != 0 {
		t.Errorf("handoffTurns=%d interrupts=%d, want 0/0", sess.handoffTurns, len(sess.interrupts))
	}
}

// The knobs default rather than degrade. A Project created before the CRD grew
// the two fields decodes them as ZERO, and zero would mean an instant grace
// window and an empty attempt ladder - escalating on the first unanswered poll,
// which is strictly MORE aggressive than the behaviour this phase replaces.
func TestStallProbeKnobDefaults(t *testing.T) {
	for _, tc := range []struct {
		name              string
		grace, attempts   int
		wantGrace         time.Duration
		wantAttemptsValue int
	}{
		{"unset (a Project older than the schema)", 0, 0, 300 * time.Second, 2},
		{"set", 90, 4, 90 * time.Second, 4},
		{"above the validated ceiling", 90, 99, 90 * time.Second, 5},
		{"negative", -5, -5, 300 * time.Second, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &tatarav1alpha1.Project{}
			p.Spec.Agent.StallProbeGraceSeconds = tc.grace
			p.Spec.Agent.StallProbeMaxAttempts = tc.attempts
			if got := stallProbeGrace(p); got != tc.wantGrace {
				t.Errorf("grace = %v, want %v", got, tc.wantGrace)
			}
			if got := stallProbeMaxAttempts(p); got != tc.wantAttemptsValue {
				t.Errorf("maxAttempts = %d, want %d", got, tc.wantAttemptsValue)
			}
		})
	}
}

// THE AGENT ASKED TO BE STOPPED, SO STOP IT - NOW, NOT ON A CLOCK.
//
// task_note(kind=handoff) with no turn in flight is the agent saying it is done
// talking and has left everything the next pod needs. Until this branch existed
// the pod then sat there holding a concurrency slot until the pod TTL, the
// conversation idle budget or the idle reaper happened to come round, up to
// hours later.
func TestReconcilePodStage_AgentHandoffNoteStopsThePodImmediately(t *testing.T) {
	for _, tc := range []struct {
		name string
		// noteAgo is how long before now the note was written, relative to a pod
		// that started 10 minutes ago.
		noteAgo      time.Duration
		noteAgent    string
		turnInFlight bool
		wantStopped  bool
	}{
		{
			name:        "this pod's agent asked: stopped",
			noteAgo:     time.Minute,
			noteAgent:   "implement",
			wantStopped: true,
		},
		{
			name: "the note predates this pod: NOT stopped",
			// Every graceful stop ends by putting a handoff note in the journal, so
			// a CONTINUATION pod is born into a Task that already carries one. An
			// unscoped check would kill every replacement pod at boot, forever.
			noteAgo:   30 * time.Minute,
			noteAgent: "implement",
		},
		{
			name: "a turn is in flight: NOT stopped",
			// A note written mid-turn is the agent narrating, not finishing.
			noteAgo:      time.Minute,
			noteAgent:    "implement",
			turnInFlight: true,
		},
		{
			name: "the operator's own synthetic note: NOT stopped",
			// It is continuation state for the next pod, never the agent asking for
			// anything.
			noteAgo:   time.Minute,
			noteAgent: agent.NoteAgentOperator,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proj, task, r, sess := newConversingExitFixture(t)
			task.Status.State = tatarav1alpha1.StateUnderImplementation
			task.Status.AgentKind = stage.AgentKindFor(tatarav1alpha1.StateUnderImplementation, "implement")
			task.Annotations = map[string]string{}
			if tc.turnInFlight {
				task.Annotations[annCurrentTurn] = "turn-live"
				task.Annotations[annTurnStartedAt] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
			}
			if err := r.Update(context.Background(), task); err != nil {
				t.Fatalf("seed annotations: %v", err)
			}
			if err := r.patchTaskStatus(context.Background(), task, func(fresh *tatarav1alpha1.Task) bool {
				fresh.Status.Notes = append(fresh.Status.Notes, tatarav1alpha1.Note{
					At:    metav1.NewTime(time.Now().Add(-tc.noteAgo)),
					Agent: tc.noteAgent,
					Kind:  agent.NoteKindHandoff,
					Body:  "handing off",
				})
				return true
			}); err != nil {
				t.Fatalf("seed note: %v", err)
			}

			if _, err := r.reconcilePodStage(context.Background(), proj, task, stage.AgentImplement, time.Now()); err != nil {
				t.Fatalf("reconcilePodStage: %v", err)
			}

			got := mdGetTask(t, r.Client, task.Name)
			stopped := got.Status.PodStartedAt == nil
			if stopped != tc.wantStopped {
				t.Fatalf("pod stopped = %v, want %v", stopped, tc.wantStopped)
			}
			if !tc.wantStopped {
				return
			}
			// No handoff TURN: the agent has already written the handoff, so the G.7
			// extraction sequence would spend a real turn re-asking an answered
			// question.
			if sess.handoffTurns != 0 {
				t.Errorf("handoff turns = %d, want 0: the agent already handed off", sess.handoffTurns)
			}
			if wrapperPodExists(t, r, agent.PodName(task)) {
				t.Error("the wrapper pod survived an agent-requested stop")
			}
			if got.Status.StateWorkStartedAt != nil {
				t.Error("pod clocks not re-armed")
			}
			if tatarav1alpha1.TaskDone(got) {
				t.Error("an agent-requested pod stop must never terminate the TASK")
			}
		})
	}
}
