package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// evictionCeilingProject is the project shape every test in this file drives:
// a real ceiling that binds, and a turn timeout so the handoff stopper's hard
// cap is computed from something finite.
func evictionCeilingProject(ceiling int) *tatarav1alpha1.Project {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxLivePods = ceiling
	proj.Spec.MaxConcurrentAgents = 6
	proj.Spec.Agent.TurnTimeoutSeconds = 600
	return proj
}

// justAdmittedTask is the shape at the centre of #561: a Task that entered a
// live state SECONDS ago, had its admission ticket admitted, and has a pod that
// has not become ready yet. status.stateWorkStartedAt is the POD-READY stamp, so
// a nil one is the operator's own record that this conversation has never
// started - no turn 0 was submitted, nothing was said, nothing can be idle.
func justAdmittedTask(name, project string, now time.Time) *tatarav1alpha1.Task {
	t := liveStateTask(name, project, now)
	t.CreationTimestamp = metav1.NewTime(now)
	podAt := metav1.NewTime(now)
	t.Status.PodStartedAt = &podAt
	t.Status.StateWorkStartedAt = nil
	return t
}

// refuseHandoff makes the capacity fixture's wrapper REFUSE the G.7 handoff turn
// (410 Gone - the wrapper past its own t0, or a pod already gone), so the
// operator falls through to its own synthetic note and no AGENT-authored handoff
// note is ever written. That is the whole discriminator for the human-gate test.
func refuseHandoff(t *testing.T, r *ProjectReconciler) {
	t.Helper()
	sess, ok := r.Tasks.Session.(*capacityHandoffSession)
	if !ok {
		t.Fatalf("fixture session is %T, want *capacityHandoffSession", r.Tasks.Session)
	}
	sess.submitErr = &agent.HTTPError{Status: 410, Body: "gone"}
}

// TestEvictionPrefersAGenuinelyIdleTaskOverAJustCreatedOne is #561 defect 1.
//
// conversationIdleSince used to answer time.Time{} for a Task with no
// ConversationLastEventAt, and sortByIdleThenName orders ASCENDING, so a Task
// that has never had an event sorted FIRST and was evicted ahead of a
// conversation that had genuinely been silent for forty minutes.
//
// THE OLD DOC COMMENT'S REASONING IS INVERTED, and it is worth stating because
// it reads plausibly: "a Task with no stamp ... has never had an event and is
// the least likely to be mid-exchange". That is true of an ANCIENT conversation
// that never got a reply. It is exactly wrong for a Task created moments ago
// that has not had TIME to produce an event - and the zero time cannot tell the
// two apart, so it sacrifices the newest work in the project first.
//
// Both Tasks here are legal victims (pod ready, no turn in flight); the ONLY
// thing under test is which one the idle key picks.
func TestEvictionPrefersAGenuinelyIdleTaskOverAJustCreatedOne(t *testing.T) {
	now := time.Now()
	proj := evictionCeilingProject(1)

	oldIdle := liveStateTask("old-idle", "infrastructure", now.Add(-40*time.Minute))
	brandNew := liveStateTask("brand-new", "infrastructure", now)
	// The defect verbatim: never stamped. Everything else about it says NEW -
	// it entered its live state and was created this instant.
	brandNew.Status.ConversationLastEventAt = nil
	brandNew.CreationTimestamp = metav1.NewTime(now)

	r := newLiveCapacityFixture(t, proj, oldIdle, brandNew)

	if _, err := r.enforceLivePodCeiling(context.Background(), proj, now); err != nil {
		t.Fatalf("enforceLivePodCeiling: %v", err)
	}

	var gotNew tatarav1alpha1.Task
	if err := r.Get(context.Background(), objectKeyOf(brandNew), &gotNew); err != nil {
		t.Fatalf("get brand-new: %v", err)
	}
	if tatarav1alpha1.Parked(&gotNew) {
		t.Fatalf("the just-created Task was evicted (parked %s): a Task that has never had an event is the NEWEST conversation in the project, not the most idle one",
			gotNew.Status.ParkReason)
	}

	var gotOld tatarav1alpha1.Task
	if err := r.Get(context.Background(), objectKeyOf(oldIdle), &gotOld); err != nil {
		t.Fatalf("get old-idle: %v", err)
	}
	if !tatarav1alpha1.Parked(&gotOld) {
		t.Fatal("the genuinely idle 40-minute-old conversation was not evicted")
	}
}

// TestConversationIdleSinceFallsBackToNewestNotOldest pins the key itself, below
// the reconciler, so the fallback ORDER is testable rather than only its effect:
// ConversationLastEventAt, then the live-state entry stamp, then the object's
// own creation instant. Every one of those means "this is as new as we can
// prove", and none of them is the zero time.
func TestConversationIdleSinceFallsBackToNewestNotOldest(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	entered := now.Add(-3 * time.Minute)
	created := now.Add(-5 * time.Minute)
	lastEvent := now.Add(-1 * time.Minute)

	build := func() *tatarav1alpha1.Task {
		task := &tatarav1alpha1.Task{}
		task.Namespace = "tatara"
		task.Name = "t"
		task.CreationTimestamp = metav1.NewTime(created)
		task.Status.State = tatarav1alpha1.StateRefined
		task.Status.StateEnteredAt = &metav1.Time{Time: entered}
		task.Status.ConversationLastEventAt = &metav1.Time{Time: lastEvent}
		return task
	}

	full := build()
	if got := conversationIdleSince(full); !got.Equal(lastEvent) {
		t.Errorf("with a conversation stamp: got %v, want %v", got, lastEvent)
	}

	noEvent := build()
	noEvent.Status.ConversationLastEventAt = nil
	if got := conversationIdleSince(noEvent); !got.Equal(entered) {
		t.Errorf("with no conversation stamp: got %v, want the live-state entry stamp %v", got, entered)
	}

	nothing := build()
	nothing.Status.ConversationLastEventAt = nil
	nothing.Status.StateEnteredAt = nil
	if got := conversationIdleSince(nothing); !got.Equal(created) {
		t.Errorf("with neither stamp: got %v, want the creation timestamp %v", got, created)
	}
	if got := conversationIdleSince(nothing); got.IsZero() {
		t.Error("the fallback is still the zero time: a Task with no stamps sorts FIRST for eviction, which is the whole defect")
	}
}

// TestEvictionSparesAConversationWhosePodHasNotStartedYet is the PRODUCTION
// SHAPE of #561, and it is a different defect from the one above.
//
// Measured on the operator's own logs for the 2026-08-08 eviction storm: 74
// live_evicting lines, median lag from the victim's own idle_since to its
// eviction 53.5 seconds, 38 of the 74 inside a minute. mt-r-tatara-cli-97-* was
// created at 06:43:57.933 and evicted at 06:44:00.382 - 2.45 seconds old, never
// having held a turn in its life - carrying idle_since=06:43:57Z, its own
// state-entry instant. Its ConversationLastEventAt was NOT nil (stage.stampEnter
// writes one on entry into every live state), so the zero-time fallback is not
// what picked it: a seconds-old stamp is simply the oldest thing on offer in a
// project whose live set is itself churning.
//
// The mid-turn siblings in this fixture are the ADVERSARIAL version of that. The
// B4 guard has already taken every busy conversation out of the running, so the
// only candidate left is the one that has been alive for eight seconds - and
// with a single candidate the sort has no say at all. That is why unstarted has
// to be an EXCLUSION and not merely a sort term.
//
// A Task whose pod has never become ready has submitted no turn, said nothing
// and asked nothing. Ending its "conversation" reclaims no warmth, because
// there is none: it destroys the newest work in the project and the admission
// slot the queue granted it one second ago, and the Task simply re-queues for
// the pod it never got.
func TestEvictionSparesAConversationWhosePodHasNotStartedYet(t *testing.T) {
	now := time.Now()
	proj := evictionCeilingProject(2)

	busyA := liveStateTask("busy-a", "infrastructure", now.Add(-40*time.Minute))
	busyA.Annotations = map[string]string{tatarav1alpha1.AnnCurrentTurn: "turn-7"}
	busyB := liveStateTask("busy-b", "infrastructure", now.Add(-35*time.Minute))
	busyB.Annotations = map[string]string{tatarav1alpha1.AnnCurrentTurn: "turn-4"}
	justAdmitted := justAdmittedTask("just-admitted", "infrastructure", now)

	r := newLiveCapacityFixture(t, proj, busyA, busyB, justAdmitted)

	requeue, err := r.enforceLivePodCeiling(context.Background(), proj, now)
	if err != nil {
		t.Fatalf("enforceLivePodCeiling: %v", err)
	}
	if requeue != 0 {
		t.Fatalf("requeue = %v, want 0: no legal victim exists this pass, and a 5s retry cannot change that", requeue)
	}

	for _, tk := range []*tatarav1alpha1.Task{busyA, busyB, justAdmitted} {
		var got tatarav1alpha1.Task
		if err := r.Get(context.Background(), objectKeyOf(tk), &got); err != nil {
			t.Fatalf("get %s: %v", tk.Name, err)
		}
		if tatarav1alpha1.Parked(&got) {
			t.Fatalf("%s was evicted (parked %s): the only non-mid-turn Task in the project had been live for eight seconds and had never had a ready pod - it is unstarted, not idle",
				tk.Name, got.Status.ParkReason)
		}
	}
}

// TestEvictionCandidatesExcludesAConversationThatNeverStarted is the direct unit
// test on the selection helper, the twin of
// TestEvictionCandidatesExcludesMidTurnTasks. An unstarted Task must be ABSENT
// from the candidate slice, not merely sorted last: sorting last still reaches
// it once every older candidate is spent, which is exactly how a project sitting
// at "one unstarted Task and N mid-turn Tasks" ends up evicting the unstarted
// one.
func TestEvictionCandidatesExcludesAConversationThatNeverStarted(t *testing.T) {
	now := time.Now()
	idle := liveStateTask("idle", "infrastructure", now.Add(-10*time.Minute))
	unstarted := justAdmittedTask("unstarted", "infrastructure", now)

	got := evictionCandidates([]tatarav1alpha1.Task{*unstarted, *idle})

	if len(got) != 1 || got[0].Name != "idle" {
		names := make([]string, len(got))
		for i := range got {
			names[i] = got[i].Name
		}
		t.Fatalf("evictionCandidates = %v, want exactly [idle]: a Task whose pod never became ready has no conversation to end", names)
	}
}

// TestEvictionWithoutAgentHandoffReArmsInsteadOfFakingAHumanGate is #561
// defect 2, and it is #557's argument applied to the path #557 deliberately
// left alone.
//
// awaiting-human is UnparkHuman: it clears on a non-bot comment and on nothing
// else. The eviction path parked there unconditionally, carrying the operator's
// own synthetic note - so a Task that was never asked anything sat waiting for
// an answer nobody owed it. Measured in the 45 minutes before #561 was filed:
// 20 unpark_declined events with park_reason=awaiting-human, on top of 30
// fabricated parks already on the books.
//
// The capacity decision is still made - the pod dies either way. What changes is
// that a Task whose agent said nothing goes back in the admission queue instead
// of behind a gate nobody will open.
func TestEvictionWithoutAgentHandoffReArmsInsteadOfFakingAHumanGate(t *testing.T) {
	now := time.Now()
	proj := evictionCeilingProject(1)

	survivor := liveStateTask("survivor", "infrastructure", now.Add(-1*time.Minute))
	victim := liveStateTask("victim", "infrastructure", now.Add(-40*time.Minute))

	r := newLiveCapacityFixture(t, proj, survivor, victim)
	refuseHandoff(t, r)

	if _, err := r.enforceLivePodCeiling(context.Background(), proj, now); err != nil {
		t.Fatalf("enforceLivePodCeiling: %v", err)
	}

	var got tatarav1alpha1.Task
	if err := r.Get(context.Background(), objectKeyOf(victim), &got); err != nil {
		t.Fatalf("get victim: %v", err)
	}
	if tatarav1alpha1.Parked(&got) {
		t.Fatalf("victim parked(%s): the agent never wrote a handoff note, so nothing was asked of a human and awaiting-human will never clear",
			got.Status.ParkReason)
	}
	if got.Status.State != tatarav1alpha1.StateRefined {
		t.Fatalf("state = %s, want unchanged (refined): re-arming never moves state", got.Status.State)
	}
	if got.Status.PodStartedAt != nil || got.Status.StateWorkStartedAt != nil {
		t.Fatalf("pod clocks not re-armed: podStartedAt=%v stateWorkStartedAt=%v",
			got.Status.PodStartedAt, got.Status.StateWorkStartedAt)
	}
	if got.Status.Stats.PodRecreations != 1 {
		t.Fatalf("podRecreations = %d, want 1: a re-arm that does not charge the recreation budget can be evicted forever",
			got.Status.Stats.PodRecreations)
	}
	// stateEnteredAt is CLOCK 1's base. Leaving it stale re-elapses the
	// admission budget on the very next pass and parks admission-starved, which
	// is UnparkNever - strictly worse than the awaiting-human this replaces
	// (#557, #513).
	if got.Status.StateEnteredAt == nil || got.Status.StateEnteredAt.Time.Before(now.Add(-time.Second)) {
		t.Fatalf("stateEnteredAt = %v, want re-stamped to ~now: clock 1 would otherwise re-elapse straight into admission-starved",
			got.Status.StateEnteredAt)
	}

	var kept tatarav1alpha1.Task
	if err := r.Get(context.Background(), objectKeyOf(survivor), &kept); err != nil {
		t.Fatalf("get survivor: %v", err)
	}
	if tatarav1alpha1.Parked(&kept) {
		t.Fatal("the survivor was evicted too: eviction took more than the overflow")
	}
}

// TestEvictionWithAnAgentHandoffStillParks is the DISCRIMINATION half. The
// re-arm above must not become "eviction never parks": a Task whose agent DID
// write a handoff note posed something a human can answer, so awaiting-human is
// the correct gate and the un-park drops it straight back into the live state it
// was talking in.
func TestEvictionWithAnAgentHandoffStillParks(t *testing.T) {
	now := time.Now()
	proj := evictionCeilingProject(1)

	survivor := liveStateTask("survivor", "infrastructure", now.Add(-1*time.Minute))
	victim := liveStateTask("victim", "infrastructure", now.Add(-40*time.Minute))

	// No refuseHandoff: the fixture's session writes an AGENT-authored handoff
	// note, exactly as a real wrapper's task_note(kind=handoff) would.
	r := newLiveCapacityFixture(t, proj, survivor, victim)

	if _, err := r.enforceLivePodCeiling(context.Background(), proj, now); err != nil {
		t.Fatalf("enforceLivePodCeiling: %v", err)
	}

	var got tatarav1alpha1.Task
	if err := r.Get(context.Background(), objectKeyOf(victim), &got); err != nil {
		t.Fatalf("get victim: %v", err)
	}
	if !tatarav1alpha1.Parked(&got) || got.Status.ParkReason != stage.ReasonAwaitingHuman {
		t.Fatalf("victim parked=%v reason=%q, want parked(awaiting-human): the agent DID hand off, so there is a real question waiting on a real reply",
			tatarav1alpha1.Parked(&got), got.Status.ParkReason)
	}
	if got.Status.Stats.PodRecreations != 0 {
		t.Fatalf("podRecreations = %d, want 0: a park is not a respawn and must not charge the recreation budget",
			got.Status.Stats.PodRecreations)
	}
}

// O3 REMOVED THIS BOUND, AND THAT IS THE ACCEPTED RISK. Repeated eviction of the
// same conversation used to terminate at pod-recreation-exhausted; there is no
// recreation ceiling left, so it re-arms every time and the loop is bounded only
// by the 24h residency dead-man switch, with
// sum by (project) (increase(operator_pod_recreations_total[1h])) > 6 as the
// compensating control. The count must therefore keep advancing.
func TestEvictionWithoutAgentHandoffReArmsEvenDeepIntoTheRecreationCount(t *testing.T) {
	now := time.Now()
	proj := evictionCeilingProject(1)

	survivor := liveStateTask("survivor", "infrastructure", now.Add(-1*time.Minute))
	victim := liveStateTask("victim", "infrastructure", now.Add(-40*time.Minute))
	victim.Status.Stats.PodRecreations = 12

	r := newLiveCapacityFixture(t, proj, survivor, victim)
	refuseHandoff(t, r)

	if _, err := r.enforceLivePodCeiling(context.Background(), proj, now); err != nil {
		t.Fatalf("enforceLivePodCeiling: %v", err)
	}

	var got tatarav1alpha1.Task
	if err := r.Get(context.Background(), objectKeyOf(victim), &got); err != nil {
		t.Fatalf("get victim: %v", err)
	}
	if tatarav1alpha1.Parked(&got) {
		t.Fatalf("parked at %q: pod-recreation-exhausted is not reachable after O3", got.Status.ParkReason)
	}
	if got.Status.Stats.PodRecreations != 13 {
		t.Fatalf("podRecreations = %d, want 13: the churn alert's input must keep advancing",
			got.Status.Stats.PodRecreations)
	}
}
