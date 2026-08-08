package agent

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// The wrapper computes its own 410-Gone deadline from podStart plus the
// AGENT_POD_TTL_SECONDS env the operator stamps. So the operator's TTLDeadline
// and the pod's env MUST come from one resolver, or a conversing pod is refused
// turns at a deadline the operator does not believe in.
func TestPodTTLSecondsIsPerStage(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.AgentPodTTLSeconds = 3600
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{ConversationIdleMinutes: 15}

	cases := []struct {
		name  string
		stage string
		want  int
	}{
		{name: "new uses the flat project TTL", stage: tatarav1alpha1.StateNew, want: 3600},
		{name: "merged uses the flat project TTL", stage: tatarav1alpha1.StateMerged, want: 3600},
		// A LIVE STATE TAKES THE LONGER OF THE TWO, never the shorter. #521
		// promoted `conversing`'s substitution to every live state, which meant an
		// implement pod was rotated at the 15m idle window - mid-turn, with no
		// completed turn to hand off from. The substitution only ever existed to
		// stop a pod outliving a conversation that is about to park, and it cannot
		// buy that here: the park tears the pod down anyway, so a LONGER pod TTL
		// costs nothing on the idle path and is the only thing that lets an agent
		// work for longer than one idle window.
		{name: "refined takes the longer of the two", stage: tatarav1alpha1.StateRefined, want: 3600},
		{name: "under-implementation takes the longer of the two", stage: tatarav1alpha1.StateUnderImplementation, want: 3600},
		{name: "awaiting-review takes the longer of the two", stage: tatarav1alpha1.StateAwaitingReview, want: 3600},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &tatarav1alpha1.Task{}
			task.Status.State = tc.stage
			if got := PodTTLSeconds(proj, task); got != tc.want {
				t.Errorf("PodTTLSeconds(%s) = %d, want %d", tc.stage, got, tc.want)
			}
		})
	}
}

// The other direction, which is what the original substitution was FOR: when the
// idle window is the longer of the two, a live pod gets the idle window, so it
// does not rotate at the flat TTL while the conversation still has time to run.
func TestPodTTLSecondsTakesTheIdleWindowWhenItIsLonger(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.AgentPodTTLSeconds = 1800
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{ConversationIdleMinutes: 90}

	task := &tatarav1alpha1.Task{}
	task.Status.State = tatarav1alpha1.StateRefined
	if got, want := PodTTLSeconds(proj, task), 5400; got != want {
		t.Errorf("PodTTLSeconds = %d, want %d", got, want)
	}
	task.Status.State = tatarav1alpha1.StateMerged
	if got, want := PodTTLSeconds(proj, task), 1800; got != want {
		t.Errorf("PodTTLSeconds(merged) = %d, want %d: only a LIVE state consults the idle window", got, want)
	}
}

// The project TTL is deliberately SHORTER than the conversation window here, so
// the live-state branch is the one under test: the wrapper refuses turns past
// podStart + this value, and a pod rotating at the flat 5m in the middle of a 15m
// conversation window would be refused turns the operator still believes in.
func TestTTLDeadlineUsesPodTTLSeconds(t *testing.T) {
	start := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	proj := &tatarav1alpha1.Project{}
	proj.Spec.AgentPodTTLSeconds = 300
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{ConversationIdleMinutes: 15}

	task := &tatarav1alpha1.Task{}
	task.Status.State = tatarav1alpha1.StateRefined
	task.Status.PodStartedAt = &metav1.Time{Time: start}

	t0, ok := TTLDeadline(proj, task)
	if !ok {
		t.Fatal("TTLDeadline ok = false, want true")
	}
	if want := start.Add(15 * time.Minute); !t0.Equal(want) {
		t.Fatalf("t0 = %v, want %v: the operator and the pod env disagree on the TTL", t0, want)
	}
}

// TestTTLDeadlineExtendsOnFreshConversationActivity is the G.7/F.4 gap issue
// #508 reports: a live pod's TTL deadline was anchored ONLY on
// podStartedAt, so a maintainer replying well inside the idle budget still
// got the pod TTL-rotated out from under the conversation at the ORIGINAL
// podStartedAt+ttl instant - the reply never bought the live pod any more
// time, even though the stage-exit idle clock (stage.ArmedClock) correctly
// reset on the same event. The pod-level TTL must track the SAME idle
// clock: t0 is podStartedAt+ttl, or conversationLastEventAt+ttl when a
// qualifying event landed AFTER the pod started - whichever is later.
func TestTTLDeadlineExtendsOnFreshConversationActivity(t *testing.T) {
	podStart := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	proj := &tatarav1alpha1.Project{}
	proj.Spec.AgentPodTTLSeconds = 300
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{ConversationIdleMinutes: 15}

	task := &tatarav1alpha1.Task{}
	task.Status.State = tatarav1alpha1.StateUnderImplementation
	task.Status.PodStartedAt = &metav1.Time{Time: podStart}

	// A human replied 10 minutes into the pod's life - well inside the 15m
	// idle budget - resetting the idle clock as AppendTaskEvent always does.
	lastEvent := podStart.Add(10 * time.Minute)
	task.Status.ConversationLastEventAt = &metav1.Time{Time: lastEvent}

	t0, ok := TTLDeadline(proj, task)
	if !ok {
		t.Fatal("TTLDeadline ok = false, want true")
	}
	want := lastEvent.Add(15 * time.Minute)
	if !t0.Equal(want) {
		t.Fatalf("t0 = %v, want %v (anchored on the last conversation event, not the stale pod start)", t0, want)
	}

	// A pod freshly rotated AFTER the last recorded event must not be
	// short-changed by a stale ConversationLastEventAt that predates it.
	task2 := &tatarav1alpha1.Task{}
	task2.Status.State = tatarav1alpha1.StateUnderImplementation
	task2.Status.ConversationLastEventAt = &metav1.Time{Time: podStart.Add(-time.Hour)}
	freshStart := podStart
	task2.Status.PodStartedAt = &metav1.Time{Time: freshStart}
	t0, ok = TTLDeadline(proj, task2)
	if !ok {
		t.Fatal("TTLDeadline ok = false, want true")
	}
	if want := freshStart.Add(15 * time.Minute); !t0.Equal(want) {
		t.Fatalf("t0 = %v, want %v (a freshly rotated pod gets a full new window)", t0, want)
	}
}

func TestAgentEnvStampsThePerStageTTL(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Name = "infrastructure"
	proj.Spec.AgentPodTTLSeconds = 300
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{ConversationIdleMinutes: 15}

	task := &tatarav1alpha1.Task{}
	task.Name = "t"
	task.Status.State = tatarav1alpha1.StateRefined
	task.Status.AgentKind = "clarify"

	got := ""
	for _, e := range AgentEnv(proj, task) {
		if e.Name == "AGENT_POD_TTL_SECONDS" {
			got = e.Value
		}
	}
	if got != "900" {
		t.Fatalf("AGENT_POD_TTL_SECONDS = %q, want \"900\"", got)
	}
}
