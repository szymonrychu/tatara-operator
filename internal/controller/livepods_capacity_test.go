package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// liveStateTask builds a Task sitting in a LIVE state (refined - one of the three
// stage.Live states, #521's dissolution of the old `conversing` stage into a
// property over refined/under-implementation/awaiting-review).
//
// stateWorkStartedAt is stamped too, because a live conversation has a READY
// pod: it is the pod-ready stamp, and a Task that has never had one has never
// submitted turn 0 and so has no conversation to end (#561). A fixture that left
// it nil would model a just-admitted Task, not a live one - see
// justAdmittedTask in livepods_eviction_victim_test.go for that shape.
func liveStateTask(name, project string, lastEvent time.Time) *tatarav1alpha1.Task {
	t := &tatarav1alpha1.Task{}
	t.Namespace = "tatara"
	t.Name = name
	t.Spec.ProjectRef = project
	t.Spec.Kind = "implement"
	t.Status.State = tatarav1alpha1.StateRefined
	t.Status.AgentKind = stage.AgentKindFor(tatarav1alpha1.StateRefined, "implement")
	t.Status.StateEnteredAt = &metav1.Time{Time: lastEvent}
	t.Status.ConversationLastEventAt = &metav1.Time{Time: lastEvent}
	t.Status.StateWorkStartedAt = &metav1.Time{Time: lastEvent}
	return t
}

func TestLiveHasRoom(t *testing.T) {
	now := time.Now()
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxLivePods = 2
	proj.Spec.MaxConcurrentAgents = 5

	cases := []struct {
		name string
		live int
		want bool
	}{
		{name: "empty", live: 0, want: true},
		{name: "one under the ceiling", live: 1, want: true},
		{name: "at the ceiling", live: 2, want: false},
		{name: "over the ceiling", live: 3, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			objs := []client.Object{proj}
			for i := 0; i < tc.live; i++ {
				objs = append(objs, liveStateTask(fmt.Sprintf("c%d", i), "infrastructure", now))
			}
			c := newFakeClientWith(t, objs...)
			got, err := LiveHasRoom(context.Background(), c, proj)
			if err != nil {
				t.Fatalf("LiveHasRoom: %v", err)
			}
			if got != tc.want {
				t.Errorf("LiveHasRoom = %v, want %v", got, tc.want)
			}
		})
	}
}

// Tasks in ANOTHER project never count against this project's ceiling.
func TestLiveHasRoomIsPerProject(t *testing.T) {
	now := time.Now()
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxLivePods = 1
	proj.Spec.MaxConcurrentAgents = 5

	c := newFakeClientWith(t, proj, liveStateTask("other", "someone-else", now))
	got, err := LiveHasRoom(context.Background(), c, proj)
	if err != nil {
		t.Fatalf("LiveHasRoom: %v", err)
	}
	if !got {
		t.Fatal("another project's live task consumed this project's ceiling")
	}
}

// v1alpha1.MaxLivePods CLAMPS a configured value that would never bind against
// MaxConcurrentAgents (2026-07-28 final review IMPORTANT 1): a project
// configured with maxLivePods >= maxConcurrentAgents has, in effect, NO
// live-pod ceiling at all, because the agent-concurrency cap always saturates
// first. LiveHasRoom must answer off the CLAMPED ceiling, not the raw spec
// value, or this test's 3 live tasks against a nominal ceiling of 5 (but a
// concurrency cap of 3, clamping the effective ceiling to 2) would wrongly
// report room.
func TestLiveHasRoom_BindsAtTheClampedCeiling(t *testing.T) {
	now := time.Now()
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxConcurrentAgents = 3
	proj.Spec.MaxLivePods = 5 // clamps to MaxConcurrentAgents-1 == 2

	if got := tatarav1alpha1.MaxLivePods(proj); got != 2 {
		t.Fatalf("v1alpha1.MaxLivePods = %d, want the clamped value 2", got)
	}

	objs := []client.Object{proj, liveStateTask("a", "infrastructure", now), liveStateTask("b", "infrastructure", now)}
	c := newFakeClientWith(t, objs...)
	got, err := LiveHasRoom(context.Background(), c, proj)
	if err != nil {
		t.Fatalf("LiveHasRoom: %v", err)
	}
	if got {
		t.Fatal("LiveHasRoom reported room at the clamped ceiling (2 live == clamped ceiling of 2); it must not use the unclamped spec value")
	}
}

// countLive's whole predicate is exactly two clauses (stage.Live(state) AND
// !Parked): this asserts it counts every unparked live-state Task and nothing
// else, across every state in the enum.
func TestCountLive_CountsExactlyTheUnparkedLiveStates(t *testing.T) {
	now := time.Now()
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"

	var objs []client.Object
	objs = append(objs, proj)
	var wantCounted []string
	for _, st := range stage.AllStates() {
		name := "t-" + st
		task := liveStateTask(name, "infrastructure", now)
		task.Status.State = st
		task.Status.AgentKind = stage.AgentKindFor(st, "implement")
		objs = append(objs, task)
		if stage.Live(st) {
			wantCounted = append(wantCounted, name)
		}
	}

	c := newFakeClientWith(t, objs...)
	live, err := countLive(context.Background(), c, proj)
	if err != nil {
		t.Fatalf("countLive: %v", err)
	}
	if len(live) != len(wantCounted) {
		var got []string
		for i := range live {
			got = append(got, live[i].Name)
		}
		t.Fatalf("countLive returned %v, want exactly the live states %v", got, wantCounted)
	}
}

// A parked Task in a live state holds no pod - park is what takes the pod
// down - so countLive must exclude it even though its state still reports
// stage.Live == true. `live` alone (without the !Parked clause) would count a
// conversation whose pod is already gone and wrongly refuse a legitimate new
// one.
func TestCountLive_ExcludesAParkedTaskInALiveState(t *testing.T) {
	now := time.Now()
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"

	parked := liveStateTask("parked-one", "infrastructure", now)
	if err := stage.Park(parked, stage.ReasonAwaitingHuman, now); err != nil {
		t.Fatalf("stage.Park: %v", err)
	}
	if !stage.Live(parked.Status.State) {
		t.Fatalf("fixture bug: parked task's state %q is not live", parked.Status.State)
	}

	c := newFakeClientWith(t, proj, parked)
	live, err := countLive(context.Background(), c, proj)
	if err != nil {
		t.Fatalf("countLive: %v", err)
	}
	if len(live) != 0 {
		t.Fatalf("countLive = %v, want empty: a parked task holds no pod however live its state", live)
	}
}

// countLive must not try to be cleverer than its own two clauses. A Task that
// re-entered awaiting-review FROM merged (the merged -> awaiting-review
// head-moved/409 edge) is CURRENTLY live and unparked, and must count exactly
// like one that entered awaiting-review the ordinary way - "did it come from a
// live state" is exactly the smarter predicate the production doc comment
// warns would under-count this Task and let the ceiling stop bounding pods
// at all.
func TestCountLive_CountsATaskThatReEnteredALiveStateFromMerged(t *testing.T) {
	now := time.Now()
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"

	reentered := liveStateTask("reentered", "infrastructure", now)
	reentered.Status.State = tatarav1alpha1.StateAwaitingReview
	reentered.Status.AgentKind = stage.AgentKindFor(tatarav1alpha1.StateAwaitingReview, "implement")
	// The marker a merged->awaiting-review re-entry leaves behind: no park, no
	// parkedFromState, just a live current state that used to be merged. There
	// is deliberately no field here recording "came from merged" - the whole
	// point is that countLive must not go looking for one.
	reentered.Status.HeadMoveReentries = 1

	c := newFakeClientWith(t, proj, reentered)
	live, err := countLive(context.Background(), c, proj)
	if err != nil {
		t.Fatalf("countLive: %v", err)
	}
	if len(live) != 1 || live[0].Name != "reentered" {
		t.Fatalf("countLive = %v, want [reentered]: a live re-entry from merged must count like any other live task", live)
	}
}

// Eviction takes the LONGEST-IDLE live task, and it takes a handoff turn
// before the pod dies. Its Task still works: parked(awaiting-human) has an F.6
// re-entry rule, so its next comment cold-spawns it - IN THE SAME STATE, since
// park never moves state (#521).
func TestEvictLongestIdleConversation(t *testing.T) {
	now := time.Now()
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxLivePods = 2
	proj.Spec.MaxConcurrentAgents = 5
	proj.Spec.Agent.TurnTimeoutSeconds = 600

	fresh := liveStateTask("fresh", "infrastructure", now.Add(-1*time.Minute))
	middle := liveStateTask("middle", "infrastructure", now.Add(-10*time.Minute))
	stalest := liveStateTask("stalest", "infrastructure", now.Add(-40*time.Minute))

	r := newLiveCapacityFixture(t, proj, fresh, middle, stalest)

	if _, err := r.enforceLivePodCeiling(context.Background(), proj, now); err != nil {
		t.Fatalf("enforceLivePodCeiling: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(stalest), got); err != nil {
		t.Fatalf("get stalest: %v", err)
	}
	if !tatarav1alpha1.Parked(got) || got.Status.ParkReason != stage.ReasonAwaitingHuman {
		t.Fatalf("stalest parkReason = %q (parked=%v), want parked(awaiting-human)", got.Status.ParkReason, tatarav1alpha1.Parked(got))
	}
	if got.Status.State != tatarav1alpha1.StateRefined {
		t.Fatalf("stalest state = %s, want unchanged (refined): park never moves state", got.Status.State)
	}
	if len(got.Status.Notes) == 0 {
		t.Error("the evicted conversation lost its continuation state: no handoff note")
	}

	for _, keep := range []*tatarav1alpha1.Task{fresh, middle} {
		var still tatarav1alpha1.Task
		if err := r.Get(context.Background(), objectKeyOf(keep), &still); err != nil {
			t.Fatalf("get %s: %v", keep.Name, err)
		}
		if tatarav1alpha1.Parked(&still) {
			t.Errorf("%s was parked: eviction took more than the overflow", keep.Name)
		}
	}
}

// TestEvictLongestIdleConversation_CapsOverflowPerPass is the CRITICAL 2
// discrimination proof (2026-07-28 final review, second half): each
// eviction runs StopWithHandoff, which blocks on real timers, and
// ProjectReconciler runs MaxConcurrentReconciles=1 across every project - a
// large overflow evicted serially in ONE call would wedge every project's
// reconcile for hours. With 3 live tasks against a ceiling of 1
// (overflow=2), a single pass must evict only the ONE longest-idle
// task (maxLivePodEvictionsPerPass) and signal the caller to
// requeue soon for the rest, rather than evicting both in the same blocking
// call. A second pass then converges the remainder.
func TestEvictLongestIdleConversation_CapsOverflowPerPass(t *testing.T) {
	now := time.Now()
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxLivePods = 1
	proj.Spec.MaxConcurrentAgents = 5
	proj.Spec.Agent.TurnTimeoutSeconds = 600

	fresh := liveStateTask("fresh", "infrastructure", now.Add(-1*time.Minute))
	middle := liveStateTask("middle", "infrastructure", now.Add(-10*time.Minute))
	stalest := liveStateTask("stalest", "infrastructure", now.Add(-40*time.Minute))

	r := newLiveCapacityFixture(t, proj, fresh, middle, stalest)

	requeue, err := r.enforceLivePodCeiling(context.Background(), proj, now)
	if err != nil {
		t.Fatalf("enforceLivePodCeiling (pass 1): %v", err)
	}
	if requeue <= 0 {
		t.Fatalf("requeue = %v, want > 0 - overflow remains after the per-pass cap, the caller must re-drive soon", requeue)
	}

	evicted := 0
	for _, tk := range []*tatarav1alpha1.Task{fresh, middle, stalest} {
		var got tatarav1alpha1.Task
		if err := r.Get(context.Background(), objectKeyOf(tk), &got); err != nil {
			t.Fatalf("get %s: %v", tk.Name, err)
		}
		if tatarav1alpha1.Parked(&got) {
			evicted++
		}
	}
	if evicted != 1 {
		t.Fatalf("evicted %d Tasks in one pass, want exactly 1 (maxLivePodEvictionsPerPass) - an unbounded pass wedges MaxConcurrentReconciles=1 for hours", evicted)
	}

	// A second pass evicts the next-longest-idle survivor and finally
	// converges (no more overflow, no further requeue asked for).
	requeue, err = r.enforceLivePodCeiling(context.Background(), proj, now)
	if err != nil {
		t.Fatalf("enforceLivePodCeiling (pass 2): %v", err)
	}
	if requeue != 0 {
		t.Fatalf("requeue = %v, want 0 - the overflow should be fully converged after the second pass", requeue)
	}
	var stillLive int
	for _, tk := range []*tatarav1alpha1.Task{fresh, middle, stalest} {
		var got tatarav1alpha1.Task
		if err := r.Get(context.Background(), objectKeyOf(tk), &got); err != nil {
			t.Fatalf("get %s: %v", tk.Name, err)
		}
		if !tatarav1alpha1.Parked(&got) {
			stillLive++
		}
	}
	if stillLive != 1 {
		t.Fatalf("still live = %d after two passes, want exactly 1 (the ceiling)", stillLive)
	}
}

// A direct, lower-level twin of TestEvictLongestIdleConversation_CapsOverflowPerPass:
// with FOUR live tasks against a ceiling of 1 (overflow=3), one pass must
// still evict AT MOST maxLivePodEvictionsPerPass (1), never more, whatever the
// overflow's size.
func TestEnforceLivePodCeiling_EvictsAtMostOnePerPass(t *testing.T) {
	now := time.Now()
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxLivePods = 1
	proj.Spec.MaxConcurrentAgents = 6
	proj.Spec.Agent.TurnTimeoutSeconds = 600

	tasks := []*tatarav1alpha1.Task{
		liveStateTask("t0", "infrastructure", now.Add(-1*time.Minute)),
		liveStateTask("t1", "infrastructure", now.Add(-5*time.Minute)),
		liveStateTask("t2", "infrastructure", now.Add(-10*time.Minute)),
		liveStateTask("t3", "infrastructure", now.Add(-40*time.Minute)),
	}
	objs := make([]client.Object, 0, len(tasks)+1)
	objs = append(objs, proj)
	for _, tk := range tasks {
		objs = append(objs, tk)
	}
	r := newLiveCapacityFixture(t, objs...)

	requeue, err := r.enforceLivePodCeiling(context.Background(), proj, now)
	if err != nil {
		t.Fatalf("enforceLivePodCeiling: %v", err)
	}
	if requeue != livePodEvictionRequeue {
		t.Fatalf("requeue = %v, want livePodEvictionRequeue (%v): 3 tasks of overflow remain after the 1-per-pass cap", requeue, livePodEvictionRequeue)
	}

	evicted := 0
	for _, tk := range tasks {
		var got tatarav1alpha1.Task
		if err := r.Get(context.Background(), objectKeyOf(tk), &got); err != nil {
			t.Fatalf("get %s: %v", tk.Name, err)
		}
		if tatarav1alpha1.Parked(&got) {
			evicted++
		}
	}
	if evicted != maxLivePodEvictionsPerPass {
		t.Fatalf("evicted %d, want exactly maxLivePodEvictionsPerPass (%d)", evicted, maxLivePodEvictionsPerPass)
	}
}

// TestEnforceLivePodCeiling_NilTasksFailsLoudInsteadOfPanicking is the M4
// discrimination proof (2026-07-28 final review): project_controller.go's own
// doc comment on the Tasks field claims "Nil ... is never dereferenced", but
// enforceLivePodCeiling used to call r.Tasks.liveHandoffAndPark(...)
// with no nil check at all - a nil r.Tasks (a misconfigured wiring, or a test
// that never sets it) would panic the moment eviction was actually needed,
// contradicting that claim. With overflow present and r.Tasks left nil, this
// must return an error, not panic.
func TestEnforceLivePodCeiling_NilTasksFailsLoudInsteadOfPanicking(t *testing.T) {
	now := time.Now()
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxLivePods = 1
	proj.Spec.MaxConcurrentAgents = 5

	fresh := liveStateTask("fresh", "infrastructure", now.Add(-1*time.Minute))
	stalest := liveStateTask("stalest", "infrastructure", now.Add(-40*time.Minute))
	c := newMirrorClient(t, proj, fresh, stalest)
	r := &ProjectReconciler{Client: c, Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}

	requeue, err := r.enforceLivePodCeiling(context.Background(), proj, now)
	if err == nil {
		t.Fatal("enforceLivePodCeiling with nil r.Tasks and overflow present returned no error - it must fail loud, not panic or silently no-op")
	}
	if requeue != 0 {
		t.Fatalf("requeue = %v, want 0 on the nil-Tasks error path", requeue)
	}
}

// Determinism (things-to-get-right #3): with two conversations idle since the
// EXACT SAME whole second (the real collision shape - metav1.Time round-trips
// at whole-second precision), eviction must not depend on the order the Tasks
// happen to come back from List. Both input orderings must evict the SAME
// task: name ascending is the tie-break countLive/sort.Slice applies
// after the idle-time comparison.
func TestEvictLongestIdleConversationTieBreakIsDeterministic(t *testing.T) {
	now := time.Now()
	tie := now.Add(-30 * time.Minute).Truncate(time.Second)

	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxLivePods = 1
	proj.Spec.MaxConcurrentAgents = 5
	proj.Spec.Agent.TurnTimeoutSeconds = 600

	for _, order := range [][2]string{{"alpha", "bravo"}, {"bravo", "alpha"}} {
		t.Run(order[0]+"-then-"+order[1], func(t *testing.T) {
			first := liveStateTask(order[0], "infrastructure", tie)
			second := liveStateTask(order[1], "infrastructure", tie)
			r := newLiveCapacityFixture(t, proj.DeepCopy(), first, second)

			if _, err := r.enforceLivePodCeiling(context.Background(), proj, now); err != nil {
				t.Fatalf("enforceLivePodCeiling: %v", err)
			}

			got := &tatarav1alpha1.Task{}
			if err := r.Get(context.Background(), objectKeyOf(&tatarav1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "alpha", Namespace: "tatara"},
			}), got); err != nil {
				t.Fatalf("get alpha: %v", err)
			}
			if !tatarav1alpha1.Parked(got) {
				t.Fatalf("alpha parked = %v, want true regardless of input order %v", tatarav1alpha1.Parked(got), order)
			}
		})
	}
}

// TestEvictionExcludesAMidTurnTaskEvenWhenItIsTheLongestIdle is THE MISSING
// TEST for B4 (2026-08-07 adversarial review HIGH): before the fix,
// enforceLivePodCeiling picked its victim by conversationIdleSince alone, and
// an agent 40 minutes into a routine turn has ConversationLastEventAt still
// pinned to its state-entry stamp - the OLDEST timestamp in the project, so it
// sorted FIRST and was evicted mid-turn. This is the concrete shape: "stalest"
// has a turn in flight (AnnCurrentTurn set, no AnnTurnComplete) and is by far
// the most idle-looking of the two, but "middle" - merely idle, no turn
// running - is the one that must be evicted.
func TestEvictionExcludesAMidTurnTaskEvenWhenItIsTheLongestIdle(t *testing.T) {
	now := time.Now()
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxLivePods = 1
	proj.Spec.MaxConcurrentAgents = 5
	proj.Spec.Agent.TurnTimeoutSeconds = 600

	middle := liveStateTask("middle", "infrastructure", now.Add(-10*time.Minute))
	stalest := liveStateTask("stalest", "infrastructure", now.Add(-40*time.Minute))
	stalest.Annotations = map[string]string{tatarav1alpha1.AnnCurrentTurn: "turn-1"}

	r := newLiveCapacityFixture(t, proj, middle, stalest)

	if _, err := r.enforceLivePodCeiling(context.Background(), proj, now); err != nil {
		t.Fatalf("enforceLivePodCeiling: %v", err)
	}

	var gotMiddle tatarav1alpha1.Task
	if err := r.Get(context.Background(), objectKeyOf(middle), &gotMiddle); err != nil {
		t.Fatalf("get middle: %v", err)
	}
	if !tatarav1alpha1.Parked(&gotMiddle) {
		t.Fatal("middle (idle, no turn in flight) was not evicted, want it evicted ahead of a mid-turn Task")
	}

	var gotStalest tatarav1alpha1.Task
	if err := r.Get(context.Background(), objectKeyOf(stalest), &gotStalest); err != nil {
		t.Fatalf("get stalest: %v", err)
	}
	if tatarav1alpha1.Parked(&gotStalest) {
		t.Fatal("stalest has a turn in flight and must survive eviction however idle-looking its ConversationLastEventAt is")
	}
}

// TestEnforceLivePodCeiling_NoIdleCandidateEvictsNothing is the wedge-proof
// counterpart: when EVERY live Task over ceiling is mid-turn, there is no
// legal victim at all. This must not error, must not evict anything, and must
// not ask for the fast eviction-cap requeue - that timer exists to converge a
// per-pass CAP against remaining IDLE overflow, not to spin every 5s against a
// turn that has not finished. The project stays over ceiling until a turn
// ends (bounded by the turn timeout / ResidencyExceeded / maxTurnsPerTask)
// and the ordinary level-triggered project reconcile converges it then.
func TestEnforceLivePodCeiling_NoIdleCandidateEvictsNothing(t *testing.T) {
	now := time.Now()
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxLivePods = 1
	proj.Spec.MaxConcurrentAgents = 5
	proj.Spec.Agent.TurnTimeoutSeconds = 600

	a := liveStateTask("a", "infrastructure", now.Add(-40*time.Minute))
	a.Annotations = map[string]string{tatarav1alpha1.AnnCurrentTurn: "turn-1"}
	b := liveStateTask("b", "infrastructure", now.Add(-10*time.Minute))
	b.Annotations = map[string]string{tatarav1alpha1.AnnCurrentTurn: "turn-1"}

	r := newLiveCapacityFixture(t, proj, a, b)

	requeue, err := r.enforceLivePodCeiling(context.Background(), proj, now)
	if err != nil {
		t.Fatalf("enforceLivePodCeiling: %v", err)
	}
	if requeue != 0 {
		t.Fatalf("requeue = %v, want 0: nothing evictable this pass, so no fast requeue should be scheduled", requeue)
	}

	for _, tk := range []*tatarav1alpha1.Task{a, b} {
		var got tatarav1alpha1.Task
		if err := r.Get(context.Background(), objectKeyOf(tk), &got); err != nil {
			t.Fatalf("get %s: %v", tk.Name, err)
		}
		if tatarav1alpha1.Parked(&got) {
			t.Fatalf("%s was evicted, want no eviction when every live Task is mid-turn", tk.Name)
		}
	}
}

// TestEvictionCandidatesExcludesMidTurnTasks is the direct unit test on the
// selection helper, in the style of TestSortByIdleThenNameIsOrderIndependent:
// it drives evictionCandidates against a hand-built slice so the
// exclude-mid-turn behaviour is testable without the full reconciler. A
// mid-turn Task must be absent from the result altogether - not merely sorted
// last - however idle its own ConversationLastEventAt looks, and the
// survivors must still come back longest-idle-first.
func TestEvictionCandidatesExcludesMidTurnTasks(t *testing.T) {
	now := time.Now()
	fresh := liveStateTask("fresh", "infrastructure", now.Add(-1*time.Minute))
	middle := liveStateTask("middle", "infrastructure", now.Add(-10*time.Minute))
	stalestButMidTurn := liveStateTask("stalest", "infrastructure", now.Add(-40*time.Minute))
	stalestButMidTurn.Annotations = map[string]string{tatarav1alpha1.AnnCurrentTurn: "turn-1"}

	live := []tatarav1alpha1.Task{*fresh, *middle, *stalestButMidTurn}
	got := evictionCandidates(live)

	if len(got) != 2 {
		t.Fatalf("evictionCandidates returned %d candidates, want 2 (the mid-turn Task excluded): %v", len(got), got)
	}
	if got[0].Name != "middle" || got[1].Name != "fresh" {
		t.Fatalf("evictionCandidates order = [%s, %s], want [middle, fresh] (longest-idle first among the non-mid-turn survivors)", got[0].Name, got[1].Name)
	}
	for _, c := range got {
		if c.Name == "stalest" {
			t.Fatal("evictionCandidates included a mid-turn Task; it must be excluded outright, not merely sorted last")
		}
	}
}

// sortByIdleThenName's tie-break must be a function of the DATA (idle time,
// then Task name), never of the order the caller's slice happens to arrive
// in. This drives the comparator directly against hand-built, differently
// shuffled inputs - unlike TestEvictLongestIdleConversationTieBreakIsDeterministic,
// which goes through client.List and would pass even with no tie-break logic
// at all, because both the fake client and a real apiserver already tend to
// return List results in roughly name-sorted order. A reversed-input case
// (charlie, bravo, alpha) is the one an unstable sort with no tie-break is
// most likely to leave unsorted, which is why it is included alongside the
// already-sorted and one-shuffled cases.
func TestSortByIdleThenNameIsOrderIndependent(t *testing.T) {
	tie := time.Now().Truncate(time.Second)
	want := []string{"alpha", "bravo", "charlie"}
	orderings := [][]string{
		{"alpha", "bravo", "charlie"},
		{"charlie", "bravo", "alpha"},
		{"bravo", "charlie", "alpha"},
	}
	for _, order := range orderings {
		t.Run(fmt.Sprintf("%v", order), func(t *testing.T) {
			live := make([]tatarav1alpha1.Task, 0, len(order))
			for _, n := range order {
				live = append(live, *liveStateTask(n, "infrastructure", tie))
			}
			sortByIdleThenName(live)
			got := make([]string, len(live))
			for i := range live {
				got[i] = live[i].Name
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("sortByIdleThenName(%v) = %v, want %v", order, got, want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// newFakeClientWith is newMirrorClient (mirror_test.go) under the name the
// task-11 brief's tests call it by; LiveHasRoom needs nothing beyond
// what newMirrorClient already wires (scheme, status subresource, indexes).
func newFakeClientWith(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return newMirrorClient(t, objs...)
}

// capacityHandoffSession is an agent.Session stub for the eviction path,
// generalising exitSession (livepods_exit_test.go) from ONE fixed taskName
// to WHICHEVER task the stopper is currently handing off - enforceLivePodCeiling
// can evict several Tasks in one pass, each through its own StopWithHandoff call
// against its own pod's baseURL. SubmitHandoffTurn writes the handoff Note
// straight onto the Task the baseURL maps to, through the SAME client the
// reconciler uses, so TTLStopper.waitHandoffNote's first poll (before any real
// sleep) observes it - exactly the idiom exitSession documents, generalised
// to N tasks instead of one.
type capacityHandoffSession struct {
	*fakeSession
	client    client.Client
	namespace string
	byBaseURL map[string]string // baseURL -> task name
	// failFor, when non-nil, makes SubmitHandoffTurn return the mapped error
	// for that TASK NAME every time it is called - a #564 test hook. This is
	// deliberately separate from fakeSession.submitErr, which is one error
	// shared by every base URL the fixture's single session serves: #564's
	// defect only shows up when ONE victim errors while ITS SIBLINGS in the
	// same pass succeed, which submitErr cannot express.
	failFor map[string]error
}

func (s *capacityHandoffSession) SubmitHandoffTurn(ctx context.Context, baseURL, text, callbackURL string) (string, error) {
	if s.failFor != nil {
		if name, ok := s.byBaseURL[baseURL]; ok {
			if err, ok := s.failFor[name]; ok {
				return "", err
			}
		}
	}
	id, err := s.fakeSession.SubmitHandoffTurn(ctx, baseURL, text, callbackURL)
	if err != nil {
		return "", err
	}
	taskName, ok := s.byBaseURL[baseURL]
	if !ok {
		return "", fmt.Errorf("capacityHandoffSession: no task registered for base URL %s", baseURL)
	}
	fresh := &tatarav1alpha1.Task{}
	if err := s.client.Get(ctx, types.NamespacedName{Namespace: s.namespace, Name: taskName}, fresh); err != nil {
		return "", err
	}
	fresh.Status.Notes = append(fresh.Status.Notes, tatarav1alpha1.Note{
		At:    metav1.Now(),
		Agent: "implement",
		Kind:  agent.NoteKindHandoff,
		Body:  "everything the next pod needs",
	})
	if err := s.client.Status().Update(ctx, fresh); err != nil {
		return "", err
	}
	return id, nil
}

// newLiveCapacityFixture builds a ProjectReconciler whose r.Tasks
// (*TaskReconciler) has Session/SpillerFor/Metrics stubs mirroring
// newLiveExitFixture (livepods_exit_test.go), so
// enforceLivePodCeiling's eviction path can drive liveHandoffAndPark
// end to end instead of stopping at "no pod, nothing to hand off from".
//
// Every live Task among objs is given a PodStartedAt and a ready wrapper
// pod if it does not already carry one: a real live Task always has a
// live pod, so a fixture that left PodStartedAt nil would make
// liveHandoffAndPark take its no-pod shortcut and never exercise
// StopWithHandoff at all, masking exactly the eviction-loses-the-handoff bug
// this task exists to catch.
func newLiveCapacityFixture(t *testing.T, objs ...client.Object) *ProjectReconciler {
	t.Helper()
	return newLiveCapacityFixtureIntercepted(t, interceptor.Funcs{}, objs...)
}

// newLiveCapacityFixtureIntercepted is newLiveCapacityFixture with the fake
// client built through newMirrorClientIntercepted instead of newMirrorClient,
// so a #564 test can fail ONE candidate's eviction attempt (e.g. its
// ownedMergeRequests List) deterministically without the capacityHandoffSession
// wrapper - which cannot express "this pass's List call fails, the next
// pass's does not" - being able to reach.
func newLiveCapacityFixtureIntercepted(t *testing.T, funcs interceptor.Funcs, objs ...client.Object) *ProjectReconciler {
	t.Helper()

	built := make([]client.Object, 0, len(objs)+2)
	built = append(built, objs...)
	byBaseURL := map[string]string{}
	for _, o := range objs {
		task, ok := o.(*tatarav1alpha1.Task)
		if !ok || !stage.Live(task.Status.State) {
			continue
		}
		if task.Status.PodStartedAt == nil {
			started := metav1.NewTime(task.Status.StateEnteredAt.Time)
			task.Status.PodStartedAt = &started
			// The pod-ready stamp goes with it. A caller that sets PodStartedAt
			// itself is modelling something more specific (a pod that started but
			// never became ready) and keeps whatever it chose.
			if task.Status.StateWorkStartedAt == nil {
				task.Status.StateWorkStartedAt = &started
			}
		}
		if task.Status.AgentKind == "" {
			task.Status.AgentKind = stage.AgentKindFor(task.Status.State, task.Spec.Kind)
		}
		built = append(built, tsReadyPod(task))
		byBaseURL[agent.BaseURL(task, task.Namespace)] = task.Name
	}
	built = append(built, mdSecret())

	c := newMirrorClientIntercepted(t, funcs, built...)
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	sess := &capacityHandoffSession{
		fakeSession: newFakeSession(),
		client:      c,
		namespace:   "tatara",
		byBaseURL:   byBaseURL,
	}

	taskR := &TaskReconciler{
		Client:    c,
		Metrics:   metrics,
		Session:   sess,
		PodConfig: tsPodConfig(),
	}

	return &ProjectReconciler{
		Client:  c,
		Metrics: metrics,
		Tasks:   taskR,
	}
}
