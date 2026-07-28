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

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

func conversingTask(name, project string, lastEvent time.Time) *tatarav1alpha1.Task {
	t := &tatarav1alpha1.Task{}
	t.Namespace = "tatara"
	t.Name = name
	t.Spec.ProjectRef = project
	t.Spec.Kind = "clarify"
	t.Status.Stage = tatarav1alpha1.StageConversing
	t.Status.AgentKind = "clarify"
	t.Status.StageEnteredAt = &metav1.Time{Time: lastEvent}
	t.Status.ConversationLastEventAt = &metav1.Time{Time: lastEvent}
	return t
}

func TestConversingHasRoom(t *testing.T) {
	now := time.Now()
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxConversingPods = 2

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
				objs = append(objs, conversingTask(fmt.Sprintf("c%d", i), "infrastructure", now))
			}
			c := newFakeClientWith(t, objs...)
			got, err := ConversingHasRoom(context.Background(), c, proj)
			if err != nil {
				t.Fatalf("ConversingHasRoom: %v", err)
			}
			if got != tc.want {
				t.Errorf("ConversingHasRoom = %v, want %v", got, tc.want)
			}
		})
	}
}

// Tasks in ANOTHER project never count against this project's ceiling.
func TestConversingHasRoomIsPerProject(t *testing.T) {
	now := time.Now()
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxConversingPods = 1

	c := newFakeClientWith(t, proj, conversingTask("other", "someone-else", now))
	got, err := ConversingHasRoom(context.Background(), c, proj)
	if err != nil {
		t.Fatalf("ConversingHasRoom: %v", err)
	}
	if !got {
		t.Fatal("another project's conversation consumed this project's ceiling")
	}
}

// Eviction takes the LONGEST-IDLE conversation, and it takes a handoff turn
// before the pod dies. Its Task still works: parked(awaiting-human) has an F.6
// re-entry rule, so its next comment cold-spawns it.
func TestEvictLongestIdleConversation(t *testing.T) {
	now := time.Now()
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxConversingPods = 2
	proj.Spec.Agent.TurnTimeoutSeconds = 600

	fresh := conversingTask("fresh", "infrastructure", now.Add(-1*time.Minute))
	middle := conversingTask("middle", "infrastructure", now.Add(-10*time.Minute))
	stalest := conversingTask("stalest", "infrastructure", now.Add(-40*time.Minute))

	r := newConversingCapacityFixture(t, proj, fresh, middle, stalest)

	if _, err := r.enforceConversingCeiling(context.Background(), proj, now); err != nil {
		t.Fatalf("enforceConversingCeiling: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(stalest), got); err != nil {
		t.Fatalf("get stalest: %v", err)
	}
	if got.Status.Stage != tatarav1alpha1.StageParked || got.Status.StageReason != stage.ReasonAwaitingHuman {
		t.Fatalf("stalest = %s(%s), want parked(awaiting-human)", got.Status.Stage, got.Status.StageReason)
	}
	if len(got.Status.Notes) == 0 {
		t.Error("the evicted conversation lost its continuation state: no handoff note")
	}

	for _, keep := range []*tatarav1alpha1.Task{fresh, middle} {
		var still tatarav1alpha1.Task
		if err := r.Get(context.Background(), objectKeyOf(keep), &still); err != nil {
			t.Fatalf("get %s: %v", keep.Name, err)
		}
		if still.Status.Stage != tatarav1alpha1.StageConversing {
			t.Errorf("%s = %s, want conversing: eviction took more than the overflow", keep.Name, still.Status.Stage)
		}
	}
}

// TestEvictLongestIdleConversation_CapsOverflowPerPass is the CRITICAL 2
// discrimination proof (2026-07-28 final review, second half): each
// eviction runs StopWithHandoff, which blocks on real timers, and
// ProjectReconciler runs MaxConcurrentReconciles=1 across every project - a
// large overflow evicted serially in ONE call would wedge every project's
// reconcile for hours. With 3 live conversations against a ceiling of 1
// (overflow=2), a single pass must evict only the ONE longest-idle
// conversation (maxConversingEvictionsPerPass) and signal the caller to
// requeue soon for the rest, rather than evicting both in the same blocking
// call. A second pass then converges the remainder.
func TestEvictLongestIdleConversation_CapsOverflowPerPass(t *testing.T) {
	now := time.Now()
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxConversingPods = 1
	proj.Spec.Agent.TurnTimeoutSeconds = 600

	fresh := conversingTask("fresh", "infrastructure", now.Add(-1*time.Minute))
	middle := conversingTask("middle", "infrastructure", now.Add(-10*time.Minute))
	stalest := conversingTask("stalest", "infrastructure", now.Add(-40*time.Minute))

	r := newConversingCapacityFixture(t, proj, fresh, middle, stalest)

	requeue, err := r.enforceConversingCeiling(context.Background(), proj, now)
	if err != nil {
		t.Fatalf("enforceConversingCeiling (pass 1): %v", err)
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
		if got.Status.Stage == tatarav1alpha1.StageParked {
			evicted++
		}
	}
	if evicted != 1 {
		t.Fatalf("evicted %d Tasks in one pass, want exactly 1 (maxConversingEvictionsPerPass) - an unbounded pass wedges MaxConcurrentReconciles=1 for hours", evicted)
	}

	// A second pass evicts the next-longest-idle survivor and finally
	// converges (no more overflow, no further requeue asked for).
	requeue, err = r.enforceConversingCeiling(context.Background(), proj, now)
	if err != nil {
		t.Fatalf("enforceConversingCeiling (pass 2): %v", err)
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
		if got.Status.Stage == tatarav1alpha1.StageConversing {
			stillLive++
		}
	}
	if stillLive != 1 {
		t.Fatalf("still conversing = %d after two passes, want exactly 1 (the ceiling)", stillLive)
	}
}

// TestEnforceConversingCeiling_NilTasksFailsLoudInsteadOfPanicking is the M4
// discrimination proof (2026-07-28 final review): project_controller.go's own
// doc comment on the Tasks field claims "Nil ... is never dereferenced", but
// enforceConversingCeiling used to call r.Tasks.conversingHandoffAndPark(...)
// with no nil check at all - a nil r.Tasks (a misconfigured wiring, or a test
// that never sets it) would panic the moment eviction was actually needed,
// contradicting that claim. With overflow present and r.Tasks left nil, this
// must return an error, not panic.
func TestEnforceConversingCeiling_NilTasksFailsLoudInsteadOfPanicking(t *testing.T) {
	now := time.Now()
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxConversingPods = 1

	fresh := conversingTask("fresh", "infrastructure", now.Add(-1*time.Minute))
	stalest := conversingTask("stalest", "infrastructure", now.Add(-40*time.Minute))
	c := newMirrorClient(t, proj, fresh, stalest)
	r := &ProjectReconciler{Client: c, Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}

	requeue, err := r.enforceConversingCeiling(context.Background(), proj, now)
	if err == nil {
		t.Fatal("enforceConversingCeiling with nil r.Tasks and overflow present returned no error - it must fail loud, not panic or silently no-op")
	}
	if requeue != 0 {
		t.Fatalf("requeue = %v, want 0 on the nil-Tasks error path", requeue)
	}
}

// Determinism (things-to-get-right #3): with two conversations idle since the
// EXACT SAME whole second (the real collision shape - metav1.Time round-trips
// at whole-second precision), eviction must not depend on the order the Tasks
// happen to come back from List. Both input orderings must evict the SAME
// task: name ascending is the tie-break countConversing/sort.Slice applies
// after the idle-time comparison.
func TestEvictLongestIdleConversationTieBreakIsDeterministic(t *testing.T) {
	now := time.Now()
	tie := now.Add(-30 * time.Minute).Truncate(time.Second)

	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxConversingPods = 1
	proj.Spec.Agent.TurnTimeoutSeconds = 600

	for _, order := range [][2]string{{"alpha", "bravo"}, {"bravo", "alpha"}} {
		t.Run(order[0]+"-then-"+order[1], func(t *testing.T) {
			first := conversingTask(order[0], "infrastructure", tie)
			second := conversingTask(order[1], "infrastructure", tie)
			r := newConversingCapacityFixture(t, proj.DeepCopy(), first, second)

			if _, err := r.enforceConversingCeiling(context.Background(), proj, now); err != nil {
				t.Fatalf("enforceConversingCeiling: %v", err)
			}

			got := &tatarav1alpha1.Task{}
			if err := r.Get(context.Background(), objectKeyOf(&tatarav1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "alpha", Namespace: "tatara"},
			}), got); err != nil {
				t.Fatalf("get alpha: %v", err)
			}
			if got.Status.Stage != tatarav1alpha1.StageParked {
				t.Fatalf("alpha = %s, want parked regardless of input order %v", got.Status.Stage, order)
			}
		})
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
				live = append(live, *conversingTask(n, "infrastructure", tie))
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
// task-11 brief's tests call it by; ConversingHasRoom needs nothing beyond
// what newMirrorClient already wires (scheme, status subresource, indexes).
func newFakeClientWith(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return newMirrorClient(t, objs...)
}

// capacityHandoffSession is an agent.Session stub for the eviction path,
// generalising exitSession (conversing_exit_test.go) from ONE fixed taskName
// to WHICHEVER task the stopper is currently handing off - enforceConversingCeiling
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
}

func (s *capacityHandoffSession) SubmitHandoffTurn(ctx context.Context, baseURL, text, callbackURL string) (string, error) {
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
		Agent: "clarify",
		Kind:  agent.NoteKindHandoff,
		Body:  "everything the next pod needs",
	})
	if err := s.client.Status().Update(ctx, fresh); err != nil {
		return "", err
	}
	return id, nil
}

// newConversingCapacityFixture builds a ProjectReconciler whose r.Tasks
// (*TaskReconciler) has Session/SpillerFor/Metrics stubs mirroring
// newConversingExitFixture (conversing_exit_test.go), so
// enforceConversingCeiling's eviction path can drive conversingHandoffAndPark
// end to end instead of stopping at "no pod, nothing to hand off from".
//
// Every conversing Task among objs is given a PodStartedAt and a ready wrapper
// pod if it does not already carry one: a real conversing Task always has a
// live pod (conversing is a pod-bearing stage - Task 5), so a fixture that left
// PodStartedAt nil would make conversingHandoffAndPark take its no-pod
// shortcut and never exercise StopWithHandoff at all, masking exactly the
// eviction-loses-the-handoff bug this task exists to catch.
func newConversingCapacityFixture(t *testing.T, objs ...client.Object) *ProjectReconciler {
	t.Helper()

	built := make([]client.Object, 0, len(objs)+2)
	built = append(built, objs...)
	byBaseURL := map[string]string{}
	for _, o := range objs {
		task, ok := o.(*tatarav1alpha1.Task)
		if !ok || task.Status.Stage != tatarav1alpha1.StageConversing {
			continue
		}
		if task.Status.PodStartedAt == nil {
			started := metav1.NewTime(task.Status.StageEnteredAt.Time)
			task.Status.PodStartedAt = &started
		}
		if task.Status.AgentKind == "" {
			task.Status.AgentKind = stage.AgentKindFor(task.Status.Stage)
		}
		built = append(built, tsReadyPod(task))
		byBaseURL[agent.BaseURL(task, task.Namespace)] = task.Name
	}
	built = append(built, mdSecret())

	c := newMirrorClient(t, built...)
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
