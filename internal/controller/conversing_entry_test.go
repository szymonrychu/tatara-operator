package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// EnterConversing is the ENTRY into a live conversation from a live pod-bearing
// stage. The first comment into a conversation pays a cold spawn - the previous
// stage's pod IS torn down by EnterStage's ordinary choke-point teardown, with no
// carve-out - so this asserts the choke point's teardown ran, not that the pod
// somehow survived.
func TestEnterConversingFromClarifying(t *testing.T) {
	proj, task, r := ceFixture(t, v1alpha1.StageClarifying, "clarify")
	now := time.Now()

	entered, err := EnterConversing(context.Background(), r.Client, nil, r.Metrics, proj, task, nil, now)
	if err != nil {
		t.Fatalf("EnterConversing: %v", err)
	}
	if !entered {
		t.Fatal("entered = false, want true")
	}

	fresh := ceGetTask(t, r.Client, task.Name)
	if fresh.Status.Stage != v1alpha1.StageConversing {
		t.Fatalf("stage = %q, want conversing", fresh.Status.Stage)
	}
	if fresh.Status.ConversationLastEventAt == nil {
		t.Fatal("ConversationLastEventAt is nil: the idle clock was never armed")
	}

	pod := &corev1.Pod{}
	err = r.Get(context.Background(), types.NamespacedName{Namespace: mdNS, Name: agent.PodName(task)}, pod)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("wrapper pod still exists after entering conversing (err=%v): the choke-point teardown did not run", err)
	}
}

// Entry from reviewing is bounded by humanReviewRounds, exactly like the
// awaiting-human re-entry: each lap can spawn a pod, and a chatty MR thread must
// not spawn one per comment.
func TestEnterConversingFromReviewing(t *testing.T) {
	proj, task, r := ceFixture(t, v1alpha1.StageReviewing, "review", func(task *v1alpha1.Task) {
		task.Status.HumanReviewRounds = 2
	})
	now := time.Now()

	entered, err := EnterConversing(context.Background(), r.Client, nil, r.Metrics, proj, task, nil, now)
	if err != nil {
		t.Fatalf("EnterConversing: %v", err)
	}
	if !entered {
		t.Fatal("entered = false, want true")
	}

	fresh := ceGetTask(t, r.Client, task.Name)
	if fresh.Status.Stage != v1alpha1.StageConversing {
		t.Fatalf("stage = %q, want conversing", fresh.Status.Stage)
	}
	if fresh.Status.HumanReviewRounds != 3 {
		t.Fatalf("HumanReviewRounds = %d, want 3 (2 + 1)", fresh.Status.HumanReviewRounds)
	}
}

// implementing is deliberately ABSENT from the entry table: an implement pod is
// mid-change, and taking it down to hold a conversation would lose in-flight
// work.
func TestEnterConversingRefusesFromImplementing(t *testing.T) {
	proj, task, r := ceFixture(t, v1alpha1.StageImplementing, "implement")
	now := time.Now()

	entered, err := EnterConversing(context.Background(), r.Client, nil, r.Metrics, proj, task, nil, now)
	if err != nil {
		t.Fatalf("EnterConversing: %v", err)
	}
	if entered {
		t.Fatal("entered = true, want false: implementing may never enter conversing")
	}

	fresh := ceGetTask(t, r.Client, task.Name)
	if fresh.Status.Stage != v1alpha1.StageImplementing {
		t.Fatalf("stage = %q, want implementing (unchanged)", fresh.Status.Stage)
	}
}

// The reviewing round cap is spent: no more pods may be spawned into a
// conversation from this Task's review thread.
func TestEnterConversingRefusesAtTheReviewRoundCap(t *testing.T) {
	proj, task, r := ceFixture(t, v1alpha1.StageReviewing, "review", func(task *v1alpha1.Task) {
		task.Status.HumanReviewRounds = v1alpha1.MaxHumanReviewRounds
	})
	now := time.Now()

	entered, err := EnterConversing(context.Background(), r.Client, nil, r.Metrics, proj, task, nil, now)
	if err != nil {
		t.Fatalf("EnterConversing: %v", err)
	}
	if entered {
		t.Fatal("entered = true, want false: the review round cap is spent")
	}

	fresh := ceGetTask(t, r.Client, task.Name)
	if fresh.Status.Stage != v1alpha1.StageReviewing {
		t.Fatalf("stage = %q, want reviewing (unchanged)", fresh.Status.Stage)
	}
	if fresh.Status.HumanReviewRounds != v1alpha1.MaxHumanReviewRounds {
		t.Fatalf("HumanReviewRounds = %d, want unchanged at %d", fresh.Status.HumanReviewRounds, v1alpha1.MaxHumanReviewRounds)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// ceFixture builds a Task at stg (with a live wrapper pod, mirroring a real
// pod-bearing stage) plus a reconciler carrying real metrics, the same
// fake-client idiom as newConversingTurnFixture (conversing_turn_test.go, Task
// 8). setup, if given, mutates task BEFORE the fake client is built: the vanilla
// fake.NewClientBuilder deep-copies every WithObjects() argument once, at Build
// (see newConversingTurnFixture's liveTaskClient doc), so a field set on the
// returned task pointer AFTER this call is invisible to a fresh Get - a test
// that wants a seeded value to round-trip through a real persisted write must
// seed it here, not after.
func ceFixture(t *testing.T, stg, kind string, setup ...func(*v1alpha1.Task)) (*v1alpha1.Project, *v1alpha1.Task, *TaskReconciler) {
	t.Helper()
	now := time.Now()
	proj := tsStablyReadyProject(3)
	task := tsTask("ce-1", kind, stg, now.Add(-time.Hour))
	podAt := metav1.NewTime(now.Add(-10 * time.Minute))
	task.Status.PodStartedAt = &podAt
	for _, fn := range setup {
		fn(task)
	}

	c := newMirrorClient(t, proj, mdSecret(), task, tsReadyPod(task))
	r := tsReconciler(c)
	return proj, task, r
}

func ceGetTask(t *testing.T, c client.Client, name string) *v1alpha1.Task {
	t.Helper()
	return mdGetTask(t, c, name)
}

// Compile-time reminder that obs.OperatorMetrics is the type EnterConversing
// expects; tsReconciler already wires one, but a future refactor of the fixture
// must not silently drop it.
var _ = (*obs.OperatorMetrics)(nil)
