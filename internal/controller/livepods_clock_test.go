package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/stretchr/testify/require"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// reconcileClocks must substitute the project's scm.conversationIdleMinutes for
// a LIVE Task's idle budget, not the F.4 table's ConversationIdleDefault. #521
// widened this from the single deleted `conversing` stage to every state
// stage.Live reports true for; refined is used here as the representative one.
// Mirrors the construction idiom of internal/controller/task_stage_test.go's
// reconcileClocks tests (mdProject/mdTask/newMirrorClient/tsReconciler).
func TestReconcileClocksUsesTheProjectIdleBudget(t *testing.T) {
	t.Run("a tighter per-project idle budget parks an idle live task", func(t *testing.T) {
		proj := mdProject()
		proj.Spec.Scm.ConversationIdleMinutes = 5

		task := mdTask("t1", "implement", tatarav1alpha1.StateRefined)
		task.Status.StateEnteredAt = &metav1.Time{Time: time.Now().Add(-10 * time.Minute)}
		task.Status.PodStartedAt = &metav1.Time{Time: time.Now().Add(-10 * time.Minute)}
		task.Status.StateWorkStartedAt = &metav1.Time{Time: time.Now().Add(-10 * time.Minute)}
		lastEvent := metav1.NewTime(time.Now().Add(-6 * time.Minute))
		task.Status.ConversationLastEventAt = &lastEvent

		c := newMirrorClient(t, proj, mdSecret(), task)
		r := tsReconciler(c)

		_, handled, err := r.reconcileClocks(context.Background(), proj, task, time.Now())
		require.NoError(t, err)
		require.True(t, handled, "a 6-minute-old event against a 5-minute budget must age out")

		got := mdGetTask(t, c, "t1")
		require.True(t, tatarav1alpha1.Parked(got), "the idle budget elapsing parks the task; it does not move state")
		require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State, "park is orthogonal to state: the task stays exactly where it was")
		require.Equal(t, "awaiting-human", got.Status.ParkReason)
	})

	t.Run("an unset per-project budget falls back to the 60-minute default", func(t *testing.T) {
		proj := mdProject()
		proj.Spec.Scm.ConversationIdleMinutes = 0

		task := mdTask("t2", "implement", tatarav1alpha1.StateRefined)
		task.Status.StateEnteredAt = &metav1.Time{Time: time.Now().Add(-10 * time.Minute)}
		task.Status.PodStartedAt = &metav1.Time{Time: time.Now().Add(-10 * time.Minute)}
		task.Status.StateWorkStartedAt = &metav1.Time{Time: time.Now().Add(-10 * time.Minute)}
		lastEvent := metav1.NewTime(time.Now().Add(-6 * time.Minute))
		task.Status.ConversationLastEventAt = &lastEvent

		c := newMirrorClient(t, proj, mdSecret(), task)
		r := tsReconciler(c)

		_, handled, err := r.reconcileClocks(context.Background(), proj, task, time.Now())
		require.NoError(t, err)
		require.False(t, handled, "the 60-minute default has not elapsed at 6 minutes")

		got := mdGetTask(t, c, "t2")
		require.False(t, tatarav1alpha1.Parked(got))
		require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State)
	})
}
