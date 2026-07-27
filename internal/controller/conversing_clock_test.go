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
// a conversing Task's budget, not the F.4 table's ConversationIdleDefault.
// Mirrors the construction idiom of internal/controller/task_stage_test.go's
// reconcileClocks tests (mdProject/mdTask/newMirrorClient/tsReconciler).
func TestReconcileClocksUsesTheProjectIdleBudget(t *testing.T) {
	t.Run("a tighter per-project idle budget parks an idle conversation", func(t *testing.T) {
		proj := mdProject()
		proj.Spec.Scm.ConversationIdleMinutes = 5

		task := mdTask("t1", "clarify", tatarav1alpha1.StageConversing)
		task.Status.StageEnteredAt = &metav1.Time{Time: time.Now().Add(-10 * time.Minute)}
		task.Status.PodStartedAt = &metav1.Time{Time: time.Now().Add(-10 * time.Minute)}
		task.Status.StageWorkStartedAt = &metav1.Time{Time: time.Now().Add(-10 * time.Minute)}
		lastEvent := metav1.NewTime(time.Now().Add(-6 * time.Minute))
		task.Status.ConversationLastEventAt = &lastEvent

		c := newMirrorClient(t, proj, mdSecret(), task)
		r := tsReconciler(c)

		_, handled, err := r.reconcileClocks(context.Background(), proj, task, time.Now())
		require.NoError(t, err)
		require.True(t, handled, "a 6-minute-old event against a 5-minute budget must age out")

		got := mdGetTask(t, c, "t1")
		require.Equal(t, tatarav1alpha1.StageParked, got.Status.Stage)
		require.Equal(t, "awaiting-human", got.Status.StageReason)
	})

	t.Run("an unset per-project budget falls back to the 60-minute default", func(t *testing.T) {
		proj := mdProject()
		proj.Spec.Scm.ConversationIdleMinutes = 0

		task := mdTask("t2", "clarify", tatarav1alpha1.StageConversing)
		task.Status.StageEnteredAt = &metav1.Time{Time: time.Now().Add(-10 * time.Minute)}
		task.Status.PodStartedAt = &metav1.Time{Time: time.Now().Add(-10 * time.Minute)}
		task.Status.StageWorkStartedAt = &metav1.Time{Time: time.Now().Add(-10 * time.Minute)}
		lastEvent := metav1.NewTime(time.Now().Add(-6 * time.Minute))
		task.Status.ConversationLastEventAt = &lastEvent

		c := newMirrorClient(t, proj, mdSecret(), task)
		r := tsReconciler(c)

		_, handled, err := r.reconcileClocks(context.Background(), proj, task, time.Now())
		require.NoError(t, err)
		require.False(t, handled, "the 60-minute default has not elapsed at 6 minutes")

		got := mdGetTask(t, c, "t2")
		require.Equal(t, tatarav1alpha1.StageConversing, got.Status.Stage)
	})
}
