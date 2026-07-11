package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// TestCDScan_ParksExhaustedStalledDeploy is the liveness-hardening fix for the
// permanent-wedge finding #1: a Deploying cascade stalled past 1.5x its budget
// whose auto-reroll budget is spent used to sit DEPLOYING forever ("leaving for a
// human", non-terminal, no comment, never GC-eligible). It must now transition to
// Parked (terminal, reaper-eligible) with an issue comment naming the stuck
// artifact so the stall is surfaced with a human signal instead of silently wedged.
func TestCDScan_ParksExhaustedStalledDeploy(t *testing.T) {
	proj := seedDeployScene(t, "cdpark", "tatara-operator")
	task := seedDeployingTask(t, "dep-cdpark", proj.Name, "dep-comp-cdpark",
		"szymonrychu/tatara-operator#7", time.Now().Add(-2000*time.Second), "v1.0.0")

	// Exhaust the bounded auto-reroll budget: no auto-recovery left.
	cur := getTask(t, task.Name)
	cur.Status.ImplementGiveUps = maxImplGiveUps
	require.NoError(t, k8sClient.Status().Update(context.Background(), cur))
	snapshot := *getTask(t, task.Name)

	w := &umbWriter{}
	r := newUmbProjectReconciler(w)
	r.cdScan(deployCtx(), proj, []tatarav1alpha1.Task{snapshot})

	got := getTask(t, task.Name)
	require.Equal(t, "Parked", got.Status.DeployState,
		"an exhausted stalled deploy must be parked terminal, not left Deploying forever")
	require.False(t, tatarav1alpha1.TaskDeploying(got), "must not remain in the Deploying phase")
	require.True(t, tatarav1alpha1.IsRecoverableGiveup(got.Status.ParkReason),
		"park reason must be a recoverable-giveup reason so cdScan counts it failed and the reaper can reclaim it")
	require.Len(t, w.comments, 1, "the stuck deploy must be surfaced with exactly one issue comment")
	require.Contains(t, w.comments[0], "szymonrychu/tatara-operator#7", "comment must target the originating issue")
}

// TestCDScan_ParkedExhaustedNotRecommented: once parked, a subsequent cdScan tick
// must NOT re-comment (the Parked+recoverable branch is counted failed and skipped).
func TestCDScan_ParkedExhaustedNotRecommented(t *testing.T) {
	proj := seedDeployScene(t, "cdpark2", "tatara-operator")
	task := seedDeployingTask(t, "dep-cdpark2", proj.Name, "dep-comp-cdpark2",
		"szymonrychu/tatara-operator#7", time.Now().Add(-2000*time.Second), "v1.0.0")
	cur := getTask(t, task.Name)
	cur.Status.ImplementGiveUps = maxImplGiveUps
	require.NoError(t, k8sClient.Status().Update(context.Background(), cur))

	w := &umbWriter{}
	r := newUmbProjectReconciler(w)
	// First tick parks + comments.
	r.cdScan(deployCtx(), proj, []tatarav1alpha1.Task{*getTask(t, task.Name)})
	require.Len(t, w.comments, 1)
	// Second tick sees it already Parked; must not comment again.
	r.cdScan(deployCtx(), proj, []tatarav1alpha1.Task{*getTask(t, task.Name)})
	require.Len(t, w.comments, 1, "a parked stalled deploy must not be re-commented on every cdScan tick")
}
