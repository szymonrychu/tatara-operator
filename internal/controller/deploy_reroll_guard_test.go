package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// TestCDScan_DoesNotResurrectResolvedTask is the finding-2 regression: cdScan
// operates on a snapshot list. Between the snapshot and the reroll write, a
// concurrent resolveDeployedSweep can resolve a converging cascade to Done (Phase
// cleared, DeployState=Done). The old reroll blindly overwrote DeployState/Phase
// with no re-check, dragging the already-resolved Task back to Implement -> a
// DUPLICATE production deploy. cdRerollStalled now re-asserts the still-Deploying +
// deadline-exceeded precondition against the freshly-read object and aborts.
func TestCDScan_DoesNotResurrectResolvedTask(t *testing.T) {
	proj := seedDeployScene(t, "cdscanrace", "tatara-operator")
	// Seed a Deploying task well past its 1.5x backstop threshold, so the cdScan
	// snapshot classifies it as stalled and eligible for reroll.
	task := seedDeployingTask(t, "dep-cdscanrace", proj.Name, "dep-comp-cdscanrace", "szymonrychu/tatara-operator#7",
		time.Now().Add(-2000*time.Second), "v1.0.0")

	// Snapshot the stalled Deploying state (what cdScan's `existing` list carries).
	snapshot := *getTask(t, task.Name)
	require.Equal(t, tatarav1alpha1.DeployStateDeploying, snapshot.Status.DeployState)

	// Concurrent resolution: the cascade applied and resolveDeployedTask marked the
	// Task Done (Phase cleared) AFTER the cdScan snapshot was taken.
	resolved := getTask(t, task.Name)
	resolved.Status.Phase = ""
	resolved.Status.DeployState = "Done"
	require.NoError(t, k8sClient.Status().Update(context.Background(), resolved))

	pr := &ProjectReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	pr.cdScan(deployCtx(), proj, []tatarav1alpha1.Task{snapshot})

	// The resolved Task must stay Done, never dragged back to Implement.
	got := getTask(t, task.Name)
	require.Equal(t, "Done", got.Status.DeployState, "a concurrently-resolved Done task must not be rerolled to Implement")
	require.Equal(t, "", got.Status.Phase)
	require.Equal(t, 0, got.Status.ImplementGiveUps, "no reroll attempt must be consumed against a resolved task")
	require.Empty(t, got.Status.ImplementContext, "no reroll context must be stamped on a resolved task")
}
