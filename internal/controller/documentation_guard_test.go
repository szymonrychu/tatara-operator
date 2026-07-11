package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"k8s.io/apimachinery/pkg/types"
)

// mkDocTask creates a documentation Task in the given lifecycle/phase state.
func mkDocTask(t *testing.T, name, project, docsRepo, phase, deployState, parkReason string) *tatarav1alpha1.Task {
	t.Helper()
	task := &tatarav1alpha1.Task{}
	task.Name = name
	task.Namespace = testNS
	task.Labels = map[string]string{labelActivity: "documentation"}
	task.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef: project, RepositoryRef: docsRepo, Kind: "documentation", Goal: "doc sync",
	}
	require.NoError(t, k8sClient.Create(context.Background(), task))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), task) })
	task.Status.Phase = phase
	task.Status.DeployState = deployState
	task.Status.ParkReason = parkReason
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))
	return task
}

// TestDocumentationScan_InFlightGuard is liveness-hardening finding #7: two doc
// Tasks for different source heads could run concurrently (the dedup key is
// per-head, so different heads never dedup). An in-flight guard (mirroring
// brainstormInFlightProject) must suppress a new doc Task while a non-terminal doc
// Task already exists in the project.
func TestDocumentationScan_InFlightGuard(t *testing.T) {
	docsURL := "https://github.com/o/docsg.git"
	proj, repos := seedDocumentationProject(t, "doc-inflight", docsURL, []string{"o/ag"})
	// A doc Task is already running (non-terminal).
	mkDocTask(t, "doc-inflight-live", proj.Name, "doc-inflight-docs", "Planning", "", "")

	reader := &docFakeReader{
		headBySlug: map[string]string{"o/ag": "headsha2"},
		commitsBySlug: map[string][]scm.CommitRef{
			"o/ag": {{SHA: "c1", Date: time.Now().Add(-30 * time.Minute)}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.documentationScan(context.Background(), proj, reader, repos)

	require.Empty(t, listDocumentationQEs(t, "doc-inflight"),
		"a new doc Task must not be created while a doc Task is already in-flight")
}

// TestDocumentationScan_ReactivatesParkedDocTask: a dropped/Parked documentation
// cycle must be re-swept (reactivated to non-terminal) so it retries, instead of
// being lost forever after LastDocumentation advanced past its window.
func TestDocumentationScan_ReactivatesParkedDocTask(t *testing.T) {
	docsURL := "https://github.com/o/docsp.git"
	proj, repos := seedDocumentationProject(t, "doc-parked", docsURL, []string{"o/ap"})
	parked := mkDocTask(t, "doc-parked-task", proj.Name, "doc-parked-docs", "Failed", "Parked", "implement-failed")

	reader := &docFakeReader{headBySlug: map[string]string{"o/ap": "h"}, commitsBySlug: map[string][]scm.CommitRef{}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.documentationScan(context.Background(), proj, reader, repos)

	got := &tatarav1alpha1.Task{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "doc-parked-task"}, got))
	require.NotEqual(t, "Parked", got.Status.DeployState, "a Parked doc task must be reactivated to retry")
	require.False(t, tatarav1alpha1.TaskTerminal(got), "the reactivated doc task must be live again")
	_ = parked
}
