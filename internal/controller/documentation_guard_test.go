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
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// mkDocTask creates a documentation Task in the given stage.
func mkDocTask(t *testing.T, name, project, docsRepo, stg string) *tatarav1alpha1.Task {
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
	task.Status.State = stg
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))
	return task
}

// TestDocumentationScan_InFlightGuard is liveness-hardening finding #7: two doc
// Tasks for different source heads could run concurrently (the dedup key is
// per-head, so different heads never dedup). An in-flight guard (mirroring
// brainstormInFlightProject) must suppress a new doc Task while a LIVE doc Task
// already exists in the project.
func TestDocumentationScan_InFlightGuard(t *testing.T) {
	docsURL := "https://github.com/o/docsg.git"
	proj, repos := seedDocumentationProject(t, "doc-inflight", docsURL, []string{"o/ag"})
	// A doc Task is already running (non-terminal).
	mkDocTask(t, "doc-inflight-live", proj.Name, "doc-inflight-docs", tatarav1alpha1.StateUnderImplementation)

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

// TestDocumentationScan_ParkedDocTaskStillBlocks: #521 made parked/failed a
// FLAG orthogonal to State, and documentationInFlightProject counts in-flight
// with tatarav1alpha1.TaskDone, which is state-only (done|rejected) and FALSE
// for a parked Task. A parked doc Task may still resume, so it must keep
// holding the in-flight guard - minting a second batch under it would let two
// doc Tasks cover the same heads concurrently, exactly the overlap this guard
// exists to prevent. (Before #521, TaskDone incorrectly reported true for a
// parked Task, which is what let this guard - wrongly - treat it as finished.)
func TestDocumentationScan_ParkedDocTaskStillBlocks(t *testing.T) {
	docsURL := "https://github.com/o/docsp.git"
	proj, repos := seedDocumentationProject(t, "doc-parked", docsURL, []string{"o/ap"})
	parked := mkDocTask(t, "doc-parked-task", proj.Name, "doc-parked-docs", tatarav1alpha1.StateUnderImplementation)
	parked.Status.ParkReason = stage.ReasonAwaitingHuman
	require.NoError(t, k8sClient.Status().Update(context.Background(), parked))

	reader := &docFakeReader{
		headBySlug: map[string]string{"o/ap": "headsha9"},
		commitsBySlug: map[string][]scm.CommitRef{
			"o/ap": {{SHA: "c9", Date: time.Now().Add(-30 * time.Minute)}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.documentationScan(context.Background(), proj, reader, repos)

	require.Empty(t, listDocumentationQEs(t, "doc-parked"),
		"a parked doc Task is NOT done; it must keep blocking a fresh doc cycle")
}
