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
	task.Status.Stage = stg
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
	mkDocTask(t, "doc-inflight-live", proj.Name, "doc-inflight-docs", tatarav1alpha1.StageDocumenting)

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

// TestDocumentationScan_FinishedDocTaskDoesNotBlock: a parked doc Task is
// FINISHED (the reaper collects it), so it must not hold the in-flight guard and
// block the next cycle from minting a fresh batch.
func TestDocumentationScan_FinishedDocTaskDoesNotBlock(t *testing.T) {
	docsURL := "https://github.com/o/docsp.git"
	proj, repos := seedDocumentationProject(t, "doc-parked", docsURL, []string{"o/ap"})
	mkDocTask(t, "doc-parked-task", proj.Name, "doc-parked-docs", tatarav1alpha1.StageParked)

	reader := &docFakeReader{
		headBySlug: map[string]string{"o/ap": "headsha9"},
		commitsBySlug: map[string][]scm.CommitRef{
			"o/ap": {{SHA: "c9", Date: time.Now().Add(-30 * time.Minute)}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.documentationScan(context.Background(), proj, reader, repos)

	require.NotEmpty(t, listDocumentationQEs(t, "doc-parked"),
		"a parked doc Task is finished and must not block a fresh doc cycle")
}
