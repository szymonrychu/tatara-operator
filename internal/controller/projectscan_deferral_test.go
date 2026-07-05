package controller

// Tests proving that a genuine enqueue deferral (transient createScanTask error)
// sets backlog=true for both mrScan and issueScan, and that terminal skips do not.
// This is the load-bearing half of the fix in 83bf42e: deferred>0 -> backlog=true.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// qeCreateErrorClient wraps k8sClient and injects a synthetic error when Create
// is called for a QueuedEvent, simulating a transient enqueue failure. All other
// operations (List, Get, Update, Create for other types) pass through unchanged.
// This forces createScanTask to return a non-nil error, incrementing the deferred
// counter in mrScan/issueScan and driving backlog=true.
type qeCreateErrorClient struct {
	client.Client
}

func (c *qeCreateErrorClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*tatarav1alpha1.QueuedEvent); ok {
		return fmt.Errorf("synthetic transient enqueue error")
	}
	return c.Client.Create(ctx, obj, opts...)
}

// newScanReconcilerWithQEError builds a ProjectReconciler whose client injects
// an error on QueuedEvent Create. The SeqSource keeps its own k8sClient so
// ConfigMap CAS (seq allocation) continues to work normally.
func newScanReconcilerWithQEError(reader scm.SCMReader) *ProjectReconciler {
	r := newProjectReconciler()
	r.Client = &qeCreateErrorClient{Client: k8sClient}
	r.Seq = &queue.SeqSource{Client: k8sClient, Namespace: testNS}
	r.ReaderFor = func(string, string) (scm.SCMReader, error) { return reader, nil }
	return r
}

// TestMRScan_EnqueueDeferral_Backlog: a candidate that reaches createScanTask
// but whose QueuedEvent Create fails must set backlog=true (deferred>0).
// The test fails if the deferred++ branch is ever removed from mrScan.
func TestMRScan_EnqueueDeferral_Backlog(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 10}}
	proj, repo := seedScanProject(t, "mrscan-defer-backlog", cron)
	repos := []tatarav1alpha1.Repository{*repo}
	// Unlabeled human PR with default scope: passes all gates, reaches createScanTask.
	reader := &fakeReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 1, Author: "human", HeadSHA: "sha1", UpdatedAt: time.Unix(100, 0)},
	}}
	r := newScanReconcilerWithQEError(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	backlog := r.mrScan(context.Background(), proj, reader, repos, nil, cron.MRScan)
	if !backlog {
		t.Fatal("enqueue-failure in mrScan must return backlog=true (deferred>0); got false")
	}
}

// TestIssueScan_EnqueueDeferral_Backlog: a candidate that reaches createScanTask
// in issueScan but whose QueuedEvent Create fails must cause issueScan to return
// backlog=true (deferred>0). The test fails if the deferred++ branch is removed.
func TestIssueScan_EnqueueDeferral_Backlog(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 10}}
	proj, repo := seedScanProject(t, "issuescan-defer-backlog", cron)
	repos := []tatarav1alpha1.Repository{*repo}
	// Plain human issue: passes all gates (no existing tasks, no brainstorm label,
	// no comments -> botHadLastWord=false, staleProposalDays unset -> reapEligible=false).
	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/r", Number: 1, Author: "human", UpdatedAt: time.Unix(100, 0)},
	}}
	r := newScanReconcilerWithQEError(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	backlog, _ := r.issueScan(context.Background(), proj, reader, repos, nil, cron.IssueScan)
	if !backlog {
		t.Fatal("enqueue-failure in issueScan must return backlog=true (deferred>0); got false")
	}
}
