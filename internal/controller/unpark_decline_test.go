package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// recordingSink is a minimal logr.LogSink that records every Info/Error
// message, so a test can assert on log.FromContext(ctx) output without
// depending on the global zap dev-mode logger's stderr destination.
type recordingSink struct {
	lines *[]string
}

func (s recordingSink) Init(logr.RuntimeInfo)  {}
func (s recordingSink) Enabled(level int) bool { return true }
func (s recordingSink) Info(level int, msg string, kv ...any) {
	*s.lines = append(*s.lines, msg)
}
func (s recordingSink) Error(err error, msg string, kv ...any) {
	*s.lines = append(*s.lines, msg)
}
func (s recordingSink) WithValues(kv ...any) logr.LogSink { return s }
func (s recordingSink) WithName(name string) logr.LogSink { return s }

func recordingCtx() (context.Context, *[]string) {
	var lines []string
	logger := logr.New(recordingSink{lines: &lines})
	return log.IntoContext(context.Background(), logger), &lines
}

func containsLine(lines []string, want string) bool {
	for _, l := range lines {
		if l == want {
			return true
		}
	}
	return false
}

// Finding 3 (guard side) + finding 2 (metric): a GUARD decline in driveUnparks
// (the live Task had already drifted from what this pass believed was
// parked) is rare and anomalous, so - unlike a RULE decline - it must be
// logged at INFO even from the reconcile-cadence sweep, and it must count
// against the new operator_unpark_declined_total{kind="guard"} series.
func TestDriveUnparks_GuardDecline_LogsAndCounts(t *testing.T) {
	task := wfParkedTask("t-guard-du", "implement", stage.ReasonAwaitingHuman)
	c := newMirrorClient(t, task)
	// APIReader disagrees with what driveUnparks' own List (via c) believes:
	// live StageReason has already moved to merge-timeout by the time the
	// retry-loop Get runs.
	drifted := task.DeepCopy()
	drifted.Status.StageReason = stage.ReasonMergeTimeout
	apiReader := &staleGetClient{Client: c, stale: drifted}

	metrics := wfMetrics()
	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: metrics, APIReader: apiReader}
	ctx, lines := recordingCtx()
	if err := r.driveUnparks(ctx, wfProject(), time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}

	if !containsLine(*lines, "unpark: declined (drift guard)") {
		t.Fatalf("no drift-guard INFO log; lines: %v", *lines)
	}
	if got := testutil.ToFloat64(metrics.UnparkDeclinedCounter(stage.ReasonAwaitingHuman, "guard")); got != 1 {
		t.Fatalf("operator_unpark_declined_total{awaiting-human,guard} = %v, want 1", got)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.Stage != tatarav1alpha1.StageParked {
		t.Fatalf("guard decline must not mutate the live Task; stage=%s", got.Status.Stage)
	}
}

// Finding 3 (rule side) + finding 2 (metric): a RULE decline in driveUnparks
// (stage.Unpark's re-entry rule simply not satisfied - normal steady state,
// e.g. no non-bot comment yet) must NOT be logged - driveUnparks sweeps every
// parked Task every pass and this is the expected outcome for most of
// them - but it must still count against operator_unpark_declined_total,
// distinguished by kind="rule", so the metric stays complete.
func TestDriveUnparks_RuleDecline_CountsButDoesNotLog(t *testing.T) {
	task := wfParkedTask("t-rule-du", "review", stage.ReasonAwaitingHuman)
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "tatara-bot", Body: "parked",
	}}
	c := newMirrorClient(t, task)
	metrics := wfMetrics()
	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: metrics}
	ctx, lines := recordingCtx()
	if err := r.driveUnparks(ctx, wfProject(), time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}

	if containsLine(*lines, "unpark: declined (drift guard)") {
		t.Fatalf("a RULE decline must never log the drift-guard line; lines: %v", *lines)
	}
	for _, l := range *lines {
		if l != "" {
			t.Fatalf("driveUnparks must stay silent on a RULE decline (log spam on every steady-state sweep); got line %q", l)
		}
	}
	if got := testutil.ToFloat64(metrics.UnparkDeclinedCounter(stage.ReasonAwaitingHuman, "rule")); got != 1 {
		t.Fatalf("operator_unpark_declined_total{awaiting-human,rule} = %v, want 1", got)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.Stage != tatarav1alpha1.StageParked {
		t.Fatalf("rule decline must not mutate the live Task; stage=%s", got.Status.Stage)
	}
}

var _ client.Reader = (*staleGetClient)(nil)
