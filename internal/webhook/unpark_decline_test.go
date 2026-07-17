package webhook

// Finding 2/3 coverage: guard-decline and rule-decline used to collapse into
// the same target=="",err==nil shape with no metric at all. driveCommentUnpark
// must now count BOTH kinds against operator_unpark_declined_total and log
// BOTH kinds (unlike driveUnparks, which stays silent on a rule decline - see
// controller/unpark_decline_test.go).

import (
	"bytes"
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// A GUARD decline (the live Task had already drifted from what the caller
// believed was parked - here, re-parked under a different reason) must be
// logged with decline_kind=guard and counted.
func TestDriveCommentUnpark_GuardDecline_LogsKindAndCounts(t *testing.T) {
	proj := peProject("tatara-bot")
	task := upTask("t-guard-webhook", "implement", stage.ReasonAwaitingHuman)
	task.Status.PendingEvents = []tatarav1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "human", Body: "go ahead",
	}}
	drifted := task.DeepCopy()
	drifted.Status.StageReason = stage.ReasonMergeTimeout

	liveClient := peClient(t, proj, task)
	apiReader := &upStaleGetClient{Client: liveClient, stale: drifted}
	var logBuf bytes.Buffer
	s := upServer(t, liveClient, apiReader, &logBuf)

	s.driveCommentUnpark(context.Background(), proj, task)

	got := getPETask(t, liveClient, task.Name)
	if got.Status.Stage != tatarav1.StageParked {
		t.Fatalf("guard decline must not mutate the live Task; stage=%s", got.Status.Stage)
	}

	var found map[string]any
	for _, line := range upLogLines(t, &logBuf) {
		if line["msg"] == "pendingEvents: comment-driven unpark declined" {
			found = line
			break
		}
	}
	if found == nil {
		t.Fatalf("no INFO log for the guard-declined un-park; log lines: %s", logBuf.String())
	}
	if found["decline_kind"] != "guard" {
		t.Fatalf("decline_kind = %v, want guard: %+v", found["decline_kind"], found)
	}
	if got := testutil.ToFloat64(s.cfg.Metrics.UnparkDeclinedCounter(stage.ReasonAwaitingHuman, "guard")); got != 1 {
		t.Fatalf("operator_unpark_declined_total{awaiting-human,guard} = %v, want 1", got)
	}
}

// A RULE decline (stage.Unpark's re-entry rule not satisfied - a bot-only
// pendingEvent never un-parks) must be logged with decline_kind=rule and
// counted, matching the existing TestDriveCommentUnpark_DeclineLogsInfo
// behavior plus the new label/metric.
func TestDriveCommentUnpark_RuleDecline_LogsKindAndCounts(t *testing.T) {
	proj := peProject("tatara-bot")
	task := upTask("t-rule-webhook", "review", stage.ReasonAwaitingHuman)
	task.Status.PendingEvents = []tatarav1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "tatara-bot", Body: "parked",
	}}
	c := peClient(t, proj, task)
	var logBuf bytes.Buffer
	s := upServer(t, c, c, &logBuf)

	s.driveCommentUnpark(context.Background(), proj, task)

	var found map[string]any
	for _, line := range upLogLines(t, &logBuf) {
		if line["msg"] == "pendingEvents: comment-driven unpark declined" {
			found = line
			break
		}
	}
	if found == nil {
		t.Fatalf("no INFO log for the rule-declined un-park; log lines: %s", logBuf.String())
	}
	if found["decline_kind"] != "rule" {
		t.Fatalf("decline_kind = %v, want rule: %+v", found["decline_kind"], found)
	}
	if got := testutil.ToFloat64(s.cfg.Metrics.UnparkDeclinedCounter(stage.ReasonAwaitingHuman, "rule")); got != 1 {
		t.Fatalf("operator_unpark_declined_total{awaiting-human,rule} = %v, want 1", got)
	}
}
