package controller

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTaskActive_ExcludesTerminalLifecycle(t *testing.T) {
	cases := []struct {
		phase     string
		lifecycle string
		want      bool
	}{
		{"Running", "", true},
		{"Planning", "", true},
		{"Pending", "", false},
		{"Succeeded", "", false},
		{"Failed", "", false},
		{"Running", "Triage", true},
		// Active phase + terminal lifecycle must NOT count as in-flight. A Task
		// Parked at maxIterations keeps a stale Planning phase; counting it would
		// inflate the inflight gauge and misrepresent running agents.
		{"Planning", "Parked", false},
		{"Running", "Parked", false},
		{"Running", "Done", false},
		{"Running", "Stopped", false},
		// Conversation = awaiting-human; externally gated, must NOT count as
		// in-flight (the agent is not running).
		{"Running", "Conversation", false},
		{"Planning", "Conversation", false},
	}
	for _, tc := range cases {
		tk := &tatarav1alpha1.Task{}
		tk.Status.Phase = tc.phase
		tk.Status.DeployState = tc.lifecycle
		if got := taskActive(tk); got != tc.want {
			t.Errorf("taskActive(phase=%q, lifecycle=%q) = %v, want %v", tc.phase, tc.lifecycle, got, tc.want)
		}
	}
}

// TestRunScans_AutonomousCapFull_StillEnqueues: the old QueuedAutonomousCap budget
// gate is removed; pre-seeding N autonomous Queued events no longer blocks new
// issueScan enqueues. runScans must create QEs for all eligible issues.
func TestRunScans_AutonomousCapFull_StillEnqueues(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *", MaxPerRepo: 5}}
	proj, _ := seedScanProject(t, "autocap-full", cron)

	// Pre-seed cap-many autonomous QEs (old behavior would have blocked here).
	oldCap := proj.QueuedAutonomousCap()
	seq := &queue.SeqSource{Client: k8sClient, Namespace: testNS}
	for i := 0; i < oldCap; i++ {
		payload := tatarav1alpha1.QueuedEventPayload{Kind: "issueLifecycle", RepositoryRef: "autocap-full-repo", Goal: "g", GenerateName: "qe-"}
		_, _, err := queue.EnqueueEvent(context.Background(), k8sClient, seq, proj, tatarav1alpha1.QueueClassNormal, true, fmt.Sprintf("prefill-%d", i), payload)
		if err != nil {
			t.Fatalf("pre-fill QE %d: %v", i, err)
		}
	}

	past := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	proj.Status.LastIssueScan = &past
	_ = k8sClient.Status().Update(context.Background(), proj)

	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/r", Number: 1, UpdatedAt: time.Unix(100, 0)},
		{Repo: "o/r", Number: 2, UpdatedAt: time.Unix(200, 0)},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	if _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}
	// Both issues must be enqueued regardless of pre-existing autonomous QE count.
	newQEs := 0
	var qel tatarav1alpha1.QueuedEventList
	if err := k8sClient.List(context.Background(), &qel); err != nil {
		t.Fatalf("list QEs: %v", err)
	}
	for _, qe := range qel.Items {
		if qe.Spec.ProjectRef == "autocap-full" && qe.Spec.Seq > int64(oldCap) {
			newQEs++
		}
	}
	if newQEs != 2 {
		t.Fatalf("budget removed: want 2 new QEs for 2 eligible issues, got %d", newQEs)
	}
}

// TestRunScans_QueuedAutonomousCapIgnored: QueuedAutonomousCap is now ignored;
// 3 eligible issues all get QEs even when cap=2 pre-exists in spec.
func TestRunScans_QueuedAutonomousCapIgnored(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *", MaxPerRepo: 5}}
	proj, _ := seedScanProject(t, "autocap-two", cron)
	// Persist Queue spec to K8s (field retained for CRD compat but ignored).
	proj.Spec.Queue = &tatarav1alpha1.QueueSpec{QueuedAutonomousCap: 2}
	if err := k8sClient.Update(context.Background(), proj); err != nil {
		t.Fatalf("update proj spec: %v", err)
	}

	past := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	proj.Status.LastIssueScan = &past
	_ = k8sClient.Status().Update(context.Background(), proj)

	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/r", Number: 10, UpdatedAt: time.Unix(100, 0)},
		{Repo: "o/r", Number: 11, UpdatedAt: time.Unix(200, 0)},
		{Repo: "o/r", Number: 12, UpdatedAt: time.Unix(300, 0)},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	if _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}
	// All 3 issues enqueued (cap=2 no longer limits creation).
	var qel tatarav1alpha1.QueuedEventList
	if err := k8sClient.List(context.Background(), &qel); err != nil {
		t.Fatalf("list QEs: %v", err)
	}
	count := 0
	for _, qe := range qel.Items {
		if qe.Spec.ProjectRef == "autocap-two" && qe.Spec.Autonomous {
			count++
		}
	}
	if count != 3 {
		t.Fatalf("cap ignored: want 3 QEs for 3 eligible issues, got %d", count)
	}
	tasks := listScanTasks(t, "autocap-two")
	if len(tasks) != 0 {
		t.Fatalf("want 0 Tasks directly created, got %d", len(tasks))
	}
}
