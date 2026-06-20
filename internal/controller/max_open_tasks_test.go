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
		tk.Status.LifecycleState = tc.lifecycle
		if got := taskActive(tk); got != tc.want {
			t.Errorf("taskActive(phase=%q, lifecycle=%q) = %v, want %v", tc.phase, tc.lifecycle, got, tc.want)
		}
	}
}

// TestQueuedAutonomousCount verifies queuedAutonomousCount counts only
// Autonomous+Queued events (not Admitted, not Done, not non-autonomous).
func TestQueuedAutonomousCount(t *testing.T) {
	qes := []tatarav1alpha1.QueuedEvent{
		{Spec: tatarav1alpha1.QueuedEventSpec{Autonomous: true}, Status: tatarav1alpha1.QueuedEventStatus{State: tatarav1alpha1.QueueStateQueued}},
		{Spec: tatarav1alpha1.QueuedEventSpec{Autonomous: true}, Status: tatarav1alpha1.QueuedEventStatus{State: tatarav1alpha1.QueueStateAdmitted}},
		{Spec: tatarav1alpha1.QueuedEventSpec{Autonomous: false}, Status: tatarav1alpha1.QueuedEventStatus{State: tatarav1alpha1.QueueStateQueued}},
		{Spec: tatarav1alpha1.QueuedEventSpec{Autonomous: true}, Status: tatarav1alpha1.QueuedEventStatus{State: tatarav1alpha1.QueueStateDone}},
	}
	if got := queuedAutonomousCount(qes); got != 1 {
		t.Fatalf("queuedAutonomousCount = %d, want 1", got)
	}
}

// TestRunScans_AutonomousCapFull_CreatesNoQEs: project with QueuedAutonomousCap=N,
// N pre-existing autonomous Queued events -> runScans must create 0 new QEs.
func TestRunScans_AutonomousCapFull_CreatesNoQEs(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *", MaxPerRepo: 5}}
	proj, _ := seedScanProject(t, "autocap-full", cron)

	cap := proj.QueuedAutonomousCap()
	alloc := queue.NewSeqAllocator()
	for i := 0; i < cap; i++ {
		payload := tatarav1alpha1.QueuedEventPayload{Kind: "issueLifecycle", RepositoryRef: "autocap-full-repo", Goal: "g", GenerateName: "qe-"}
		_, _, err := queue.EnqueueEvent(context.Background(), k8sClient, alloc, proj, tatarav1alpha1.QueueClassNormal, true, fmt.Sprintf("prefill-%d", i), payload)
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
	var qel tatarav1alpha1.QueuedEventList
	if err := k8sClient.List(context.Background(), &qel); err != nil {
		t.Fatalf("list QEs: %v", err)
	}
	newQEs := 0
	for _, qe := range qel.Items {
		if qe.Spec.ProjectRef == "autocap-full" && qe.Spec.Seq > int64(cap) {
			newQEs++
		}
	}
	if newQEs != 0 {
		t.Fatalf("cap full: want 0 new QEs, got %d", newQEs)
	}
}

// TestRunScans_AutonomousCapTwo_EnqueuesAtMostTwo: project with QueuedAutonomousCap=2
// and 3 eligible issues -> at most 2 QEs created, 0 Tasks directly.
func TestRunScans_AutonomousCapTwo_EnqueuesAtMostTwo(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *", MaxPerRepo: 5}}
	proj, _ := seedScanProject(t, "autocap-two", cron)
	// Persist Queue spec to K8s so runScans reads the correct cap.
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
	if count > 2 {
		t.Fatalf("cap=2: want at most 2 QEs, got %d", count)
	}
	if count == 0 {
		t.Fatalf("cap=2: want at least 1 QE, got 0")
	}
	tasks := listScanTasks(t, "autocap-two")
	if len(tasks) != 0 {
		t.Fatalf("want 0 Tasks directly created, got %d", len(tasks))
	}
}
