package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// seedConvTask creates an issueLifecycle Task in the given lifecycle state with
// LastActivityAt set to activityAt and DeadlineAt set to deadline.
func seedConvTask(t *testing.T, projName, repoName, taskName, state string, activityAt, deadline time.Time) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()
	dl := metav1.NewTime(deadline)
	act := metav1.NewTime(activityAt)
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskName,
			Namespace: testNS,
			Labels: map[string]string{
				labelSourceRepo:   "o.r",
				labelSourceNumber: "10",
				labelSourceKind:   "issueLifecycle",
				labelActivity:     "issueScan",
			},
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    projName,
			RepositoryRef: repoName,
			Kind:          "issueLifecycle",
			Goal:          "issue #10",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#10", Number: 10,
			},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create conv task: %v", err)
	}
	task.Status.LifecycleState = state
	task.Status.LastActivityAt = &act
	task.Status.DeadlineAt = &dl
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed conv task status: %v", err)
	}
	return task
}

// TestIssueScan_ReactivatesConversationTask verifies that when issueScan finds
// an issue whose updatedAt is newer than the bound lifecycle Task's LastActivityAt
// while the Task is in Conversation state, it patches the Task to Triage and
// resets the timers instead of creating a duplicate Task.
func TestIssueScan_ReactivatesConversationTask(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 2}}
	proj, _ := seedScanProject(t, "conv-reactivate", cron)

	issueUpdatedAt := time.Now().Add(-5 * time.Minute)
	lastActivityAt := issueUpdatedAt.Add(-30 * time.Minute) // issue newer than last activity
	deadline := time.Now().Add(10 * time.Minute)

	_ = seedConvTask(t, "conv-reactivate", "conv-reactivate-repo", "conv-task-1", "Conversation",
		lastActivityAt, deadline)

	reader := &fakeReader{
		issues: []scm.IssueRef{
			{Repo: "o/r", Number: 10, UpdatedAt: issueUpdatedAt},
		},
		// Supply a human comment newer than lastActivityAt so the author-aware gate passes.
		comments: []scm.IssueComment{
			{Author: "szymon", CreatedAt: issueUpdatedAt},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	existing := listAllTasks(t, "conv-reactivate")
	b := 99
	r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{
		{ObjectMeta: metav1.ObjectMeta{Name: "conv-reactivate-repo", Namespace: testNS},
			Spec: tatarav1alpha1.RepositorySpec{ProjectRef: "conv-reactivate", URL: "https://github.com/o/r.git", DefaultBranch: "main"}},
	}, existing, cron.IssueScan, &b)

	// Must NOT create a new task.
	tasks := listAllTasks(t, "conv-reactivate")
	if len(tasks) != 1 {
		t.Fatalf("issueScan must not create a duplicate Task for a Conversation issue; got %d tasks", len(tasks))
	}

	// The existing task must be reactivated to Triage.
	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "conv-task-1"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status.LifecycleState != "Triage" {
		t.Errorf("LifecycleState = %q, want Triage (reactivated)", got.Status.LifecycleState)
	}
	if got.Status.LastActivityAt == nil || !got.Status.LastActivityAt.After(lastActivityAt) {
		t.Error("LastActivityAt must be updated to a time after the original last activity")
	}
}

// TestIssueScan_ReactivatesStoppedTask verifies the same reactivation path for
// a Stopped (idle-parked) Task - a new comment was missed during downtime.
func TestIssueScan_ReactivatesStoppedTask(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 2}}
	proj, _ := seedScanProject(t, "stopped-reactivate", cron)

	issueUpdatedAt := time.Now().Add(-3 * time.Minute)
	lastActivityAt := issueUpdatedAt.Add(-60 * time.Minute) // issue newer

	_ = seedConvTask(t, "stopped-reactivate", "stopped-reactivate-repo", "stopped-task-1", "Stopped",
		lastActivityAt, time.Now().Add(-1*time.Minute))

	reader := &fakeReader{
		issues: []scm.IssueRef{
			{Repo: "o/r", Number: 10, UpdatedAt: issueUpdatedAt},
		},
		// Supply a human comment newer than lastActivityAt so the author-aware gate passes.
		comments: []scm.IssueComment{
			{Author: "szymon", CreatedAt: issueUpdatedAt},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	existing := listAllTasks(t, "stopped-reactivate")
	b2 := 99
	r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{
		{ObjectMeta: metav1.ObjectMeta{Name: "stopped-reactivate-repo", Namespace: testNS},
			Spec: tatarav1alpha1.RepositorySpec{ProjectRef: "stopped-reactivate", URL: "https://github.com/o/r.git", DefaultBranch: "main"}},
	}, existing, cron.IssueScan, &b2)

	tasks := listAllTasks(t, "stopped-reactivate")
	if len(tasks) != 1 {
		t.Fatalf("must not create duplicate; got %d tasks", len(tasks))
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "stopped-task-1"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status.LifecycleState != "Triage" {
		t.Errorf("LifecycleState = %q, want Triage (Stopped re-open)", got.Status.LifecycleState)
	}
}

// TestIssueScan_NoReactivationWhenIssueNotNewer verifies that when the issue
// updatedAt is NOT newer than LastActivityAt, no reactivation occurs
// (normal dedup: the issue is already handled).
func TestIssueScan_NoReactivationWhenIssueNotNewer(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 2}}
	proj, _ := seedScanProject(t, "conv-no-reactivate", cron)

	lastActivityAt := time.Now().Add(-5 * time.Minute)
	issueUpdatedAt := lastActivityAt.Add(-10 * time.Minute) // issue OLDER than activity

	_ = seedConvTask(t, "conv-no-reactivate", "conv-no-reactivate-repo", "conv-no-task-1", "Conversation",
		lastActivityAt, time.Now().Add(time.Hour))

	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/r", Number: 10, UpdatedAt: issueUpdatedAt},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	existing := listAllTasks(t, "conv-no-reactivate")
	b3 := 99
	r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{
		{ObjectMeta: metav1.ObjectMeta{Name: "conv-no-reactivate-repo", Namespace: testNS},
			Spec: tatarav1alpha1.RepositorySpec{ProjectRef: "conv-no-reactivate", URL: "https://github.com/o/r.git", DefaultBranch: "main"}},
	}, existing, cron.IssueScan, &b3)

	// Task count unchanged, state unchanged.
	tasks := listAllTasks(t, "conv-no-reactivate")
	if len(tasks) != 1 {
		t.Fatalf("no new task expected; got %d", len(tasks))
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "conv-no-task-1"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status.LifecycleState != "Conversation" {
		t.Errorf("LifecycleState = %q, want Conversation (no reactivation)", got.Status.LifecycleState)
	}
}

// listAllTasks returns all Tasks for a given project regardless of scan activity label.
func listAllTasks(t *testing.T, projName string) []tatarav1alpha1.Task {
	t.Helper()
	var list tatarav1alpha1.TaskList
	if err := k8sClient.List(context.Background(), &list, client.InNamespace(testNS)); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	var out []tatarav1alpha1.Task
	for _, tk := range list.Items {
		if tk.Spec.ProjectRef == projName {
			out = append(out, tk)
		}
	}
	return out
}
