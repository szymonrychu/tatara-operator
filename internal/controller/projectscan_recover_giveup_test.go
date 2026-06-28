package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"k8s.io/apimachinery/pkg/types"
)

// mkGiveUpTask creates a terminal Parked issueLifecycle Task for (slug, number)
// with the given ParkReason and ImplementGiveUps counter.
func mkGiveUpTask(t *testing.T, project, repoRef, slug string, number, giveUps int, parkReason string) *tatarav1alpha1.Task {
	t.Helper()
	task := &tatarav1alpha1.Task{}
	task.GenerateName = "scan-giveup-"
	task.Namespace = testNS
	task.Labels = scanTaskLabels(candidate{repo: slug, number: number}, "issueScan", "issueLifecycle")
	task.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef:    project,
		RepositoryRef: repoRef,
		Goal:          "implement " + slug,
		Kind:          "issueLifecycle",
		Source:        &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: fmt.Sprintf("%s#%d", slug, number), Number: number},
	}
	if err := k8sClient.Create(context.Background(), task); err != nil {
		t.Fatalf("mkGiveUpTask create: %v", err)
	}
	task.Status.LifecycleState = "Parked"
	task.Status.ParkReason = parkReason
	task.Status.ImplementGiveUps = giveUps
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("mkGiveUpTask status: %v", err)
	}
	return task
}

// getTaskByRef fetches the task by name.
func getGiveUpTask(t *testing.T, name string) *tatarav1alpha1.Task {
	t.Helper()
	tk := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, tk); err != nil {
		t.Fatalf("getGiveUpTask %s: %v", name, err)
	}
	return tk
}

// TestRecoverOrphans_GiveUp_UnderCap_Rerolls verifies that a Parked issueLifecycle
// task with a recoverable reason and ImplementGiveUps < maxImplGiveUps is adopted
// in-place to Implement rather than spawning a new QueuedEvent.
func TestRecoverOrphans_GiveUp_UnderCap_Rerolls(t *testing.T) {
	proj, repo := seedBackstopProject(t, "gu-under")
	parked := mkGiveUpTask(t, "gu-under", repo.Name, "o/r", 11, 1, "implement-failed")

	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/r": {{Repo: "o/r", Number: 11, Labels: []string{"tatara-implementation"}, UpdatedAt: time.Unix(100, 0)}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.recoverOrphans(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo}, nil)

	// No new QE: task was adopted in-place.
	qes := listScanQEs(t, "gu-under")
	if len(qes) != 0 {
		t.Fatalf("want 0 QEs (task adopted in-place), got %d", len(qes))
	}

	// Task must be adopted to Implement.
	got := getGiveUpTask(t, parked.Name)
	if got.Status.LifecycleState != "Implement" {
		t.Errorf("LifecycleState = %q, want Implement", got.Status.LifecycleState)
	}
	if got.Status.ImplementGiveUps != 1 {
		t.Errorf("ImplementGiveUps = %d, want 1 (preserved)", got.Status.ImplementGiveUps)
	}
}

// TestRecoverOrphans_GiveUp_AtCap_Skips verifies that a Parked task at the
// give-up cap is not rerolled and no QueuedEvent is created.
func TestRecoverOrphans_GiveUp_AtCap_Skips(t *testing.T) {
	proj, repo := seedBackstopProject(t, "gu-cap")
	parked := mkGiveUpTask(t, "gu-cap", repo.Name, "o/r", 12, maxImplGiveUps, "maxIterations")

	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/r": {{Repo: "o/r", Number: 12, Labels: []string{"tatara-implementation"}, UpdatedAt: time.Unix(100, 0)}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.recoverOrphans(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo}, nil)

	// No QE: at-cap means skip.
	qes := listScanQEs(t, "gu-cap")
	if len(qes) != 0 {
		t.Fatalf("want 0 QEs (at cap, skipped), got %d", len(qes))
	}

	// Task must NOT be adopted (still Parked).
	got := getGiveUpTask(t, parked.Name)
	if got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked (not adopted at cap)", got.Status.LifecycleState)
	}
}

// TestRecoverOrphans_GiveUp_LiveTask_NoAction verifies that a live (non-terminal)
// lifecycle task for the issue suppresses the give-up reroll (existing dedup).
func TestRecoverOrphans_GiveUp_LiveTask_NoAction(t *testing.T) {
	proj, repo := seedBackstopProject(t, "gu-live")
	// Live (non-terminal) task: this blocks the backstop entirely.
	live := &tatarav1alpha1.Task{}
	live.GenerateName = "scan-live-"
	live.Namespace = testNS
	live.Labels = scanTaskLabels(candidate{repo: "o/r", number: 13}, "issueScan", "issueLifecycle")
	live.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef: "gu-live", RepositoryRef: repo.Name,
		Goal: "g", Kind: "issueLifecycle",
		Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#13", Number: 13},
	}
	if err := k8sClient.Create(context.Background(), live); err != nil {
		t.Fatalf("create live task: %v", err)
	}
	live.Status.LifecycleState = "Implement"
	if err := k8sClient.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("set live lifecycle: %v", err)
	}

	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/r": {{Repo: "o/r", Number: 13, Labels: []string{"tatara-implementation"}, UpdatedAt: time.Unix(100, 0)}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.recoverOrphans(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo}, nil)

	// No QE: live task blocks recovery.
	qes := listScanQEs(t, "gu-live")
	if len(qes) != 0 {
		t.Fatalf("want 0 QEs (live task present), got %d", len(qes))
	}
}

// TestRecoverOrphans_GiveUp_NonRecoverable_CreatesQE verifies that a Parked task
// with a non-recoverable reason does not trigger the reroll path: the normal
// createScanTask path runs instead, creating a new QueuedEvent.
func TestRecoverOrphans_GiveUp_NonRecoverable_CreatesQE(t *testing.T) {
	proj, repo := seedBackstopProject(t, "gu-nonrec")
	// Parked with non-recoverable reason: refused-declined.
	mkGiveUpTask(t, "gu-nonrec", repo.Name, "o/r", 14, 1, "refused-declined")

	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/r": {{Repo: "o/r", Number: 14, Labels: []string{"tatara-implementation"}, UpdatedAt: time.Unix(100, 0)}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.recoverOrphans(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo}, nil)

	// QE created: non-recoverable park does not block normal recovery.
	qes := listScanQEs(t, "gu-nonrec")
	if len(qes) != 1 {
		t.Fatalf("want 1 QE (non-recoverable, normal create), got %d", len(qes))
	}
}

// TestRecoverOrphans_GiveUp_IssueClosed_MarkedDone verifies the closed-issue
// sweep: a spared recoverable give-up task whose issue is no longer open (the
// repo was listed but the number is absent) is transitioned to Done so the
// reaper can reclaim it.
func TestRecoverOrphans_GiveUp_IssueClosed_MarkedDone(t *testing.T) {
	proj, repo := seedBackstopProject(t, "gu-closed")
	parked := mkGiveUpTask(t, "gu-closed", repo.Name, "o/r", 15, 2, "implement-failed")

	// o/r is listed but its open set does NOT contain #15 -> issue is closed.
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{"o/r": {}},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.recoverOrphans(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo}, nil)

	got := getGiveUpTask(t, parked.Name)
	if got.Status.LifecycleState != "Done" {
		t.Errorf("LifecycleState = %q, want Done (issue closed)", got.Status.LifecycleState)
	}
	if got.Status.ParkReason != "issue-closed" {
		t.Errorf("ParkReason = %q, want issue-closed", got.Status.ParkReason)
	}
}

// TestAdoptLifecycleTask_TriageResetsGiveUps verifies that a human re-engaging a
// blocked issue (Triage re-entry) resets the give-up counter to zero, while an
// Implement re-entry preserves it (covered by the reroll test).
func TestAdoptLifecycleTask_TriageResetsGiveUps(t *testing.T) {
	proj, repo := seedBackstopProject(t, "gu-triage")
	parked := mkGiveUpTask(t, "gu-triage", repo.Name, "o/r", 16, maxImplGiveUps, "maxIterations")

	r := newScanReconciler(&perRepoFakeReader{})
	if err := r.adoptLifecycleTask(context.Background(), proj, parked); err != nil {
		t.Fatalf("adoptLifecycleTask: %v", err)
	}

	got := getGiveUpTask(t, parked.Name)
	if got.Status.LifecycleState != "Triage" {
		t.Errorf("LifecycleState = %q, want Triage", got.Status.LifecycleState)
	}
	if got.Status.ImplementGiveUps != 0 {
		t.Errorf("ImplementGiveUps = %d, want 0 (reset on human Triage revival)", got.Status.ImplementGiveUps)
	}
}
