package controller

// TDD test for O1: autonomous-enqueue budget removal.
// Written BEFORE the implementation; must fail until remaining *int is removed
// from brainstorm (and the budget guard deleted).

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
	"k8s.io/apimachinery/pkg/types"
)

// TestBrainstormEnqueuesDespiteQueuedAutonomousCount asserts that when the
// queued-autonomous count already exceeds the old MaxOpenTasks cap, a due
// brainstorm still enqueues (the budget gate no longer blocks creation).
//
// Setup: maxOpenTasks=1 (old cap), brainstorm enabled + due, proposal backlog
// under maxOpenProposals. Two Queued autonomous QueuedEvents pre-seeded
// (count=2 > old cap 1). After runScans, a brainstorm QE with
// dedupKey "brainstorm-<proj>" must exist.
func TestBrainstormEnqueuesDespiteQueuedAutonomousCount(t *testing.T) {
	const projName = "no-budget-proj"
	cron := &tatarav1alpha1.ScmCron{
		Brainstorm: tatarav1alpha1.BrainstormActivity{
			Enabled:          true,
			Schedule:         "* * * * *", // always due
			MaxOpenProposals: 5,
		},
	}
	proj, _ := seedScanProject(t, projName, cron)

	// Set maxOpenTasks=1 so the old cap would block at count=2.
	proj.Spec.MaxOpenTasks = 1
	if err := k8sClient.Update(context.Background(), proj); err != nil {
		t.Fatalf("update proj spec: %v", err)
	}

	// Pre-seed 2 Queued autonomous QEs (count exceeds old cap of 1).
	seq := &queue.SeqSource{Client: k8sClient, Namespace: testNS}
	for i := 0; i < 2; i++ {
		payload := tatarav1alpha1.QueuedEventPayload{
			Kind:         "clarify",
			Goal:         "pre-filled",
			GenerateName: "prefill-",
		}
		_, _, err := queue.EnqueueEvent(
			context.Background(), k8sClient, seq, proj,
			tatarav1alpha1.QueueClassNormal, true,
			fmt.Sprintf("prefill-no-budget-%d", i), payload,
		)
		if err != nil {
			t.Fatalf("pre-fill QE %d: %v", i, err)
		}
	}

	// Backdate LastBrainstorm so the * * * * * schedule fires immediately.
	// The project was just created, so CreationTimestamp is recent; the cron's
	// next-fire would be in the next minute, making due=false. Stamp with a
	// 2-minute-ago time so next-fire is in the past.
	past := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	proj.Status.LastBrainstorm = &past
	// Stamp LastRefine to now (after the brainstorm due-base) so the refine
	// pre-scan barrier (merged onto the brainstorm tick) is already satisfied
	// this cycle; this test targets the budget gate, not the refine barrier.
	now := metav1.Now()
	proj.Status.LastRefine = &now
	if err := k8sClient.Status().Update(context.Background(), proj); err != nil {
		t.Fatalf("status update LastBrainstorm: %v", err)
	}
	// Re-fetch to get consistent ResourceVersion before passing to runScans.
	fresh := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: projName}, fresh); err != nil {
		t.Fatalf("get proj: %v", err)
	}
	proj = fresh

	// Add a repo so brainstorm has at least one valid slug.
	_ = mkScanRepo(t, projName, projName+"-br-repo", "https://github.com/o/nb.git")

	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/nb": {}, // 0 proposals -> under maxOpenProposals cap
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	if _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}

	qes := listBrainstormQEs(t, projName)
	if len(qes) == 0 {
		t.Fatal("brainstorm event was not enqueued; budget gate still blocks creation")
	}
}
