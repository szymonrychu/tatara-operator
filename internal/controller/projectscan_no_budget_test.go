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
// queued-autonomous count already exceeds the old MaxOpenTasks cap,
// brainstorm still enqueues (the budget gate no longer blocks creation).
//
// Setup: maxOpenTasks=1 (old cap), proposal backlog under maxOpenProposals.
// Two Queued autonomous QueuedEvents pre-seeded (count=2 > old cap 1). After
// r.brainstorm(), a brainstorm QE with dedupKey "brainstorm-<proj>" must
// exist.
//
// Brainstorm has no cron of its own any more (Task 3: refine re-homing), so
// this calls r.brainstorm() directly instead of driving it through
// r.runScans() - runScans no longer touches brainstorm at all, matching every
// other direct-call brainstorm test in projectscan_brainstorm_target_test.go.
func TestBrainstormEnqueuesDespiteQueuedAutonomousCount(t *testing.T) {
	const projName = "no-budget-proj"
	cron := &tatarav1alpha1.ScmCron{
		Brainstorm: tatarav1alpha1.BrainstormActivity{
			Enabled:          true,
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

	// Backdate LastBrainstorm well outside C2's
	// DefaultBrainstormMinSessionIntervalMinutes cooldown floor - this test
	// targets the budget gate, not the cooldown gate.
	past := metav1.NewTime(time.Now().Add(-30 * time.Minute))
	proj.Status.LastBrainstorm = &past
	if err := k8sClient.Status().Update(context.Background(), proj); err != nil {
		t.Fatalf("status update LastBrainstorm: %v", err)
	}
	// Re-fetch to get consistent ResourceVersion before passing to brainstorm.
	fresh := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: projName}, fresh); err != nil {
		t.Fatalf("get proj: %v", err)
	}
	proj = fresh

	// Add a repo so brainstorm has at least one valid slug.
	repo := mkScanRepo(t, projName, projName+"-br-repo", "https://github.com/o/nb.git")

	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/nb": {}, // 0 proposals -> under maxOpenProposals cap
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.brainstorm(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo}, nil, cron.Brainstorm)

	qes := listBrainstormQEs(t, projName)
	if len(qes) == 0 {
		t.Fatal("brainstorm event was not enqueued; budget gate still blocks creation")
	}
}
