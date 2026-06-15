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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func mkPRTask(repo string, pr int, lc string) tatarav1alpha1.Task {
	return tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{labelSourceRepo: sanitizeRepoLabel(repo)}},
		Spec: tatarav1alpha1.TaskSpec{
			Kind:   "issueLifecycle",
			Source: &tatarav1alpha1.TaskSource{Number: pr, IsPR: true},
		},
		Status: tatarav1alpha1.TaskStatus{LifecycleState: lc},
	}
}

func TestPriorTerminalAttempts_CountsTerminalPRTasks(t *testing.T) {
	existing := []tatarav1alpha1.Task{
		mkPRTask("o/r", 50, "Parked"),
		mkPRTask("o/r", 50, "Done"),
		mkPRTask("o/r", 50, "Implement"), // non-terminal: not counted
		mkPRTask("o/r", 51, "Parked"),    // different PR: not counted
		mkPRTask("o/x", 50, "Parked"),    // different repo: not counted
	}
	require.Equal(t, 2, priorTerminalAttempts(existing, "o/r", 50))
	require.Equal(t, 0, priorTerminalAttempts(existing, "o/r", 99))
}

func TestRecoveryBoundThreshold(t *testing.T) {
	require.Equal(t, 3, maxRecoveryAttempts)
}

// TestMRScanRecoveryExhaustedClosesPR asserts that when a bot PR has
// maxRecoveryAttempts (3) prior terminal tasks, mrScan calls ClosePR on the
// writer rather than silently skipping, and does NOT create a new adoption task.
func TestMRScanRecoveryExhaustedClosesPR(t *testing.T) {
	const projName = "recovery-close-proj"
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 2}}
	proj, repoObj := seedScanProject(t, projName, cron)

	// Fake writer capturing ClosePR calls.
	fw := &fullFakeSCMWriter{}

	// Fake reader returning one open bot PR (#50).
	reader := &fakeReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 50, Author: "tatara-bot", HeadSHA: "sha50", UpdatedAt: time.Unix(100, 0)},
	}}

	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	repos := []tatarav1alpha1.Repository{*repoObj}

	// 3 prior terminal tasks for (o/r, #50) — exhaustion threshold reached.
	existing := []tatarav1alpha1.Task{
		mkPRTask("o/r", 50, "Parked"),
		mkPRTask("o/r", 50, "Stopped"),
		mkPRTask("o/r", 50, "Done"),
	}

	budget := 99
	r.mrScan(context.Background(), proj, reader, repos, existing, cron.MRScan, &budget)

	// ClosePR must have been called with PR number 50.
	require.True(t, fw.closePRCalled, "expected ClosePR to be called for exhausted bot PR")
	require.Equal(t, 50, fw.closePRNumber, "ClosePR called with wrong PR number")

	// No new adoption task must have been created.
	tasks := listScanTasks(t, projName)
	for _, tk := range tasks {
		if tk.Spec.Source != nil && tk.Spec.Source.Number == 50 && tk.Spec.Kind == "issueLifecycle" {
			t.Fatalf("unexpected adoption task created for exhausted bot PR #50: %s", tk.Name)
		}
	}
}
