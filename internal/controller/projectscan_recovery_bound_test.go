package controller

import (
	"context"
	"strconv"
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
	// Phase 2: IssueRef added so taskMatchesItem can resolve the repo; the
	// LabelSourceRepo is kept for backward-compat (legacy-label fallback path).
	return tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"tatara.io/source-repo": sanitizeRepoLabel(repo)}},
		Spec: tatarav1alpha1.TaskSpec{
			Kind: "issueLifecycle",
			Source: &tatarav1alpha1.TaskSource{
				IssueRef: repo + "#" + strconv.Itoa(pr),
				Number:   pr,
				IsPR:     true,
			},
		},
		Status: tatarav1alpha1.TaskStatus{LifecycleState: lc},
	}
}

// mkPRTaskLabelless builds a terminal PR task in the Phase-1 shape: NO
// source-repo label, identity carried only by Spec.Source (IssueRef + Number).
// priorTerminalAttempts must count it via taskMatchesItem.
func mkPRTaskLabelless(repo string, pr int, lc string) tatarav1alpha1.Task {
	return tatarav1alpha1.Task{
		Spec: tatarav1alpha1.TaskSpec{
			Kind: "issueLifecycle",
			Source: &tatarav1alpha1.TaskSource{
				IssueRef: repo + "#" + strconv.Itoa(pr),
				Number:   pr,
				IsPR:     true,
			},
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

// TestPriorTerminalAttempts_CountsLabelLessTasks is the migration regression:
// new-generation terminal PR tasks carry no source-repo label and must still be
// counted via Spec.Source identity, else the recovery cap never trips and mrScan
// re-adopts an unfixable bot PR unboundedly.
func TestPriorTerminalAttempts_CountsLabelLessTasks(t *testing.T) {
	existing := []tatarav1alpha1.Task{
		mkPRTaskLabelless("o/r", 50, "Parked"),
		mkPRTaskLabelless("o/r", 50, "Stopped"),
		mkPRTaskLabelless("o/r", 50, "Implement"), // non-terminal: not counted
		mkPRTaskLabelless("o/r", 51, "Parked"),    // different PR: not counted
		mkPRTaskLabelless("o/x", 50, "Parked"),    // different repo: not counted
	}
	require.Equal(t, 2, priorTerminalAttempts(existing, "o/r", 50))
}

// mkIssueTask builds a terminal ISSUE-slot lifecycle task (IsPR=false). On
// GitLab issue #N and MR !N share no numbering space, so a terminal issue task
// whose Number equals a PR number must NOT be counted toward that PR's recovery
// budget. Spec.Source identity carries the slot via IsPR.
func mkIssueTask(repo string, num int, lc string) tatarav1alpha1.Task {
	return tatarav1alpha1.Task{
		Spec: tatarav1alpha1.TaskSpec{
			Kind: "issueLifecycle",
			Source: &tatarav1alpha1.TaskSource{
				IssueRef: repo + "#" + strconv.Itoa(num),
				Number:   num,
				IsPR:     false,
			},
		},
		Status: tatarav1alpha1.TaskStatus{LifecycleState: lc},
	}
}

// TestPriorTerminalAttempts_IgnoresIssueSlotSameNumber is the GitLab slot-leak
// regression: a terminal ISSUE task whose number equals the PR number must NOT
// inflate the PR's recovery-attempt count, or closeExhaustedPR can fire on a
// still-recoverable bot MR. Only PR-slot terminal tasks count.
func TestPriorTerminalAttempts_IgnoresIssueSlotSameNumber(t *testing.T) {
	existing := []tatarav1alpha1.Task{
		mkIssueTask("o/r", 50, "Parked"), // issue !50, terminal: must NOT count toward MR !50
		mkIssueTask("o/r", 50, "Done"),   // issue !50, terminal: must NOT count
		mkIssueTask("o/r", 50, "Stopped"),
	}
	require.Equal(t, 0, priorTerminalAttempts(existing, "o/r", 50),
		"terminal issue-slot tasks must not be counted toward a same-numbered PR's recovery cap")

	// Mixed: 1 PR-slot terminal + 3 issue-slot terminal at the same number ->
	// count must be 1 (issue tasks excluded), well under maxRecoveryAttempts.
	mixed := []tatarav1alpha1.Task{
		mkPRTask("o/r", 50, "Parked"),
		mkIssueTask("o/r", 50, "Done"),
		mkIssueTask("o/r", 50, "Stopped"),
		mkIssueTask("o/r", 50, "Parked"),
	}
	require.Equal(t, 1, priorTerminalAttempts(mixed, "o/r", 50),
		"only PR-slot terminal tasks count; issue-slot tasks at the same number are excluded")
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

	r.mrScan(context.Background(), proj, reader, repos, existing, cron.MRScan)

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
