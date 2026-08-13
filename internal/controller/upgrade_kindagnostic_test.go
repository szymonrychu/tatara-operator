package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// THE PATHS THIS FILE PINS ARE NOT ALLOWED TO GROW A KIND. AdoptPR, ClassifyPR,
// taskForBranch, agent.TaskBranch, the merge driver and the deploy driver are
// already kind-agnostic, and the upgrade design depends on their staying that
// way. Every test here passes with NO production change; they exist so a future
// kind-specific "fix" to one of them breaks a test instead of the platform.

// THE WHOLE "does an upgrade MR mint a spurious review Task" QUESTION, SETTLED
// BY TEST.
//
// AdoptPR keys on (bot author, agent.TaskBranch(task), same repo) and NEVER on
// spec.Kind, so an upgrade Task's own merge request adopts INTO that Task -
// which is what puts it in front of the review agent on the same Task, with no
// second Task anywhere. ClassifyPR clause 2 then sends any bot-authored,
// non-adoptable merge request to PRIgnore, so PRReview is unreachable for one of
// ours in both directions. A stray review Task for our own MR is the exact bug
// this design exists to avoid.
func TestClassifyPR_UpgradeMRAdoptsAndNeverMintsAReviewTask(t *testing.T) {
	proj := sweepProject("upg-proj")
	repo := sweepRepo("upg-proj")
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "upgrade-abc123", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "upgrade", Goal: "g"},
	}
	pr := scm.PRRef{
		Repo: "szymonrychu/tatara-operator", HeadRepo: "szymonrychu/tatara-operator",
		Number: 11, Author: "tatara-bot", HeadBranch: agent.TaskBranch(task),
	}

	require.True(t, AdoptPR(proj, task, pr),
		"AdoptPR keys on the deterministic task branch, never on spec.kind")
	require.Equal(t, PRAdopt, ClassifyPR(proj, repo, pr, task, ""),
		"an upgrade Task's own merge request adopts into that Task")

	// The same merge request with NO owning task: bot-authored and
	// non-adoptable, so ignored - never reviewed, never minting a review Task.
	require.Equal(t, PRIgnore, ClassifyPR(proj, repo, pr, nil, ""),
		"a bot-authored merge request the sweep cannot adopt is IGNORED; PRReview is for humans' PRs only")
}

// taskForBranch resolves a branch by scanning the project's Tasks for one whose
// agent.TaskBranch matches. A cron-minted upgrade Task carries no Source.Number,
// so its branch is the tatara/task-<name> fallback form - the same form every
// numberless kind uses, and the one the sweep matches back to the Task.
func TestTaskForBranch_MatchesAnUpgradeTaskByItsDeterministicBranch(t *testing.T) {
	ctx := context.Background()
	proj := sweepProject("upg-branch-proj")
	upg := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "upg-branch-proj-upgrade-abc123", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "upgrade"},
	}
	c := newMirrorClient(t, proj, upg)
	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}

	require.Equal(t, "tatara/task-upg-branch-proj-upgrade-abc123", agent.TaskBranch(upg))

	got, err := r.taskForBranch(ctx, proj, agent.TaskBranch(upg), "")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, upg.Name, got.Name)
}
