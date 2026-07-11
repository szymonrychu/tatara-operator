package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// TestRerollDeploy_ClearsMemberDeployState is the M2 regression: rerollDeploy's
// reroll-to-Implement branch resets the scalar cascade fields (clearDeployState)
// but, before this fix, left each openedPR member's per-member WorkItemRef.
// DeployState/DeployedVersion untouched. On re-entry after the reroll, the learn
// loop in reconcileDeployingUmbrella skips a member whose DeployedVersion is
// still set (`m.DeployedVersion != ""`), and the confirm loop checks that STALE
// version against the NEW pin state, which never matches - the member can never
// re-confirm, so a successful redeploy parks instead of resolving. rerollDeploy
// must reset every openedPR member's DeployState/DeployedVersion to empty so the
// umbrella re-learns and re-confirms from scratch after a reroll.
func TestRerollDeploy_ClearsMemberDeployState(t *testing.T) {
	proj := seedDeployScene(t, "rerollmember", "tatara-operator")
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "dep-rerollmember", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj.Name, RepositoryRef: "dep-comp-rerollmember", Kind: "implement",
			Goal:   "ship it",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "szymonrychu/tatara-operator#7", Number: 7},
		},
	}
	require.NoError(t, k8sClient.Create(context.Background(), task))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), task) })
	task.Status.Phase = tatarav1alpha1.PhaseDeploying
	task.Status.DeployState = tatarav1alpha1.DeployStateDeploying
	task.Status.ImplementGiveUps = 0
	// Seed two openedPR members: one already learned a stale cut version
	// (deploying), one already confirmed applied - both must reset on reroll.
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{
			Provider: "github", Repo: "o/comp-a", Number: 42,
			Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR,
			State: tatarav1alpha1.WIMerged, DeployedVersion: "v1.0.0", DeployState: memberDeployStateDeploying,
		},
		{
			Provider: "github", Repo: "o/comp-b", Number: 43,
			Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR,
			State: tatarav1alpha1.WIMerged, DeployedVersion: "v2.0.0", DeployState: memberDeployStateApplied,
		},
	}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	r := &TaskReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	_, err := r.rerollDeploy(deployCtx(), proj, task, "deploy_timeout", "stuck member")
	require.NoError(t, err)

	got := getTask(t, "dep-rerollmember")
	require.Equal(t, "Implement", got.Status.DeployState, "reroll must re-enter Implement")
	require.Equal(t, 1, got.Status.ImplementGiveUps, "reroll must consume one auto-reroll attempt")
	for _, wi := range got.Status.WorkItems {
		require.Empty(t, wi.DeployState, "member %s/%d DeployState must reset on reroll so it re-learns", wi.Repo, wi.Number)
		require.Empty(t, wi.DeployedVersion, "member %s/%d DeployedVersion must reset on reroll so it re-confirms", wi.Repo, wi.Number)
	}
}

// TestRerollDeploy_ExhaustedParkLeavesMemberStateUntouched: the park-exhausted
// branch (bumpGiveup=false) terminates the umbrella - it does not re-enter
// Implement, so member deploy state is irrelevant there and must NOT be reset
// (keeping the two branches' side effects scoped to what each actually needs).
func TestRerollDeploy_ExhaustedParkLeavesMemberStateUntouched(t *testing.T) {
	proj := seedDeployScene(t, "rerollparkmember", "tatara-operator")
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "dep-rerollparkmember", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj.Name, RepositoryRef: "dep-comp-rerollparkmember", Kind: "implement",
			Goal:   "ship it",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "szymonrychu/tatara-operator#7", Number: 7},
		},
	}
	require.NoError(t, k8sClient.Create(context.Background(), task))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), task) })
	task.Status.Phase = tatarav1alpha1.PhaseDeploying
	task.Status.DeployState = tatarav1alpha1.DeployStateDeploying
	task.Status.ImplementGiveUps = maxImplGiveUps
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{
			Provider: "github", Repo: "o/comp-a", Number: 42,
			Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR,
			State: tatarav1alpha1.WIMerged, DeployedVersion: "v1.0.0", DeployState: memberDeployStateDeploying,
		},
	}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	r := &TaskReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	_, err := r.rerollDeploy(deployCtx(), proj, task, "deploy_timeout", "stuck member")
	require.NoError(t, err)

	got := getTask(t, "dep-rerollparkmember")
	require.Equal(t, "Parked", got.Status.DeployState, "exhausted budget must park, not reroll")
	require.Equal(t, "v1.0.0", got.Status.WorkItems[0].DeployedVersion, "park branch does not re-enter Implement; member state is irrelevant and left untouched")
}
