package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// TestReconcileDeploying_HelmfileSelfTarget_ResolvesOnApply: a Deploying Task
// whose target repo IS tatara-helmfile (a GoalTierRevert incident's revert MR)
// cuts NO semver tag, so tag+pin resolution would deadlock. It must instead
// resolve on the tatara-helmfile apply.yaml success whose head is this change's
// merge commit - closing the issue and marking Done without ever waiting on a tag.
func TestReconcileDeploying_HelmfileSelfTarget_ResolvesOnApply(t *testing.T) {
	proj := seedDeployScene(t, "hfself", "tatara-operator")
	// RepositoryRef points at the helmfile repo (dep-hf-hfself), Kind issueLifecycle
	// (scalar path). MergeCommitSHA is the merge commit on tatara-helmfile main.
	task := seedDeployingTask(t, "dep-hfself", proj.Name, "dep-hf-hfself", "szymonrychu/tatara-helmfile#9",
		time.Now().Add(30*time.Minute), "")

	fw := &deployFakeWriter{}
	rd := &deployFakeReader{
		// tatara-helmfile cuts no tag - the resolver must NOT depend on this.
		tagFound: false,
		run:      scm.WorkflowRun{HeadSHA: "abcdef1234567", Status: "completed", Conclusion: "success", HTMLURL: "https://run/hf"},
		runFound: true,
	}
	r := newDeployReconciler(fw, rd)

	_, err := r.reconcileDeploying(deployCtx(), proj, getTask(t, task.Name))
	require.NoError(t, err)

	got := getTask(t, task.Name)
	require.Equal(t, "Done", got.Status.DeployState, "helmfile self-target resolves Done on apply success")
	require.Equal(t, "", got.Status.Phase)
	require.True(t, tatarav1alpha1.TaskTerminal(got))
	require.Len(t, fw.closeCalls, 1, "issue closed on apply")
	require.Contains(t, fw.closeCalls[0], "szymonrychu/tatara-helmfile|9|")
	require.Contains(t, fw.closeCalls[0], "tatara-helmfile@abcdef1")
}

// TestReconcileDeploying_HelmfileSelfTarget_WaitsForOurApply: a successful apply
// whose head is NOT this change's merge commit (an older apply predating our
// merge) must not resolve the Task - it stays Deploying.
func TestReconcileDeploying_HelmfileSelfTarget_WaitsForOurApply(t *testing.T) {
	proj := seedDeployScene(t, "hfwait", "tatara-operator")
	task := seedDeployingTask(t, "dep-hfwait", proj.Name, "dep-hf-hfwait", "szymonrychu/tatara-helmfile#9",
		time.Now().Add(30*time.Minute), "")

	fw := &deployFakeWriter{}
	rd := &deployFakeReader{
		tagFound: false,
		run:      scm.WorkflowRun{HeadSHA: "OLDER0000", Status: "completed", Conclusion: "success"},
		runFound: true,
	}
	r := newDeployReconciler(fw, rd)

	res, err := r.reconcileDeploying(deployCtx(), proj, getTask(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, deployPollRequeue, res.RequeueAfter)
	require.Equal(t, tatarav1alpha1.DeployStateDeploying, getTask(t, task.Name).Status.DeployState)
	require.Empty(t, fw.closeCalls)
}

// TestReconcileDeploying_HelmfileSelfTarget_RerollsOnApplyFailure: our revert MR's
// apply (head == merge commit) failing rerolls the change to fix the cascade.
func TestReconcileDeploying_HelmfileSelfTarget_RerollsOnApplyFailure(t *testing.T) {
	proj := seedDeployScene(t, "hffail", "tatara-operator")
	task := seedDeployingTask(t, "dep-hffail", proj.Name, "dep-hf-hffail", "szymonrychu/tatara-helmfile#9",
		time.Now().Add(30*time.Minute), "")

	fw := &deployFakeWriter{}
	rd := &deployFakeReader{
		tagFound: false,
		run:      scm.WorkflowRun{HeadSHA: "abcdef1234567", Status: "completed", Conclusion: "failure", HTMLURL: "https://run/hffail"},
		runFound: true,
	}
	r := newDeployReconciler(fw, rd)

	_, err := r.reconcileDeploying(deployCtx(), proj, getTask(t, task.Name))
	require.NoError(t, err)
	got := getTask(t, task.Name)
	require.Equal(t, "Implement", got.Status.DeployState, "failed apply of our revert rerolls to Implement")
	require.Empty(t, fw.closeCalls)
}

// TestUmbrella_HelmfileMember_ConfirmsOnApply: a discrete-implement umbrella whose
// member is tatara-helmfile (no cut tag) must confirm that member off the apply
// outcome, not a semver tag, so the umbrella does not deadlock waiting on a tag.
func TestUmbrella_HelmfileMember_ConfirmsOnApply(t *testing.T) {
	proj := seedUmbrellaScene(t, "hfmember", "tatara-operator")
	task := seedDeployingUmbrella(t, proj, "umb-hfmember", map[string]int{
		"szymonrychu/tatara-helmfile": 77,
	}, time.Now().Add(30*time.Minute))

	w := &umbWriter{}
	rd := &umbReader{
		tags:     map[string]string{}, // tatara-helmfile cuts no tag
		run:      scm.WorkflowRun{HeadSHA: "hfapply1", Status: "completed", Conclusion: "success", HTMLURL: "https://run/hf"},
		runFound: true,
	}
	r := newUmbTaskReconciler(w, rd)

	_, err := r.reconcileDeploying(deployCtx(), proj, getTask(t, task.Name))
	require.NoError(t, err)

	got := getTask(t, task.Name)
	require.Equal(t, "applied", umbMember(got, "szymonrychu/tatara-helmfile").DeployState, "helmfile member confirmed off apply")
	require.Equal(t, "Done", got.Status.DeployState, "sole helmfile member applied: umbrella resolves Done")
	require.Len(t, w.closeCalls, 1, "issue closes once the helmfile member is confirmed")
}
