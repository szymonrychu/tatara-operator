package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// Phase 8 item 3: superviseMergedPRs drives a merged approved bot PR's umbrella
// implement Task into pod-less Deploying (so the cascade is supervised + the issue
// closes on apply), WITHOUT calling Merge (single merge egress preserved) and
// guarded against double-deploy.

// seedImplementUmbrella creates an implement Task that opened PR #42 for issue
// o/r#5 with a declared change significance (push-CD eligible).
func seedImplementUmbrella(t *testing.T, proj *tatarav1alpha1.Project, name string, significance string) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()
	var repos tatarav1alpha1.RepositoryList
	require.NoError(t, k8sClient.List(ctx, &repos))
	repoName := ""
	for i := range repos.Items {
		if repos.Items[i].Spec.ProjectRef == proj.Name {
			repoName = repos.Items[i].Name
			break
		}
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj.Name, RepositoryRef: repoName, Kind: "implement",
			Goal:   "Issue #5: fix it",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5, IsPR: false},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, task))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), task) })
	task.Status.Phase = "Succeeded"
	task.Status.PRNumber = 42
	if significance != "" {
		task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{Significance: significance}
	}
	require.NoError(t, k8sClient.Status().Update(ctx, task))
	return task
}

func TestSuperviseMergedPRs_EntersDeployingOnMergedImplement(t *testing.T) {
	proj := seedMergeGateScene(t, "md-ok")
	var repos tatarav1alpha1.RepositoryList
	require.NoError(t, k8sClient.List(context.Background(), &repos))
	seedImplementUmbrella(t, proj, "impl-md-ok", "patch")
	fw := &mergeGateWriter{prState: scm.PRState{Merged: true, CIStatus: "success"}}
	r := newMergeGateReconciler(fw, &mergeGateReader{})

	r.superviseMergedPRs(context.Background(), proj, filterRepos(repos.Items, proj.Name))

	require.False(t, fw.mergeCalled, "sweep must NOT call Merge (single merge egress preserved)")
	got := getTaskByName(t, "impl-md-ok")
	require.True(t, tatarav1alpha1.TaskDeploying(got), "merged implement Task must enter Deploying")
	require.Equal(t, tatarav1alpha1.DeployStateDeploying, got.Status.DeployState)
	require.NotNil(t, got.Status.DeployDeadline, "Deploying entry must stamp a deploy deadline")
}

func TestSuperviseMergedPRs_SkipsUnmergedPR(t *testing.T) {
	proj := seedMergeGateScene(t, "md-unmerged")
	var repos tatarav1alpha1.RepositoryList
	require.NoError(t, k8sClient.List(context.Background(), &repos))
	seedImplementUmbrella(t, proj, "impl-md-unmerged", "patch")
	fw := &mergeGateWriter{prState: scm.PRState{Merged: false, CIStatus: "success"}}
	r := newMergeGateReconciler(fw, &mergeGateReader{})

	r.superviseMergedPRs(context.Background(), proj, filterRepos(repos.Items, proj.Name))

	got := getTaskByName(t, "impl-md-unmerged")
	require.False(t, tatarav1alpha1.TaskDeploying(got), "unmerged PR must not enter Deploying")
}

func TestSuperviseMergedPRs_SkipsNonPushCD(t *testing.T) {
	proj := seedMergeGateScene(t, "md-nocd")
	var repos tatarav1alpha1.RepositoryList
	require.NoError(t, k8sClient.List(context.Background(), &repos))
	seedImplementUmbrella(t, proj, "impl-md-nocd", "") // no declared significance
	fw := &mergeGateWriter{prState: scm.PRState{Merged: true, CIStatus: "success"}}
	r := newMergeGateReconciler(fw, &mergeGateReader{})

	r.superviseMergedPRs(context.Background(), proj, filterRepos(repos.Items, proj.Name))

	got := getTaskByName(t, "impl-md-nocd")
	require.False(t, tatarav1alpha1.TaskDeploying(got), "a change with no declared significance must not deploy")
}

// The issueLifecycle bridge (handleMainCI) still enters Deploying for its own
// Tasks; the sweep must skip non-implement kinds to avoid a double-deploy.
func TestSuperviseMergedPRs_SkipsNonImplementKind(t *testing.T) {
	proj := seedMergeGateScene(t, "md-kind")
	var repos tatarav1alpha1.RepositoryList
	require.NoError(t, k8sClient.List(context.Background(), &repos))
	task := seedImplementUmbrella(t, proj, "impl-md-kind", "patch")
	task.Spec.Kind = "issueLifecycle"
	require.NoError(t, k8sClient.Update(context.Background(), task))
	fw := &mergeGateWriter{prState: scm.PRState{Merged: true, CIStatus: "success"}}
	r := newMergeGateReconciler(fw, &mergeGateReader{})

	r.superviseMergedPRs(context.Background(), proj, filterRepos(repos.Items, proj.Name))

	got := getTaskByName(t, "impl-md-kind")
	require.False(t, tatarav1alpha1.TaskDeploying(got), "issueLifecycle Tasks are owned by the bridge, not this sweep")
}

// Double-deploy fence: once a Task is Deploying (in the ledger), a second sweep
// must not re-drive it.
func TestSuperviseMergedPRs_IdempotentAfterEntry(t *testing.T) {
	proj := seedMergeGateScene(t, "md-idem")
	var repos tatarav1alpha1.RepositoryList
	require.NoError(t, k8sClient.List(context.Background(), &repos))
	seedImplementUmbrella(t, proj, "impl-md-idem", "patch")
	fw := &mergeGateWriter{prState: scm.PRState{Merged: true, CIStatus: "success"}}
	r := newMergeGateReconciler(fw, &mergeGateReader{})
	scoped := filterRepos(repos.Items, proj.Name)

	r.superviseMergedPRs(context.Background(), proj, scoped)
	first := getTaskByName(t, "impl-md-idem")
	require.True(t, tatarav1alpha1.TaskDeploying(first))
	firstDeadline := first.Status.DeployDeadline

	// Second sweep: TaskDeploying + ledger guard both skip it; deadline unchanged.
	r.superviseMergedPRs(context.Background(), proj, scoped)
	second := getTaskByName(t, "impl-md-idem")
	require.Equal(t, firstDeadline, second.Status.DeployDeadline, "must not re-drive an already-deploying Task")
}
