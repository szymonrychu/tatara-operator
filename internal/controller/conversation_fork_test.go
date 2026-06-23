package controller

import (
	"context"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

func forkReconciler(s3 string) *TaskReconciler {
	return &TaskReconciler{
		Client:    k8sClient,
		Scheme:    k8sClient.Scheme(),
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		PodConfig: agent.PodConfig{Namespace: testNS, S3Bucket: s3},
	}
}

func mkProposalWithParentKey(t *testing.T, name, repo string, number int, parentKey string) {
	t.Helper()
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: testNS,
			Annotations: map[string]string{annParentConversationKey: parentKey},
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "demo", RepositoryRef: repo, Kind: "implement", Goal: "g",
			ProposedIssue: &tatarav1alpha1.ProposedIssueSpec{
				RepositoryRef: repo, Title: "T", Body: "B", Kind: "bug",
			},
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#" + strconv.Itoa(number), Number: number},
		},
	}
	require.NoError(t, k8sClient.Create(context.Background(), task))
}

func mkLifecycleIssueTask(t *testing.T, name, repo string, number int) *tatarav1alpha1.Task {
	t.Helper()
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "demo", RepositoryRef: repo, Kind: "issueLifecycle", Goal: "g",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#" + strconv.Itoa(number), Number: number},
		},
	}
	require.NoError(t, k8sClient.Create(context.Background(), task))
	return task
}

func TestMaybeSetupConversationFork_CorrelatesProposal(t *testing.T) {
	ctx := context.Background()
	r := forkReconciler("conv")
	mkProposalWithParentKey(t, "fork-prop-1", "cr-fork1", 321, "demo/task-brainstorm.jsonl")
	lc := mkLifecycleIssueTask(t, "fork-lc-1", "cr-fork1", 321)

	require.NoError(t, r.maybeSetupConversationFork(ctx, lc))
	got := getTask(t, "fork-lc-1")
	require.Equal(t, "demo/task-brainstorm.jsonl", got.Annotations[annForkFromConversationKey])
}

func TestMaybeSetupConversationFork_OffWhenNoS3(t *testing.T) {
	ctx := context.Background()
	r := forkReconciler("") // S3 disabled
	mkProposalWithParentKey(t, "fork-prop-2", "cr-fork2", 322, "demo/task-brainstorm.jsonl")
	lc := mkLifecycleIssueTask(t, "fork-lc-2", "cr-fork2", 322)

	require.NoError(t, r.maybeSetupConversationFork(ctx, lc))
	got := getTask(t, "fork-lc-2")
	require.Empty(t, got.Annotations[annForkFromConversationKey], "no fork pointer when S3 is off")
}

func TestMaybeSetupConversationFork_NoMatchingProposal(t *testing.T) {
	ctx := context.Background()
	r := forkReconciler("conv")
	lc := mkLifecycleIssueTask(t, "fork-lc-3", "cr-fork3", 999) // no proposal for 999

	require.NoError(t, r.maybeSetupConversationFork(ctx, lc))
	got := getTask(t, "fork-lc-3")
	require.Empty(t, got.Annotations[annForkFromConversationKey], "no fork pointer without a matching proposal")
}

func TestMaybeSetupConversationFork_SkipsWhenAlreadyOwnsConversation(t *testing.T) {
	ctx := context.Background()
	r := forkReconciler("conv")
	mkProposalWithParentKey(t, "fork-prop-4", "cr-fork4", 324, "demo/task-brainstorm.jsonl")
	lc := mkLifecycleIssueTask(t, "fork-lc-4", "cr-fork4", 324)
	// Already has its own conversation recorded -> must not fork.
	lc.Status.SessionID = "sid-existing"
	require.NoError(t, k8sClient.Status().Update(ctx, lc))

	require.NoError(t, r.maybeSetupConversationFork(ctx, getTask(t, "fork-lc-4")))
	got := getTask(t, "fork-lc-4")
	require.Empty(t, got.Annotations[annForkFromConversationKey], "must not fork once the issue owns a conversation")
}
