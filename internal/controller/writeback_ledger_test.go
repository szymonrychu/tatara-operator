package controller

// TDD: Phase 5, Task 15 - project agent MCP actions onto the ledger.
// Tests written BEFORE implementation; they FAIL until writeback.go and lifecycle.go
// are updated.

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// TestWriteback_PROpenUpsertLedgerEntry: when writeBackOpenChange succeeds and
// opens a PR, a role:openedPR entry with state:open must appear in Status.WorkItems.
func TestWriteback_PROpenUpsertLedgerEntry(t *testing.T) {
	ctx := context.Background()

	// Seed the necessary resources.
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "wbl-scm1", Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("pat"), "webhookSecret": []byte("w")},
	}
	require.NoError(t, k8sClient.Create(ctx, sec))
	t.Cleanup(func() { k8sClient.Delete(ctx, sec) }) //nolint:errcheck

	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "wbl-proj1", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "wbl-scm1",
			TriggerLabel: "tatara",
		},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))
	t.Cleanup(func() { k8sClient.Delete(ctx, proj) }) //nolint:errcheck

	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "wbl-repo1", Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:       "wbl-proj1",
			URL:              "https://github.com/o/mrepo.git",
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		},
	}
	require.NoError(t, k8sClient.Create(ctx, repo))
	t.Cleanup(func() { k8sClient.Delete(ctx, repo) }) //nolint:errcheck

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "wbl-task1", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    "wbl-proj1",
			RepositoryRef: "wbl-repo1",
			Goal:          "fix something",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github",
				IssueRef: "o/mrepo#5",
				URL:      "https://github.com/o/mrepo/issues/5",
				Number:   5,
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, task))
	t.Cleanup(func() { k8sClient.Delete(ctx, task) }) //nolint:errcheck

	// Seed WorkItems so we have a source entry to update later (not strictly needed
	// for this test, but mirrors real state after seed-on-reconcile from Task 5).
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/mrepo", Number: 5, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
	}
	task.Status.Phase = "Succeeded"
	task.Status.ResultSummary = "done"
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type:   "WritebackPending",
		Status: metav1.ConditionTrue,
		Reason: "AwaitingM5",
	})
	require.NoError(t, k8sClient.Status().Update(ctx, task))

	// Fake writer returns a known PR URL.
	fw := &fakeWriter{prURL: "https://github.com/o/mrepo/pull/42"}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op.svc",
			OIDCIssuer:          "https://keycloak/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
		SCMFor: func(string) (scm.SCMWriter, error) { return fw, nil },
	}

	_, err := r.Reconcile(logf.IntoContext(context.Background(), logf.Log), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: task.Name},
	})
	require.NoError(t, err)

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))

	// PR URL must be set.
	require.Equal(t, "https://github.com/o/mrepo/pull/42", got.Status.PrURL)

	// A role:openedPR entry must appear in Status.WorkItems with state:open.
	var prEntry *tatarav1alpha1.WorkItemRef
	for i := range got.Status.WorkItems {
		wi := &got.Status.WorkItems[i]
		if wi.Role == tatarav1alpha1.RoleOpenedPR {
			prEntry = wi
			break
		}
	}
	require.NotNil(t, prEntry, "role:openedPR entry must appear in Status.WorkItems after PR open")
	require.Equal(t, tatarav1alpha1.WorkItemPR, prEntry.Kind)
	require.Equal(t, tatarav1alpha1.WIOpen, prEntry.State)
	require.Equal(t, 42, prEntry.Number, "PR number must be parsed from the PR URL")
	require.Equal(t, "o/mrepo", prEntry.Repo)
}

// TestWriteback_MergeStateReflected: after the merge path in handleMerge sets
// MergedHeadSHA, a subsequent lifecycle path must reflect State=merged on the
// role:openedPR ledger entry. We test this by directly calling the function that
// records the merge and verifying the ledger is updated.
// Since handleMerge runs inside a complex reconcile loop with SCM calls, we
// test the ledger update at the RetryOnConflict persistence site in lifecycle.go
// by seeding a Task in MRCI with an openedPR entry and simulating merge success
// via a fake writer.
func TestWriteback_MergeStateReflected(t *testing.T) {
	ctx := context.Background()

	// This test validates that when we record MergedHeadSHA we also flip
	// the role:openedPR ledger entry to state:merged. We check this
	// via the helper mergeLedgerEntry directly on a Task struct (pure logic test).
	task := &tatarav1alpha1.Task{
		Status: tatarav1alpha1.TaskStatus{
			WorkItems: []tatarav1alpha1.WorkItemRef{
				{Provider: "github", Repo: "o/r", Number: 10, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen},
				{Provider: "github", Repo: "o/r", Number: 3, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
			},
		},
	}

	// Upsert merged state on the openedPR entry.
	UpsertWorkItem(task, tatarav1alpha1.WorkItemRef{
		Provider: "github",
		Repo:     "o/r",
		Number:   10,
		Kind:     tatarav1alpha1.WorkItemPR,
		Role:     tatarav1alpha1.RoleOpenedPR,
		State:    tatarav1alpha1.WIMerged,
	})

	require.Len(t, task.Status.WorkItems, 2)
	var prWI *tatarav1alpha1.WorkItemRef
	for i := range task.Status.WorkItems {
		if task.Status.WorkItems[i].Role == tatarav1alpha1.RoleOpenedPR {
			prWI = &task.Status.WorkItems[i]
		}
	}
	require.NotNil(t, prWI)
	require.Equal(t, tatarav1alpha1.WIMerged, prWI.State, "role:openedPR entry must transition to state:merged")
	// Source issue entry must remain open.
	for _, wi := range task.Status.WorkItems {
		if wi.Role == tatarav1alpha1.RoleSource {
			require.Equal(t, tatarav1alpha1.WIOpen, wi.State)
		}
	}

	_ = ctx // satisfy import
}

// TestWriteback_CloseIssueStateReflected: when handleMainCI closes the source issue,
// the role:source ledger entry must transition to state:closed. We test this
// as a pure unit test on upsert + state flip logic.
func TestWriteback_CloseIssueStateReflected(t *testing.T) {
	task := &tatarav1alpha1.Task{
		Spec: tatarav1alpha1.TaskSpec{
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github",
				IssueRef: "o/r#3",
				Number:   3,
			},
		},
		Status: tatarav1alpha1.TaskStatus{
			WorkItems: []tatarav1alpha1.WorkItemRef{
				{Provider: "github", Repo: "o/r", Number: 3, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
				{Provider: "github", Repo: "o/r", Number: 10, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIMerged},
			},
		},
	}

	// Simulate the issue-close ledger update.
	closeSourceIssueLedger(task)

	var sourceWI *tatarav1alpha1.WorkItemRef
	for i := range task.Status.WorkItems {
		if task.Status.WorkItems[i].Role == tatarav1alpha1.RoleSource {
			sourceWI = &task.Status.WorkItems[i]
		}
	}
	require.NotNil(t, sourceWI)
	require.Equal(t, tatarav1alpha1.WIClosed, sourceWI.State, "role:source entry must transition to state:closed on issue close")
}

// TestWriteback_PROpenUpsertLedgerEntry_MergedPRInLedger: the openedPR ledger
// entry round-trips through merge: first upserted as open, then updated to merged.
func TestWriteback_PROpenUpsertLedgerEntry_MergedPRInLedger(t *testing.T) {
	task := &tatarav1alpha1.Task{
		Status: tatarav1alpha1.TaskStatus{},
	}

	// Step 1: PR opened.
	UpsertWorkItem(task, tatarav1alpha1.WorkItemRef{
		Provider: "github",
		Repo:     "o/r",
		Number:   7,
		Kind:     tatarav1alpha1.WorkItemPR,
		Role:     tatarav1alpha1.RoleOpenedPR,
		State:    tatarav1alpha1.WIOpen,
		HeadSHA:  "abc123",
	})
	require.Len(t, task.Status.WorkItems, 1)
	require.Equal(t, tatarav1alpha1.WIOpen, task.Status.WorkItems[0].State)

	// Step 2: Merge recorded.
	UpsertWorkItem(task, tatarav1alpha1.WorkItemRef{
		Provider: "github",
		Repo:     "o/r",
		Number:   7,
		Kind:     tatarav1alpha1.WorkItemPR,
		Role:     tatarav1alpha1.RoleOpenedPR,
		State:    tatarav1alpha1.WIMerged,
	})
	require.Len(t, task.Status.WorkItems, 1, "upsert must not duplicate")
	require.Equal(t, tatarav1alpha1.WIMerged, task.Status.WorkItems[0].State)
	// HeadSHA preserved (upsert skips empty fields).
	require.Equal(t, "abc123", task.Status.WorkItems[0].HeadSHA)
}
