package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// seedProjectionTask creates an issueLifecycle Task with a role:proposed
// WorkItemRef (simulating a brainstorm-originated proposal) in state WIProposed.
func seedProjectionTask(t *testing.T, suffix string) (*TaskReconciler, *tatarav1alpha1.Task, *labelWriter) {
	t.Helper()
	ctx := context.Background()
	mkSecret(t, "proj-scm-"+suffix, map[string][]byte{"token": []byte("tok")})
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "proj-" + suffix, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "proj-scm-" + suffix,
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))
	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "proj-repo-" + suffix, Namespace: testNS},
		Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: proj.Name, URL: "https://github.com/o/r.git", DefaultBranch: "main", ReingestSchedule: "0 6 * * *"},
	}
	require.NoError(t, k8sClient.Create(ctx, repo))
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "proj-task-" + suffix, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj.Name,
			RepositoryRef: repo.Name,
			Kind:          "issueLifecycle",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github",
				IssueRef: "o/r#42",
				Number:   42,
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, task))
	// Seed with a role:proposed WorkItemRef.
	var fresh tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &fresh))
	fresh.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{
			Provider: "github",
			Repo:     "o/r",
			Number:   42,
			Kind:     tatarav1alpha1.WorkItemIssue,
			Role:     tatarav1alpha1.RoleProposed,
			State:    tatarav1alpha1.WIProposed,
			Title:    "test proposal",
		},
	}
	require.NoError(t, k8sClient.Status().Update(ctx, &fresh))
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &fresh))
	w := &labelWriter{}
	rdr := &labelReader{current: []string{"tatara-brainstorming"}}
	r := &TaskReconciler{
		Client:    k8sClient,
		Scheme:    k8sClient.Scheme(),
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:    func(string) (scm.SCMWriter, error) { return w, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return rdr, nil },
	}
	return r, &fresh, w
}

// TestSetLifecycleLabel_ApprovedUpsertProposedEntry verifies that calling
// setLifecycleLabel with tatara-approved also updates the role:proposed
// ledger entry State to WIApproved.
func TestSetLifecycleLabel_ApprovedUpsertProposedEntry(t *testing.T) {
	r, task, _ := seedProjectionTask(t, "approved")
	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))
	require.NoError(t, r.setLifecycleLabel(context.Background(), &proj, task, "tatara-approved"))

	// Reload and check ledger entry state.
	var updated tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &updated))
	require.NotEmpty(t, updated.Status.WorkItems, "WorkItems must be non-empty")
	found := false
	for _, wi := range updated.Status.WorkItems {
		if wi.Role == tatarav1alpha1.RoleProposed && wi.Repo == "o/r" && wi.Number == 42 {
			require.Equal(t, tatarav1alpha1.WIApproved, wi.State, "role:proposed entry must be WIApproved after tatara-approved label")
			found = true
		}
	}
	require.True(t, found, "role:proposed entry for o/r#42 must exist")
}

// TestSetLifecycleLabel_DeclinedUpsertProposedEntry verifies that setting
// tatara-declined also updates the role:proposed entry to WIDeclined.
func TestSetLifecycleLabel_DeclinedUpsertProposedEntry(t *testing.T) {
	r, task, _ := seedProjectionTask(t, "declined")
	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))
	require.NoError(t, r.setLifecycleLabel(context.Background(), &proj, task, "tatara-declined"))

	var updated tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &updated))
	for _, wi := range updated.Status.WorkItems {
		if wi.Role == tatarav1alpha1.RoleProposed && wi.Repo == "o/r" && wi.Number == 42 {
			require.Equal(t, tatarav1alpha1.WIDeclined, wi.State, "role:proposed entry must be WIDeclined after tatara-declined label")
			return
		}
	}
	t.Fatal("role:proposed entry for o/r#42 not found")
}

// TestSetLifecycleLabel_BrainstormingMapsToProposed verifies the actual mapping:
// an entry seeded in WIApproved is moved BACK to WIProposed when the issue is
// relabeled tatara-brainstorming. This exercises the changed-state path of
// upsertProposedEntryState, not just the seeded value.
func TestSetLifecycleLabel_BrainstormingMapsToProposed(t *testing.T) {
	r, task, _ := seedProjectionTask(t, "bs-map")
	ctx := context.Background()
	// Seed the entry in WIApproved first.
	var seeded tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &seeded))
	for i := range seeded.Status.WorkItems {
		if seeded.Status.WorkItems[i].Role == tatarav1alpha1.RoleProposed {
			seeded.Status.WorkItems[i].State = tatarav1alpha1.WIApproved
		}
	}
	require.NoError(t, k8sClient.Status().Update(ctx, &seeded))

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))
	require.NoError(t, r.setLifecycleLabel(ctx, &proj, &seeded, "tatara-brainstorming"))

	var updated tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &updated))
	for _, wi := range updated.Status.WorkItems {
		if wi.Role == tatarav1alpha1.RoleProposed && wi.Repo == "o/r" && wi.Number == 42 {
			require.Equal(t, tatarav1alpha1.WIProposed, wi.State, "brainstorming label must map WIApproved -> WIProposed")
			return
		}
	}
	t.Fatal("role:proposed entry for o/r#42 not found")
}

// observeLabelReader simulates a case where the issue has been relabeled by a
// human (tatara-approved or tatara-declined) since the last reconcile.
type observeLabelReader struct {
	fakeProposalReader
	labels []string
}

func (r *observeLabelReader) ListOpenIssues(_ context.Context, _, _ string) ([]scm.IssueRef, error) {
	return []scm.IssueRef{{Repo: "o/r", Number: 42, Labels: r.labels}}, nil
}

// TestObserveHumanDeclinedLabel verifies that when the issue now carries
// tatara-declined (set by a human), reconcileLifecycle reflects this back to the
// role:proposed ledger entry (State=WIDeclined) and parks the task with the
// "human-declined" reason.
func TestObserveHumanDeclinedLabel(t *testing.T) {
	ctx := context.Background()
	// Seed task with a role:proposed entry in WIProposed state, in Conversation state
	// (awaiting approval).
	r, task, w := seedProjectionTask(t, "obs-declined")
	setProjectMemoryReady(t, task.Spec.ProjectRef, "http://mem-obs-declined.tatara.svc:8080")

	// Change reader to return tatara-declined on the issue.
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &observeLabelReader{labels: []string{"tatara-declined"}}, nil
	}
	_ = w

	// Set task to Conversation state (awaiting human approval).
	var fresh tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &fresh))
	fresh.Status.LifecycleState = "Conversation"
	now := metav1.Now()
	fresh.Status.LastActivityAt = &now
	future := metav1.NewTime(now.Add(1e9)) // far future deadline (not passed)
	fresh.Status.DeadlineAt = &future
	require.NoError(t, k8sClient.Status().Update(ctx, &fresh))

	// reconcileLifecycle should observe tatara-declined and update the ledger entry.
	_, err := r.reconcileLifecycle(ctx, getTaskByName(t, task.Name))
	require.NoError(t, err)

	var updated tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &updated))
	require.Equal(t, "Parked", updated.Status.LifecycleState, "declined readback must park the task")
	require.Equal(t, "human-declined", updated.Status.ParkReason, "park reason must be human-declined")
	found := false
	for _, wi := range updated.Status.WorkItems {
		if wi.Role == tatarav1alpha1.RoleProposed && wi.Repo == "o/r" && wi.Number == 42 {
			require.Equal(t, tatarav1alpha1.WIDeclined, wi.State, "role:proposed entry must be WIDeclined after human relabel")
			found = true
		}
	}
	require.True(t, found, "role:proposed entry for o/r#42 not found or not updated to declined")
}

// TestObserveHumanApprovedLabel verifies that when the issue carries
// tatara-approved, reconcileLifecycle updates the role:proposed entry to
// WIApproved and transitions to Implement.
func TestObserveHumanApprovedLabel(t *testing.T) {
	ctx := context.Background()
	r, task, _ := seedProjectionTask(t, "obs-approved")
	setProjectMemoryReady(t, task.Spec.ProjectRef, "http://mem-obs-approved.tatara.svc:8080")

	// Issue now has tatara-approved (human relabeled).
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &observeLabelReader{labels: []string{"tatara-approved"}}, nil
	}

	// Set task to Conversation state (awaiting human approval).
	var fresh tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &fresh))
	fresh.Status.LifecycleState = "Conversation"
	now := metav1.Now()
	fresh.Status.LastActivityAt = &now
	future := metav1.NewTime(now.Add(1e9)) // far future deadline
	fresh.Status.DeadlineAt = &future
	require.NoError(t, k8sClient.Status().Update(ctx, &fresh))

	_, err := r.reconcileLifecycle(ctx, getTaskByName(t, task.Name))
	require.NoError(t, err)

	var updated tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &updated))
	require.Equal(t, "Implement", updated.Status.LifecycleState, "approved readback must drive the task to Implement")
	found := false
	for _, wi := range updated.Status.WorkItems {
		if wi.Role == tatarav1alpha1.RoleProposed && wi.Repo == "o/r" && wi.Number == 42 {
			require.Equal(t, tatarav1alpha1.WIApproved, wi.State, "role:proposed entry must be WIApproved after human approval")
			found = true
		}
	}
	require.True(t, found, "role:proposed entry for o/r#42 not found or not updated to approved")
}
