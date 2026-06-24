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

// seedBotProposalLifecycleTask creates an issueLifecycle Task for a bot-authored
// proposal issue with NO ledger entries (the real production shape: the issue
// flows through the normal lifecycle after createProposal, and seedLedgerFromSpec
// only mints a role:source entry - never role:proposed).
func seedBotProposalLifecycleTask(t *testing.T, suffix string, authorLogin string) (*TaskReconciler, *tatarav1alpha1.Task, *tatarav1alpha1.Project) {
	t.Helper()
	ctx := context.Background()
	mkSecret(t, "prod-scm-"+suffix, map[string][]byte{"token": []byte("tok")})
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "prod-proj-" + suffix, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "prod-scm-" + suffix,
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))
	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "prod-repo-" + suffix, Namespace: testNS},
		Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: proj.Name, URL: "https://github.com/o/r.git", DefaultBranch: "main", ReingestSchedule: "0 6 * * *"},
	}
	require.NoError(t, k8sClient.Create(ctx, repo))
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "prod-task-" + suffix, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj.Name,
			RepositoryRef: repo.Name,
			Kind:          "issueLifecycle",
			Source: &tatarav1alpha1.TaskSource{
				Provider:    "github",
				IssueRef:    "o/r#42",
				Number:      42,
				AuthorLogin: authorLogin,
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, task))
	var fresh tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &fresh))
	w := &labelWriter{}
	rdr := &labelReader{current: []string{}}
	r := &TaskReconciler{
		Client:    k8sClient,
		Scheme:    k8sClient.Scheme(),
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:    func(string) (scm.SCMWriter, error) { return w, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return rdr, nil },
	}
	return r, &fresh, proj
}

// TestSetLifecycleLabel_SeedsProposedEntry_BotAuthored is the regression test for
// the missing role:proposed producer. Projecting a proposal label onto a
// bot-authored proposal issue must MINT a role:proposed ledger entry (with the
// real issue number) when none exists, so proposalBacklogFromTasks, the readback
// guard, and the projection all have an entry to operate on in production.
func TestSetLifecycleLabel_SeedsProposedEntry_BotAuthored(t *testing.T) {
	r, task, proj := seedBotProposalLifecycleTask(t, "seed-bot", "tatara-bot")
	require.Empty(t, task.Status.WorkItems, "precondition: no ledger entries yet")

	require.NoError(t, r.setLifecycleLabel(context.Background(), proj, task, "tatara-brainstorming"))

	var updated tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &updated))
	found := false
	for _, wi := range updated.Status.WorkItems {
		if wi.Role == tatarav1alpha1.RoleProposed {
			require.Equal(t, "o/r", wi.Repo)
			require.Equal(t, 42, wi.Number, "role:proposed entry must carry the real issue number")
			require.Equal(t, tatarav1alpha1.WIProposed, wi.State)
			found = true
		}
	}
	require.True(t, found, "a role:proposed entry must be minted for the bot-authored proposal")

	// And proposalBacklogFromTasks must now count it.
	require.Equal(t, 1, proposalBacklogFromTasks([]tatarav1alpha1.Task{updated}),
		"the minted proposal must be counted by proposalBacklogFromTasks")
}

// TestSetLifecycleLabel_NoProposedEntry_HumanAuthored verifies that a
// human-reported (non-bot) issue does NOT get a spurious role:proposed entry -
// only tatara-authored proposals are ledgered as proposals.
func TestSetLifecycleLabel_NoProposedEntry_HumanAuthored(t *testing.T) {
	r, task, proj := seedBotProposalLifecycleTask(t, "seed-human", "some-human")

	require.NoError(t, r.setLifecycleLabel(context.Background(), proj, task, "tatara-approved"))

	var updated tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &updated))
	for _, wi := range updated.Status.WorkItems {
		require.NotEqual(t, tatarav1alpha1.RoleProposed, wi.Role,
			"human-authored issue must not get a role:proposed entry")
	}
}

// TestSetLifecycleLabel_SeedThenProject_Idempotent verifies the producer is
// idempotent: minting then re-projecting updates the same entry, never appends.
func TestSetLifecycleLabel_SeedThenProject_Idempotent(t *testing.T) {
	r, task, proj := seedBotProposalLifecycleTask(t, "seed-idem", "tatara-bot")
	ctx := context.Background()
	require.NoError(t, r.setLifecycleLabel(ctx, proj, task, "tatara-brainstorming"))

	var afterFirst tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &afterFirst))
	require.NoError(t, r.setLifecycleLabel(ctx, proj, &afterFirst, "tatara-approved"))

	var updated tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &updated))
	proposed := 0
	for _, wi := range updated.Status.WorkItems {
		if wi.Role == tatarav1alpha1.RoleProposed {
			proposed++
			require.Equal(t, tatarav1alpha1.WIApproved, wi.State)
		}
	}
	require.Equal(t, 1, proposed, "exactly one role:proposed entry after two projections")
}
