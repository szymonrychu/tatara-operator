package controller

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

func minterFor(t *testing.T, objs ...client.Object) (*Minter, client.Client) {
	t.Helper()
	c := newMirrorClient(t, objs...)
	return &Minter{Client: c, APIReader: c, Scheme: c.Scheme()}, c
}

// A webhook-originated issue mints an ACTIVE (triaging) clarify Task that owns
// its Issue CR - the same outcome the sweep produces, on the same natural key.
func TestMintForItem_IssueWebhookOriginated_MintsTriagingClarify(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	m, c := minterFor(t, proj, repo)

	item := ForgeItem{Issue: scm.Issue{Number: 353, State: "open", Author: "alice",
		Title: "login 500s", URL: "https://github.com/o/r/issues/353"}}
	task, created, err := m.MintForItem(context.Background(), proj, repo, item, true, nil)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, SweepIssueKind, task.Spec.Kind)
	require.Equal(t, tatarav1alpha1.StageTriaging, task.Spec.InitialStage)
	require.Equal(t, tatarav1alpha1.IntakeTaskName("p", "clarify", "tatara-operator", 353), task.Name)

	// Issue CR is owned by the minted Task (the durable natural-key anchor).
	var iss tatarav1alpha1.Issue
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: tatarav1alpha1.IssueName("tatara-operator", 353)}, &iss))
	owner, ok := own.ControllerOwner(&iss)
	require.True(t, ok)
	require.Equal(t, task.Name, owner)
}

// A non-webhook (cold-backlog) issue mints parked(backlog-sweep).
func TestMintForItem_ColdIssue_MintsParked(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	m, _ := minterFor(t, proj, repo)
	item := ForgeItem{Issue: scm.Issue{Number: 7, State: "open", Author: "alice"}}
	task, created, err := m.MintForItem(context.Background(), proj, repo, item, false, nil)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, tatarav1alpha1.StageParked, task.Spec.InitialStage)
	require.Equal(t, stage.ReasonBacklogSweep, task.Spec.InitialStageReason)
}

// An already-owned issue is not re-minted (the steady-state backstop dedup).
func TestMintForItem_OwnedIssue_NoOp(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	m, _ := minterFor(t, proj, repo)
	item := ForgeItem{Issue: scm.Issue{Number: 9, State: "open", Author: "alice"}}
	_, created, err := m.MintForItem(context.Background(), proj, repo, item, true, nil)
	require.NoError(t, err)
	require.True(t, created)
	_, created2, err := m.MintForItem(context.Background(), proj, repo, item, true, nil)
	require.NoError(t, err)
	require.False(t, created2, "an owned issue is not an orphan; the backstop no-ops")
}

// A human PR in reaction scope mints a review Task (triaging, no prior verdict).
func TestMintForItem_HumanPR_MintsReview(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	m, _ := minterFor(t, proj, repo)
	item := ForgeItem{IsPR: true, PR: scm.PRRef{Number: 42, Author: "alice",
		HeadSHA: "abc", HeadBranch: "fix", Repo: "o/r"}}
	task, created, err := m.MintForItem(context.Background(), proj, repo, item, false, nil)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, SweepReviewKind, task.Spec.Kind)
	require.Equal(t, tatarav1alpha1.StageTriaging, task.Spec.InitialStage)
}

// A bot-authored PR is ignored (ClassifyPR clause 2): no mint.
func TestMintForItem_BotPR_NoMint(t *testing.T) {
	proj := sweepProject("p") // BotLogin "tatara-bot"
	repo := sweepRepo("p")
	m, _ := minterFor(t, proj, repo)
	item := ForgeItem{IsPR: true, PR: scm.PRRef{Number: 43, Author: "tatara-bot",
		HeadSHA: "abc", HeadBranch: "chore", Repo: "o/r"}}
	task, created, err := m.MintForItem(context.Background(), proj, repo, item, false, nil)
	require.NoError(t, err)
	require.False(t, created)
	require.Nil(t, task)
}

// Two concurrent mints for the same issue natural key collapse to ONE Task.
func TestMintForItem_ConcurrentSameKey_OneTask(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	m, c := minterFor(t, proj, repo)
	item := ForgeItem{Issue: scm.Issue{Number: 100, State: "open", Author: "alice"}}

	const n = 6
	var wg sync.WaitGroup
	wins := make([]bool, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, ok, err := m.MintForItem(context.Background(), proj, repo, item, true, nil)
			wins[i], errs[i] = ok, err
		}(i)
	}
	wg.Wait()
	got := 0
	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
		if wins[i] {
			got++
		}
	}
	require.Equal(t, 1, got)
	var tl tatarav1alpha1.TaskList
	require.NoError(t, c.List(context.Background(), &tl))
	require.Len(t, tl.Items, 1)
}
