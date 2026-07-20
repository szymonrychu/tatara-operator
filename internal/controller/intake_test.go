package controller

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// mrUpdateCountingClient wraps a client.Client, counting real (non-status)
// Update calls against a *MergeRequest object - the atomic controller-handover
// write fix #408 needs counted exactly, across ownMergeRequest, reMintReviewOwner,
// and the takeover endpoint's fresh mint.
type mrUpdateCountingClient struct {
	client.Client
	mrUpdates *int32
}

func (c *mrUpdateCountingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if _, ok := obj.(*tatarav1alpha1.MergeRequest); ok {
		atomic.AddInt32(c.mrUpdates, 1)
	}
	return c.Client.Update(ctx, obj, opts...)
}

// wrapMRUpdateCounting wraps ANY client.Client (a fake mirror client or the
// envtest-backed k8sClient) with mrUpdateCountingClient, for asserting an
// atomic ownership handover costs exactly one MR Update (fix #408) regardless
// of which backing store the test otherwise seeds through.
func wrapMRUpdateCounting(c client.Client) (client.Client, *int32) {
	n := new(int32)
	return &mrUpdateCountingClient{Client: c, mrUpdates: n}, n
}

// newMRUpdateCountingClient builds a fake mirror client (newMirrorClient) that
// also counts MergeRequest Update calls.
func newMRUpdateCountingClient(t *testing.T, objs ...client.Object) (client.Client, *int32) {
	t.Helper()
	return wrapMRUpdateCounting(newMirrorClient(t, objs...))
}

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

// TestOwnMergeRequest_ExpectFromAtomicHandover is fix #408's unit coverage on
// ownMergeRequest directly: a hand-over from a KNOWN current controller
// (expectFrom) must land in exactly ONE MergeRequest Update - no standalone
// Controller=false demote Update followed by a separate promote, which would
// leave a zero-controller window a RepairZeroController race could jump into.
// An unexpected current owner (set, != task, != expectFrom) must refuse with
// NO mutation at all.
func TestOwnMergeRequest_ExpectFromAtomicHandover(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	taskA := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "task-a", Namespace: testNS}}
	taskB := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "task-b", Namespace: testNS}}
	taskC := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "task-c", Namespace: testNS}}

	mrName := tatarav1alpha1.MergeRequestName(repo.Name, 55)
	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{Name: mrName, Namespace: testNS},
		Spec:       tatarav1alpha1.MergeRequestSpec{RepositoryRef: repo.Name, ProjectRef: proj.Name, Number: 55},
	}
	own.AddPlainOwner(mr, taskA)
	require.NoError(t, own.HandOverController(mr, nil, taskA))

	c, mrUpdates := newMRUpdateCountingClient(t, proj, repo, mr)
	m := &Minter{Client: c, APIReader: c, Scheme: c.Scheme()}

	// Hand from A to B: exactly ONE Update, B is controller, A survives as a
	// plain (non-controller) ref.
	require.NoError(t, m.ownMergeRequest(context.Background(), proj, mrName, taskB, "task-a"))
	require.EqualValues(t, 1, atomic.LoadInt32(mrUpdates), "atomic handover must cost exactly one MR Update")

	var got tatarav1alpha1.MergeRequest
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: mrName}, &got))
	ctrl, ok := own.ControllerOwner(&got)
	require.True(t, ok)
	require.Equal(t, "task-b", ctrl)
	foundA := false
	for _, ref := range got.GetOwnerReferences() {
		if ref.Name == "task-a" {
			foundA = true
			require.False(t, ref.Controller != nil && *ref.Controller, "task-a must be demoted to controller=false, not removed")
		}
	}
	require.True(t, foundA, "task-a must survive hand-back as a plain ref")

	// An unexpected current owner (C, neither task nor expectFrom) refuses
	// with no mutation at all.
	mr2Name := tatarav1alpha1.MergeRequestName(repo.Name, 56)
	mr2 := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{Name: mr2Name, Namespace: testNS},
		Spec:       tatarav1alpha1.MergeRequestSpec{RepositoryRef: repo.Name, ProjectRef: proj.Name, Number: 56},
	}
	own.AddPlainOwner(mr2, taskC)
	require.NoError(t, own.HandOverController(mr2, nil, taskC))
	require.NoError(t, c.Create(context.Background(), mr2))

	atomic.StoreInt32(mrUpdates, 0)
	err := m.ownMergeRequest(context.Background(), proj, mr2Name, taskB, "task-a")
	require.Error(t, err)
	require.EqualValues(t, 0, atomic.LoadInt32(mrUpdates), "a refused handover must not mutate the MR at all")

	var after tatarav1alpha1.MergeRequest
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: mr2Name}, &after))
	ctrl2, ok2 := own.ControllerOwner(&after)
	require.True(t, ok2)
	require.Equal(t, "task-c", ctrl2, "controller must remain unchanged on refusal")
}
