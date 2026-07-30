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
	"github.com/szymonrychu/tatara-operator/internal/agent"
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

// --- pod-name stamping on the intake mints --------------------------------
//
// Issue #517's descriptive pod name (<type>-<project>-<repo>-<i|p><id>) is
// carried by the tatara.dev/pod-name annotation, stamped at Task CREATION.
// agent.StampPodName only ever ran on the queue path, so every Task the
// reactive intake minted carried NO annotation and fell back to the legacy
// wrapper-<task-name>. Both intake mints must stamp the SAME name the queue
// path produces for the same (project, repo, kind, number).

func TestMintIssueTask_StampsPodName(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	m, c := minterFor(t, proj, repo)

	item := ForgeItem{Issue: scm.Issue{Number: 353, State: "open", Author: "alice"}}
	task, created, err := m.MintForItem(context.Background(), proj, repo, item, true, nil)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, "clr-p-tatara-operator-i353", task.Annotations[agent.PodNameAnnotation])

	var stored tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(task), &stored))
	require.Equal(t, "clr-p-tatara-operator-i353", agent.PodName(&stored),
		"the stamp must land on the STORED Task, not just the local literal")
}

func TestMintReviewTask_StampsPodName(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	m, c := minterFor(t, proj, repo)

	item := ForgeItem{IsPR: true, PR: scm.PRRef{Number: 59, Author: "alice",
		HeadSHA: "abc", HeadBranch: "fix", Repo: "o/r"}}
	task, created, err := m.MintForItem(context.Background(), proj, repo, item, false, nil)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, "rev-p-tatara-operator-p59", task.Annotations[agent.PodNameAnnotation])

	var stored tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(task), &stored))
	require.Equal(t, "rev-p-tatara-operator-p59", agent.PodName(&stored))
}

// The longest real repo name on the platform (tatara-memory-repo-ingester)
// still fits the 63-char DNS-1123 budget untrimmed: the live legacy pod name
// mt-r-tatara-memory-repo-inges-34-... is what showed repo names here get long.
func TestMintReviewTask_StampedPodNameFitsDNS1123(t *testing.T) {
	proj := sweepProject("tatara")
	repo := sweepRepo("tatara")
	repo.Name = "tatara-memory-repo-ingester"
	m, _ := minterFor(t, proj, repo)

	item := ForgeItem{IsPR: true, PR: scm.PRRef{Number: 34, Author: "alice", HeadSHA: "abc", Repo: "o/r"}}
	task, created, err := m.MintForItem(context.Background(), proj, repo, item, false, nil)
	require.NoError(t, err)
	require.True(t, created)
	name := task.Annotations[agent.PodNameAnnotation]
	require.Equal(t, "rev-tatara-tatara-memory-repo-ingester-p34", name)
	require.LessOrEqual(t, len(name), 63)
}

// createTaskRaceSafe is ADOPT-OR-CREATE: on a live natural-key twin it creates
// NOTHING. Stamping the local literal before the call must therefore never
// reach the adopted Task, and a legacy twin minted before #517 must keep
// working through agent.PodName's wrapper-<name> fallback.
func TestMintIssueTask_AdoptedLegacyTwin_KeepsFallbackPodName(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	legacy := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.IntakeTaskName("p", SweepIssueKind, "tatara-operator", 353),
			Namespace: testNS,
		},
		Spec: tatarav1alpha1.TaskSpec{ProjectRef: "p", Kind: SweepIssueKind},
	}
	m, c := minterFor(t, proj, repo, legacy)

	item := ForgeItem{Issue: scm.Issue{Number: 353, State: "open", Author: "alice"}}
	_, created, err := m.MintForItem(context.Background(), proj, repo, item, true, nil)
	require.NoError(t, err)
	require.False(t, created, "a live natural-key twin is adopted, not re-minted")

	var stored tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(legacy), &stored))
	require.NotContains(t, stored.Annotations, agent.PodNameAnnotation,
		"adoption must not stamp a pod name onto a pre-existing Task")
	require.Equal(t, "wrapper-"+legacy.Name, agent.PodName(&stored))
}

// The stamp is written ONCE at creation and never recomputed, so the pod name
// is stable even after the Task's agent kind advances (agent.AgentKind prefers
// status.agentKind, which BuildPodName would otherwise re-read).
func TestMintIssueTask_StampedPodNameStableAcrossStageChange(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	m, c := minterFor(t, proj, repo)

	item := ForgeItem{Issue: scm.Issue{Number: 12, State: "open", Author: "alice"}}
	task, created, err := m.MintForItem(context.Background(), proj, repo, item, true, nil)
	require.NoError(t, err)
	require.True(t, created)
	want := agent.PodName(task)

	var fresh tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(task), &fresh))
	fresh.Status.AgentKind = "implement"
	fresh.Status.Stage = tatarav1alpha1.StageImplementing
	require.NoError(t, c.Status().Update(context.Background(), &fresh))

	var after tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(task), &after))
	require.Equal(t, want, agent.PodName(&after),
		"the annotation is written once at creation; it must not follow status.agentKind")
}
