package controller

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// These tests cover the mr-mirror-selfheal repair: a mint interrupted between the
// Task create and its MergeRequest/Issue bind (a restart, e.g. during a rollout)
// leaves the Task alive but the artifact an UNBOUND stub - no controller owner,
// empty status - and the Task with no ref. Every later mint pass hit the
// created=false backstop and returned WITHOUT re-binding, so the artifact stayed
// orphaned forever: ownedMRs found nothing and the review agent's submit_outcome
// 400'd ("this task owns no open MR") on every retry. The backstop now repairs
// the binding against the existing live twin.

func reviewTwin(num int) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.IntakeTaskName("p", SweepReviewKind, "tatara-operator", num),
			Namespace: testNS,
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "p",
			Kind:       SweepReviewKind,
			Source:     &tatarav1alpha1.TaskSource{Number: num, IsPR: true, Provider: "github"},
		},
	}
}

func issueTwin(num int) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.IntakeTaskName("p", SweepIssueKind, "tatara-operator", num),
			Namespace: testNS,
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "p",
			Kind:       SweepIssueKind,
			Source:     &tatarav1alpha1.TaskSource{Number: num, Provider: "github"},
		},
	}
}

func unboundMRStub(num int) *tatarav1alpha1.MergeRequest {
	return &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.MergeRequestName("tatara-operator", num),
			Namespace: testNS,
		},
		Spec: tatarav1alpha1.MergeRequestSpec{
			Number:        num,
			RepositoryRef: "tatara-operator",
			ProjectRef:    "p",
			URL:           "https://github.com/szymonrychu/tatara-operator/pull/" + strconv.Itoa(num),
		},
	}
}

func unboundIssueStub(num int) *tatarav1alpha1.Issue {
	return &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.IssueName("tatara-operator", num),
			Namespace: testNS,
		},
		Spec: tatarav1alpha1.IssueSpec{
			Number:        num,
			RepositoryRef: "tatara-operator",
			ProjectRef:    "p",
			URL:           "https://github.com/szymonrychu/tatara-operator/issues/" + strconv.Itoa(num),
		},
	}
}

func reviewPRItem(num int) ForgeItem {
	return ForgeItem{IsPR: true, PR: scm.PRRef{
		Number: num, Author: "alice", HeadSHA: "b403908", HeadBranch: "fix/x", Repo: "o/r",
	}}
}

func nn(name string) types.NamespacedName {
	return types.NamespacedName{Namespace: testNS, Name: name}
}

// The live-twin backstop, on an UNBOUND MR stub, now binds the stub to the twin
// AND populates its empty status from the forge snapshot AND stamps the Task's
// mrRefs - the interrupted mint is repaired.
func TestMintReviewTask_RepairsUnboundMRStub(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	num := 373
	twin := reviewTwin(num)
	stub := unboundMRStub(num)
	m, c := minterFor(t, proj, repo, twin, stub)

	_, created, err := m.MintForItem(context.Background(), proj, repo, reviewPRItem(num), false, nil)
	require.NoError(t, err)
	require.False(t, created, "the live twin already holds the natural key; the create no-ops")

	var mr tatarav1alpha1.MergeRequest
	require.NoError(t, c.Get(context.Background(), nn(tatarav1alpha1.MergeRequestName("tatara-operator", num)), &mr))
	owner, ok := own.ControllerOwner(&mr)
	require.True(t, ok, "the unbound stub must be bound to its review Task")
	require.Equal(t, twin.Name, owner)
	require.Equal(t, "open", mr.Status.State, "the stub's empty status must be populated from the forge snapshot")
	require.Equal(t, "fix/x", mr.Status.HeadBranch)

	var tk tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), nn(twin.Name), &tk))
	require.Contains(t, tk.Status.MRRefs, mr.Name, "the Task must carry the repaired MR in its mrRefs")
}

// The repair also covers the case where the mint was interrupted BEFORE the MR CR
// existed at all: the backstop mints the mirror and binds it.
func TestMintReviewTask_RepairsMissingMR(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	num := 374
	twin := reviewTwin(num)
	m, c := minterFor(t, proj, repo, twin) // no MR CR at all

	_, created, err := m.MintForItem(context.Background(), proj, repo, reviewPRItem(num), false, nil)
	require.NoError(t, err)
	require.False(t, created)

	var mr tatarav1alpha1.MergeRequest
	require.NoError(t, c.Get(context.Background(), nn(tatarav1alpha1.MergeRequestName("tatara-operator", num)), &mr),
		"a missing MR mirror must be minted by the repair")
	owner, ok := own.ControllerOwner(&mr)
	require.True(t, ok)
	require.Equal(t, twin.Name, owner)
}

// The repair is idempotent: a second backstop pass over an ALREADY-bound MR does
// not re-mirror, does not duplicate the mrRef, and does not error.
func TestMintReviewTask_RepairIdempotent(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	num := 375
	twin := reviewTwin(num)
	stub := unboundMRStub(num)
	m, c := minterFor(t, proj, repo, twin, stub)

	for i := 0; i < 3; i++ {
		_, created, err := m.MintForItem(context.Background(), proj, repo, reviewPRItem(num), false, nil)
		require.NoError(t, err)
		require.False(t, created)
	}

	var tk tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), nn(twin.Name), &tk))
	mrName := tatarav1alpha1.MergeRequestName("tatara-operator", num)
	count := 0
	for _, ref := range tk.Status.MRRefs {
		if ref == mrName {
			count++
		}
	}
	require.Equal(t, 1, count, "the mrRef must not be duplicated across repeated backstop passes")
}

// NEVER STEAL. A stub already controller-owned by a DIFFERENT Task is not
// repaired: the owner is left untouched and the twin gains no mrRef. (ClassifyPR
// routes an owned MR to PRIgnore upstream, so this is exercised by calling the
// mint directly - the invariant must hold even if the gate is ever bypassed.)
func TestMintReviewTask_RepairNeverStealsForeignOwner(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	num := 376
	twin := reviewTwin(num)
	stub := unboundMRStub(num)
	foreign := reviewTwin(999)
	foreign.Name = "mt-someone-else"
	own.AddPlainOwner(stub, foreign)
	require.NoError(t, own.HandOverController(stub, nil, foreign))

	m, c := minterFor(t, proj, repo, twin, stub)
	pr := reviewPRItem(num).PR
	_, created, err := m.MintReviewTask(context.Background(), proj, repo, pr, stub,
		tatarav1alpha1.StageTriaging, "", nil)
	require.NoError(t, err, "refusing to steal is a clean no-op, never a heartbeat-suppressing error")
	require.False(t, created)

	var mr tatarav1alpha1.MergeRequest
	require.NoError(t, c.Get(context.Background(), nn(stub.Name), &mr))
	owner, ok := own.ControllerOwner(&mr)
	require.True(t, ok)
	require.Equal(t, "mt-someone-else", owner, "a foreign owner must never be stolen")

	var tk tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), nn(twin.Name), &tk))
	require.NotContains(t, tk.Status.MRRefs, mr.Name)
}

// Concurrent backstop passes over the same unbound stub converge to ONE binding:
// the never-steal guard makes the repair race-safe.
func TestMintReviewTask_RepairConcurrent(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	num := 377
	twin := reviewTwin(num)
	stub := unboundMRStub(num)
	m, c := minterFor(t, proj, repo, twin, stub)

	const n = 6
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, _, err := m.MintForItem(context.Background(), proj, repo, reviewPRItem(num), false, nil)
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
	}

	var mr tatarav1alpha1.MergeRequest
	require.NoError(t, c.Get(context.Background(), nn(tatarav1alpha1.MergeRequestName("tatara-operator", num)), &mr))
	owner, ok := own.ControllerOwner(&mr)
	require.True(t, ok)
	require.Equal(t, twin.Name, owner)

	var tk tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), nn(twin.Name), &tk))
	mrName := tatarav1alpha1.MergeRequestName("tatara-operator", num)
	count := 0
	for _, ref := range tk.Status.MRRefs {
		if ref == mrName {
			count++
		}
	}
	require.Equal(t, 1, count)
}

// The Issue-mint path carries the identical interrupted-bind gap; its backstop
// repairs the same way.
func TestMintIssueTask_RepairsUnboundIssueStub(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	num := 370
	twin := issueTwin(num)
	stub := unboundIssueStub(num)
	m, c := minterFor(t, proj, repo, twin, stub)

	ext := scm.Issue{Number: num, State: "open", Author: "alice", Title: "boom",
		URL: "https://github.com/szymonrychu/tatara-operator/issues/" + strconv.Itoa(num)}
	_, created, err := m.MintIssueTask(context.Background(), proj, repo, ext,
		tatarav1alpha1.StageTriaging, "", nil)
	require.NoError(t, err)
	require.False(t, created, "the live twin already holds the natural key; the create no-ops")

	var iss tatarav1alpha1.Issue
	require.NoError(t, c.Get(context.Background(), nn(tatarav1alpha1.IssueName("tatara-operator", num)), &iss))
	owner, ok := own.ControllerOwner(&iss)
	require.True(t, ok, "the unbound issue stub must be bound to its Task")
	require.Equal(t, twin.Name, owner)
	require.Equal(t, "open", iss.Status.State)

	var tk tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), nn(twin.Name), &tk))
	require.Contains(t, tk.Status.IssueRefs, iss.Name)
}
