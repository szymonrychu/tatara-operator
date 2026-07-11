package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// addRepo enrolls an extra Repository CR into an existing project so an umbrella
// review can resolve a second member repo's URL by slug.
func addRepo(t *testing.T, name, project, url string) {
	t.Helper()
	r := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: project, URL: url, DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
		},
	}
	require.NoError(t, k8sClient.Create(context.Background(), r))
}

// TestSeedReviewSpanFromUmbrella verifies U-C's controller span seed: a stream
// review Task carrying AnnReviewHeadBranch inherits every role:openedPR member the
// sibling implement umbrella opened on that branch, so the review spans the stream.
func TestSeedReviewSpanFromUmbrella(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	ctx := context.Background()

	impl := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "span-impl", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "span-proj", Kind: "implement",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, impl))
	impl.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 9, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
		{Provider: "github", Repo: "o/r2", Number: 21, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
		{Provider: "github", Repo: "o/r3", Number: 4, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "other"},
	}
	require.NoError(t, k8sClient.Status().Update(ctx, impl))

	review := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "span-review", Namespace: testNS, Annotations: map[string]string{tatarav1alpha1.AnnReviewHeadBranch: "feat/x"}},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "span-proj", Kind: "review",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, review))

	r.seedReviewSpanFromUmbrella(ctx, review, "feat/x")

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "span-review"}, &got))
	members := umbrellaPRMembers(&got)
	require.Len(t, members, 2, "only same-branch openedPR members are seeded (o/r#9, o/r2#21)")
	require.True(t, hasPRMember(&got, "o/r", 9))
	require.True(t, hasPRMember(&got, "o/r2", 21))
	require.False(t, hasPRMember(&got, "o/r3", 4), "a different-branch PR must NOT be pulled in")
}

// TestSeedReviewSpanFromUmbrella_TerminalSiblingNotSeededLive is finding #3: a
// sibling openedPR that shares the branch but is already terminal (merged/closed,
// still carrying HeadBranch==branch) must be seeded with its REAL terminal state,
// not hardcoded WIOpen - so umbrellaPRMembers (which drops terminal members) does
// not treat an already-merged sibling as a live PR to re-approve.
func TestSeedReviewSpanFromUmbrella_TerminalSiblingNotSeededLive(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	ctx := context.Background()

	impl := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "span-term-impl", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "span-term-proj", Kind: "implement",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, impl))
	impl.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 9, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
		{Provider: "github", Repo: "o/r2", Number: 21, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIMerged, HeadBranch: "feat/x"},
	}
	require.NoError(t, k8sClient.Status().Update(ctx, impl))

	review := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "span-term-review", Namespace: testNS, Annotations: map[string]string{tatarav1alpha1.AnnReviewHeadBranch: "feat/x"}},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "span-term-proj", Kind: "review",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, review))

	r.seedReviewSpanFromUmbrella(ctx, review, "feat/x")

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "span-term-review"}, &got))
	// Both siblings are seeded into the ledger...
	require.True(t, hasPRMember(&got, "o/r", 9))
	require.True(t, hasPRMember(&got, "o/r2", 21))
	// ...but the merged sibling keeps its terminal state, so it is not a live member.
	members := umbrellaPRMembers(&got)
	require.Len(t, members, 1, "the merged sibling must not be treated as a live openedPR member")
	require.Equal(t, 9, members[0].Number, "only the still-open o/r#9 is live")
}

// TestWriteBackReview_UmbrellaWithholdsWhenOneMemberUnmergeable is U-D (b): an
// umbrella review approve verdict is withheld when ANY role:openedPR member PR is
// unmergeable, re-adding tatara-implementation to route the whole stream back to
// implement. No member PR is approved.
func TestWriteBackReview_UmbrellaWithholdsWhenOneMemberUnmergeable(t *testing.T) {
	fw := &fullFakeSCMWriter{mergeStateByNumber: map[int]scm.MergeState{21: scm.MergeStateDirty}}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "rev-umb-withhold", "ruw-proj", "ruw-repo", "ruw-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review the stream",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#5", IsPR: false, Number: 5,
			},
		}, nil)
	addRepo(t, "ruw-repo2", "ruw-proj", "https://github.com/o/r2.git")
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 9, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
		{Provider: "github", Repo: "o/r2", Number: 21, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
	}
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.False(t, fw.approveCalled, "no member PR may be approved when one is unmergeable")
	require.False(t, fw.mergeCalled, "review must NEVER call Merge")
	require.Contains(t, fw.addLabelLabels, "tatara-implementation",
		"unmergeable member routes the stream back to implement")
	require.Equal(t, "o/r#5", fw.addLabelIssueRef,
		"tatara-implementation lands on the originating issue (Spec.Source)")
}

// TestWriteBackReview_UmbrellaApprovesAllMembers is the green-path peer: when every
// role:openedPR member PR is mergeable the review fans out native Approve +
// tatara-approved to EACH member PR (across repos), never merging.
func TestWriteBackReview_UmbrellaApprovesAllMembers(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "rev-umb-approve", "rua-proj", "rua-repo", "rua-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review the stream",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#5", IsPR: false, Number: 5,
			},
		}, nil)
	addRepo(t, "rua-repo2", "rua-proj", "https://github.com/o/r2.git")
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 9, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
		{Provider: "github", Repo: "o/r2", Number: 21, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
	}
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.ElementsMatch(t, []int{9, 21}, fw.approveNumbers, "every member PR must be approved")
	require.False(t, fw.mergeCalled, "review must NEVER call Merge")
	require.Contains(t, fw.addLabelLabels, "tatara-approved", "tatara-approved must be applied to members")
	require.Contains(t, fw.addLabelRefs, "o/r#9", "member o/r#9 gets tatara-approved")
	require.Contains(t, fw.addLabelRefs, "o/r2#21", "member o/r2#21 gets tatara-approved")
}

// TestWriteBackReview_UmbrellaWithholdsWhenMemberURLUnresolvable is finding #1: a
// cross-repo umbrella review must never fall back to the review Task's own repo URL
// for a member whose repo URL is unresolvable (un-enrolled member repo, or a
// projectRepoURLBySlug List error). When ANY member URL cannot be resolved the
// stream is not yet verifiable: withhold approval and requeue (return an error) -
// approving nothing on a wrong URL and NOT clearing writeback-pending.
func TestWriteBackReview_UmbrellaWithholdsWhenMemberURLUnresolvable(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "rev-umb-unresolvable", "ruu-proj", "ruu-repo", "ruu-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review the stream",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#5", IsPR: false, Number: 5,
			},
		}, nil)
	// o/r is enrolled by seedWritebackKindTask; o/r2 is deliberately NOT enrolled, so
	// its member URL is unresolvable.
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 9, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
		{Provider: "github", Repo: "o/r2", Number: 21, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
	}
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.Error(t, err, "an unresolvable member URL must withhold + requeue")

	require.False(t, fw.approveCalled, "nothing may be approved when a member URL is unresolvable")
	require.False(t, fw.mergeCalled, "review must NEVER call Merge")
	require.NotContains(t, fw.addLabelLabels, "tatara-implementation",
		"an unresolvable member is a transient withhold, NOT an unmergeable route-to-implement")

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionTrue, cond.Status, "writeback-pending must NOT be cleared on a withheld approval")
}

// TestWriteBackReview_UmbrellaRequeuesOnPartialApproveFailure is finding #2: if one
// member's Approve fails while others succeed, the stream must NOT be marked
// complete / writeback-pending cleared - the reconcile returns an error to requeue
// and re-drive until ALL members are approved+labeled (idempotent re-approve).
func TestWriteBackReview_UmbrellaRequeuesOnPartialApproveFailure(t *testing.T) {
	fw := &fullFakeSCMWriter{approveErrByNumber: map[int]error{21: fmt.Errorf("boom")}}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "rev-umb-partial", "rup-proj", "rup-repo", "rup-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review the stream",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#5", IsPR: false, Number: 5,
			},
		}, nil)
	addRepo(t, "rup-repo2", "rup-proj", "https://github.com/o/r2.git")
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 9, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
		{Provider: "github", Repo: "o/r2", Number: 21, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
	}
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.Error(t, err, "a partial approve fan-out failure must requeue")

	require.Contains(t, fw.approveNumbers, 9, "the healthy member must still be approved (idempotent)")
	require.False(t, fw.mergeCalled, "review must NEVER call Merge")

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionTrue, cond.Status, "writeback-pending must stay set until ALL members are approved")
}

// Phase 6 sub-step 2: review approval applies tatara-approved + native Approve
// and NEVER merges; an unmergeable PR (or request_changes) re-adds
// tatara-implementation instead of approving.

func TestWriteBackReview_ApproveAppliesApprovedLabel(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "rev-approve-label", "rap-proj", "rap-repo", "rap-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review a PR",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#9", IsPR: true, Number: 9,
			},
		}, nil)
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.True(t, fw.approveCalled, "Approve must be called on approve verdict")
	require.True(t, fw.addLabelCalled, "tatara-approved must be applied on approve")
	require.Contains(t, fw.addLabelLabels, "tatara-approved", "approve must add tatara-approved")
	// The approve path also stamps a best-effort semver label (patch fallback here,
	// no verdict assignment / change_significance) so push-CD can cut the tag.
	require.True(t, hasLabelPair(fw, "o/r#9", "semver:patch"), "approve stamps a best-effort semver label")
	require.False(t, fw.mergeCalled, "review must NEVER call Merge")
}

func TestWriteBackReview_UnmergeableRoutesToImplement(t *testing.T) {
	fw := &fullFakeSCMWriter{mergeState: scm.MergeStateDirty}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "rev-unmergeable", "run-proj", "run-repo", "run-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review a PR",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#11", IsPR: true, Number: 11,
			},
		}, nil)
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.False(t, fw.approveCalled, "an unmergeable PR must NOT be approved")
	require.False(t, fw.mergeCalled, "review must NEVER call Merge")
	require.True(t, fw.addLabelCalled, "unmergeable must re-add tatara-implementation")
	require.Equal(t, "tatara-implementation", fw.addLabelLabel, "unmergeable routes back to implement")
}

func TestWriteBackReview_RequestChangesReAddsImplementation(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "rev-rc-impl", "rrc-proj", "rrc-repo", "rrc-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review a PR",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#12", IsPR: true, Number: 12,
			},
		}, nil)
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "request_changes", Body: "fix it"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.True(t, fw.requestChangesCalled, "request_changes must post RequestChanges")
	require.False(t, fw.mergeCalled, "review must NEVER call Merge")
	require.True(t, fw.addLabelCalled, "request_changes must re-add tatara-implementation")
	require.Equal(t, "tatara-implementation", fw.addLabelLabel, "request_changes routes back to implement")
}
