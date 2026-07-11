package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// semverFakeReader serves a fixed open-PR list (with labels) regardless of
// owner/repo, so prHasSemverLabel's respect-existing read can be exercised.
type semverFakeReader struct {
	scm.SCMReader
	prs []scm.PRRef
}

func (r *semverFakeReader) ListOpenPRs(_ context.Context, _, _ string) ([]scm.PRRef, error) {
	return r.prs, nil
}

// hasLabelPair reports whether AddLabel was called with (ref, label).
func hasLabelPair(fw *fullFakeSCMWriter, ref, label string) bool {
	for i := range fw.addLabelRefs {
		if fw.addLabelRefs[i] == ref && fw.addLabelLabels[i] == label {
			return true
		}
	}
	return false
}

// semverAddedForRef reports whether ANY semver:* label was AddLabel'd for ref.
func semverAddedForRef(fw *fullFakeSCMWriter, ref string) bool {
	for i := range fw.addLabelRefs {
		if fw.addLabelRefs[i] == ref && strings.HasPrefix(fw.addLabelLabels[i], "semver:") {
			return true
		}
	}
	return false
}

// TestWriteBackReview_UmbrellaStampsPerMRSemverFromVerdict: an approve verdict
// carrying a per-MR SemverAssignment stamps each member PR with its assigned level.
func TestWriteBackReview_UmbrellaStampsPerMRSemverFromVerdict(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "rev-sv-verdict", "rsv-proj", "rsv-repo", "rsv-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review the stream", Kind: "review",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", IsPR: false, Number: 5},
		}, nil)
	addRepo(t, "rsv-repo2", "rsv-proj", "https://github.com/o/r2.git")
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 9, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
		{Provider: "github", Repo: "o/r2", Number: 21, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
	}
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{
		Decision: "approve", Body: "lgtm",
		Semver: []tatarav1alpha1.SemverAssignment{
			{Repo: "o/r", Number: 9, Level: "minor"},
			{Repo: "o/r2", Number: 21, Level: "major"},
		},
	}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.True(t, hasLabelPair(fw, "o/r#9", "semver:minor"), "o/r#9 gets its assigned semver:minor")
	require.True(t, hasLabelPair(fw, "o/r2#21", "semver:major"), "o/r2#21 gets its assigned semver:major")
	require.False(t, fw.mergeCalled, "review must NEVER call Merge")
}

// TestWriteBackReview_SemverRespectsExistingLabel: a member MR that already carries
// a semver:* label is left untouched (a deliberate human semver is authoritative),
// while a member with no existing label is stamped from the verdict.
func TestWriteBackReview_SemverRespectsExistingLabel(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &semverFakeReader{prs: []scm.PRRef{{Number: 9, Labels: []string{"semver:major"}}}}, nil
	}
	task := seedWritebackKindTask(t, "rev-sv-respect", "rsr-proj", "rsr-repo", "rsr-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review the stream", Kind: "review",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", IsPR: false, Number: 5},
		}, nil)
	addRepo(t, "rsr-repo2", "rsr-proj", "https://github.com/o/r2.git")
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 9, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
		{Provider: "github", Repo: "o/r2", Number: 21, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
	}
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{
		Decision: "approve", Body: "lgtm",
		Semver: []tatarav1alpha1.SemverAssignment{
			{Repo: "o/r", Number: 9, Level: "minor"},
			{Repo: "o/r2", Number: 21, Level: "minor"},
		},
	}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.False(t, semverAddedForRef(fw, "o/r#9"), "existing semver:* on o/r#9 is respected; no new semver label added")
	require.True(t, hasLabelPair(fw, "o/r2#21", "semver:minor"), "o/r2#21 (no existing label) is stamped from the verdict")
}

// TestWriteBackReview_SemverFallsBackToChangeSignificance: a member with no verdict
// assignment falls back to its implement-agent change_significance.
func TestWriteBackReview_SemverFallsBackToChangeSignificance(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	ctx := context.Background()
	task := seedWritebackKindTask(t, "rev-sv-cs", "rcs-proj", "rcs-repo", "rcs-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review the stream", Kind: "review",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", IsPR: false, Number: 5},
		}, nil)
	addRepo(t, "rcs-repo2", "rcs-proj", "https://github.com/o/r2.git")
	// Sibling implement task in the same project opened o/r2#21 and declared major.
	impl := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "rcs-impl", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "rcs-proj", Kind: "implement",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, impl))
	impl.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r2", Number: 21, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
	}
	impl.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{Significance: "major"}
	require.NoError(t, k8sClient.Status().Update(ctx, impl))

	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r2", Number: 21, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
	}
	// No verdict assignment for o/r2#21 -> fall back to the implement change_significance.
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
	require.NoError(t, k8sClient.Status().Update(ctx, task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.True(t, hasLabelPair(fw, "o/r2#21", "semver:major"), "fallback uses the implement change_significance (major)")
}

// TestWriteBackReview_SemverFallsBackToPatch: a member with no verdict assignment
// and no sibling change_significance falls back to patch.
func TestWriteBackReview_SemverFallsBackToPatch(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "rev-sv-patch", "rpt-proj", "rpt-repo", "rpt-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review the stream", Kind: "review",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", IsPR: false, Number: 5},
		}, nil)
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 9, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
	}
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.True(t, hasLabelPair(fw, "o/r#9", "semver:patch"), "no verdict + no change_significance -> patch")
}

// TestWriteBackReview_HumanSinglePRStampsSource: a single-PR (human) review stamps
// the Spec.Source PR from the verdict's assignment.
func TestWriteBackReview_HumanSinglePRStampsSource(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "rev-sv-human", "rhs-proj", "rhs-repo", "rhs-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review a human PR", Kind: "review",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#9", IsPR: true, Number: 9},
		}, nil)
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{
		Decision: "approve", Body: "lgtm",
		Semver: []tatarav1alpha1.SemverAssignment{{Repo: "o/r", Number: 9, Level: "minor"}},
	}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.True(t, fw.approveCalled, "human PR is approved")
	require.True(t, hasLabelPair(fw, "o/r#9", "semver:minor"), "human single-PR review stamps Spec.Source from the verdict")
	require.False(t, fw.mergeCalled, "review must NEVER call Merge")
}

// TestWriteBackReview_HumanSinglePRSemverDefaultsToPatch: a single-PR human review
// with no verdict assignment falls back to patch on the Spec.Source PR.
func TestWriteBackReview_HumanSinglePRSemverDefaultsToPatch(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "rev-sv-human-def", "rhd-proj", "rhd-repo", "rhd-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review a human PR", Kind: "review",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#9", IsPR: true, Number: 9},
		}, nil)
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.True(t, hasLabelPair(fw, "o/r#9", "semver:patch"), "no assignment on a human MR -> patch")
}

// TestWriteBackReview_SemverBestEffortDoesNotBlockApprove: a semver AddLabel failure
// must NOT block the approve verb, drop the tatara-approved fan-out, or trigger the
// umbrella approve-failure requeue (writeback-pending still clears; no error).
func TestWriteBackReview_SemverBestEffortDoesNotBlockApprove(t *testing.T) {
	fw := &fullFakeSCMWriter{addLabelErrByLabel: map[string]error{"semver:minor": fmt.Errorf("boom")}}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "rev-sv-besteffort", "rbe-proj", "rbe-repo", "rbe-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review the stream", Kind: "review",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", IsPR: false, Number: 5},
		}, nil)
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 9, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"},
	}
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{
		Decision: "approve", Body: "lgtm",
		Semver: []tatarav1alpha1.SemverAssignment{{Repo: "o/r", Number: 9, Level: "minor"}},
	}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err, "a semver stamp failure must NOT propagate / requeue")

	require.Contains(t, fw.approveNumbers, 9, "the member PR is still approved")
	require.True(t, hasLabelPair(fw, "o/r#9", "tatara-approved"), "tatara-approved still landed")
	require.False(t, fw.mergeCalled, "review must NEVER call Merge")

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status, "writeback-pending clears; semver failure is best-effort")
}
