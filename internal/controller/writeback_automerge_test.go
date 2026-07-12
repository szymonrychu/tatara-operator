package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// TestApplySemverAutoMerge_GitLabEnsuresLabel guards the provider-agnostic
// EnsureLabel call: the label color must still be ensured for provider=="gitlab"
// even though the PR-label AddLabel step is GitHub-only. Without this, the S21
// helper extraction could silently pull EnsureLabel inside the github gate and
// regress GitLab label-color maintenance unnoticed.
func TestApplySemverAutoMerge_GitLabEnsuresLabel(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	proj := &tatarav1alpha1.Project{
		Spec: tatarav1alpha1.ProjectSpec{
			Scm: &tatarav1alpha1.ScmSpec{Provider: "gitlab", BotLogin: "tatara-bot"},
		},
	}
	repo := tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "gl-repo"},
		Spec:       tatarav1alpha1.RepositorySpec{URL: "https://gitlab.com/o/r.git"},
	}
	cs := &tatarav1alpha1.ChangeSummary{Significance: "minor"}

	r.applySemverAutoMerge(context.Background(), proj, repo, fw, "tok", "gitlab",
		"https://gitlab.com/o/r/-/merge_requests/7", cs)

	require.True(t, fw.ensureLabelCalled, "EnsureLabel must fire for provider=gitlab")
	require.Equal(t, "semver:minor", fw.ensureLabelName)
	require.Equal(t, "d93f0b", fw.ensureLabelColor)
	require.False(t, fw.addLabelCalled, "AddLabel is GitHub-only; must NOT fire for gitlab")
}

// TestWriteBackOpenChange_SemverLabelAndAutoMerge covers the push-CD writeback
// hook: after a bot PR opens, the operator stamps the declared significance
// label and enables native auto-merge (D5), gated on significance-present AND a
// project bot login.
func TestWriteBackOpenChange_SemverLabelAndAutoMerge(t *testing.T) {
	t.Run("significance + bot login -> label stamped and auto-merge enabled", func(t *testing.T) {
		fw := &fullFakeSCMWriter{}
		r := newFullFakeReconciler(t, fw)
		task := seedWritebackKindTask(t, "am-ok", "am-proj-ok", "am-repo-ok", "am-scm-ok",
			tatarav1alpha1.TaskSpec{
				Goal: "implement", Kind: "implement",
				Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#7", Number: 7},
			},
			&tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"})
		task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{
			PRTitle: "feat: x", PRBody: "body", DeliveredScope: "did x", Significance: "minor",
		}
		require.NoError(t, k8sClient.Status().Update(context.Background(), task))

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		require.True(t, fw.ensureLabelCalled, "EnsureLabel must be called")
		require.Equal(t, "semver:minor", fw.ensureLabelName)
		require.Equal(t, "d93f0b", fw.ensureLabelColor)
		require.True(t, fw.addLabelCalled, "AddLabel must stamp the PR")
		require.Equal(t, "o/r#99", fw.addLabelIssueRef)
		require.Equal(t, "semver:minor", fw.addLabelLabel)
		require.True(t, fw.autoMergeCalled, "EnableAutoMerge must be called")
		require.Equal(t, "https://example/pr/99", fw.autoMergePRURL)
		require.Equal(t, "squash", fw.autoMergeMethod)
	})

	t.Run("no significance -> no label, no auto-merge", func(t *testing.T) {
		fw := &fullFakeSCMWriter{}
		r := newFullFakeReconciler(t, fw)
		task := seedWritebackKindTask(t, "am-nosig", "am-proj-nosig", "am-repo-nosig", "am-scm-nosig",
			tatarav1alpha1.TaskSpec{
				Goal: "implement", Kind: "implement",
				Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#7", Number: 7},
			},
			&tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"})
		// ChangeSummary present but no significance must not trigger the cascade.
		task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{PRTitle: "feat: x", PRBody: "body"}
		require.NoError(t, k8sClient.Status().Update(context.Background(), task))

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		require.False(t, fw.ensureLabelCalled, "no significance: EnsureLabel must NOT be called")
		require.False(t, fw.addLabelCalled, "no significance: AddLabel must NOT be called")
		require.False(t, fw.autoMergeCalled, "no significance: EnableAutoMerge must NOT be called")
	})

	t.Run("significance but no bot login -> label stamped, auto-merge withheld", func(t *testing.T) {
		fw := &fullFakeSCMWriter{}
		r := newFullFakeReconciler(t, fw)
		task := seedWritebackKindTask(t, "am-nobot", "am-proj-nobot", "am-repo-nobot", "am-scm-nobot",
			tatarav1alpha1.TaskSpec{
				Goal: "implement", Kind: "implement",
				Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#7", Number: 7},
			},
			// CRD admission now rejects an empty botLogin (MinLength=1), so the
			// "no bot configured" runtime branch is exercised via a nil Scm instead.
			nil)
		task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{PRTitle: "fix: y", Significance: "patch"}
		require.NoError(t, k8sClient.Status().Update(context.Background(), task))

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		require.True(t, fw.ensureLabelCalled, "label still ensured")
		require.Equal(t, "semver:patch", fw.addLabelLabel)
		require.False(t, fw.autoMergeCalled, "no bot login: auto-merge must be withheld")
	})

	t.Run("documentation kind -> auto-merge without significance or semver label", func(t *testing.T) {
		fw := &fullFakeSCMWriter{}
		r := newFullFakeReconciler(t, fw)
		task := seedWritebackKindTask(t, "am-doc", "am-proj-doc", "am-repo-doc", "am-scm-doc",
			tatarav1alpha1.TaskSpec{
				Goal: "update docs for the merge", Kind: "documentation",
				Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#7", Number: 7},
			},
			&tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"})
		// Documentation is not a versioned artifact (no release cascade): it declares
		// no ChangeSummary/significance, yet its bot PR must still auto-merge on the
		// Build check, and must NOT get a (meaningless) semver label.
		require.NoError(t, k8sClient.Status().Update(context.Background(), task))

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		require.True(t, fw.autoMergeCalled, "documentation PR must auto-merge without significance")
		require.Equal(t, "squash", fw.autoMergeMethod)
		require.False(t, fw.ensureLabelCalled, "documentation: no semver label ensured")
		require.False(t, fw.addLabelCalled, "documentation: no semver label stamped on PR")
	})
}

// TestDoWriteBack_ReviewKind_RemainingScopeStillPostsVerdict is the D3
// regression. The change_summary REST gate rejected only PROJECT-scoped kinds,
// so kind=review could carry a change_summary - and the full-scope-or-decline
// hard-fail is hoisted ABOVE doWriteBack's kind switch. A review agent that
// misfired remainingScope (plausibly describing what the PR UNDER REVIEW still
// lacks) therefore hard-failed its own REVIEW Task before writeBackReview ran:
// the verdict never posted and the PR sat unreviewed forever. A review Task
// opens no PR, so remaining scope on it can never ship anything - the hard-fail
// must not apply to it.
func TestDoWriteBack_ReviewKind_RemainingScopeStillPostsVerdict(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "wbk-rev-remscope", "wbk-rrs-p", "wbk-rrs-r", "wbk-rrs-s",
		tatarav1alpha1.TaskSpec{
			Goal: "review a PR",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#9", IsPR: true, Number: 9,
			},
		}, nil)
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{
		RemainingScope: "the PR under review still lacks logout",
		Significance:   "minor",
	}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.True(t, fw.approveCalled, "the review verdict must still post: a review Task opens no PR, so remaining scope cannot ship anything")
	require.Equal(t, 9, fw.approveNumber)

	got := getTask(t, task.Name)
	require.NotEqual(t, "Failed", got.Status.Phase, "a review Task must never hard-fail on remaining scope")
}
