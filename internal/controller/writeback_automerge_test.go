package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

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
			&tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o"}) // BotLogin empty
		task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{PRTitle: "fix: y", Significance: "patch"}
		require.NoError(t, k8sClient.Status().Update(context.Background(), task))

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		require.True(t, fw.ensureLabelCalled, "label still ensured")
		require.Equal(t, "semver:patch", fw.addLabelLabel)
		require.False(t, fw.autoMergeCalled, "no bot login: auto-merge must be withheld")
	})
}
