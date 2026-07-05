package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// seedSelfImproveSpawn creates a project (with scm.botLogin), repo, secret, and a
// pending selfImprove Task with a PR Source, ready to enter Planning. It mirrors
// seedWritebackKindTask but leaves the Task in the pre-spawn state.
func seedSelfImproveSpawn(t *testing.T, name, project, repo, scmSecret, botLogin string) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: scmSecret, Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("pat"), "webhookSecret": []byte("w")},
	}
	if err := k8sClient.Create(ctx, sec); err != nil {
		t.Fatalf("create secret %s: %v", scmSecret, err)
	}
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: project, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: scmSecret,
			TriggerLabel: "tatara",
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
				MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
			},
			Scm: &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: botLogin},
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project %s: %v", project, err)
	}
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Ready", Endpoint: "http://mem.svc"}
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("set project memory ready: %v", err)
	}
	r := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: repo, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: project, URL: "https://github.com/o/r.git", DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, r); err != nil {
		t.Fatalf("create repo %s: %v", repo, err)
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: project, RepositoryRef: repo, Goal: "self-improve",
			Kind: "selfImprove",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#9", IsPR: true, Number: 9, AuthorLogin: botLogin,
			},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task %s: %v", name, err)
	}
	var fresh tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, &fresh); err != nil {
		t.Fatalf("reload task: %v", err)
	}
	return &fresh
}

func TestSelfImprovePreSpawnAuthorshipGate(t *testing.T) {
	t.Run("non-bot PR author -> Failed NotBotAuthored, no pod", func(t *testing.T) {
		fw := &fullFakeSCMWriter{prState: scm.PRState{Author: "mallory"}}
		r := newFullFakeReconciler(t, fw)
		task := seedSelfImproveSpawn(t, "si-gate-bad", "si-proj-bad", "si-repo-bad", "si-scm-bad", "tatara-bot")

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		var got tatarav1alpha1.Task
		require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
		require.Equal(t, "Failed", got.Status.Phase)
		cond := apimeta.FindStatusCondition(got.Status.Conditions, "Ready")
		require.NotNil(t, cond)
		require.Equal(t, "NotBotAuthored", cond.Reason)

		// No wrapper pod was created.
		var pod corev1.Pod
		err = k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: agent.PodName(&got)}, &pod)
		require.Error(t, err, "no pod should be spawned for a non-bot-authored selfImprove task")
	})

	t.Run("bot PR author -> proceeds to Planning, pod spawned", func(t *testing.T) {
		fw := &fullFakeSCMWriter{prState: scm.PRState{Author: "tatara-bot"}}
		r := newFullFakeReconciler(t, fw)
		task := seedSelfImproveSpawn(t, "si-gate-ok", "si-proj-ok", "si-repo-ok", "si-scm-ok", "tatara-bot")

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		var got tatarav1alpha1.Task
		require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
		require.Equal(t, "Planning", got.Status.Phase, "bot-authored selfImprove must proceed to Planning")
		var pod corev1.Pod
		require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: agent.PodName(&got)}, &pod))
	})
}

func TestSelfImproveWriteBackAuthorshipGate(t *testing.T) {
	botScm := func() *tatarav1alpha1.ScmSpec {
		return &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot", MergePolicy: "afterApproval"}
	}

	t.Run("merge refused when PR author is not the bot", func(t *testing.T) {
		fw := &fullFakeSCMWriter{prState: scm.PRState{Author: "mallory", CIStatus: "success"}}
		r := newFullFakeReconciler(t, fw)
		task := seedWritebackKindTask(t, "si-wb-merge-bad", "si-wb-proj-mb", "si-wb-repo-mb", "si-wb-scm-mb",
			tatarav1alpha1.TaskSpec{
				Goal: "self-improve", Kind: "selfImprove",
				Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#9", IsPR: true, Number: 9},
			}, botScm())
		task.Status.PROutcome = &tatarav1alpha1.PROutcome{Action: "merge", Reason: "approved"}
		require.NoError(t, k8sClient.Status().Update(context.Background(), task))

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		require.False(t, fw.mergeCalled, "Merge must NOT be called for a non-bot-authored PR")
		var got tatarav1alpha1.Task
		require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
		cond := apimeta.FindStatusCondition(got.Status.Conditions, "WritebackPending")
		require.NotNil(t, cond)
		require.Equal(t, metav1.ConditionFalse, cond.Status)
		require.Equal(t, "AuthorshipWithheld", cond.Reason)
	})

	t.Run("merge deferred to auto-merge when PR author is the bot", func(t *testing.T) {
		fw := &fullFakeSCMWriter{prState: scm.PRState{Author: "tatara-bot"}}
		r := newFullFakeReconciler(t, fw)
		task := seedWritebackKindTask(t, "si-wb-merge-ok", "si-wb-proj-mo", "si-wb-repo-mo", "si-wb-scm-mo",
			tatarav1alpha1.TaskSpec{
				Goal: "self-improve", Kind: "selfImprove",
				Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#9", IsPR: true, Number: 9},
			}, botScm())
		task.Status.PROutcome = &tatarav1alpha1.PROutcome{Action: "merge", Reason: "approved"}
		require.NoError(t, k8sClient.Status().Update(context.Background(), task))

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)
		// push-CD: bot-authored PR passes the authorship gate, but pr_outcome=merge
		// no longer force-merges - native auto-merge owns the merge.
		require.False(t, fw.mergeCalled, "Merge must NOT be called - deferred to native auto-merge")
		var got tatarav1alpha1.Task
		require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
		cond := apimeta.FindStatusCondition(got.Status.Conditions, "WritebackPending")
		require.NotNil(t, cond)
		require.Equal(t, "PROutcomeApplied", cond.Reason)
	})

	t.Run("close refused when PR author is not the bot", func(t *testing.T) {
		fw := &fullFakeSCMWriter{prState: scm.PRState{Author: "mallory"}}
		r := newFullFakeReconciler(t, fw)
		task := seedWritebackKindTask(t, "si-wb-close-bad", "si-wb-proj-cb", "si-wb-repo-cb", "si-wb-scm-cb",
			tatarav1alpha1.TaskSpec{
				Goal: "self-improve", Kind: "selfImprove",
				Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#9", IsPR: true, Number: 9},
			}, botScm())
		task.Status.PROutcome = &tatarav1alpha1.PROutcome{Action: "close", Reason: "reject"}
		require.NoError(t, k8sClient.Status().Update(context.Background(), task))

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		require.False(t, fw.closePRCalled, "ClosePR must NOT be called for a non-bot-authored PR")
		var got tatarav1alpha1.Task
		require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
		cond := apimeta.FindStatusCondition(got.Status.Conditions, "WritebackPending")
		require.NotNil(t, cond)
		require.Equal(t, "AuthorshipWithheld", cond.Reason)
	})

	t.Run("close proceeds when PR author is the bot", func(t *testing.T) {
		fw := &fullFakeSCMWriter{prState: scm.PRState{Author: "tatara-bot"}}
		r := newFullFakeReconciler(t, fw)
		task := seedWritebackKindTask(t, "si-wb-close-ok", "si-wb-proj-co", "si-wb-repo-co", "si-wb-scm-co",
			tatarav1alpha1.TaskSpec{
				Goal: "self-improve", Kind: "selfImprove",
				Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#9", IsPR: true, Number: 9},
			}, botScm())
		task.Status.PROutcome = &tatarav1alpha1.PROutcome{Action: "close", Reason: "reject"}
		require.NoError(t, k8sClient.Status().Update(context.Background(), task))

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)
		require.True(t, fw.closePRCalled, "ClosePR must proceed for a bot-authored PR")
	})
}
