package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestDoWriteBackIncidentDoesNotOpenPR verifies that an incident Task (a
// project-scoped kind with an empty RepositoryRef) never reaches the bare
// repository Get in writeBackOpenChange. Before the fix, the incident kind fell
// through doWriteBack's switch to writeBackOpenChange, which Gets a Repository by
// the empty RepositoryRef and fails `resource name may not be empty`
// (`Repository.tatara.dev "" not found` in-cluster), so WritebackPending never
// cleared and the reconcile error-looped after the task already terminated
// Succeeded (the incident-qe-bw5hw incident). It must clear WritebackPending
// cleanly instead.
func TestDoWriteBackIncidentDoesNotOpenPR(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	// seedWritebackKindTask forces RepositoryRef = repo; an incident Task is
	// project-scoped and must carry an EMPTY RepositoryRef, so blank it.
	task := seedWritebackKindTask(t, "psw-incident-task", "psw-incident-proj",
		"psw-incident-repo", "psw-incident-scm",
		tatarav1alpha1.TaskSpec{Goal: "investigate alert", Kind: "incident"}, nil)
	task.Spec.RepositoryRef = ""
	require.NoError(t, k8sClient.Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err, "incident writeback must not error-loop on empty RepositoryRef")

	require.Zero(t, fw.openCalls, "incident Task must not call OpenChange")

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status, "WritebackPending must be cleared")
	require.Equal(t, "ProjectScopedComplete", cond.Reason)
}

// TestIsProjectScopedKind locks the project-scoped kind set: these never carry a
// RepositoryRef and must never reach writeBackOpenChange's repository Get.
func TestIsProjectScopedKind(t *testing.T) {
	cases := []struct {
		kind string
		want bool
	}{
		{"incident", true},
		{"brainstorm", true},
		{"healthCheck", true},
		{"implement", false},
		{"review", false},
		{"selfImprove", false},
		{"triageIssue", false},
		{"issueLifecycle", false},
		{"", false},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			require.Equal(t, tc.want, tatarav1alpha1.IsProjectScopedKind(tc.kind))
		})
	}
}
