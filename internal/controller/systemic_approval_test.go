package controller

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func approvedTask(name, repoSlug string, number int, maintainer string) tatarav1alpha1.Task {
	return tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec: tatarav1alpha1.TaskSpec{
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: repoSlug + "#" + strconv.Itoa(number), Number: number},
		},
		Status: tatarav1alpha1.TaskStatus{ApprovedByMaintainer: maintainer},
	}
}

// TestFilterSystemicGroupByApproval: only maintainer-approved siblings survive; the
// declined/unapproved ones fall into the unapproved list.
func TestFilterSystemicGroupByApproval(t *testing.T) {
	tasks := []tatarav1alpha1.Task{
		approvedTask("lead", "o/r1", 12, "maint"),
		approvedTask("sib7", "o/r1", 7, "maint"),   // approved sibling
		approvedTask("cross3", "o/r2", 3, "maint"), // approved cross-repo
		// #8 has a task but NOT approved (declined or pending): no ApprovedByMaintainer.
		{
			ObjectMeta: metav1.ObjectMeta{Name: "sib8", Namespace: "tatara"},
			Spec:       tatarav1alpha1.TaskSpec{Source: &tatarav1alpha1.TaskSource{IssueRef: "o/r1#8", Number: 8}},
		},
	}
	sg := &tatarav1alpha1.SystemicGroup{
		SystemicID:       "abc",
		SameRepoSiblings: []int{7, 8, 9}, // 7 approved, 8 has-unapproved-task, 9 no task
		CrossRepo:        []string{"o/r2#3 - approved thing", "o/r2#4 - unapproved thing"},
	}

	filtered, unapproved := filterSystemicGroupByApproval(sg, "o/r1", tasks)
	require.Equal(t, []int{7}, filtered.SameRepoSiblings, "only #7 is maintainer-approved")
	require.ElementsMatch(t, []int{8, 9}, unapproved, "#8 (unapproved task) and #9 (no task) are unapproved")
	require.Equal(t, []string{"o/r2#3 - approved thing"}, filtered.CrossRepo, "only the approved cross-repo ref survives")
	require.Equal(t, "abc", filtered.SystemicID)
}

func TestFilterSystemicGroupByApproval_Nil(t *testing.T) {
	sg, un := filterSystemicGroupByApproval(nil, "o/r1", nil)
	require.Nil(t, sg)
	require.Nil(t, un)
}

// TestNeutralizeUnapprovedCloses: close directives for unapproved siblings become
// plain references; approved siblings and the lead stay closable; cross-repo forms
// are untouched.
func TestNeutralizeUnapprovedCloses(t *testing.T) {
	body := "Fixes the thing.\n\nCloses #12\nCloses #8\nfixes #7\nResolved #9\ncloses o/r2#3"
	out := neutralizeUnapprovedCloses(body, map[int]bool{8: true, 9: true})
	require.Contains(t, out, "Closes #12", "lead close intact")
	require.Contains(t, out, "fixes #7", "approved sibling close intact")
	require.Contains(t, out, "refs #8", "unapproved #8 neutralized")
	require.Contains(t, out, "refs #9", "unapproved #9 neutralized")
	require.NotContains(t, out, "Closes #8")
	require.NotContains(t, out, "Resolved #9")
	require.Contains(t, out, "closes o/r2#3", "cross-repo close form untouched")
}

func TestNeutralizeUnapprovedCloses_Empty(t *testing.T) {
	body := "Closes #8"
	require.Equal(t, body, neutralizeUnapprovedCloses(body, nil), "no unapproved set: body unchanged")
}

// TestElectSystemicLeads_ExcludesDeclined: a maintainer-declined issue is never
// grouped (as lead or sibling), so approving another member cannot force-close it.
func TestElectSystemicLeads_ExcludesDeclined(t *testing.T) {
	cands := []candidate{
		{repo: "o/r1", number: 12, labels: []string{"tatara/systemic-abc"}, title: "lead"},
		{repo: "o/r1", number: 15, labels: []string{"tatara/systemic-abc"}, title: "approved sib"},
		{repo: "o/r1", number: 18, labels: []string{"tatara/systemic-abc", "tatara-declined"}, title: "declined sib"},
	}
	got := electSystemicLeads(cands, "tatara-declined")
	lead := got["o/r1#12"]
	require.True(t, lead.isLead)
	require.Equal(t, []int{15}, lead.sameRepoSiblings, "declined #18 must be excluded from the group")
	_, present := got["o/r1#18"]
	require.False(t, present, "declined issue is not part of any systemic decision")
}

// TestReconciler_SystemicApprovalWiring proves the reconciler methods list Tasks
// and apply the approval filter: withApprovedSystemicGroup narrows the prompt group
// and unapprovedSystemicSiblings reports the stripped set.
func TestReconciler_SystemicApprovalWiring(t *testing.T) {
	ctx := context.Background()
	lead := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "wire-lead", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "wire-proj", Kind: "issueLifecycle", Goal: "g",
			Source:        &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/wr#12", Number: 12},
			SystemicGroup: &tatarav1alpha1.SystemicGroup{SystemicID: "abc", SameRepoSiblings: []int{7, 8}},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, lead))
	// Sibling #7 approved; #8 has a task with no recorded approval.
	sib7 := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "wire-sib7", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "wire-proj", Kind: "issueLifecycle", Goal: "g", Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/wr#7", Number: 7}},
	}
	require.NoError(t, k8sClient.Create(ctx, sib7))
	sib7.Status.ApprovedByMaintainer = "maint"
	require.NoError(t, k8sClient.Status().Update(ctx, sib7))
	sib8 := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "wire-sib8", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "wire-proj", Kind: "issueLifecycle", Goal: "g", Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/wr#8", Number: 8}},
	}
	require.NoError(t, k8sClient.Create(ctx, sib8))

	r := &TaskReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	got := getTask(t, "wire-lead")

	pt := r.withApprovedSystemicGroup(ctx, got)
	require.Equal(t, []int{7}, pt.Spec.SystemicGroup.SameRepoSiblings, "prompt group keeps only approved #7")
	// original task object unchanged (shallow copy)
	require.Equal(t, []int{7, 8}, got.Spec.SystemicGroup.SameRepoSiblings, "original spec group not mutated")

	un := r.unapprovedSystemicSiblings(ctx, got)
	require.True(t, un[8], "#8 reported unapproved for writeback strip")
	require.False(t, un[7], "#7 not in unapproved set")
}
