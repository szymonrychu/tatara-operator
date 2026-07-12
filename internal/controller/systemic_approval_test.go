package controller

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
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

	filtered, unapproved, fannedOut := filterSystemicGroupByApproval(sg, "o/r1", false, tasks)
	require.Equal(t, []int{7}, filtered.SameRepoSiblings, "only #7 is maintainer-approved")
	require.ElementsMatch(t, []int{8, 9}, unapproved, "#8 (unapproved task) and #9 (no task) are unapproved")
	require.Equal(t, []string{"o/r2#3 - approved thing"}, filtered.CrossRepo, "only the approved cross-repo ref survives")
	require.Equal(t, "abc", filtered.SystemicID)
	require.Empty(t, fannedOut, "leadApproved=false must never fan out, regardless of locked siblings")
}

// TestIssueHasRecordedApproval_LeadLedgerSelfMatchExcluded verifies F2: a
// systemic lead Task's OWN ledger is seeded (seedLedgerFromSpec) with a
// role:closes WorkItem entry for every sibling it references. Before the fix,
// issueHasRecordedApproval matched on ANY ledger role (TaskMatchesItem), so
// the lead's own ApprovedByMaintainer made it look like EVERY sibling was
// independently approved too - making the systemic sibling-approval gate
// vacuous. Source-identity-only matching must reject this self-match.
func TestIssueHasRecordedApproval_LeadLedgerSelfMatchExcluded(t *testing.T) {
	lead := approvedTask("lead", "o/r1", 12, "maint")
	// Simulate seedLedgerFromSpec: the lead's own ledger carries a role:closes
	// entry for sibling #7 - NOT a role:source entry.
	lead.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Repo: "o/r1", Number: 7, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleCloses},
	}
	tasks := []tatarav1alpha1.Task{lead}

	require.False(t, issueHasRecordedApproval(tasks, "o/r1", 7),
		"the lead's own role:closes ledger entry for #7 must NOT count as #7's own approval")
	require.True(t, issueHasRecordedApproval(tasks, "o/r1", 12),
		"the lead's own source identity (#12) must still resolve to its own approval")
}

// TestIssueIsImplementationLocked_LeadLedgerSelfMatchExcluded is the
// issueIsImplementationLocked half of F2's self-match bug.
func TestIssueIsImplementationLocked_LeadLedgerSelfMatchExcluded(t *testing.T) {
	lead := tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "lead", Namespace: "tatara"},
		Spec: tatarav1alpha1.TaskSpec{
			Source: &tatarav1alpha1.TaskSource{IssueRef: "o/r1#12", Number: 12},
		},
		Status: tatarav1alpha1.TaskStatus{
			ImplementationLocked: true,
			WorkItems: []tatarav1alpha1.WorkItemRef{
				{Repo: "o/r1", Number: 7, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleCloses},
			},
		},
	}
	tasks := []tatarav1alpha1.Task{lead}

	require.False(t, issueIsImplementationLocked(tasks, "o/r1", 7),
		"the lead's own locked status must NOT be reported as sibling #7's own lock state")
	require.True(t, issueIsImplementationLocked(tasks, "o/r1", 12),
		"the lead's own source identity (#12) must still resolve to its own lock state")
}

// TestIssueIsImplementationLocked_NewestTaskWins_StaleLockedIgnored verifies
// F3: a stale, terminal, locked Task from an EARLIER triage cycle must not
// outlive its NEWER, unlocked successor (a re-triage after new human
// questions reopened the conversation).
func TestIssueIsImplementationLocked_NewestTaskWins_StaleLockedIgnored(t *testing.T) {
	older := lockedTask("old-clarify", "o/r1", 7)
	older.CreationTimestamp = metav1.NewTime(time.Unix(1000, 0))
	newer := tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "new-clarify", Namespace: "tatara", CreationTimestamp: metav1.NewTime(time.Unix(2000, 0))},
		Spec: tatarav1alpha1.TaskSpec{
			Source: &tatarav1alpha1.TaskSource{IssueRef: "o/r1#7", Number: 7},
		},
		// Unlocked: the re-opened conversation has not settled again yet.
	}
	tasks := []tatarav1alpha1.Task{older, newer}

	require.False(t, issueIsImplementationLocked(tasks, "o/r1", 7),
		"the newer, unlocked Task must win over the stale locked predecessor")
}

// TestIssueIsImplementationLocked_NewestTaskWins_FreshLockHonored is the
// reverse direction: a newer LOCKED Task must be honored even when an older
// Task for the same issue was unlocked.
func TestIssueIsImplementationLocked_NewestTaskWins_FreshLockHonored(t *testing.T) {
	older := tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "old-clarify", Namespace: "tatara", CreationTimestamp: metav1.NewTime(time.Unix(1000, 0))},
		Spec: tatarav1alpha1.TaskSpec{
			Source: &tatarav1alpha1.TaskSource{IssueRef: "o/r1#7", Number: 7},
		},
	}
	newer := lockedTask("new-clarify", "o/r1", 7)
	newer.CreationTimestamp = metav1.NewTime(time.Unix(2000, 0))
	tasks := []tatarav1alpha1.Task{older, newer}

	require.True(t, issueIsImplementationLocked(tasks, "o/r1", 7),
		"the newer, locked Task must be honored")
}

// TestWithApprovedSystemicGroup_FanoutMetricFires_ForGenuineLockedSibling
// verifies the fan-out counter/log still fire for a REAL (non-self-match)
// locked sibling after the F2 identity-matching fix - the fix must narrow
// matching to source identity, not silently break the legitimate fan-out path.
func TestWithApprovedSystemicGroup_FanoutMetricFires_ForGenuineLockedSibling(t *testing.T) {
	ctx := context.Background()
	lead := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "wire-lead-fo", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "wire-proj-fo", Kind: "issueLifecycle", Goal: "g",
			Source:        &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/wrfo#12", Number: 12},
			SystemicGroup: &tatarav1alpha1.SystemicGroup{SystemicID: "abc", SameRepoSiblings: []int{7}},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, lead))
	lead.Status.ApprovedByMaintainer = "maint"
	require.NoError(t, k8sClient.Status().Update(ctx, lead))

	sib7 := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "wire-sib7-fo", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "wire-proj-fo", Kind: "clarify", Goal: "g", Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/wrfo#7", Number: 7}},
	}
	require.NoError(t, k8sClient.Create(ctx, sib7))
	sib7.Status.ImplementationLocked = true
	require.NoError(t, k8sClient.Status().Update(ctx, sib7))

	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := &TaskReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Metrics: m}
	got := getTask(t, "wire-lead-fo")

	pt := r.withApprovedSystemicGroup(ctx, got)
	require.Equal(t, []int{7}, pt.Spec.SystemicGroup.SameRepoSiblings, "genuinely locked sibling #7 still fans out")
	require.Equal(t, float64(1), testutil.ToFloat64(m.SystemicApprovalFanoutCounter()),
		"tatara_systemic_approval_fanout_total must increment for a genuine fan-out")
}

func lockedTask(name, repoSlug string, number int) tatarav1alpha1.Task {
	return tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec: tatarav1alpha1.TaskSpec{
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: repoSlug + "#" + strconv.Itoa(number), Number: number},
		},
		Status: tatarav1alpha1.TaskStatus{ImplementationLocked: true},
	}
}

func TestFilterSystemicGroupByApproval_FansOutToLockedSiblingsWhenLeadApproved(t *testing.T) {
	tasks := []tatarav1alpha1.Task{
		approvedTask("lead", "o/r1", 12, "maint"),
		lockedTask("sib7", "o/r1", 7), // no direct approval, but implementation-locked
		{
			ObjectMeta: metav1.ObjectMeta{Name: "sib8", Namespace: "tatara"},
			Spec:       tatarav1alpha1.TaskSpec{Source: &tatarav1alpha1.TaskSource{IssueRef: "o/r1#8", Number: 8}},
			// sib8: neither approved nor locked - stays excluded.
		},
	}
	sg := &tatarav1alpha1.SystemicGroup{
		SystemicID:       "abc",
		SameRepoSiblings: []int{7, 8},
	}

	filtered, unapproved, fannedOut := filterSystemicGroupByApproval(sg, "o/r1", true, tasks)
	require.Equal(t, []int{7}, filtered.SameRepoSiblings, "locked sibling #7 fans out under an approved lead")
	require.Equal(t, []int{8}, unapproved, "unlocked, unapproved sibling #8 stays excluded")
	require.Equal(t, []string{"o/r1#7"}, fannedOut, "fan-out audit trail records #7")
}

func TestFilterSystemicGroupByApproval_NoFanOutWhenLeadNotApproved(t *testing.T) {
	tasks := []tatarav1alpha1.Task{
		lockedTask("sib7", "o/r1", 7),
	}
	sg := &tatarav1alpha1.SystemicGroup{SystemicID: "abc", SameRepoSiblings: []int{7}}

	filtered, unapproved, fannedOut := filterSystemicGroupByApproval(sg, "o/r1", false, tasks)
	require.Empty(t, filtered.SameRepoSiblings, "a locked sibling never fans out without the lead's own approval")
	require.Equal(t, []int{7}, unapproved)
	require.Empty(t, fannedOut)
}

func TestFilterSystemicGroupByApproval_Nil(t *testing.T) {
	sg, un, fo := filterSystemicGroupByApproval(nil, "o/r1", false, nil)
	require.Nil(t, sg)
	require.Nil(t, un)
	require.Nil(t, fo)
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
