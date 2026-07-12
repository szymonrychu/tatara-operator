package controller

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

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

// TestFilterSystemicGroupByApproval_UnapprovedSiblingAlwaysStripped: an
// unapproved sibling is stripped unconditionally - an approved lead's own
// approval never extends to it, whatever state its own conversation reached.
func TestFilterSystemicGroupByApproval_UnapprovedSiblingAlwaysStripped(t *testing.T) {
	tasks := []tatarav1alpha1.Task{
		approvedTask("lead", "o/r1", 12, "maint"),
		{
			ObjectMeta: metav1.ObjectMeta{Name: "sib7", Namespace: "tatara"},
			Spec:       tatarav1alpha1.TaskSpec{Source: &tatarav1alpha1.TaskSource{IssueRef: "o/r1#7", Number: 7}},
		},
	}
	sg := &tatarav1alpha1.SystemicGroup{SystemicID: "abc", SameRepoSiblings: []int{7}}

	filtered, unapproved := filterSystemicGroupByApproval(sg, "o/r1", tasks)
	require.Empty(t, filtered.SameRepoSiblings, "an unapproved sibling never co-resolves with an approved lead")
	require.Equal(t, []int{7}, unapproved)
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

// TestNewestTaskForSource_TieBreakDeterministic is the m6 regression:
// metav1.Time is second-granularity, so two Tasks created in the same second
// have equal CreationTimestamp and CreationTimestamp.Before() never picks a
// winner - the scan order (map/list iteration) decided non-deterministically.
// A stable name tie-break must pick the same winner regardless of input order.
func TestNewestTaskForSource_TieBreakDeterministic(t *testing.T) {
	same := metav1.NewTime(time.Unix(5000, 0))
	a := tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-a", Namespace: "tatara", CreationTimestamp: same},
		Spec:       tatarav1alpha1.TaskSpec{Source: &tatarav1alpha1.TaskSource{IssueRef: "o/r1#7", Number: 7}},
	}
	b := tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-b", Namespace: "tatara", CreationTimestamp: same},
		Spec:       tatarav1alpha1.TaskSpec{Source: &tatarav1alpha1.TaskSource{IssueRef: "o/r1#7", Number: 7}},
	}

	got1 := newestTaskForSource([]tatarav1alpha1.Task{a, b}, "o/r1", 7)
	got2 := newestTaskForSource([]tatarav1alpha1.Task{b, a}, "o/r1", 7)
	require.NotNil(t, got1)
	require.NotNil(t, got2)
	require.Equal(t, got1.Name, got2.Name, "the tie-break winner must not depend on input order")
}

// TestIssueHasRecordedApproval_NewestTaskWins is the m7 regression:
// issueHasRecordedApproval scanned ANY matching Task, so a stale approved Task
// from an earlier cycle could outlive a newer, unapproved re-triage for the
// same source identity.
func TestIssueHasRecordedApproval_NewestTaskWins(t *testing.T) {
	older := approvedTask("old", "o/r1", 7, "maint")
	older.CreationTimestamp = metav1.NewTime(time.Unix(1000, 0))
	newer := tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "new", Namespace: "tatara", CreationTimestamp: metav1.NewTime(time.Unix(2000, 0))},
		Spec:       tatarav1alpha1.TaskSpec{Source: &tatarav1alpha1.TaskSource{IssueRef: "o/r1#7", Number: 7}},
		// Unapproved: the re-triage has not been approved again.
	}
	tasks := []tatarav1alpha1.Task{older, newer}

	require.False(t, issueHasRecordedApproval(tasks, "o/r1", 7),
		"the newer, unapproved Task must win over the stale approved predecessor")
}

// TestIssueHasRecordedApproval_AutoApproveSentinelCounts documents that the
// auto-approve sentinel ("<tatara:auto:kind>") IS a recorded approval for this
// check's purposes (accepted design) - the function must not special-case it
// away.
func TestIssueHasRecordedApproval_AutoApproveSentinelCounts(t *testing.T) {
	tasks := []tatarav1alpha1.Task{approvedTask("auto", "o/r1", 7, "<tatara:auto:clarify>")}
	require.True(t, issueHasRecordedApproval(tasks, "o/r1", 7),
		"the auto-approve sentinel must count as a recorded approval")
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

// TestNeutralizeUnapprovedCloses_LeakingForms is the M2 regression: the
// original regex only matched a narrow keyword set immediately followed by
// "#N", so every one of these agent-authored forms survived neutralization
// and would force-close an unapproved sibling on merge.
func TestNeutralizeUnapprovedCloses_LeakingForms(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"colon", "Closes: #7"},
		{"closing_ing_form", "Closing #7"},
		{"fixing_ing_form", "Fixing #7"},
		{"resolving_ing_form", "Resolving #7"},
		{"implements_keyword", "Implements #7"},
		{"issue_word", "Closes issue #7"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := neutralizeUnapprovedCloses(c.body, map[int]bool{7: true})
			require.NotContains(t, out, "#7\n", out) // sanity: body was rewritten
			require.Regexp(t, `refs #7`, out, "unapproved #7 must be neutralized in %q -> %q", c.body, out)
			require.NotRegexp(t, `(?i)\b(?:clos|fix|resolv|implement)\w*\s*:?\s*(?:issues?\s+)?#7\b`, out,
				"no closing directive for #7 may survive in %q -> %q", c.body, out)
		})
	}
}

// TestNeutralizeUnapprovedCloses_CommaList: every unapproved ref in a
// comma-separated list must be neutralized, not only the first.
func TestNeutralizeUnapprovedCloses_CommaList(t *testing.T) {
	out := neutralizeUnapprovedCloses("Closes #7, #8", map[int]bool{7: true, 8: true})
	require.Contains(t, out, "refs #7")
	require.Contains(t, out, "refs #8")
	require.NotContains(t, out, "Closes #7")
	require.NotContains(t, out, "Closes #8")
}

// TestNeutralizeUnapprovedCloses_MixedList: a list with both approved and
// unapproved refs neutralizes only the unapproved ones, keeping the approved
// ref closable.
func TestNeutralizeUnapprovedCloses_MixedList(t *testing.T) {
	out := neutralizeUnapprovedCloses("Closes #7, #8", map[int]bool{7: true})
	require.Contains(t, out, "refs #7", "unapproved #7 neutralized")
	require.Contains(t, out, "Closes #8", "approved #8 stays closable")
	require.NotContains(t, out, "Closes #7")
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

// TestUnapprovedSystemicSiblings_ListErrorFailsClosed is the M3 regression: on
// a client List error, the function used to return nil (no unapproved
// siblings), which the writeback caller reads as "nothing to neutralize" -
// every "Closes #N" in the PR body survives untouched. It must fail CLOSED:
// treat every same-repo sibling as unapproved so neutralizeUnapprovedCloses
// strips all of them.
func TestUnapprovedSystemicSiblings_ListErrorFailsClosed(t *testing.T) {
	ctx := context.Background()
	errClient := fake.NewClientBuilder().
		WithScheme(k8sClient.Scheme()).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				return errors.New("injected list failure")
			},
		}).Build()
	r := &TaskReconciler{Client: errClient, Scheme: k8sClient.Scheme()}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "lead", Namespace: "tatara"},
		Spec: tatarav1alpha1.TaskSpec{
			Source:        &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r1#12", Number: 12},
			SystemicGroup: &tatarav1alpha1.SystemicGroup{SystemicID: "abc", SameRepoSiblings: []int{7, 8}},
		},
	}

	un := r.unapprovedSystemicSiblings(ctx, task)
	require.True(t, un[7], "fail-closed: #7 must be treated as unapproved on a List error")
	require.True(t, un[8], "fail-closed: #8 must be treated as unapproved on a List error")
}

// TestWithApprovedSystemicGroup_ListErrorFailsClosed is withApprovedSystemicGroup's
// half of M3: on a List error it used to return task UNCHANGED, handing the agent
// prompt the full unfiltered group (every member, approved or not). It must fail
// CLOSED: the returned copy's SystemicGroup carries no members.
func TestWithApprovedSystemicGroup_ListErrorFailsClosed(t *testing.T) {
	ctx := context.Background()
	errClient := fake.NewClientBuilder().
		WithScheme(k8sClient.Scheme()).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				return errors.New("injected list failure")
			},
		}).Build()
	r := &TaskReconciler{Client: errClient, Scheme: k8sClient.Scheme()}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "lead", Namespace: "tatara"},
		Spec: tatarav1alpha1.TaskSpec{
			Source:        &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r1#12", Number: 12},
			SystemicGroup: &tatarav1alpha1.SystemicGroup{SystemicID: "abc", SameRepoSiblings: []int{7, 8}},
		},
		Status: tatarav1alpha1.TaskStatus{ApprovedByMaintainer: "maint"},
	}

	pt := r.withApprovedSystemicGroup(ctx, task)
	require.NotNil(t, pt.Spec.SystemicGroup)
	require.Empty(t, pt.Spec.SystemicGroup.SameRepoSiblings, "fail-closed: no member fans out on a List error")
	require.Empty(t, pt.Spec.SystemicGroup.CrossRepo, "fail-closed: no cross-repo member fans out on a List error")
}
