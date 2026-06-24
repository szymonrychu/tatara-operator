package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ---- fake SCM reader for backstop tests --------------------------------

// backstopFakeReader implements scm.SCMReader. openIssues and openPRs are keyed
// by "owner/repo"; items present in the map are open, absent = closed/merged.
type backstopFakeReader struct {
	// openIssues maps "owner/repo" -> []IssueRef that are currently open.
	openIssues map[string][]scm.IssueRef
	// openPRs maps "owner/repo" -> []PRRef that are currently open.
	openPRs map[string][]scm.PRRef
}

func (f *backstopFakeReader) ListOpenIssues(_ context.Context, owner, repo string) ([]scm.IssueRef, error) {
	key := owner + "/" + repo
	if f.openIssues == nil {
		return nil, nil
	}
	return f.openIssues[key], nil
}
func (f *backstopFakeReader) ListOpenPRs(_ context.Context, owner, repo string) ([]scm.PRRef, error) {
	key := owner + "/" + repo
	if f.openPRs == nil {
		return nil, nil
	}
	return f.openPRs[key], nil
}
func (f *backstopFakeReader) ListBoardItems(context.Context, scm.BoardRef) ([]scm.BoardItem, error) {
	return nil, nil
}
func (f *backstopFakeReader) GetCommitCIStatus(context.Context, string, string, string) (string, error) {
	return "", nil
}
func (f *backstopFakeReader) ListIssueComments(context.Context, string, string, int) ([]scm.IssueComment, error) {
	return nil, nil
}
func (f *backstopFakeReader) GetIssue(context.Context, string, string, int) (scm.IssueContent, error) {
	return scm.IssueContent{}, nil
}
func (f *backstopFakeReader) GetDefaultBranchHeadSHA(context.Context, string, string) (string, error) {
	return "", nil
}

// ---- helpers -----------------------------------------------------------

func makeTaskWithLedger(items []tatarav1alpha1.WorkItemRef) *tatarav1alpha1.Task {
	t := &tatarav1alpha1.Task{}
	t.Status.WorkItems = items
	return t
}

// ---- Task 10: refreshLedger tests -------------------------------------

// TestRefreshLedger_ClosedIssueAndAdvancedPR: issue was open but SCM now shows
// it closed; PR head SHA advanced -> refreshLedger returns true and updates State
// and HeadSHA. The LastRefreshedAt field is set on both entries.
func TestRefreshLedger_ClosedIssueAndAdvancedPR(t *testing.T) {
	oldSHA := "abc123"
	newSHA := "def456"

	task := makeTaskWithLedger([]tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
		{Provider: "github", Repo: "o/r", Number: 50, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadSHA: oldSHA},
	})

	reader := &backstopFakeReader{
		// Issue #7 NOT in open list -> closed.
		openIssues: map[string][]scm.IssueRef{
			"o/r": {},
		},
		// PR #50 in open list but with a new SHA.
		openPRs: map[string][]scm.PRRef{
			"o/r": {{Repo: "o/r", Number: 50, HeadSHA: newSHA}},
		},
	}

	changed := refreshLedger(context.Background(), reader, task)
	require.True(t, changed, "expected refreshLedger to return changed=true")

	// Issue #7 must now be closed.
	var issueEntry *tatarav1alpha1.WorkItemRef
	for i := range task.Status.WorkItems {
		if task.Status.WorkItems[i].Number == 7 && task.Status.WorkItems[i].Kind == tatarav1alpha1.WorkItemIssue {
			issueEntry = &task.Status.WorkItems[i]
		}
	}
	require.NotNil(t, issueEntry, "issue entry not found")
	require.Equal(t, tatarav1alpha1.WIClosed, issueEntry.State, "issue #7 must be closed")
	require.NotNil(t, issueEntry.LastRefreshedAt, "issue #7 LastRefreshedAt must be set")

	// PR #50 head SHA must be updated.
	var prEntry *tatarav1alpha1.WorkItemRef
	for i := range task.Status.WorkItems {
		if task.Status.WorkItems[i].Number == 50 && task.Status.WorkItems[i].Kind == tatarav1alpha1.WorkItemPR {
			prEntry = &task.Status.WorkItems[i]
		}
	}
	require.NotNil(t, prEntry, "PR entry not found")
	require.Equal(t, newSHA, prEntry.HeadSHA, "PR #50 HeadSHA must be updated")
	require.Equal(t, tatarav1alpha1.WIOpen, prEntry.State, "PR #50 must still be open")
	require.NotNil(t, prEntry.LastRefreshedAt, "PR #50 LastRefreshedAt must be set")
}

// TestRefreshLedger_NoChange: issue is still open, PR SHA unchanged -> no-op, returns false.
func TestRefreshLedger_NoChange(t *testing.T) {
	sha := "abc123"
	now := metav1.NewTime(time.Now())

	task := makeTaskWithLedger([]tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen, LastRefreshedAt: &now},
		{Provider: "github", Repo: "o/r", Number: 50, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadSHA: sha, LastRefreshedAt: &now},
	})

	reader := &backstopFakeReader{
		openIssues: map[string][]scm.IssueRef{
			"o/r": {{Repo: "o/r", Number: 7}}, // still open
		},
		openPRs: map[string][]scm.PRRef{
			"o/r": {{Repo: "o/r", Number: 50, HeadSHA: sha}}, // same SHA
		},
	}

	changed := refreshLedger(context.Background(), reader, task)
	require.False(t, changed, "no state change expected when SCM matches ledger")
}

// TestRefreshLedger_ClosedPR: PR absent from open list -> state transitions to WIClosed.
func TestRefreshLedger_ClosedPR(t *testing.T) {
	task := makeTaskWithLedger([]tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 50, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadSHA: "sha1"},
	})

	reader := &backstopFakeReader{
		openPRs: map[string][]scm.PRRef{
			"o/r": {}, // empty -> PR #50 is no longer open
		},
	}

	changed := refreshLedger(context.Background(), reader, task)
	require.True(t, changed)

	var pr *tatarav1alpha1.WorkItemRef
	for i := range task.Status.WorkItems {
		if task.Status.WorkItems[i].Number == 50 {
			pr = &task.Status.WorkItems[i]
		}
	}
	require.NotNil(t, pr)
	require.Equal(t, tatarav1alpha1.WIClosed, pr.State, "PR not in open list -> closed")
}

// TestRefreshLedger_AlreadyTerminalSkipped: entries already in a terminal state
// (closed/merged) are not re-fetched and no change is reported for them.
func TestRefreshLedger_AlreadyTerminalSkipped(t *testing.T) {
	task := makeTaskWithLedger([]tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIClosed},
	})

	reader := &backstopFakeReader{
		// Even if the reader returns something, already-terminal entries are skipped.
		openIssues: map[string][]scm.IssueRef{"o/r": {}},
	}

	changed := refreshLedger(context.Background(), reader, task)
	require.False(t, changed, "already-terminal entry must not generate a change")
}

// ---- Task 11: backstopAction tests ------------------------------------

// TestBackstopAction_None_NoOpenPR: no open PR in ledger -> None.
func TestBackstopAction_None_NoOpenPR(t *testing.T) {
	task := makeTaskWithLedger([]tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
	})
	task.Status.PodName = ""

	dec := backstopAction(task)
	require.Equal(t, bsActionNone, dec)
}

// TestBackstopAction_None_LivePod: open MR in ledger but a live pod is present -> None.
func TestBackstopAction_None_LivePod(t *testing.T) {
	task := makeTaskWithLedger([]tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
		{Provider: "github", Repo: "o/r", Number: 50, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen},
	})
	task.Status.PodName = "agent-pod-xyz" // live pod

	dec := backstopAction(task)
	require.Equal(t, bsActionNone, dec)
}

// TestBackstopAction_CloseObsolete: all source/closes issues are closed and
// there is an open MR -> CloseObsolete.
func TestBackstopAction_CloseObsolete(t *testing.T) {
	task := makeTaskWithLedger([]tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIClosed},
		{Provider: "github", Repo: "o/r", Number: 8, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleCloses, State: tatarav1alpha1.WIClosed},
		{Provider: "github", Repo: "o/r", Number: 50, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen},
	})
	task.Status.PodName = ""

	dec := backstopAction(task)
	require.Equal(t, bsActionCloseObsolete, dec)
}

// TestBackstopAction_Reactivate: open source issue + open MR + no live pod -> Reactivate.
func TestBackstopAction_Reactivate(t *testing.T) {
	task := makeTaskWithLedger([]tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
		{Provider: "github", Repo: "o/r", Number: 50, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen},
	})
	task.Status.PodName = ""

	dec := backstopAction(task)
	require.Equal(t, bsActionReactivate, dec)
}

// TestBackstopAction_PureRefresh: no open MR in ledger, issue still open -> None.
func TestBackstopAction_PureRefresh(t *testing.T) {
	task := makeTaskWithLedger([]tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
	})
	task.Status.PodName = ""

	dec := backstopAction(task)
	require.Equal(t, bsActionNone, dec)
}
