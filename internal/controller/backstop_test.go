package controller

import (
	"context"
	"fmt"
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
	// prListErr maps "owner/repo" -> true to force ListOpenPRs to error (simulates
	// a transient GitHub/GitLab list failure mid-sweep).
	prListErr map[string]bool
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
	if f.prListErr != nil && f.prListErr[key] {
		return nil, fmt.Errorf("transient list error for %s", key)
	}
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

	changed, confirmed := refreshLedger(context.Background(), reader, task)
	require.True(t, changed, "expected refreshLedger to return changed=true")
	require.True(t, confirmed["o/r"], "o/r PR list fetch succeeded -> repo confirmed")

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

	changed, _ := refreshLedger(context.Background(), reader, task)
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

	changed, _ := refreshLedger(context.Background(), reader, task)
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

	changed, _ := refreshLedger(context.Background(), reader, task)
	require.False(t, changed, "already-terminal entry must not generate a change")
}

// TestRefreshLedger_PRListErrorNotConfirmed: when ListOpenPRs errors for a repo,
// the open-PR entry is left untouched (still WIOpen, seeded value) and the repo is
// NOT in the confirmed set. The sweep relies on this to avoid acting on
// never-confirmed seed-open state (migration-safety: ~1148 lazily-seeded tasks).
func TestRefreshLedger_PRListErrorNotConfirmed(t *testing.T) {
	task := makeTaskWithLedger([]tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 50, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadSHA: "sha1"},
	})

	reader := &backstopFakeReader{
		prListErr: map[string]bool{"o/r": true},
	}

	changed, confirmed := refreshLedger(context.Background(), reader, task)
	require.False(t, changed, "no change when the fetch failed")
	require.False(t, confirmed["o/r"], "failed PR fetch must NOT confirm the repo")
	require.Equal(t, tatarav1alpha1.WIOpen, task.Status.WorkItems[0].State, "seed-open state untouched on fetch error")
}

// ---- Task 11: backstopAction tests ------------------------------------

// TestBackstopAction_None_NoOpenPR: no open PR in ledger -> None.
func TestBackstopAction_None_NoOpenPR(t *testing.T) {
	task := makeTaskWithLedger([]tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
	})

	dec := backstopAction(task, false)
	require.Equal(t, bsActionNone, dec)
}

// TestBackstopAction_None_LivePod: open MR in ledger but a live pod is present -> None.
// podLive=true is the authoritative liveness signal (an actual pod Get), NOT the
// permanent Status.PodName flag.
func TestBackstopAction_None_LivePod(t *testing.T) {
	task := makeTaskWithLedger([]tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
		{Provider: "github", Repo: "o/r", Number: 50, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen},
	})

	dec := backstopAction(task, true) // live pod
	require.Equal(t, bsActionNone, dec)
}

// TestBackstopAction_Reactivate_PodNameSetButGone: a real stranded task always
// carries a non-empty Status.PodName (set once at pod creation, never cleared).
// When the pod is GONE (podLive=false), the backstop must still reactivate - the
// PodName presence must NOT short-circuit. This is the core production-correctness
// case: regression-guards finding "PodName false-guard makes Tier-2 inert".
func TestBackstopAction_Reactivate_PodNameSetButGone(t *testing.T) {
	task := makeTaskWithLedger([]tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
		{Provider: "github", Repo: "o/r", Number: 50, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen},
	})
	task.Status.PodName = "wrapper-stranded" // set once, never cleared on park

	dec := backstopAction(task, false) // pod is gone
	require.Equal(t, bsActionReactivate, dec, "PodName presence must not block reactivation when pod is gone")
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

	dec := backstopAction(task, false)
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

	dec := backstopAction(task, false)
	require.Equal(t, bsActionReactivate, dec)
}

// TestBackstopAction_None_OpenPRNoSourceIssue: open MR + no live pod but ZERO
// role:source/role:closes issues recorded in the ledger -> None (NOT Reactivate).
// Spec section 4: reactivate is "open MR + at least one open source/closes issue".
// A bare openedPR entry (e.g. review task, or a task before its source is
// projected) must not spawn an implement agent for a driverless PR.
func TestBackstopAction_None_OpenPRNoSourceIssue(t *testing.T) {
	task := makeTaskWithLedger([]tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 50, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen},
	})

	dec := backstopAction(task, false)
	require.Equal(t, bsActionNone, dec)
}

// TestBackstopAction_None_ReviewedPR: a human PR under review (role:reviewed, not
// role:openedPR) must never be targeted by the backstop, even when no live pod
// and all source issues are closed. backstopAction's open-PR detection must use
// the SAME predicate as openPRCandidate (Role==RoleOpenedPR).
func TestBackstopAction_None_ReviewedPR(t *testing.T) {
	task := makeTaskWithLedger([]tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIClosed},
		{Provider: "github", Repo: "o/r", Number: 50, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleReviewed, State: tatarav1alpha1.WIOpen},
	})

	dec := backstopAction(task, false)
	require.Equal(t, bsActionNone, dec, "role:reviewed (human) PRs must never be backstop-targeted")
}

// TestBackstopAction_PureRefresh: no open MR in ledger, issue still open -> None.
func TestBackstopAction_PureRefresh(t *testing.T) {
	task := makeTaskWithLedger([]tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
	})

	dec := backstopAction(task, false)
	require.Equal(t, bsActionNone, dec)
}
