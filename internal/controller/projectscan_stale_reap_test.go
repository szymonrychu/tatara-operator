package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

const (
	reapBrainstorming = "tatara-brainstorming"
	reapApproved      = "tatara-approved"
	reapImplemented   = "tatara-implementation"
	reapDeclined      = "tatara-declined"
	reapBot           = "tatara-bot"
)

// matchingTask builds a terminal-or-not Task whose Spec.Source matches (o/r, num).
func matchingTask(num int, lifecycleState string, prURL, headBranch string) tatarav1alpha1.Task {
	t := tatarav1alpha1.Task{}
	t.Spec.Source = &tatarav1alpha1.TaskSource{IssueRef: "o/r#" + itoa(num), Number: num}
	t.Status.LifecycleState = lifecycleState
	t.Status.PrURL = prURL
	t.Status.HeadBranch = headBranch
	return t
}

// TestIsStaleUnengagedProposal exercises the pure predicate across every gate.
func TestIsStaleUnengagedProposal(t *testing.T) {
	window := 14 * 24 * time.Hour
	old := time.Now().Add(-30 * 24 * time.Hour)
	recent := time.Now().Add(-1 * time.Hour)

	base := func() scm.IssueRef {
		return scm.IssueRef{Repo: "o/r", Number: 5, Author: reapBot,
			Labels: []string{reapBrainstorming}, UpdatedAt: old, IsPR: false}
	}

	tests := []struct {
		name     string
		iss      scm.IssueRef
		existing []tatarav1alpha1.Task
		window   time.Duration
		want     bool
	}{
		{name: "ClosesStaleBotProposal_predicate", iss: base(), window: window, want: true},
		{name: "SkipsWindowZero", iss: base(), window: 0, want: false},
		{
			name:   "SkipsRecentlyUpdated",
			iss:    func() scm.IssueRef { i := base(); i.UpdatedAt = recent; return i }(),
			window: window, want: false,
		},
		{
			name:   "SkipsZeroUpdatedAt",
			iss:    func() scm.IssueRef { i := base(); i.UpdatedAt = time.Time{}; return i }(),
			window: window, want: false,
		},
		{
			name:   "SkipsApprovedLabel",
			iss:    func() scm.IssueRef { i := base(); i.Labels = []string{reapBrainstorming, reapApproved}; return i }(),
			window: window, want: false,
		},
		{
			name:   "SkipsImplementationLabel",
			iss:    func() scm.IssueRef { i := base(); i.Labels = []string{reapBrainstorming, reapImplemented}; return i }(),
			window: window, want: false,
		},
		{
			name:   "SkipsDeclined",
			iss:    func() scm.IssueRef { i := base(); i.Labels = []string{reapBrainstorming, reapDeclined}; return i }(),
			window: window, want: false,
		},
		{
			name:   "SkipsMissingBrainstormingLabel",
			iss:    func() scm.IssueRef { i := base(); i.Labels = []string{"tatara"}; return i }(),
			window: window, want: false,
		},
		{
			name:   "SkipsHumanAuthored",
			iss:    func() scm.IssueRef { i := base(); i.Author = "alice"; return i }(),
			window: window, want: false,
		},
		{
			name:   "SkipsEmptyAuthor",
			iss:    func() scm.IssueRef { i := base(); i.Author = ""; return i }(),
			window: window, want: false,
		},
		{
			name:     "SkipsLiveTask",
			iss:      base(),
			existing: []tatarav1alpha1.Task{matchingTask(5, "Running", "", "")},
			window:   window, want: false,
		},
		{
			name:     "SkipsUnmergedChange_PrURL",
			iss:      base(),
			existing: []tatarav1alpha1.Task{matchingTask(5, "Stopped", "https://github.com/o/r/pull/9", "")},
			window:   window, want: false,
		},
		{
			name:     "SkipsUnmergedChange_HeadBranch",
			iss:      base(),
			existing: []tatarav1alpha1.Task{matchingTask(5, "Stopped", "", "feature/x")},
			window:   window, want: false,
		},
		{
			name:     "AllowsTerminalTaskNoChange",
			iss:      base(),
			existing: []tatarav1alpha1.Task{matchingTask(5, "Stopped", "", "")},
			window:   window, want: true,
		},
		{
			name:   "SkipsPR",
			iss:    func() scm.IssueRef { i := base(); i.IsPR = true; return i }(),
			window: window, want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isStaleUnengagedProposal(tc.iss, tc.existing,
				reapBrainstorming, reapApproved, reapImplemented, reapDeclined, reapBot, tc.window)
			require.Equal(t, tc.want, got)
		})
	}
}

// reapFakeReader returns canned issue comments (and optional error) so the
// reaper's single SCM read (humanCommentAfter) can be steered per-test.
type reapFakeReader struct {
	scm.SCMReader
	comments []scm.IssueComment
	err      error
}

func (f *reapFakeReader) ListIssueComments(context.Context, string, string, int) ([]scm.IssueComment, error) {
	return f.comments, f.err
}

// reapFakeWriter records the label swap and the close call.
type reapFakeWriter struct {
	scm.SCMWriter
	addLabelCalls    []struct{ issueRef, label string }
	removeLabelCalls []struct{ issueRef, label string }
	closeIssueCalls  []struct {
		repo   string
		number int
		note   string
	}
	addLabelErr   error
	closeIssueErr error
}

func (f *reapFakeWriter) AddLabel(_ context.Context, _, issueRef, label string) error {
	f.addLabelCalls = append(f.addLabelCalls, struct{ issueRef, label string }{issueRef, label})
	return f.addLabelErr
}

func (f *reapFakeWriter) RemoveLabel(_ context.Context, _, issueRef, label string) error {
	f.removeLabelCalls = append(f.removeLabelCalls, struct{ issueRef, label string }{issueRef, label})
	return nil
}

func (f *reapFakeWriter) CloseIssue(_ context.Context, _, repo string, number int, note string) error {
	if f.closeIssueErr != nil {
		return f.closeIssueErr
	}
	f.closeIssueCalls = append(f.closeIssueCalls, struct {
		repo   string
		number int
		note   string
	}{repo, number, note})
	return nil
}

// staleIssueCache builds a single-issue cache that passes the cheap predicate.
func staleIssueCache() map[string][]scm.IssueRef {
	return map[string][]scm.IssueRef{
		"o/r": {{Repo: "o/r", Number: 5, Author: reapBot,
			Labels: []string{reapBrainstorming}, UpdatedAt: time.Now().Add(-30 * 24 * time.Hour)}},
	}
}

func newReapReconciler(t *testing.T, name string, reader scm.SCMReader, fw scm.SCMWriter) (*ProjectReconciler, *tatarav1alpha1.Project) {
	t.Helper()
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *", MaxPerRepo: 5}}
	proj, _ := seedScanProject(t, name, cron)
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	return r, proj
}

func TestReapStaleProposals_ClosesUnengagedStaleProposal(t *testing.T) {
	reader := &reapFakeReader{comments: []scm.IssueComment{{Author: reapBot, Body: "still awaiting go-ahead"}}}
	fw := &reapFakeWriter{}
	r, proj := newReapReconciler(t, "reap-closes", reader, fw)
	act := tatarav1alpha1.BrainstormActivity{StaleProposalDays: 14}

	r.reapStaleProposals(context.Background(), proj, reader, staleIssueCache(), nil, act)

	require.Len(t, fw.closeIssueCalls, 1, "want exactly one CloseIssue")
	require.Equal(t, "o/r", fw.closeIssueCalls[0].repo)
	require.Equal(t, 5, fw.closeIssueCalls[0].number)
	require.Contains(t, fw.closeIssueCalls[0].note, "no human engagement")
	require.Contains(t, fw.closeIssueCalls[0].note, "14 days")

	require.Len(t, fw.addLabelCalls, 1)
	require.Equal(t, "o/r#5", fw.addLabelCalls[0].issueRef)
	require.Equal(t, reapDeclined, fw.addLabelCalls[0].label)
	require.Len(t, fw.removeLabelCalls, 1)
	require.Equal(t, "o/r#5", fw.removeLabelCalls[0].issueRef)
	require.Equal(t, reapBrainstorming, fw.removeLabelCalls[0].label)

	require.Equal(t, float64(1), testutil.ToFloat64(r.Metrics.IssueOutcomeTotal("stale-close")))
}

func TestReapStaleProposals_SkipsHumanEngaged(t *testing.T) {
	reader := &reapFakeReader{comments: []scm.IssueComment{
		{Author: reapBot, Body: "awaiting go-ahead", CreatedAt: time.Now().Add(-48 * time.Hour)},
		{Author: "alice", Body: "I think we should do this", CreatedAt: time.Now().Add(-24 * time.Hour)},
	}}
	fw := &reapFakeWriter{}
	r, proj := newReapReconciler(t, "reap-human", reader, fw)
	act := tatarav1alpha1.BrainstormActivity{StaleProposalDays: 14}

	r.reapStaleProposals(context.Background(), proj, reader, staleIssueCache(), nil, act)

	require.Empty(t, fw.closeIssueCalls, "must NOT close a human-engaged proposal")
	require.Equal(t, float64(0), testutil.ToFloat64(r.Metrics.IssueOutcomeTotal("stale-close")))
}

func TestReapStaleProposals_CommentReadErrorFailsClosed(t *testing.T) {
	reader := &reapFakeReader{err: context.DeadlineExceeded}
	fw := &reapFakeWriter{}
	r, proj := newReapReconciler(t, "reap-readerr", reader, fw)
	act := tatarav1alpha1.BrainstormActivity{StaleProposalDays: 14}

	r.reapStaleProposals(context.Background(), proj, reader, staleIssueCache(), nil, act)

	require.Empty(t, fw.closeIssueCalls, "a comment read error must SKIP the close (fail-closed)")
	require.Equal(t, float64(0), testutil.ToFloat64(r.Metrics.IssueOutcomeTotal("stale-close")))
}

func TestReapStaleProposals_DisabledNoSCMWrites(t *testing.T) {
	reader := &reapFakeReader{comments: []scm.IssueComment{{Author: reapBot}}}
	fw := &reapFakeWriter{}
	r, proj := newReapReconciler(t, "reap-disabled", reader, fw)
	act := tatarav1alpha1.BrainstormActivity{StaleProposalDays: 0}

	r.reapStaleProposals(context.Background(), proj, reader, staleIssueCache(), nil, act)

	require.Empty(t, fw.closeIssueCalls, "disabled reaper must perform zero writes")
	require.Empty(t, fw.addLabelCalls)
	require.Equal(t, float64(0), testutil.ToFloat64(r.Metrics.IssueOutcomeTotal("stale-close")))
}

func TestReapStaleProposals_AddLabelErrorLeavesOpen(t *testing.T) {
	reader := &reapFakeReader{comments: []scm.IssueComment{{Author: reapBot}}}
	fw := &reapFakeWriter{addLabelErr: context.DeadlineExceeded}
	r, proj := newReapReconciler(t, "reap-addlabelerr", reader, fw)
	act := tatarav1alpha1.BrainstormActivity{StaleProposalDays: 14}

	r.reapStaleProposals(context.Background(), proj, reader, staleIssueCache(), nil, act)

	require.Empty(t, fw.closeIssueCalls, "must not close when the declined label could not be added")
	require.Equal(t, float64(0), testutil.ToFloat64(r.Metrics.IssueOutcomeTotal("stale-close")))
}
