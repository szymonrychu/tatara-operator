package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// freshBrainstormIssue is a bot-authored, brainstorming-labelled proposal that is
// NOT stale (recent UpdatedAt) so the reaper's stale gate does not fire; only the
// human-activity gate should suppress a fresh triage Task.
func freshBrainstormIssue() scm.IssueRef {
	return scm.IssueRef{Repo: "o/r", Number: 7, Author: "tatara-bot",
		Labels: []string{"tatara-brainstorming"}, UpdatedAt: time.Now().Add(-2 * time.Hour)}
}

// TestIsBotBrainstormProposal is the pure label predicate.
func TestIsBotBrainstormProposal(t *testing.T) {
	brs, app, impl, dec := "tatara-brainstorming", "tatara-approved", "tatara-implementation", "tatara-declined"
	cases := []struct {
		name string
		c    candidate
		want bool
	}{
		{"bot brainstorming proposal", candidate{repo: "o/r", number: 7, author: "tatara-bot", labels: []string{brs}}, true},
		{"human authored", candidate{repo: "o/r", number: 7, author: "szymonrychu", labels: []string{brs}}, false},
		{"empty author", candidate{repo: "o/r", number: 7, author: "", labels: []string{brs}}, false},
		{"advanced to approved", candidate{repo: "o/r", number: 7, author: "tatara-bot", labels: []string{brs, app}}, false},
		{"already declined", candidate{repo: "o/r", number: 7, author: "tatara-bot", labels: []string{brs, dec}}, false},
		{"no brainstorming label", candidate{repo: "o/r", number: 7, author: "tatara-bot", labels: []string{"other"}}, false},
		{"is a PR", candidate{repo: "o/r", number: 7, author: "tatara-bot", labels: []string{brs}, isPR: true}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isBotBrainstormProposal(tc.c, brs, app, impl, dec, "tatara-bot"); got != tc.want {
				t.Errorf("isBotBrainstormProposal = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestIssueScan_SkipsBrainstormProposalWithoutHumanActivity: a fresh bot proposal
// with no comments must NOT create a triage Task.
func TestIssueScan_SkipsBrainstormProposalWithoutHumanActivity(t *testing.T) {
	proj, repo := seedBackstopProject(t, "bsgate-nohuman")
	reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{"o/r": {freshBrainstormIssue()}}}
	// fakeReader.ListIssueComments returns nil -> no human comment ever.
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo}, nil, proj.Spec.Scm.Cron.IssueScan)

	qes := listScanQEs(t, "bsgate-nohuman")
	require.Empty(t, qes, "issueScan must not triage a bot brainstorming proposal with no human activity")
}

// TestIssueScan_TriagesBrainstormProposalWithHumanActivity: the same proposal WITH
// a human comment must be triaged (one QueuedEvent created).
func TestIssueScan_TriagesBrainstormProposalWithHumanActivity(t *testing.T) {
	proj, repo := seedBackstopProject(t, "bsgate-human")
	reader := &perRepoFakeReader{
		fakeReader:   fakeReader{comments: []scm.IssueComment{{Author: "szymonrychu", Body: "please build this", CreatedAt: time.Now()}}},
		issuesByRepo: map[string][]scm.IssueRef{"o/r": {freshBrainstormIssue()}},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo}, nil, proj.Spec.Scm.Cron.IssueScan)

	qes := listScanQEs(t, "bsgate-human")
	require.Len(t, qes, 1, "issueScan must triage a bot brainstorming proposal once a human has engaged")
}

// TestIssueScan_TriagesBrainstormProposalOnEditOrReaction: a proposal a human
// edited or reacted to (issue UpdatedAt moved past CreatedAt, no comment at all)
// must NOT be starved by the comment-only gate.
func TestIssueScan_TriagesBrainstormProposalOnEditOrReaction(t *testing.T) {
	proj, repo := seedBackstopProject(t, "bsgate-edit")
	edited := scm.IssueRef{
		Repo: "o/r", Number: 7, Author: "tatara-bot", Labels: []string{"tatara-brainstorming"},
		CreatedAt: time.Now().Add(-3 * time.Hour), UpdatedAt: time.Now().Add(-1 * time.Hour),
	}
	// fakeReader.ListIssueComments returns nil -> no comment was ever posted.
	reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{"o/r": {edited}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo}, nil, proj.Spec.Scm.Cron.IssueScan)

	qes := listScanQEs(t, "bsgate-edit")
	require.Len(t, qes, 1, "issueScan must triage a bot brainstorming proposal edited/reacted to after creation")
}

// TestIssueScan_BrainstormGateMemoizesCommentFetch: the adoption/fresh-creation/
// bot-last-word/brainstorm-churn gates in one issueScan pass over a single fresh
// proposal must share one ListIssueComments call, not one per gate.
func TestIssueScan_BrainstormGateMemoizesCommentFetch(t *testing.T) {
	proj, repo := seedBackstopProject(t, "bsgate-memo")
	reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{"o/r": {freshBrainstormIssue()}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo}, nil, proj.Spec.Scm.Cron.IssueScan)

	require.LessOrEqual(t, reader.commentCalls, 1,
		"issueScan must fetch comments at most once per issue per cycle, got %d calls", reader.commentCalls)
}
