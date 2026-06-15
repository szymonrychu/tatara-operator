package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

func TestLaneOccupancy(t *testing.T) {
	mk := func(repo, kind, phase string) tatarav1alpha1.Task {
		return tatarav1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{labelSourceRepo: sanitizeRepoLabel(repo)}},
			Spec:       tatarav1alpha1.TaskSpec{Kind: kind},
			Status:     tatarav1alpha1.TaskStatus{Phase: phase},
		}
	}
	existing := []tatarav1alpha1.Task{
		mk("o/a", "triageIssue", "Running"),          // occupies a/issue lane
		mk("o/a", "triageIssue", "AwaitingApproval"), // occupies (no longer a special Phase; Succeeded/Failed only are terminal)
		mk("o/a", "triageIssue", "Succeeded"),        // terminal, no
		mk("o/b", "review", "Planning"),              // occupies b/mr lane
		mk("o/a", "selfImprove", "Running"),          // wrong kind for issue lane
	}
	require.Equal(t, 2, laneOccupancy(existing, "o/a", "triageIssue"))
	require.Equal(t, 1, laneOccupancy(existing, "o/b", "review", "selfImprove"))
	require.Equal(t, 0, laneOccupancy(existing, "o/c", "triageIssue"))
}

func TestSelectPerRepo(t *testing.T) {
	c := func(repo string, n int, age time.Duration) candidate {
		return candidate{repo: repo, number: n, updatedAt: time.Now().Add(-age)}
	}
	eligible := []candidate{
		c("o/a", 1, 3*time.Hour), c("o/a", 2, 1*time.Hour), // a: two items
		c("o/b", 3, 2*time.Hour), // b: one item
	}

	// a lane already has 1 active -> a gets 0; b has 0 -> b gets its 1.
	occ := func(slug string) int {
		if slug == "o/a" {
			return 1
		}
		return 0
	}
	got := selectPerRepo(eligible, "", 1, occ)
	require.Len(t, got, 1)
	require.Equal(t, "o/b", got[0].repo)

	// empty lanes, maxPerRepo 1 -> one per repo, stale-first within repo.
	got2 := selectPerRepo(eligible, "", 1, func(string) int { return 0 })
	require.Len(t, got2, 2)
	require.ElementsMatch(t, []string{"o/a", "o/b"}, []string{got2[0].repo, got2[1].repo})
	for _, g := range got2 {
		if g.repo == "o/a" {
			require.Equal(t, 1, g.number) // a#1 is older -> stale-first
		}
	}
}

func TestSelectPriorityThenStale(t *testing.T) {
	base := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	cands := []candidate{
		{repo: "o/r", number: 1, labels: nil, updatedAt: base.Add(3 * time.Hour)},
		{repo: "o/r", number: 2, labels: []string{"tatara/priority"}, updatedAt: base.Add(2 * time.Hour)},
		{repo: "o/r", number: 3, labels: nil, updatedAt: base.Add(1 * time.Hour)},
		{repo: "o/r", number: 4, labels: []string{"tatara/priority"}, updatedAt: base.Add(4 * time.Hour)},
	}
	cases := []struct {
		name      string
		priority  string
		n         int
		wantOrder []int
	}{
		{"priority first then stale, cap 3", "tatara/priority", 3, []int{2, 4, 3}},
		{"no priority label = pure stale", "", 2, []int{3, 2}},
		{"cap 1 picks stalest priority", "tatara/priority", 1, []int{2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := selectCandidates(cands, tc.priority, tc.n)
			if len(got) != len(tc.wantOrder) {
				t.Fatalf("len = %d, want %d (%+v)", len(got), len(tc.wantOrder), got)
			}
			for i, want := range tc.wantOrder {
				if got[i].number != want {
					t.Fatalf("pos %d = #%d, want #%d (%+v)", i, got[i].number, want, got)
				}
			}
		})
	}
}

func TestLaneOccupancy_TerminalAndConversationLifecycleFreeLane(t *testing.T) {
	mkLC := func(repo, kind, lc string) tatarav1alpha1.Task {
		return tatarav1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{labelSourceRepo: sanitizeRepoLabel(repo)}},
			Spec:       tatarav1alpha1.TaskSpec{Kind: kind},
			// Phase intentionally empty: real issueLifecycle tasks never set it.
			Status: tatarav1alpha1.TaskStatus{LifecycleState: lc},
		}
	}
	existing := []tatarav1alpha1.Task{
		mkLC("o/r", "issueLifecycle", "Done"),
		mkLC("o/r", "issueLifecycle", "Parked"),
		mkLC("o/r", "issueLifecycle", "Stopped"),
		mkLC("o/r", "issueLifecycle", "Conversation"),
		mkLC("o/r", "issueLifecycle", "Implement"), // active -> occupies the lane
	}
	// Only the active task holds the lane; terminal + Conversation free it.
	require.Equal(t, 1, laneOccupancy(existing, "o/r", "issueLifecycle", "review"))
}

var _ = scm.PRRef{}

func TestCandidatesFromPRs(t *testing.T) {
	prs := []scm.PRRef{
		{Repo: "o/r", Number: 5, Author: "alice", HeadSHA: "abc", Labels: []string{"x"}, UpdatedAt: time.Unix(100, 0)},
	}
	got := candidatesFromPRs(prs)
	if len(got) != 1 || got[0].number != 5 || got[0].author != "alice" || got[0].headSHA != "abc" || !got[0].isPR {
		t.Fatalf("candidatesFromPRs = %+v", got)
	}
}

func TestCandidatesFromIssues(t *testing.T) {
	iss := []scm.IssueRef{
		{Repo: "o/r", Number: 7, Labels: []string{"bug"}, UpdatedAt: time.Unix(100, 0), IsPR: false},
		{Repo: "o/r", Number: 8, IsPR: true}, // filtered out
	}
	got := candidatesFromIssues(iss)
	if len(got) != 1 || got[0].number != 7 || got[0].isPR {
		t.Fatalf("candidatesFromIssues should drop IsPR rows: %+v", got)
	}
}
