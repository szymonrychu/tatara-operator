package controller

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// iss builds an Issue CR fixture: provenance kind, forge state, platform status,
// owning repo, and mirrored forge labels.
func iss(kind, state, status, repo string, labels ...string) tatarav1alpha1.Issue {
	return tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: "iss-" + repo + "-1", Namespace: "tatara"},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo, Number: 1, ProjectRef: "demo", ProposalKind: kind,
		},
		Status: tatarav1alpha1.IssueStatus{State: state, Status: status, Labels: labels},
	}
}

func TestProposalPending(t *testing.T) {
	tests := []struct {
		name string
		in   tatarav1alpha1.Issue
		want bool
	}{
		{"open brainstorm proposal awaiting a decision", iss("brainstorm", "open", "new", "r1"), true},
		{"open brainstorm proposal with an empty status", iss("brainstorm", "open", "", "r1"), true},
		{"approved frees its slot even while the forge issue stays open", iss("brainstorm", "open", "approved", "r1"), false},
		{"rejected does not count", iss("brainstorm", "open", "rejected", "r1"), false},
		{"done does not count", iss("brainstorm", "open", "done", "r1"), false},
		{"closed does not count", iss("brainstorm", "closed", "new", "r1"), false},
		{"incident provenance is not a brainstorm proposal", iss("incident", "open", "new", "r1"), false},
		{"unstamped provenance does not count", iss("", "open", "new", "r1"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := tc.in
			if got := proposalPending(&in); got != tc.want {
				t.Fatalf("proposalPending = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestProposalDisplayStatus(t *testing.T) {
	tests := []struct {
		name string
		in   tatarav1alpha1.Issue
		want string
	}{
		{"approved wins regardless of state", iss("brainstorm", "closed", "approved", "r1"), "approved"},
		{"explicit rejection is declined", iss("brainstorm", "open", "rejected", "r1"), "declined"},
		{"a maintainer who just closes it has discarded it", iss("brainstorm", "closed", "new", "r1"), "declined"},
		{"still awaiting a decision", iss("brainstorm", "open", "new", "r1"), "open"},
		{"done is not a proposal verdict; it reads as open", iss("brainstorm", "open", "done", "r1"), "open"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := tc.in
			if got := proposalDisplayStatus(&in); got != tc.want {
				t.Fatalf("proposalDisplayStatus = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPendingProposalCount(t *testing.T) {
	sysA := "tatara/systemic-abc"
	tests := []struct {
		name string
		in   []tatarav1alpha1.Issue
		want int
	}{
		{"empty", nil, 0},
		{"three standalone pending proposals", []tatarav1alpha1.Issue{
			iss("brainstorm", "open", "new", "r1"),
			iss("brainstorm", "open", "new", "r2"),
			iss("brainstorm", "open", "new", "r3"),
		}, 3},
		{"a systemic group across three repos is one slot", []tatarav1alpha1.Issue{
			iss("brainstorm", "open", "new", "r1", sysA),
			iss("brainstorm", "open", "new", "r2", sysA),
			iss("brainstorm", "open", "new", "r3", sysA),
		}, 1},
		{"one systemic group plus one standalone", []tatarav1alpha1.Issue{
			iss("brainstorm", "open", "new", "r1", sysA),
			iss("brainstorm", "open", "new", "r2", sysA),
			iss("brainstorm", "open", "new", "r3"),
		}, 2},
		{"excluded issues never reach the collapse", []tatarav1alpha1.Issue{
			iss("brainstorm", "open", "approved", "r1"),
			iss("brainstorm", "closed", "new", "r2"),
			iss("incident", "open", "new", "r3"),
		}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := pendingProposalCount(tc.in); got != tc.want {
				t.Fatalf("pendingProposalCount = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestPendingProposalCountByRepo(t *testing.T) {
	sysA := "tatara/systemic-abc"
	got := pendingProposalCountByRepo([]tatarav1alpha1.Issue{
		iss("brainstorm", "open", "new", "r1", sysA),
		iss("brainstorm", "open", "new", "r2", sysA),
		iss("brainstorm", "open", "new", "r1"),
		iss("brainstorm", "open", "approved", "r1"),
	})
	// Per-repo is deliberately UNCOLLAPSED: a systemic group spans repos, so
	// collapsing it would have to attribute the one slot to an arbitrary repo.
	want := map[string]int{"r1": 2, "r2": 1}
	if len(got) != len(want) {
		t.Fatalf("pendingProposalCountByRepo = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("pendingProposalCountByRepo[%q] = %d, want %d", k, got[k], v)
		}
	}
}

func TestBrainstormDeficit(t *testing.T) {
	tests := []struct {
		name                      string
		target, pending, inflight int
		want                      int
	}{
		{"empty backlog refills to target", 3, 0, 0, 3},
		{"partial backlog refills the gap", 3, 2, 0, 1},
		{"an in-flight session already covers the gap", 3, 2, 1, 0},
		{"at target", 3, 3, 0, 0},
		{"over target after lowering N clamps at zero", 3, 7, 0, 0},
		{"over target with a session in flight still clamps at zero", 3, 7, 1, 0},
		{"target zero disables refill", 0, 0, 0, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := brainstormDeficit(tc.target, tc.pending, tc.inflight); got != tc.want {
				t.Fatalf("brainstormDeficit(%d,%d,%d) = %d, want %d",
					tc.target, tc.pending, tc.inflight, got, tc.want)
			}
		})
	}
}

func TestBrainstormRefillDecision(t *testing.T) {
	target := func(n int) tatarav1alpha1.BrainstormActivity {
		v := n
		return tatarav1alpha1.BrainstormActivity{TargetOpenProposals: &v}
	}
	tests := []struct {
		name                     string
		act                      tatarav1alpha1.BrainstormActivity
		pending, inflight, skips int
		trigger                  string
		wantQuota                int
		wantRefill               bool
		wantReason               string
	}{
		{"empty backlog on an event", target(3), 0, 0, 0, "event", 3, true, ""},
		{"at target on an event", target(3), 3, 0, 0, "event", 0, false, "at-target"},
		{"in flight on an event", target(3), 0, 1, 0, "event", 2, true, ""},
		{"breaker tripped suppresses the event path", target(3), 0, 0, 3, "event", 0, false, "breaker-tripped"},
		{"breaker below threshold does not suppress", target(3), 0, 0, 2, "event", 3, true, ""},
		{"a cron tick ignores the breaker", target(3), 0, 0, 9, "cron", 3, true, ""},
		{"a large target is clamped to the submit_outcome ceiling", target(20), 0, 0, 0, "cron", 5, true, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			quota, refill, reason := brainstormRefillDecision(tc.act, tc.pending, tc.inflight, tc.skips, tc.trigger)
			if quota != tc.wantQuota || refill != tc.wantRefill || reason != tc.wantReason {
				t.Fatalf("brainstormRefillDecision = (%d, %v, %q), want (%d, %v, %q)",
					quota, refill, reason, tc.wantQuota, tc.wantRefill, tc.wantReason)
			}
		})
	}
}
