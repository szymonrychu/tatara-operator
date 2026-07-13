package controller

import (
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/scm"
)

func TestProposalBacklogCount_GroupsSystemic(t *testing.T) {
	const bs = "tatara-brainstorming"
	mk := func(n int, labels ...string) scm.IssueRef {
		return scm.IssueRef{Repo: "o/r", Number: n, Labels: append([]string{bs}, labels...)}
	}
	tests := []struct {
		name string
		iss  []scm.IssueRef
		want int
	}{
		{"three standalone count three", []scm.IssueRef{mk(1), mk(2), mk(3)}, 3},
		{"systemic group counts one", []scm.IssueRef{mk(1, "tatara/systemic-abc"), mk(2, "tatara/systemic-abc"), mk(3, "tatara/systemic-abc")}, 1},
		{"mixed: group + standalone", []scm.IssueRef{mk(1, "tatara/systemic-abc"), mk(2, "tatara/systemic-abc"), mk(3)}, 2},
		{"two distinct groups", []scm.IssueRef{mk(1, "tatara/systemic-abc"), mk(2, "tatara/systemic-def")}, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := proposalBacklogCount(tc.iss, bs); got != tc.want {
				t.Fatalf("proposalBacklogCount = %d, want %d", got, tc.want)
			}
		})
	}
}
