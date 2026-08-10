// Copyright 2026 tatara authors.

package controller

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func invariantMR(name, state string) tatarav1alpha1.MergeRequest {
	mr := tatarav1alpha1.MergeRequest{ObjectMeta: metav1.ObjectMeta{Name: name}}
	mr.Status.State = state
	return mr
}

// TestOpenOwnedMRs is the C.1 predicate's whole table. The two settled states
// pass, the two unsettled ones block, and the empty set is a pass - a refine or
// clarify Task that owns no MR at all must still be able to close its issue.
func TestOpenOwnedMRs(t *testing.T) {
	cases := []struct {
		name string
		mrs  []tatarav1alpha1.MergeRequest
		want []string
	}{
		{"no owned MRs closes", nil, nil},
		{"all merged closes", []tatarav1alpha1.MergeRequest{
			invariantMR("a", "merged"), invariantMR("b", "merged"),
		}, nil},
		{"an abandoned (closed) MR strands nothing and still closes", []tatarav1alpha1.MergeRequest{
			invariantMR("a", "merged"), invariantMR("b", "closed"),
		}, nil},
		{"one open MR blocks and is named", []tatarav1alpha1.MergeRequest{
			invariantMR("a", "merged"), invariantMR("b", "open"),
		}, []string{"b"}},
		{"an unmirrored MR fails CLOSED", []tatarav1alpha1.MergeRequest{
			invariantMR("a", ""),
		}, []string{"a"}},
		{"every blocker is named, not just the first", []tatarav1alpha1.MergeRequest{
			invariantMR("a", "open"), invariantMR("b", "open"),
		}, []string{"a", "b"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, OpenOwnedMRs(c.mrs))
		})
	}
}

// TestFilterCloseDirectivesForTask_SiblingOpenStripsTheKeyword is the write-time
// half of the invariant: the forge auto-closes on the FIRST merge, so a body
// written while a sibling PR is still live may carry the LINK but never the
// keyword. With no live sibling the body is untouched, because that PR merging
// genuinely is every PR merged.
func TestFilterCloseDirectivesForTask_SiblingOpenStripsTheKeyword(t *testing.T) {
	allowed := map[RepoNum]bool{{Repo: "acme/widgets", Number: 48}: true}
	body := "Work.\n\nCloses #48"

	kept := FilterCloseDirectivesForTask(body, "acme/widgets", allowed, nil)
	require.Equal(t, body, kept, "the only PR of a task keeps its close keyword")

	stripped := FilterCloseDirectivesForTask(body, "acme/widgets", allowed, []string{"mr-other-7"})
	require.NotContains(t, strings.ToLower(stripped), "closes #48",
		"a live sibling PR strips the keyword so the first merge cannot auto-close")
	require.Contains(t, stripped, "#48", "the cross-link itself must survive")
}

// TestParkedForOpenMRsComment_NamesTheBlockers: the only action that clears this
// park is finishing or abandoning one of the PRs, so a refusal that does not
// name them costs the reader a kubectl.
func TestParkedForOpenMRsComment_NamesTheBlockers(t *testing.T) {
	got := ParkedForOpenMRsComment("t1", []string{"mr-a-1", "mr-b-2"})
	require.Contains(t, got, "t1")
	require.Contains(t, got, "mr-a-1")
	require.Contains(t, got, "mr-b-2")
	require.Contains(t, got, TataraParkedLabel)
}

// TestMaxAutoReentriesIsBounded pins the C.3 bound as a real, small number. A
// zero would disable the automatic pickup silently; an unbounded one is the
// spin the dead end exists to prevent.
func TestMaxAutoReentriesIsBounded(t *testing.T) {
	require.Equal(t, 3, tatarav1alpha1.MaxAutoReentries)
}
