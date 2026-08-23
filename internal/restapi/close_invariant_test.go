// Copyright 2026 tatara authors.

package restapi_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// pendingBodies renders an Issue's queued intents so a test can assert WHICH
// intent landed, not merely that one did.
func pendingBodies(iss *tatarav1alpha1.Issue) string {
	var b strings.Builder
	for _, pc := range iss.Status.PendingComments {
		b.WriteString(pc.Body)
		b.WriteString("\n")
	}
	return b.String()
}

// TestOutcome_Gate_RejectedParksTheIssueWhileAnMRIsOpen is THE CLOSE INVARIANT:
// an issue may be closed only once every one of its PRs is merged.
//
// Before this, the gate's rejected(declined) arm queued an unconditional close.
// An agent that declined an issue while its own PR was still open on the forge
// therefore closed the issue out from under live work - the exact strand the
// invariant exists to forbid. The refusal is not a 4xx: the decline itself is
// legitimate and the Task still goes rejected. It is the ISSUE that is parked
// instead of closed.
func TestOutcome_Gate_RejectedParksTheIssueWhileAnMRIsOpen(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateRefined, "implement"),
		issueV2("tatara-operator", 291, "t1"),
		mrV2("tatara-operator", 41, "t1")) // state "open"

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"rejected","reason":"wont fix"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StateRejected, e.task(t, "t1").Status.State,
		"the decline itself is legitimate; only the issue close is refused")

	iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 291))
	require.Len(t, iss.Status.PendingComments, 1)
	body := pendingBodies(iss)
	require.Contains(t, body, "<!-- tatara-park -->", "the issue is PARKED, not closed")
	require.NotContains(t, body, "<!-- tatara-close -->")
	require.Contains(t, body, "mr-tatara-operator-41", "the refusal names the PR that blocked it")
}

// TestOutcome_Gate_RejectedClosesWhenEveryMRIsSettled: the invariant is not a
// blanket ban. A merged PR is the case it exists to allow, and an ABANDONED
// (closed) one strands nothing either - blocking on that would have built a new
// permanent dead end, an issue nothing could ever close because one dead PR sat
// in its history.
func TestOutcome_Gate_RejectedClosesWhenEveryMRIsSettled(t *testing.T) {
	settled := func(state string) func(*tatarav1alpha1.MergeRequest) {
		return func(mr *tatarav1alpha1.MergeRequest) { mr.Status.State = state }
	}
	for _, state := range []string{"merged", "closed"} {
		t.Run(state, func(t *testing.T) {
			e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "implement", tatarav1alpha1.StateRefined, "implement"),
				issueV2("tatara-operator", 291, "t1"),
				mrV2("tatara-operator", 41, "t1", settled(state)))

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
				`{"kind":"implement","payload":{"action":"rejected","reason":"wont fix"}}`)
			require.Equal(t, http.StatusOK, w.Code)

			body := pendingBodies(e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 291)))
			require.Contains(t, body, "<!-- tatara-close -->")
			require.NotContains(t, body, "<!-- tatara-park -->")
		})
	}
}

// TestMROpen_StripsClosesWhileASiblingPRIsOpen is the invariant's WRITE-TIME
// half. The allowlist alone cannot hold it: it answers "may this task close
// issue N", and the answer is legitimately yes - but the forge closes on the
// FIRST merge, so a two-repo task that writes `Closes #N` into both bodies
// closes the issue when the first lands and strands the second.
func TestMROpen_StripsClosesWhileASiblingPRIsOpen(t *testing.T) {
	forge := newRecordingForge()
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), repoV2("charts", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		approvedIssueV2("tatara-operator", 48, "t1"),
		mrV2("charts", 9, "t1")) // the SIBLING, still open

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t1","action":"open","repo":"tatara-operator","title":"T","body":"Work.\n\nCloses #48"}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, forge.openedURLs, 1)
	require.NotContains(t, strings.ToLower(forge.openedURLs[0]), "closes #48",
		"a live sibling PR strips the close keyword")
	require.Contains(t, forge.openedURLs[0], "#48", "the cross-link survives")
}

// TestMROpen_KeepsClosesForTheOnlyPR: with no sibling live, this PR merging IS
// every PR merged, so the agent's own keyword is left byte-for-byte alone.
func TestMROpen_KeepsClosesForTheOnlyPR(t *testing.T) {
	forge := newRecordingForge()
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		approvedIssueV2("tatara-operator", 48, "t1"))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t1","action":"open","repo":"tatara-operator","title":"T","body":"Work.\n\nCloses #48"}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, forge.openedURLs, 1)
	require.Contains(t, forge.openedURLs[0], "Closes #48")
}
