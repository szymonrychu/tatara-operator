package controller

// Tests for issue #178: a repo the task never changed returns GitHub
// 422 {field:head, code:invalid} on PR-create (the task branch does not exist
// there). That benign cross-repo fan-out no-op must record the "no_branch"
// outcome, NOT "skip_4xx", and must not arm the issue-166 4xx-skip cap.

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// headInvalid422 is GitHub's PR-create response when the head branch does not
// exist in the target repo.
const headInvalid422 = `{"message":"Validation Failed","errors":[{"resource":"PullRequest","field":"head","code":"invalid"}],"status":"422"}`

func TestWriteback_OutOfScopeHeadInvalidRecordsNoBranch(t *testing.T) {
	fw := &fakeWriter{openErr: &scm.HTTPError{Status: 422, Body: headInvalid422, Path: "/repos/o/r/pulls"}}
	r := newWriteBackReconciler(t, fw)
	task := seedWritebackPending(t, "wb-nobranch", "wb-scm-nobranch", "wb-proj-nobranch", "wb-repo-nobranch")
	// ReposInScope left nil: this repo was simply not touched by the task.

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.Equal(t, float64(1), testutil.ToFloat64(r.Metrics.WritebackOutcomeCounter("no_branch")),
		"untouched repo (422 head invalid) must record the no_branch outcome")
	require.Equal(t, float64(0), testutil.ToFloat64(r.Metrics.WritebackOutcomeCounter("skip_4xx")),
		"untouched repo must NOT be miscounted as skip_4xx (issue #178)")

	got := getTask(t, task.Name)
	require.Equal(t, 0, got.Status.WritebackSkip4xxAttempts,
		"a benign no-branch skip must not arm the issue-166 4xx-skip cap")

	fw.mu.Lock()
	defer fw.mu.Unlock()
	for _, c := range fw.commentArgs {
		if strings.Contains(strings.ToLower(c), "warning") {
			t.Fatalf("out-of-scope no-branch repo must not warn; got comment %q", c)
		}
	}
}

func TestWriteback_InScopeHeadInvalidWarns(t *testing.T) {
	fw := &fakeWriter{openErr: &scm.HTTPError{Status: 422, Body: headInvalid422, Path: "/repos/o/r/pulls"}}
	r := newWriteBackReconciler(t, fw)
	task := seedWritebackPending(t, "wb-nobranch-in", "wb-scm-nobranch-in", "wb-proj-nobranch-in", "wb-repo-nobranch-in")

	task.Spec.ReposInScope = []string{"wb-repo-nobranch-in"}
	require.NoError(t, k8sClient.Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.Equal(t, float64(1), testutil.ToFloat64(r.Metrics.WritebackOutcomeCounter("in_scope_no_branch")),
		"in-scope repo with no branch must record in_scope_no_branch")
	require.Equal(t, float64(0), testutil.ToFloat64(r.Metrics.WritebackOutcomeCounter("skip_4xx")),
		"in-scope no-branch must not be counted as skip_4xx")

	fw.mu.Lock()
	defer fw.mu.Unlock()
	var warned bool
	for _, c := range fw.commentArgs {
		if strings.Contains(c, "o/r#7|") && strings.Contains(c, "wb-repo-nobranch-in") && strings.Contains(strings.ToLower(c), "warning") {
			warned = true
		}
	}
	require.True(t, warned, "in-scope repo with no branch must produce a WARNING comment; got %v", fw.commentArgs)
}

// TestWritebackNoBranchWarning_ClosedIssue_Suppressed verifies the
// writeback.go in-scope no-branch warning site (item 1) now routes through
// the comment gate: an already-closed source issue must not receive the
// warning comment.
func TestWritebackNoBranchWarning_ClosedIssue_Suppressed(t *testing.T) {
	fw := &fakeWriter{openErr: &scm.HTTPError{Status: 422, Body: headInvalid422, Path: "/repos/o/r/pulls"}, issueClosed: true}
	r := newWriteBackReconciler(t, fw)
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) { return &botLastWordReader{}, nil }
	task := seedWritebackPending(t, "wb-nobranch-closed", "wb-scm-nobranch-closed", "wb-proj-nobranch-closed", "wb-repo-nobranch-closed")
	task.Spec.ReposInScope = []string{"wb-repo-nobranch-closed"}
	require.NoError(t, k8sClient.Update(context.Background(), task))

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "wb-proj-nobranch-closed"}, &proj))
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{Provider: "github", BotLogin: "tatara-bot"}
	require.NoError(t, k8sClient.Update(context.Background(), &proj))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	fw.mu.Lock()
	defer fw.mu.Unlock()
	require.Empty(t, fw.commentArgs, "warning comment must be suppressed on an already-closed issue, got %v", fw.commentArgs)
}
