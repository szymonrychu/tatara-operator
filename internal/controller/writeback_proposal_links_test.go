package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"k8s.io/apimachinery/pkg/types"
)

// TestCompleteProposal_TwoSiblings_SeedsLinksBlock: a Task whose ledger already
// holds one tracked issue gains a second via createProposal's existing-title
// dedup path; both siblings must get the tatara-links block naming the OTHER.
func TestCompleteProposal_TwoSiblings_SeedsLinksBlock(t *testing.T) {
	fw := &fakeProposalWriter{}
	reader := &fakeProposalReader{
		issues: []scm.IssueRef{{Repo: "o/r", Number: 42, Title: "Existing sibling issue"}},
		bodies: map[string]string{
			"o/r#42": "first sibling body",
			"o/r#7":  "second sibling body",
		},
	}
	r := newProposalReconciler(t, fw, func(_, _ string) (scm.SCMReader, error) { return reader, nil })

	task := seedProposalTask(t, "prop-links-two", "prop-links-two-proj", "prop-links-two-repo", "prop-links-two-scm", "Existing sibling issue")
	// Seed the ledger with a prior sibling so this call is the SECOND issue.
	ctx := context.Background()
	fresh := &tatarav1alpha1.Task{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, fresh))
	fresh.Status.DiscoveredIssues = []string{"https://github.com/o/r/issues/7"}
	require.NoError(t, k8sClient.Status().Update(ctx, fresh))

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))

	_, err := r.createProposal(ctx, &proj, task)
	require.NoError(t, err)

	edits := fw.editCallsSnapshot()
	require.Len(t, edits, 2, "both siblings must be edited")
	byRepoNum := map[string]string{}
	for _, e := range edits {
		byRepoNum[fmt.Sprintf("%s#%d", e.repo, e.number)] = e.body
	}
	require.Contains(t, byRepoNum["o/r#42"], "https://github.com/o/r/issues/7", "sibling #42's block must name #7")
	require.Contains(t, byRepoNum["o/r#7"], "https://github.com/o/r/issues/42", "sibling #7's block must name #42")
	require.Contains(t, byRepoNum["o/r#42"], "first sibling body", "original body must be preserved")
}

// TestCompleteProposal_SingleIssue_NoLinksBlockWritten: a fresh Task with no
// prior ledger entry gets no sibling-link edit (nothing to cross-link yet).
func TestCompleteProposal_SingleIssue_NoLinksBlockWritten(t *testing.T) {
	fw := &fakeProposalWriter{}
	r := newProposalReconciler(t, fw, nil)

	task := seedProposalTask(t, "prop-links-one", "prop-links-one-proj", "prop-links-one-repo", "prop-links-one-scm", "Solo Proposal")

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))

	_, err := r.createProposal(context.Background(), &proj, task)
	require.NoError(t, err)

	require.Empty(t, fw.editCallsSnapshot(), "a lone issue has nothing to cross-link")
}

// TestSyncSiblingLinks_IdempotentOnSecondCall: calling syncSiblingLinks twice
// with the same sibling set must not accumulate a duplicate block on the
// second call once the first has already rewritten it.
func TestSyncSiblingLinks_IdempotentOnSecondCall(t *testing.T) {
	fw := &fakeProposalWriter{}
	reader := &fakeProposalReader{
		bodies: map[string]string{
			"o/r#1": "body one",
			"o/r#2": "body two",
		},
	}
	r := newProposalReconciler(t, fw, func(_, _ string) (scm.SCMReader, error) { return reader, nil })

	siblings := []string{"https://github.com/o/r/issues/1", "https://github.com/o/r/issues/2"}
	r.syncSiblingLinks(context.Background(), "github", "tok", siblings)
	first := fw.editCallsSnapshot()
	require.Len(t, first, 2)

	// Simulate the SCM now returning the just-written body on the next read.
	reader.bodies["o/r#1"] = first[0].body
	reader.bodies["o/r#2"] = first[1].body
	r.syncSiblingLinks(context.Background(), "github", "tok", siblings)
	second := fw.editCallsSnapshot()
	require.Len(t, second, 2, "idempotent re-sync must not issue further edits once the block already matches")
}
