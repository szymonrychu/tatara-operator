package controller

import (
	"context"
	"fmt"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestStampProposalKind covers the write-once provenance backfill: an Issue
// minted before Spec.ProposalKind existed carries an empty kind, so it would
// count as 0 pending on rollout and the controller would refill to N on top of
// the backlog that is already open.
//
// The two "already-stamped" cases are the security property, not a nicety: the
// mirror writes Status.Body from the forge, so if the backfill recomputed a
// stamped kind, a maintainer (or anyone with write access to the issue) could
// flip or clear provenance by editing the body.
func TestStampProposalKind(t *testing.T) {
	brainstormBody := tatarav1alpha1.StampProposalMarker("body", tatarav1alpha1.ProposalKindBrainstorm)
	incidentBody := tatarav1alpha1.StampProposalMarker("body", tatarav1alpha1.ProposalKindIncident)

	tests := []struct {
		name     string
		specKind string
		body     string
		author   string
		wantKind string
	}{
		{"empty kind with a brainstorm marker is backfilled", "", brainstormBody, testBotLogin, "brainstorm"},
		{"empty kind with an incident marker is backfilled", "", incidentBody, testBotLogin, "incident"},
		{"no marker is left alone", "", "a plain body", testBotLogin, ""},
		{"an already-stamped kind is never recomputed", "brainstorm", incidentBody, testBotLogin, "brainstorm"},
		{"a body edit that strips the marker cannot clear a stamped kind", "brainstorm", "edited away", testBotLogin, "brainstorm"},

		// The authorship anchor. Without it, anyone with forge write access to any
		// issue on a tracked repo could paste the marker into it and have the
		// operator PERMANENTLY (write-once) stamp it as a proposal.
		{"a marker planted in a human-authored issue is never stamped", "", brainstormBody, "mallory", ""},
		{"an empty author is never the bot", "", brainstormBody, "", ""},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			r := newIssueReconciler(k8sClient, nil, nil)
			name := fmt.Sprintf("iss-pk-%d", i)
			iss := &tatarav1alpha1.Issue{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
				Spec: tatarav1alpha1.IssueSpec{
					RepositoryRef: "pk", Number: i + 1, ProjectRef: "demo",
					URL:          "https://github.com/o/pk/issues/1",
					ProposalKind: tc.specKind,
				},
			}
			if err := r.Create(ctx, iss); err != nil {
				t.Fatalf("create: %v", err)
			}
			t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), iss) })
			iss.Status.Body = tc.body
			iss.Status.Author = tc.author
			if err := r.Status().Update(ctx, iss); err != nil {
				t.Fatalf("seed status: %v", err)
			}
			if err := r.stampProposalKind(ctx, iss, testBotLogin); err != nil {
				t.Fatalf("stampProposalKind: %v", err)
			}
			var got tatarav1alpha1.Issue
			if err := r.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, &got); err != nil {
				t.Fatalf("get: %v", err)
			}
			if got.Spec.ProposalKind != tc.wantKind {
				t.Fatalf("ProposalKind = %q, want %q", got.Spec.ProposalKind, tc.wantKind)
			}
			// The backfill must also update the in-memory copy the rest of the
			// reconcile keeps working with, so a same-pass reader sees the stamp.
			if iss.Spec.ProposalKind != tc.wantKind {
				t.Fatalf("in-memory ProposalKind = %q, want %q", iss.Spec.ProposalKind, tc.wantKind)
			}
		})
	}
}
