package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"k8s.io/apimachinery/pkg/types"
)

// recordApproval stamps a verified maintainer approval on the seeded task's
// status (the identity-verified fact the webhook records when a MaintainerLogins
// member applies the approved label).
func recordApproval(t *testing.T, name, maintainer string) {
	t.Helper()
	ctx := context.Background()
	tk := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); err != nil {
		t.Fatalf("get task: %v", err)
	}
	tk.Status.ApprovedByMaintainer = maintainer
	if err := k8sClient.Status().Update(ctx, tk); err != nil {
		t.Fatalf("record approval: %v", err)
	}
}

// TestApprovalGate_NoApproval_FailsClosed: a triage that returned implement with
// NO recorded maintainer approval must fail CLOSED to Conversation - never
// advance the autonomous chain to Implement.
func TestApprovalGate_NoApproval_FailsClosed(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "gate-noappr", "szymon", []string{"szymon"})
	if got := reconcileTriageState(t, r, name); got != "Conversation" {
		t.Fatalf("DeployState = %q, want Conversation (no recorded approval must fail closed)", got)
	}
}

// TestApprovalGate_MaintainerApproval_Implements: a recorded verified maintainer
// approval is the ONLY signal that releases the implement outcome to Implement.
func TestApprovalGate_MaintainerApproval_Implements(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "gate-appr", "szymon", []string{"szymon"})
	recordApproval(t, name, "szymon")
	if got := reconcileTriageState(t, r, name); got != "Implement" {
		t.Fatalf("DeployState = %q, want Implement (recorded maintainer approval releases)", got)
	}
}

// TestApprovalGate_MaintainerComment_DoesNotRelease: a maintainer COMMENT (no
// recorded label approval) must NOT release the gate - comments are no longer an
// approval signal (the any-comment / approver-comment release is removed).
func TestApprovalGate_MaintainerComment_DoesNotRelease(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "gate-cmt", "szymon", []string{"szymon"})
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &commentReader{body: tataraAuthoredMarker,
			comments: []scm.IssueComment{{Author: "szymon", Body: "approved"}}}, nil
	}
	if got := reconcileTriageState(t, r, name); got != "Conversation" {
		t.Fatalf("DeployState = %q, want Conversation (maintainer comment must not release the gate)", got)
	}
}

// TestApprovalGate_ThirdPartyNoApproval_FailsClosed: a third-party-authored issue
// with no recorded approval must fail closed - the old author-tier autoapprove
// bypass (thirdPartyAuthor) is removed from the implement decision.
func TestApprovalGate_ThirdPartyNoApproval_FailsClosed(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "gate-3p", "third-party-dev", []string{"szymon"})
	if got := reconcileTriageState(t, r, name); got != "Conversation" {
		t.Fatalf("DeployState = %q, want Conversation (third-party without recorded approval must fail closed)", got)
	}
}

// TestApprovalGate_EmptyMaintainerList_FailsClosed: with no maintainers
// configured a recorded approval cannot exist, so every implement outcome fails
// closed (closed-by-default trust).
func TestApprovalGate_EmptyMaintainerList_FailsClosed(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "gate-empty", "third-party-dev", nil)
	if got := reconcileTriageState(t, r, name); got != "Conversation" {
		t.Fatalf("DeployState = %q, want Conversation (no maintainers => nothing can be approved)", got)
	}
}
