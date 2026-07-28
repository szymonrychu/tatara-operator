package v1alpha1_test

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// The grammar verdict is the ONE input the periodic unpark backstop cannot
// reconstruct from live cluster state, so it must survive on the Task itself.
func TestApprovalVerdictRoundTrips(t *testing.T) {
	at := metav1.NewTime(time.Date(2026, 7, 27, 15, 23, 55, 0, time.UTC))
	task := &v1alpha1.Task{}
	task.Status.ApprovalVerdict = &v1alpha1.ApprovalVerdict{
		At:                at,
		IssueRef:          "iss-helmfile-26",
		CommentExternalID: "3606943691",
		Author:            "szymonrychu",
		Phrase:            "go ahead",
	}

	cp := task.DeepCopy()
	cp.Status.ApprovalVerdict.Author = "someone-else"

	if task.Status.ApprovalVerdict.Author != "szymonrychu" {
		t.Fatalf("DeepCopy shares the ApprovalVerdict pointer: mutating the copy changed the original")
	}
	if cp.Status.ApprovalVerdict.IssueRef != "iss-helmfile-26" {
		t.Fatalf("IssueRef = %q, want iss-helmfile-26", cp.Status.ApprovalVerdict.IssueRef)
	}
	if !cp.Status.ApprovalVerdict.At.Equal(&at) {
		t.Fatalf("At = %v, want %v", cp.Status.ApprovalVerdict.At, at)
	}
}

// A Task that never had an approving comment carries a nil verdict, and that
// is the state the backstop must read as "the grammar has not passed".
func TestApprovalVerdictAbsentByDefault(t *testing.T) {
	task := &v1alpha1.Task{}
	if task.Status.ApprovalVerdict != nil {
		t.Fatalf("ApprovalVerdict = %v on a fresh Task, want nil", task.Status.ApprovalVerdict)
	}
}
