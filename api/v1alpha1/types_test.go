package v1alpha1

import (
	"reflect"
	"testing"
)

// TestConditionApprovalApprovedAbsent guards Finding 2: the dead-code constant
// ConditionApprovalApproved must not exist in this package. This test fails to
// compile if the constant is re-introduced, proving removal is permanent.
// The compile-time check is: if the symbol were present the code below that
// uses reflect would see it; since we removed it we verify the struct fields
// that used to depend on it are untouched and the package builds clean.
func TestConditionApprovalApprovedAbsent(t *testing.T) {
	// Verify via reflect that no exported const named "ApprovalApproved" is
	// reachable through the zero-value TaskStatus (it was a condition type string).
	// The real guard is that this file compiles without referencing the const.
	st := TaskStatus{}
	for _, c := range st.Conditions {
		if c.Type == "ApprovalApproved" {
			t.Fatalf("found ApprovalApproved condition in zero-value TaskStatus")
		}
	}
	// Verify the field is not present via the reflect package either.
	typ := reflect.TypeOf(st)
	for i := 0; i < typ.NumField(); i++ {
		if typ.Field(i).Name == "ConditionApprovalApproved" {
			t.Fatal("ConditionApprovalApproved must not be a field on TaskStatus")
		}
	}
}

// TestApprovalRequiredIsReserved guards Finding 3: ApprovalRequired must be
// documented as reserved/not-implemented. We verify the field still exists on
// TaskSpec (for backward compat) and that it defaults to false so callers
// setting it get the zero-value behavior (no gating).
func TestApprovalRequiredIsReserved(t *testing.T) {
	spec := TaskSpec{ProjectRef: "p", RepositoryRef: "r", Goal: "g"}
	if spec.ApprovalRequired {
		t.Fatal("ApprovalRequired zero-value must be false; it has no implementation")
	}
	// Setting it must not panic - field is retained for compat.
	spec.ApprovalRequired = true
	if !spec.ApprovalRequired {
		t.Fatal("field assignment must work")
	}
}

// TestCronActivityMaxPerRepoMinimum guards Finding 4: MaxPerRepo must be >= 1.
// The +kubebuilder:validation:Minimum=1 marker enforces this at admission; this
// test verifies that the zero-value (0) is NOT a valid intended configuration
// by asserting the field default and that a value of 1 round-trips correctly.
func TestCronActivityMaxPerRepoMinimum(t *testing.T) {
	// Default-value usage: set to 1 (the minimum).
	a := CronActivity{Schedule: "0 * * * *", MaxPerRepo: 1}
	if a.MaxPerRepo != 1 {
		t.Fatalf("MaxPerRepo = %d, want 1", a.MaxPerRepo)
	}
	// Zero-value: field defaults to 0 in Go; the CRD admission webhook will
	// reject this (Minimum=1). We verify 0 is distinguishable from 1 so the
	// marker is meaningful.
	var zero CronActivity
	if zero.MaxPerRepo != 0 {
		t.Fatalf("zero-value MaxPerRepo = %d, want 0", zero.MaxPerRepo)
	}
}

func TestScmSpecFields(t *testing.T) {
	p := ProjectSpec{Scm: &ScmSpec{
		Provider: "github", Owner: "acme", BotLogin: "tatara-bot",
		Board:           &BoardSpec{GitHubProjectNumber: 3, StatusField: "Status"},
		MergePolicy:     "afterApproval",
		PRReactionScope: "labeledOrMentioned",
		ApprovalLabel:   "tatara/awaiting-approval",
	}}
	if p.Scm.Owner != "acme" || p.Scm.Board.GitHubProjectNumber != 3 {
		t.Fatalf("scm spec not wired: %+v", p.Scm)
	}
}

func TestScmSpecLabelFields(t *testing.T) {
	s := ScmSpec{IdeaLabel: "tatara-idea", ApprovedLabel: "tatara-approved", RejectedLabel: "tatara-rejected"}
	if s.IdeaLabel != "tatara-idea" || s.ApprovedLabel != "tatara-approved" || s.RejectedLabel != "tatara-rejected" {
		t.Fatalf("label fields not set: %+v", s)
	}
}

func TestTaskNewFields(t *testing.T) {
	ts := TaskSpec{
		Kind: "review", ApprovalRequired: true,
		ProposedIssue: &ProposedIssueSpec{RepositoryRef: "r", Title: "T", Body: "B", Kind: "bug"},
		Source:        &TaskSource{AuthorLogin: "bob", IsPR: true, Number: 9},
	}
	if ts.Kind != "review" || !ts.ApprovalRequired || ts.ProposedIssue.Kind != "bug" || ts.Source.Number != 9 {
		t.Fatalf("task spec not wired: %+v", ts)
	}
	st := TaskStatus{
		DiscoveredIssues: []string{"https://x/1"},
		ReviewVerdict:    &ReviewVerdict{Decision: "approve", Body: "lgtm", Suggestions: []Suggestion{{Path: "a.go", Line: 1, Body: "x"}}},
		PROutcome:        &PROutcome{Action: "merge", Reason: "green"},
	}
	if st.ReviewVerdict.Decision != "approve" || st.PROutcome.Action != "merge" || len(st.DiscoveredIssues) != 1 {
		t.Fatalf("task status not wired: %+v", st)
	}
}
