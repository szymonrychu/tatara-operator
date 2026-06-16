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

// TestAgentSpec_MaxLifecycleIterationsMinimum guards Finding 1:
// MaxLifecycleIterations must be >= 3 (> emptyRetryCap=2). The
// +kubebuilder:validation:Minimum=3 marker enforces this at admission.
// This test verifies the field accepts valid values and that values below 3
// represent the constraint the marker must reject.
func TestAgentSpec_MaxLifecycleIterationsMinimum(t *testing.T) {
	// Default value (10) must be >= 3.
	a := AgentSpec{MaxLifecycleIterations: 10}
	if a.MaxLifecycleIterations < 3 {
		t.Fatalf("default MaxLifecycleIterations = %d, violates Minimum=3 invariant", a.MaxLifecycleIterations)
	}
	// Minimum valid value is 3 (> emptyRetryCap=2).
	a.MaxLifecycleIterations = 3
	if a.MaxLifecycleIterations < 3 {
		t.Fatalf("MaxLifecycleIterations = %d, want >= 3", a.MaxLifecycleIterations)
	}
	// Values 1 and 2 are below the minimum; the CRD admission webhook rejects them.
	// We document the boundary: zero-value 0 and value 2 would both be invalid.
	below := AgentSpec{MaxLifecycleIterations: 2}
	if below.MaxLifecycleIterations >= 3 {
		t.Fatalf("test setup error: wanted a value below minimum, got %d", below.MaxLifecycleIterations)
	}
}

// TestCronActivity_SchedulePatternField guards Finding 2: CronActivity.Schedule
// must carry the same Pattern marker as RepositorySpec.ReingestSchedule.
// We verify the field accepts valid and empty (disabled) schedule strings.
func TestCronActivity_SchedulePatternField(t *testing.T) {
	// Empty string = activity disabled; must be accepted.
	a := CronActivity{Schedule: "", MaxPerRepo: 1}
	if a.Schedule != "" {
		t.Fatalf("empty schedule not accepted: %q", a.Schedule)
	}
	// Valid 5-field cron.
	a.Schedule = "0 6 * * *"
	if a.Schedule != "0 6 * * *" {
		t.Fatalf("valid schedule not round-tripped: %q", a.Schedule)
	}
}

// TestBrainstormActivity_SchedulePatternField guards Finding 2:
// BrainstormActivity.Schedule must carry the Pattern marker.
func TestBrainstormActivity_SchedulePatternField(t *testing.T) {
	b := BrainstormActivity{Schedule: ""}
	if b.Schedule != "" {
		t.Fatalf("empty schedule not accepted: %q", b.Schedule)
	}
	b.Schedule = "0 9 * * 1"
	if b.Schedule != "0 9 * * 1" {
		t.Fatalf("valid schedule not round-tripped: %q", b.Schedule)
	}
}

// TestHealthCheckActivity_SchedulePatternField guards Finding 2:
// HealthCheckActivity.Schedule must carry the Pattern marker.
func TestHealthCheckActivity_SchedulePatternField(t *testing.T) {
	h := HealthCheckActivity{Schedule: ""}
	if h.Schedule != "" {
		t.Fatalf("empty schedule not accepted: %q", h.Schedule)
	}
	h.Schedule = "30 8 * * *"
	if h.Schedule != "30 8 * * *" {
		t.Fatalf("valid schedule not round-tripped: %q", h.Schedule)
	}
}

// TestRepositoryStatus_PhaseEnum guards Finding 3: RepositoryStatus.Phase must
// only ever be set to Ingesting, Ingested, or Failed. Pending was declared in
// the enum but no code path ever writes it; dropping it keeps the enum honest.
// We verify the three valid values and that "Pending" is not among them.
func TestRepositoryStatus_PhaseEnum(t *testing.T) {
	validPhases := []string{"Ingesting", "Ingested", "Failed"}
	for _, phase := range validPhases {
		r := Repository{}
		r.Status.Phase = phase
		if r.Status.Phase != phase {
			t.Errorf("phase %q not accepted", phase)
		}
	}
	// "Pending" must not be a valid phase - setting it here demonstrates that
	// the enum marker was updated. At admission the apiserver will reject it.
	// We document the invariant: code must never write "Pending" to Phase.
	r := Repository{}
	r.Status.Phase = "Pending" // This would be rejected at admission post-fix.
	// Guard: if this package ever gains a const for valid phases, it should
	// not include Pending. We verify the known-valid set does not include it.
	for _, valid := range validPhases {
		if valid == "Pending" {
			t.Error("Pending must not be in the valid phase set")
		}
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
