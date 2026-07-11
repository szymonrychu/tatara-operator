package v1alpha1_test

import (
	"reflect"
	"testing"

	v1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// TestIsUmbrellaKind verifies the umbrella-kind classification: clarify/implement/
// review scope to all project repos; every other kind does not.
func TestIsUmbrellaKind(t *testing.T) {
	for _, k := range []string{"clarify", "implement", "review"} {
		if !v1alpha1.IsUmbrellaKind(k) {
			t.Fatalf("kind %q must be an umbrella kind", k)
		}
	}
	for _, k := range []string{"documentation", "brainstorm", "incident", "refine", "issueLifecycle", "", "triageIssue"} {
		if v1alpha1.IsUmbrellaKind(k) {
			t.Fatalf("kind %q must NOT be an umbrella kind", k)
		}
	}
}

// TestEffectiveReposInScope_UmbrellaAllProjectRepos verifies that an umbrella kind
// resolves to ALL enrolled project repo slugs, even when the ledger/source only
// spans one of them (the U-B fix: implement/review/clarify see every repo).
func TestEffectiveReposInScope_UmbrellaAllProjectRepos(t *testing.T) {
	task := &v1alpha1.Task{
		Spec: v1alpha1.TaskSpec{
			Kind: "implement",
			Source: &v1alpha1.TaskSource{
				Provider: "github",
				IssueRef: "o/r1#5",
				Number:   5,
			},
		},
		Status: v1alpha1.TaskStatus{
			WorkItems: []v1alpha1.WorkItemRef{
				{Provider: "github", Repo: "o/r1", Number: 5, Kind: v1alpha1.WorkItemIssue, Role: v1alpha1.RoleSource, State: v1alpha1.WIOpen},
			},
		},
	}
	all := []string{"o/r1", "o/r2", "o/r3"}
	got := v1alpha1.EffectiveReposInScope(task, all)
	want := []string{"o/r1", "o/r2", "o/r3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("umbrella scope = %v, want all project repos %v", got, want)
	}
}

// TestEffectiveReposInScope_NonUmbrellaLedgerOnly verifies that a non-umbrella
// kind keeps the ledger-derived scope (documentation stays repo-scoped, not
// widened to all project repos).
func TestEffectiveReposInScope_NonUmbrellaLedgerOnly(t *testing.T) {
	task := &v1alpha1.Task{
		Spec: v1alpha1.TaskSpec{
			Kind: "documentation",
			Source: &v1alpha1.TaskSource{
				Provider: "github",
				IssueRef: "o/r1#5",
				Number:   5,
			},
		},
		Status: v1alpha1.TaskStatus{
			WorkItems: []v1alpha1.WorkItemRef{
				{Provider: "github", Repo: "o/r1", Number: 5, Kind: v1alpha1.WorkItemIssue, Role: v1alpha1.RoleSource, State: v1alpha1.WIOpen},
			},
		},
	}
	all := []string{"o/r1", "o/r2", "o/r3"}
	got := v1alpha1.EffectiveReposInScope(task, all)
	want := []string{"o/r1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("non-umbrella scope = %v, want ledger-only %v", got, want)
	}
}
