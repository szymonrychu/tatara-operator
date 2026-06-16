package v1alpha1_test

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestTaskFields(t *testing.T) {
	task := v1alpha1.Task{
		Spec: v1alpha1.TaskSpec{
			ProjectRef:    "p",
			RepositoryRef: "r",
			Goal:          "do the thing",
			Source: &v1alpha1.TaskSource{
				Provider: "github",
				IssueRef: "owner/repo#123",
				URL:      "https://github.com/owner/repo/issues/123",
			},
			MaxTurns: 25,
		},
		Status: v1alpha1.TaskStatus{
			Phase:          "Running",
			PodName:        "task-p-1",
			TurnsCompleted: 4,
			PrURL:          "https://github.com/owner/repo/pull/5",
			ResultSummary:  "opened PR",
		},
	}
	if task.Spec.Source.Provider != "github" {
		t.Fatalf("Source.Provider = %q, want github", task.Spec.Source.Provider)
	}
	if task.Status.TurnsCompleted != 4 {
		t.Fatalf("TurnsCompleted = %d, want 4", task.Status.TurnsCompleted)
	}
}

func TestSubtaskFields(t *testing.T) {
	s := v1alpha1.Subtask{
		Spec: v1alpha1.SubtaskSpec{
			TaskRef: "task-p-1",
			Title:   "write test",
			Detail:  "add the failing test",
			Order:   1,
		},
		Status: v1alpha1.SubtaskStatus{
			Phase:  "Done",
			TurnID: "turn-abc",
			Result: "test added",
		},
	}
	if s.Spec.Order != 1 {
		t.Fatalf("Order = %d, want 1", s.Spec.Order)
	}
	if s.Status.TurnID != "turn-abc" {
		t.Fatalf("TurnID = %q, want turn-abc", s.Status.TurnID)
	}
}

// TestTaskLifecycleStatusFields asserts that all new lifecycle fields can be
// set on TaskStatus and round-trip through DeepCopy without loss.
func TestTaskLifecycleStatusFields(t *testing.T) {
	now := metav1.Now()
	later := metav1.NewTime(now.Add(60 * 1e9))

	task := &v1alpha1.Task{
		Spec: v1alpha1.TaskSpec{
			ProjectRef:    "p",
			RepositoryRef: "r",
			Goal:          "issue lifecycle",
			Kind:          "issueLifecycle",
		},
		Status: v1alpha1.TaskStatus{
			LifecycleState:      "Triage",
			LastActivityAt:      &now,
			DeadlineAt:          &later,
			HeadBranch:          "tatara/task-foo",
			PRNumber:            42,
			MergeCommitSHA:      "abc123",
			CumulativeTokens:    100000,
			LastTurnInputTokens: 50000,
			LifecycleIterations: 2,
			Handover:            "resume from here",
		},
	}

	cp := task.DeepCopy()

	if cp.Spec.Kind != "issueLifecycle" {
		t.Errorf("Kind = %q, want issueLifecycle", cp.Spec.Kind)
	}
	if cp.Status.LifecycleState != "Triage" {
		t.Errorf("LifecycleState = %q, want Triage", cp.Status.LifecycleState)
	}
	if cp.Status.LastActivityAt == nil || !cp.Status.LastActivityAt.Time.Equal(now.Time) {
		t.Errorf("LastActivityAt mismatch")
	}
	if cp.Status.DeadlineAt == nil || !cp.Status.DeadlineAt.Time.Equal(later.Time) {
		t.Errorf("DeadlineAt mismatch")
	}
	if cp.Status.HeadBranch != "tatara/task-foo" {
		t.Errorf("HeadBranch = %q, want tatara/task-foo", cp.Status.HeadBranch)
	}
	if cp.Status.PRNumber != 42 {
		t.Errorf("PRNumber = %d, want 42", cp.Status.PRNumber)
	}
	if cp.Status.MergeCommitSHA != "abc123" {
		t.Errorf("MergeCommitSHA = %q, want abc123", cp.Status.MergeCommitSHA)
	}
	if cp.Status.CumulativeTokens != 100000 {
		t.Errorf("CumulativeTokens = %d, want 100000", cp.Status.CumulativeTokens)
	}
	if cp.Status.LastTurnInputTokens != 50000 {
		t.Errorf("LastTurnInputTokens = %d, want 50000", cp.Status.LastTurnInputTokens)
	}
	if cp.Status.LifecycleIterations != 2 {
		t.Errorf("LifecycleIterations = %d, want 2", cp.Status.LifecycleIterations)
	}
	if cp.Status.Handover != "resume from here" {
		t.Errorf("Handover = %q, want 'resume from here'", cp.Status.Handover)
	}
	// Mutation safety: changing copy must not affect original
	cp.Status.LifecycleState = "Done"
	if task.Status.LifecycleState == "Done" {
		t.Error("mutating copy mutated original")
	}
}

func TestTaskAndSubtaskRegisteredInScheme(t *testing.T) {
	sch := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(sch); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	for _, kind := range []string{"Task", "Subtask"} {
		if !sch.Recognizes(v1alpha1.GroupVersion.WithKind(kind)) {
			t.Fatalf("%s kind not recognized by scheme", kind)
		}
	}
}

// TestTaskTerminal covers Finding 3: the TaskTerminal helper must correctly
// identify terminal tasks across both Phase and LifecycleState, so that
// issueLifecycle tasks (which leave Phase empty for their whole life) are not
// treated as indefinitely open by lane-occupancy and similar predicates.
func TestTaskTerminal(t *testing.T) {
	cases := []struct {
		name  string
		phase string
		ls    string
		want  bool
	}{
		{"phase succeeded", "Succeeded", "", true},
		{"phase failed", "Failed", "", true},
		{"phase running", "Running", "", false},
		{"phase planning", "Planning", "", false},
		{"phase empty+ls done", "", "Done", true},
		{"phase empty+ls stopped", "", "Stopped", true},
		{"phase empty+ls parked", "", "Parked", true},
		{"phase empty+ls implement", "", "Implement", false},
		{"phase empty+ls triage", "", "Triage", false},
		{"phase empty+ls empty", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &v1alpha1.Task{
				Status: v1alpha1.TaskStatus{
					Phase:          tc.phase,
					LifecycleState: tc.ls,
				},
			}
			if got := v1alpha1.TaskTerminal(task); got != tc.want {
				t.Errorf("TaskTerminal = %v, want %v (phase=%q ls=%q)", got, tc.want, tc.phase, tc.ls)
			}
		})
	}
}

// TestTaskSpec_ValidateRepositoryRef tests the ValidateTaskSpec helper that
// enforces the RepositoryRef contract:
//   - repo-scoped kinds (implement, review, selfImprove, triageIssue, issueLifecycle)
//     REQUIRE a non-empty RepositoryRef.
//   - project-scoped kinds (brainstorm, healthCheck) MUST have an empty RepositoryRef.
func TestTaskSpec_ValidateRepositoryRef(t *testing.T) {
	cases := []struct {
		name    string
		kind    string
		repoRef string
		wantErr bool
	}{
		// repo-scoped: ref required
		{"implement with ref", "implement", "my-repo", false},
		{"review with ref", "review", "my-repo", false},
		{"selfImprove with ref", "selfImprove", "my-repo", false},
		{"triageIssue with ref", "triageIssue", "my-repo", false},
		{"issueLifecycle with ref", "issueLifecycle", "my-repo", false},
		// repo-scoped: ref missing -> error
		{"implement empty ref", "implement", "", true},
		{"review empty ref", "review", "", true},
		{"selfImprove empty ref", "selfImprove", "", true},
		{"triageIssue empty ref", "triageIssue", "", true},
		{"issueLifecycle empty ref", "issueLifecycle", "", true},
		// project-scoped: ref must be empty
		{"brainstorm empty ref", "brainstorm", "", false},
		{"healthCheck empty ref", "healthCheck", "", false},
		// project-scoped: ref non-empty -> error
		{"brainstorm with ref", "brainstorm", "my-repo", true},
		{"healthCheck with ref", "healthCheck", "my-repo", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := v1alpha1.ValidateTaskSpec(v1alpha1.TaskSpec{
				ProjectRef:    "proj",
				RepositoryRef: tc.repoRef,
				Kind:          tc.kind,
				Goal:          "do something",
			})
			if tc.wantErr && err == nil {
				t.Fatalf("want validation error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("want no error, got %v", err)
			}
		})
	}
}

// TestSuggestionLineMarker verifies Suggestion.Line is present and accepts a
// valid positive value (the +kubebuilder:validation:Minimum=1 marker is a
// CRD-schema concern; this test guards the struct field exists and is
// round-trippable).
func TestSuggestionLineField(t *testing.T) {
	s := v1alpha1.Suggestion{Path: "foo.go", Line: 42, Body: "comment"}
	if s.Line != 42 {
		t.Fatalf("Line = %d, want 42", s.Line)
	}
}

// TestImplementOutcomeDeepCopy verifies that a non-nil ImplementOutcome is
// deep-copied (not shallow-aliased) so mutations through the copy cannot
// corrupt the informer-cache original (Finding 2).
func TestImplementOutcomeDeepCopy(t *testing.T) {
	task := &v1alpha1.Task{
		Status: v1alpha1.TaskStatus{
			ImplementOutcome: &v1alpha1.ImplementOutcome{Action: "declined", Reason: "scope too large"},
		},
	}
	cp := task.DeepCopy()
	if cp.Status.ImplementOutcome == task.Status.ImplementOutcome {
		t.Fatal("ImplementOutcome must be a distinct pointer after DeepCopy")
	}
	if cp.Status.ImplementOutcome.Action != "declined" {
		t.Errorf("Action = %q, want declined", cp.Status.ImplementOutcome.Action)
	}
	cp.Status.ImplementOutcome.Reason = "mutated"
	if task.Status.ImplementOutcome.Reason == "mutated" {
		t.Error("mutating copy's ImplementOutcome mutated original (shallow copy)")
	}
}
