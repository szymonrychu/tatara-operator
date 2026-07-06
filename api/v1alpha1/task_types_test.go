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
		{"phase deploying non-terminal", "Deploying", "", false},
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
		{"documentation with ref", "documentation", "tatara-documentation", false},
		// repo-scoped: ref missing -> error
		{"implement empty ref", "implement", "", true},
		{"review empty ref", "review", "", true},
		{"selfImprove empty ref", "selfImprove", "", true},
		{"triageIssue empty ref", "triageIssue", "", true},
		{"issueLifecycle empty ref", "issueLifecycle", "", true},
		{"documentation empty ref", "documentation", "", true},
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

// TestValidateTaskSpec_Refine verifies that "refine" is project-scoped:
// empty RepositoryRef is valid; a non-empty one must be rejected.
func TestValidateTaskSpec_Refine(t *testing.T) {
	if err := v1alpha1.ValidateTaskSpec(v1alpha1.TaskSpec{Kind: "refine"}); err != nil {
		t.Fatalf("refine with empty repositoryRef must be valid (project-scoped): %v", err)
	}
	if err := v1alpha1.ValidateTaskSpec(v1alpha1.TaskSpec{Kind: "refine", RepositoryRef: "r"}); err == nil {
		t.Fatalf("refine with a repositoryRef must be rejected (project-scoped)")
	}
	if !v1alpha1.IsProjectScopedKind("refine") {
		t.Fatalf("refine must be project-scoped")
	}
}

// TestValidateTaskSpec_Incident verifies that "incident" is project-scoped:
// empty RepositoryRef is valid; a non-empty one must be rejected.
func TestValidateTaskSpec_Incident(t *testing.T) {
	if err := v1alpha1.ValidateTaskSpec(v1alpha1.TaskSpec{Kind: "incident"}); err != nil {
		t.Fatalf("incident with empty repositoryRef must be valid: %v", err)
	}
	if err := v1alpha1.ValidateTaskSpec(v1alpha1.TaskSpec{Kind: "incident", RepositoryRef: "r"}); err == nil {
		t.Fatalf("incident with a repositoryRef must be rejected (project-scoped)")
	}
}

// TestInfraIncidentExempt covers the memory-gate exemption predicate (#236):
// only incident-kind Tasks whose AlertRule implicates core memory/storage infra
// qualify; all other kinds and non-infra alerts keep the gate.
func TestInfraIncidentExempt(t *testing.T) {
	cases := []struct {
		name string
		spec v1alpha1.TaskSpec
		want bool
	}{
		{"incident memory alert", v1alpha1.TaskSpec{Kind: "incident", AlertRule: "Memory HTTP 5xx error ratio high"}, true},
		{"incident lightrag alert", v1alpha1.TaskSpec{Kind: "incident", AlertRule: "LightRAG retrieval unreachable"}, true},
		{"incident postgres/cnpg alert", v1alpha1.TaskSpec{Kind: "incident", AlertRule: "CNPG cluster mem-tatara-pg PVC near full"}, true},
		{"incident neo4j alert", v1alpha1.TaskSpec{Kind: "incident", AlertRule: "Neo4j quorum lost"}, true},
		{"incident case-insensitive", v1alpha1.TaskSpec{Kind: "incident", AlertRule: "MEMORY stack stuck not ready"}, true},
		{"incident non-infra alert", v1alpha1.TaskSpec{Kind: "incident", AlertRule: "Agent pod OOMKilled"}, false},
		{"incident empty alert", v1alpha1.TaskSpec{Kind: "incident"}, false},
		{"non-incident memory alert", v1alpha1.TaskSpec{Kind: "implement", AlertRule: "Memory HTTP 5xx error ratio high"}, false},
		{"default kind memory alert", v1alpha1.TaskSpec{AlertRule: "Memory down"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := v1alpha1.InfraIncidentExempt(tc.spec); got != tc.want {
				t.Fatalf("InfraIncidentExempt(%+v) = %v, want %v", tc.spec, got, tc.want)
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

func TestTaskSpecReposInScope(t *testing.T) {
	// Absent field defaults to nil (single-repo behavior, no regression).
	var empty v1alpha1.TaskSpec
	if empty.ReposInScope != nil {
		t.Fatalf("zero-value ReposInScope = %v, want nil", empty.ReposInScope)
	}

	// Populated field round-trips and deep-copies independently.
	spec := v1alpha1.TaskSpec{
		ProjectRef:    "proj",
		RepositoryRef: "tatara-helmfile",
		Goal:          "fix #8",
		ReposInScope:  []string{"tatara-helmfile", "terraform", "ansible"},
	}
	task := &v1alpha1.Task{Spec: spec}
	cp := task.DeepCopy()
	cp.Spec.ReposInScope[0] = "mutated"
	if task.Spec.ReposInScope[0] != "tatara-helmfile" {
		t.Fatalf("DeepCopy did not isolate ReposInScope: original mutated to %q", task.Spec.ReposInScope[0])
	}
	if len(cp.Spec.ReposInScope) != 3 {
		t.Fatalf("DeepCopy lost elements: got %d, want 3", len(cp.Spec.ReposInScope))
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

func TestIsRecoverableGiveup(t *testing.T) {
	rec := []string{"implement-failed", "maxIterations", "refused-no-explanation", "deadline", "deploy-timeout"}
	for _, r := range rec {
		if !v1alpha1.IsRecoverableGiveup(r) {
			t.Errorf("%q should be recoverable", r)
		}
	}
	notRec := []string{"refused-declined", "already_done", "human-declined", "triage-failed", "implement-done", ""}
	for _, r := range notRec {
		if v1alpha1.IsRecoverableGiveup(r) {
			t.Errorf("%q should NOT be recoverable", r)
		}
	}
}

// TestDeployingPhaseAndPredicate covers the push-CD Deploying phase: the const,
// the TaskDeploying predicate, and that Deploying is non-terminal (a Task in
// Deploying is alive-but-podless, not finished).
func TestDeployingPhaseAndPredicate(t *testing.T) {
	if v1alpha1.PhaseDeploying != "Deploying" {
		t.Fatalf("PhaseDeploying = %q, want Deploying", v1alpha1.PhaseDeploying)
	}
	deploying := &v1alpha1.Task{Status: v1alpha1.TaskStatus{Phase: v1alpha1.PhaseDeploying}}
	if !v1alpha1.TaskDeploying(deploying) {
		t.Fatal("TaskDeploying(Deploying) = false, want true")
	}
	if v1alpha1.TaskTerminal(deploying) {
		t.Fatal("Deploying must be non-terminal")
	}
	running := &v1alpha1.Task{Status: v1alpha1.TaskStatus{Phase: v1alpha1.PhaseRunning}}
	if v1alpha1.TaskDeploying(running) {
		t.Fatal("TaskDeploying(Running) = true, want false")
	}
}

// TestChangeSummarySignificance verifies the new Significance field round-trips
// through DeepCopy independently of the original.
func TestChangeSummarySignificance(t *testing.T) {
	task := &v1alpha1.Task{
		Status: v1alpha1.TaskStatus{
			ChangeSummary: &v1alpha1.ChangeSummary{PRTitle: "feat: x", Significance: "minor"},
		},
	}
	cp := task.DeepCopy()
	if cp.Status.ChangeSummary == task.Status.ChangeSummary {
		t.Fatal("ChangeSummary must be a distinct pointer after DeepCopy")
	}
	if cp.Status.ChangeSummary.Significance != "minor" {
		t.Fatalf("Significance = %q, want minor", cp.Status.ChangeSummary.Significance)
	}
	cp.Status.ChangeSummary.Significance = "major"
	if task.Status.ChangeSummary.Significance == "major" {
		t.Fatal("mutating copy mutated original")
	}
}

// TestDeploySupervisionStatusFields verifies the deploy-supervision status fields
// set and round-trip through DeepCopy without loss or aliasing.
func TestDeploySupervisionStatusFields(t *testing.T) {
	deadline := metav1.Now()
	task := &v1alpha1.Task{
		Status: v1alpha1.TaskStatus{
			Phase:           v1alpha1.PhaseDeploying,
			DeployDeadline:  &deadline,
			CascadeStage:    "parent-pr-open",
			DeployedVersion: "v1.4.0",
			DeployArtifact:  "tatara-operator@v1.4.0",
		},
	}
	cp := task.DeepCopy()
	if cp.Status.DeployDeadline == task.Status.DeployDeadline {
		t.Fatal("DeployDeadline must be a distinct pointer after DeepCopy")
	}
	if cp.Status.DeployDeadline == nil || !cp.Status.DeployDeadline.Time.Equal(deadline.Time) {
		t.Fatal("DeployDeadline mismatch after DeepCopy")
	}
	if cp.Status.CascadeStage != "parent-pr-open" {
		t.Fatalf("CascadeStage = %q", cp.Status.CascadeStage)
	}
	if cp.Status.DeployedVersion != "v1.4.0" {
		t.Fatalf("DeployedVersion = %q", cp.Status.DeployedVersion)
	}
	if cp.Status.DeployArtifact != "tatara-operator@v1.4.0" {
		t.Fatalf("DeployArtifact = %q", cp.Status.DeployArtifact)
	}
}
