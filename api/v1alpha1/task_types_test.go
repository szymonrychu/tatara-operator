package v1alpha1_test

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestTaskSpec_DedupKeyRoundTrips(t *testing.T) {
	spec := v1alpha1.TaskSpec{DedupKey: "deadbeefcafe1234"}
	if spec.DedupKey != "deadbeefcafe1234" {
		t.Fatalf("DedupKey = %q, want deadbeefcafe1234", spec.DedupKey)
	}
}

func TestTaskRegisteredInScheme(t *testing.T) {
	sch := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(sch); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	if !sch.Recognizes(v1alpha1.GroupVersion.WithKind("Task")) {
		t.Fatalf("Task kind not recognized by scheme")
	}
}

// TestValidateTaskSpec_UmbrellaScopes verifies the 7-kind redesign scoping:
// implement/review/clarify are project-scoped umbrellas whose validator accepts
// either an empty RepositoryRef (the umbrella norm) or a stored/legacy repo ref;
// documentation is the one repo-scoped agent kind (empty ref -> error); retired
// kinds stay valid for stored CRs.
func TestValidateTaskSpec_UmbrellaScopes(t *testing.T) {
	cases := []struct {
		name    string
		kind    string
		repoRef string
		wantErr bool
	}{
		{"implement umbrella empty ref", "implement", "", false},
		{"review umbrella empty ref", "review", "", false},
		{"clarify umbrella empty ref", "clarify", "", false},
		{"documentation empty ref rejected", "documentation", "", true},
		{"documentation with ref", "documentation", "tatara-documentation", false},
		{"retired issueLifecycle with ref stays valid", "issueLifecycle", "my-repo", false},
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
	// clarify is not project-scoped in the IsProjectScopedKind sense (it must not
	// be caught by the writeback project-scoped fence).
	if v1alpha1.IsProjectScopedKind("clarify") {
		t.Fatalf("clarify must not be classified project-scoped by IsProjectScopedKind")
	}
	if v1alpha1.IsProjectScopedKind("implement") {
		t.Fatalf("implement must not be classified project-scoped (it opens PRs)")
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
		{"incident memory alert", v1alpha1.TaskSpec{Kind: "incident", AlertRules: []string{"Memory HTTP 5xx error ratio high"}}, true},
		{"incident lightrag alert", v1alpha1.TaskSpec{Kind: "incident", AlertRules: []string{"LightRAG retrieval unreachable"}}, true},
		{"incident postgres/cnpg alert", v1alpha1.TaskSpec{Kind: "incident", AlertRules: []string{"CNPG cluster mem-tatara-pg PVC near full"}}, true},
		{"incident neo4j alert", v1alpha1.TaskSpec{Kind: "incident", AlertRules: []string{"Neo4j quorum lost"}}, true},
		{"incident case-insensitive", v1alpha1.TaskSpec{Kind: "incident", AlertRules: []string{"MEMORY stack stuck not ready"}}, true},
		{"incident non-infra alert", v1alpha1.TaskSpec{Kind: "incident", AlertRules: []string{"Agent pod OOMKilled"}}, false},
		{"incident empty alert", v1alpha1.TaskSpec{Kind: "incident"}, false},
		{"non-incident memory alert", v1alpha1.TaskSpec{Kind: "implement", AlertRules: []string{"Memory HTTP 5xx error ratio high"}}, false},
		{"default kind memory alert", v1alpha1.TaskSpec{AlertRules: []string{"Memory down"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := v1alpha1.InfraIncidentExempt(tc.spec); got != tc.want {
				t.Fatalf("InfraIncidentExempt(%+v) = %v, want %v", tc.spec, got, tc.want)
			}
		})
	}
}
