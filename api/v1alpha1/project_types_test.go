package v1alpha1_test

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestProjectFields(t *testing.T) {
	p := v1alpha1.Project{
		Spec: v1alpha1.ProjectSpec{
			ScmSecretRef:       "scm-secret",
			TriggerLabel:       "tatara",
			MaxConcurrentTasks: 3,
			Agent: v1alpha1.AgentSpec{
				Model:              "claude-sonnet-4-6",
				Image:              "wrapper:latest",
				PermissionMode:     "bypassPermissions",
				MaxTurnsPerTask:    50,
				TurnTimeoutSeconds: 1800,
			},
		},
		Status: v1alpha1.ProjectStatus{
			WebhookURL: "https://example/operator/webhooks/p",
			Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
		},
	}
	if p.Spec.Agent.MaxTurnsPerTask != 50 {
		t.Fatalf("MaxTurnsPerTask = %d, want 50", p.Spec.Agent.MaxTurnsPerTask)
	}
	if p.Status.WebhookURL == "" {
		t.Fatal("WebhookURL empty")
	}
}

func TestRepositoryFields(t *testing.T) {
	r := v1alpha1.Repository{
		Spec: v1alpha1.RepositorySpec{
			ProjectRef:    "p",
			URL:           "https://example/repo.git",
			DefaultBranch: "main",
			IngestEnabled: true,
		},
		Status: v1alpha1.RepositoryStatus{
			Phase:              "Ingested",
			LastIngestedCommit: "abc123",
			JobName:            "ingest-1",
		},
	}
	if r.Spec.DefaultBranch != "main" {
		t.Fatalf("DefaultBranch = %q, want main", r.Spec.DefaultBranch)
	}
	if r.Status.Phase != "Ingested" {
		t.Fatalf("Phase = %q, want Ingested", r.Status.Phase)
	}
}

func TestProjectRegisteredInScheme(t *testing.T) {
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	if !s.Recognizes(v1alpha1.GroupVersion.WithKind("Project")) {
		t.Fatal("Project kind not recognized by scheme")
	}
	if !s.Recognizes(v1alpha1.GroupVersion.WithKind("Repository")) {
		t.Fatal("Repository kind not recognized by scheme")
	}
}
