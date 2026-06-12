package v1alpha1_test

import (
	"reflect"
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

func TestProject_MemorySpecStatusDeepCopy(t *testing.T) {
	p := &v1alpha1.Project{
		Spec: v1alpha1.ProjectSpec{
			Memory: &v1alpha1.MemorySpec{
				PgInstances:  2,
				PgStorage:    "20Gi",
				Neo4jStorage: "5Gi",
			},
		},
		Status: v1alpha1.ProjectStatus{
			Memory: &v1alpha1.MemoryStatus{
				Phase:    "Ready",
				Endpoint: "http://mem-acme.tatara.svc:8080",
			},
		},
	}
	cp := p.DeepCopy()
	if cp.Spec.Memory == p.Spec.Memory {
		t.Fatal("spec.memory pointer not deep-copied")
	}
	if cp.Status.Memory == p.Status.Memory {
		t.Fatal("status.memory pointer not deep-copied")
	}
	if !reflect.DeepEqual(cp.Spec.Memory, p.Spec.Memory) {
		t.Fatalf("spec.memory mismatch: %+v vs %+v", cp.Spec.Memory, p.Spec.Memory)
	}
	if !reflect.DeepEqual(cp.Status.Memory, p.Status.Memory) {
		t.Fatalf("status.memory mismatch: %+v vs %+v", cp.Status.Memory, p.Status.Memory)
	}
	// Mutating the copy must not affect the original.
	cp.Spec.Memory.PgInstances = 9
	if p.Spec.Memory.PgInstances == 9 {
		t.Fatal("mutating copy mutated original (shallow copy)")
	}
}

func TestProject_MemoryNilSafe(t *testing.T) {
	p := &v1alpha1.Project{}
	cp := p.DeepCopy()
	if cp.Spec.Memory != nil || cp.Status.Memory != nil {
		t.Fatal("nil memory must deep-copy to nil")
	}
}

// TestProjectLifecycleConfigFields asserts the new lifecycle config fields
// exist on AgentSpec and ScmSpec and round-trip correctly through DeepCopy.
func TestProjectLifecycleConfigFields(t *testing.T) {
	p := &v1alpha1.Project{
		Spec: v1alpha1.ProjectSpec{
			ScmSecretRef: "scm",
			Agent: v1alpha1.AgentSpec{
				ContextWindowTokens:      200000,
				HandoverThresholdPercent: 50,
				MaxLifecycleIterations:   10,
			},
			Scm: &v1alpha1.ScmSpec{
				Provider:                "github",
				Owner:                   "acme",
				BotLogin:                "bot",
				BabysitDeadlineMinutes:  60,
				ConversationIdleMinutes: 60,
			},
		},
	}

	cp := p.DeepCopy()

	if cp.Spec.Agent.ContextWindowTokens != 200000 {
		t.Errorf("ContextWindowTokens = %d, want 200000", cp.Spec.Agent.ContextWindowTokens)
	}
	if cp.Spec.Agent.HandoverThresholdPercent != 50 {
		t.Errorf("HandoverThresholdPercent = %d, want 50", cp.Spec.Agent.HandoverThresholdPercent)
	}
	if cp.Spec.Agent.MaxLifecycleIterations != 10 {
		t.Errorf("MaxLifecycleIterations = %d, want 10", cp.Spec.Agent.MaxLifecycleIterations)
	}
	if cp.Spec.Scm.BabysitDeadlineMinutes != 60 {
		t.Errorf("BabysitDeadlineMinutes = %d, want 60", cp.Spec.Scm.BabysitDeadlineMinutes)
	}
	if cp.Spec.Scm.ConversationIdleMinutes != 60 {
		t.Errorf("ConversationIdleMinutes = %d, want 60", cp.Spec.Scm.ConversationIdleMinutes)
	}
	// Mutation safety
	cp.Spec.Agent.ContextWindowTokens = 999
	if p.Spec.Agent.ContextWindowTokens == 999 {
		t.Error("mutating copy mutated original")
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
