package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestProjectTierMapValidation_RejectsBadKindKey asserts the apiserver
// rejects a modelByKind/effortByKind entry keyed on a Task Kind that isn't in
// the known enum (a typo like "triage-issue" must be rejected, not silently
// no-op the tiering for that kind).
func TestProjectTierMapValidation_RejectsBadKindKey(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "tier-bad-key", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "tier-bad-key-scm",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"},
			Agent: tatarav1alpha1.AgentSpec{
				ModelByKind: map[string]string{"triage-issue": "claude-sonnet-5"},
			},
		},
	}
	if err := k8sClient.Create(ctx, proj); err == nil {
		t.Fatalf("expected apiserver to reject unknown modelByKind key, got no error")
	}
}

// TestProjectTierMapValidation_RejectsBadModelValue asserts a modelByKind
// value that doesn't look like a claude model ID is rejected (prevents
// `claude --model <garbage>` BootCrashLoop).
func TestProjectTierMapValidation_RejectsBadModelValue(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "tier-bad-model", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "tier-bad-model-scm",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"},
			Agent: tatarav1alpha1.AgentSpec{
				ModelByKind: map[string]string{"review": "gpt-5"},
			},
		},
	}
	if err := k8sClient.Create(ctx, proj); err == nil {
		t.Fatalf("expected apiserver to reject non-claude modelByKind value, got no error")
	}
}

// TestProjectTierMapValidation_RejectsBadEffortValue asserts an effortByKind
// value outside the effort enum is rejected.
func TestProjectTierMapValidation_RejectsBadEffortValue(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "tier-bad-effort", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "tier-bad-effort-scm",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"},
			Agent: tatarav1alpha1.AgentSpec{
				EffortByKind: map[string]string{"review": "turbo"},
			},
		},
	}
	if err := k8sClient.Create(ctx, proj); err == nil {
		t.Fatalf("expected apiserver to reject unknown effortByKind value, got no error")
	}
}

// TestProjectTierMapValidation_AcceptsValidTierMap asserts a well-formed
// tier map (known kind keys, claude model IDs, effort enum values) is
// accepted.
func TestProjectTierMapValidation_AcceptsValidTierMap(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "tier-valid", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "tier-valid-scm",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"},
			Agent: tatarav1alpha1.AgentSpec{
				ModelByKind: map[string]string{
					"review":    "claude-sonnet-5",
					"implement": "claude-opus-4-8",
				},
				EffortByKind: map[string]string{
					"review":    "medium",
					"implement": "xhigh",
				},
			},
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("expected valid tier map to be accepted, got error: %v", err)
	}
}

// TestProjectTierMapValidation_AcceptsHealthCheckKey asserts the apiserver
// admits the "healthCheck" pseudo-key in modelByKind/effortByKind, letting
// healthCheck (which shares Kind=brainstorm) be tiered separately from
// brainstorm proper.
func TestProjectTierMapValidation_AcceptsHealthCheckKey(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "tier-healthcheck", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "tier-healthcheck-scm",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"},
			Agent: tatarav1alpha1.AgentSpec{
				ModelByKind:  map[string]string{"healthCheck": "claude-sonnet-5", "brainstorm": "claude-opus-4-8"},
				EffortByKind: map[string]string{"healthCheck": "medium", "brainstorm": "high"},
			},
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("expected healthCheck key to be accepted, got error: %v", err)
	}
}

// TestProjectTierMapValidation_AcceptsDocumentationKey asserts the apiserver
// admits "documentation" in modelByKind/effortByKind (landmine 1: the CEL key
// enum must be kept in sync with the kubebuilder Task Kind Enum).
func TestProjectTierMapValidation_AcceptsDocumentationKey(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "tier-documentation", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "tier-documentation-scm",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"},
			Agent: tatarav1alpha1.AgentSpec{
				ModelByKind:  map[string]string{"documentation": "claude-sonnet-5"},
				EffortByKind: map[string]string{"documentation": "medium"},
			},
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("expected documentation key to be accepted, got error: %v", err)
	}
}

// TestProjectTierMapValidation_SpawnCeilingByKindAcceptsDocumentation asserts
// the apiserver admits "documentation" as a spawnCeilingByKind key and
// rejects an unknown key, mirroring the modelByKind/effortByKind coverage
// (landmine 1: this is a THIRD, independent CEL gate on the same string).
func TestProjectTierMapValidation_SpawnCeilingByKindAcceptsDocumentation(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "tier-spawn-ceiling-doc", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "tier-spawn-ceiling-doc-scm",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"},
			TokenBudget: &tatarav1alpha1.TokenBudgetSpec{
				SpawnCeilingByKind: map[string]int32{"documentation": 80},
			},
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("expected documentation spawnCeilingByKind key to be accepted, got error: %v", err)
	}

	bad := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "tier-spawn-ceiling-bad", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "tier-spawn-ceiling-bad-scm",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"},
			TokenBudget: &tatarav1alpha1.TokenBudgetSpec{
				SpawnCeilingByKind: map[string]int32{"docs": 80},
			},
		},
	}
	if err := k8sClient.Create(ctx, bad); err == nil {
		t.Fatalf("expected apiserver to reject unknown spawnCeilingByKind key, got no error")
	}
}
