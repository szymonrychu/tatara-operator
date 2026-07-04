package agent

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestModelForKind(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Agent.Model = "claude-opus-4-8"
	proj.Spec.Agent.ModelByKind = map[string]string{
		"review":      "claude-sonnet-5",
		"triageIssue": "claude-sonnet-5",
	}
	cases := []struct {
		name, kind, want string
	}{
		{"override present review", "review", "claude-sonnet-5"},
		{"override present triage", "triageIssue", "claude-sonnet-5"},
		{"override absent falls back", "implement", "claude-opus-4-8"},
		{"unknown kind falls back", "bogus", "claude-opus-4-8"},
		{"empty kind falls back", "", "claude-opus-4-8"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := modelForKind(proj, tc.kind); got != tc.want {
				t.Fatalf("modelForKind(%q) = %q, want %q", tc.kind, got, tc.want)
			}
		})
	}
}

func TestModelForKind_NilAndEmptyOverride(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Agent.Model = "claude-opus-4-8"
	if got := modelForKind(proj, "review"); got != "claude-opus-4-8" {
		t.Fatalf("nil map: modelForKind = %q, want claude-opus-4-8", got)
	}
	proj.Spec.Agent.ModelByKind = map[string]string{"review": ""}
	if got := modelForKind(proj, "review"); got != "claude-opus-4-8" {
		t.Fatalf("empty override treated as set: modelForKind = %q, want claude-opus-4-8", got)
	}
}

func TestEffortForKind(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Agent.Effort = "high"
	proj.Spec.Agent.EffortByKind = map[string]string{
		"review":      "medium",
		"triageIssue": "low",
	}
	cases := []struct {
		name, kind, want string
	}{
		{"override present review", "review", "medium"},
		{"override present triage", "triageIssue", "low"},
		{"override absent falls back", "implement", "high"},
		{"unknown kind falls back", "bogus", "high"},
		{"empty kind falls back", "", "high"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effortForKind(proj, tc.kind); got != tc.want {
				t.Fatalf("effortForKind(%q) = %q, want %q", tc.kind, got, tc.want)
			}
		})
	}
}

func TestEffortForKind_NilAndEmptyOverride(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Agent.Effort = "high"
	if got := effortForKind(proj, "review"); got != "high" {
		t.Fatalf("nil map: effortForKind = %q, want high", got)
	}
	proj.Spec.Agent.EffortByKind = map[string]string{"review": ""}
	if got := effortForKind(proj, "review"); got != "high" {
		t.Fatalf("empty override treated as set: effortForKind = %q, want high", got)
	}
}
