package controller

import (
	"strings"
	"testing"
)

// TestRequiredSkillsForKind asserts the per-kind required-skills map.
func TestRequiredSkillsForKind(t *testing.T) {
	cases := []struct {
		kind string
		want []string
	}{
		{"implement", []string{"tatara-implement-workflow", "test-driven-development"}},
		{"review", []string{"tatara-review-checklist"}},
		{"clarify", []string{"tatara-clarify-conversation"}},
		{"triageIssue", nil}, // retired kind -> no skills
		{"brainstorm", []string{"tatara-brainstorm-guardrails"}},
		{"issueLifecycle", nil}, // retired kind -> no skills
		{"incident", []string{"tatara-incident-investigation", "systematic-debugging"}},
		{"documentation", []string{"tatara-documentation-workflow"}},
		{"selfImprove", nil}, // selfImprove removed; falls to default nil (no required skills)
		{"healthCheck", nil}, // healthCheck -> empty (fail-open)
		{"refine", nil},      // refine -> empty
		{"", nil},            // unknown -> empty
		{"unknown", nil},     // unknown -> empty
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			got := requiredSkillsForKind(tc.kind)
			if len(tc.want) == 0 {
				if len(got) != 0 {
					t.Errorf("requiredSkillsForKind(%q) = %v, want nil/empty", tc.kind, got)
				}
				return
			}
			if len(got) != len(tc.want) {
				t.Errorf("requiredSkillsForKind(%q) = %v, want %v", tc.kind, got, tc.want)
				return
			}
			for i, s := range tc.want {
				if got[i] != s {
					t.Errorf("requiredSkillsForKind(%q)[%d] = %q, want %q", tc.kind, i, got[i], s)
				}
			}
		})
	}
}

// TestSkillsDirective_RequiredWording asserts "Required skills" wording for non-reference kinds.
func TestSkillsDirective_RequiredWording(t *testing.T) {
	for _, kind := range []string{"implement", "review", "incident", "documentation"} {
		t.Run(kind, func(t *testing.T) {
			d := skillsDirective(kind)
			if d == "" {
				t.Fatalf("skillsDirective(%q) returned empty; expected required-skills line", kind)
			}
			if !strings.HasPrefix(d, "Required skills this turn:") {
				t.Errorf("skillsDirective(%q) = %q, want prefix 'Required skills this turn:'", kind, d)
			}
			if !strings.Contains(d, "Invoke each before acting") {
				t.Errorf("skillsDirective(%q) = %q, want 'Invoke each before acting'", kind, d)
			}
		})
	}
}

// TestSkillsDirective_ConsultWording asserts advisory "Consult" wording for REFERENCE kinds.
func TestSkillsDirective_ConsultWording(t *testing.T) {
	for _, kind := range []string{"brainstorm", "clarify"} {
		t.Run(kind, func(t *testing.T) {
			d := skillsDirective(kind)
			if d == "" {
				t.Fatalf("skillsDirective(%q) returned empty; expected consult-skills line", kind)
			}
			if !strings.HasPrefix(d, "Consult these skills this turn:") {
				t.Errorf("skillsDirective(%q) = %q, want prefix 'Consult these skills this turn:'", kind, d)
			}
			if strings.Contains(d, "Required skills") {
				t.Errorf("skillsDirective(%q) must not use 'Required skills' wording for REFERENCE kind", kind)
			}
			if strings.Contains(d, "Invoke each before acting") {
				t.Errorf("skillsDirective(%q) must not use 'Invoke each before acting' for REFERENCE kind", kind)
			}
		})
	}
}

// TestSkillsDirective_EmptyForUnknown asserts empty string for kinds with no required skills.
func TestSkillsDirective_EmptyForUnknown(t *testing.T) {
	for _, kind := range []string{"", "unknown", "healthCheck", "refine"} {
		t.Run(kind, func(t *testing.T) {
			d := skillsDirective(kind)
			if d != "" {
				t.Errorf("skillsDirective(%q) = %q, want empty string", kind, d)
			}
		})
	}
}

// TestSkillsDirective_NamesAppearsInOutput asserts skill names appear in the directive.
func TestSkillsDirective_NamesAppearsInOutput(t *testing.T) {
	cases := []struct {
		kind  string
		skill string
	}{
		{"implement", "tatara-implement-workflow"},
		{"implement", "test-driven-development"},
		{"review", "tatara-review-checklist"},
		{"brainstorm", "tatara-brainstorm-guardrails"},
		{"incident", "tatara-incident-investigation"},
		{"incident", "systematic-debugging"},
	}
	for _, tc := range cases {
		t.Run(tc.kind+"/"+tc.skill, func(t *testing.T) {
			d := skillsDirective(tc.kind)
			if !strings.Contains(d, tc.skill) {
				t.Errorf("skillsDirective(%q) = %q, want to contain skill %q", tc.kind, d, tc.skill)
			}
		})
	}
}

// TestBrainstormSkillsDirective_ConsultWording is an explicit guard that "brainstorm"
// uses advisory "Consult" rather than mandatory "Required" wording.
func TestBrainstormSkillsDirective_ConsultWording(t *testing.T) {
	d := skillsDirective("brainstorm")
	if !strings.HasPrefix(d, "Consult these skills this turn:") {
		t.Errorf("brainstorm directive must start with 'Consult these skills this turn:', got: %q", d)
	}
	if strings.Contains(d, "Invoke each before acting") {
		t.Errorf("brainstorm directive must not include 'Invoke each before acting': %q", d)
	}
}
