package controller

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func turnCapProject(maxPerTask int) *tatarav1alpha1.Project {
	p := &tatarav1alpha1.Project{}
	p.Spec.Agent.MaxTurnsPerTask = maxPerTask
	return p
}

func turnCapTask(kind string, maxTurns int) *tatarav1alpha1.Task {
	tk := &tatarav1alpha1.Task{}
	tk.Spec.Kind = kind
	tk.Spec.MaxTurns = maxTurns
	return tk
}

func TestTurnCap(t *testing.T) {
	tests := []struct {
		name       string
		kind       string
		maxTurns   int
		projMax    int
		wantCap    int
		wantCapped bool
	}{
		{"implement uncapped", "implement", 0, 50, 0, false},
		{"issueLifecycle uncapped", "issueLifecycle", 0, 50, 0, false},
		{"implement uncapped even with no project setting", "implement", 0, 0, 0, false},
		{"explicit MaxTurns caps implement", "implement", 10, 50, 10, true},
		{"explicit MaxTurns caps issueLifecycle", "issueLifecycle", 7, 50, 7, true},
		{"review capped at project value", "review", 0, 50, 50, true},
		{"brainstorm capped at project value", "brainstorm", 0, 50, 50, true},
		{"triageIssue default cap", "triageIssue", 0, 0, 50, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limit, capped := turnCap(turnCapProject(tt.projMax), turnCapTask(tt.kind, tt.maxTurns))
			if capped != tt.wantCapped {
				t.Fatalf("capped = %v, want %v", capped, tt.wantCapped)
			}
			if capped && limit != tt.wantCap {
				t.Errorf("cap = %d, want %d", limit, tt.wantCap)
			}
		})
	}
}
