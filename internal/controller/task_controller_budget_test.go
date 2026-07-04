package controller

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestTaskTokenBudgetExceeded(t *testing.T) {
	mk := func(kind string, limit, used int64) (*tatarav1alpha1.Project, *tatarav1alpha1.Task) {
		p := &tatarav1alpha1.Project{}
		p.Spec.Agent.MaxTaskTokens = limit
		tk := &tatarav1alpha1.Task{}
		tk.Spec.Kind = kind
		tk.Status.CumulativeTokens = used
		return p, tk
	}
	cases := []struct {
		name  string
		kind  string
		limit int64
		used  int64
		want  bool
	}{
		{"implement under budget continues", "implement", 1000, 500, false},
		{"implement at budget stops", "implement", 1000, 1000, true},
		{"implement over budget stops", "implement", 1000, 1500, true},
		{"issueLifecycle over budget stops", "issueLifecycle", 1000, 2000, true},
		{"review over budget not gated", "review", 1000, 5000, false},
		{"triageIssue over budget not gated", "triageIssue", 1000, 5000, false},
		{"implement disabled when limit zero", "implement", 0, 9_999_999, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, tk := mk(tc.kind, tc.limit, tc.used)
			if got := taskTokenBudgetExceeded(p, tk); got != tc.want {
				t.Errorf("taskTokenBudgetExceeded(kind=%s limit=%d used=%d) = %v, want %v",
					tc.kind, tc.limit, tc.used, got, tc.want)
			}
		})
	}
}
