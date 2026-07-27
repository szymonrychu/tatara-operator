package restapi

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestBrainstormQuota pins brainstormQuota's fail-open-to-ceiling behaviour:
// an absent or unparseable AnnBrainstormQuota must never block an outcome, and
// the resolved value is always clamped to [1, MaxProposalsPerOutcome].
func TestBrainstormQuota(t *testing.T) {
	task := func(v string) *tatarav1alpha1.Task {
		m := metav1.ObjectMeta{Name: "brainstorm-1", Namespace: "tatara"}
		if v != "" {
			m.Annotations = map[string]string{tatarav1alpha1.AnnBrainstormQuota: v}
		}
		return &tatarav1alpha1.Task{ObjectMeta: m}
	}
	tests := []struct {
		name string
		in   *tatarav1alpha1.Task
		want int
	}{
		{"a stamped quota is honoured", task("2"), 2},
		{"an absent annotation falls back to the schema ceiling", task(""), tatarav1alpha1.MaxProposalsPerOutcome},
		{"a garbage annotation falls back to the schema ceiling", task("banana"), tatarav1alpha1.MaxProposalsPerOutcome},
		{"a quota over the ceiling is clamped", task("99"), tatarav1alpha1.MaxProposalsPerOutcome},
		{"a zero quota still permits one proposal", task("0"), 1},
		{"a negative quota still permits one proposal", task("-3"), 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := brainstormQuota(tc.in); got != tc.want {
				t.Fatalf("brainstormQuota = %d, want %d", got, tc.want)
			}
		})
	}
}
