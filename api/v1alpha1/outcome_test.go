package v1alpha1

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func taskWithOutcome(reason string, status metav1.ConditionStatus) *Task {
	return &Task{
		Status: TaskStatus{
			Conditions: []metav1.Condition{{
				Type:               ConditionOutcomeAccepted,
				Status:             status,
				Reason:             reason,
				Message:            "deadbeef",
				LastTransitionTime: metav1.NewTime(time.Unix(0, 0)),
			}},
		},
	}
}

func TestOutcomeCommitted(t *testing.T) {
	tests := []struct {
		name string
		task *Task
		want bool
	}{
		{"no condition at all", &Task{}, false},
		{"a BARE CLAIM is not committed", taskWithOutcome(OutcomeReasonClaimed, metav1.ConditionTrue), false},
		{"a committed review outcome", taskWithOutcome("Review", metav1.ConditionTrue), true},
		{"a committed clarify outcome", taskWithOutcome("Clarify", metav1.ConditionTrue), true},
		{"status False is never committed", taskWithOutcome("Review", metav1.ConditionFalse), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := OutcomeCommitted(tc.task); got != tc.want {
				t.Fatalf("OutcomeCommitted = %v, want %v", got, tc.want)
			}
		})
	}
}

// The per-TASK condition survives across stages, so "is anything committed" is
// NOT a safe guard: an implement Task arrives at reviewing with Reason=Implement
// already committed, and a guard keying on that would suppress the review pod
// that has not spawned yet. The guard must ask "did THIS stage's own agent
// commit".
func TestOutcomeCommittedFor(t *testing.T) {
	tests := []struct {
		name      string
		task      *Task
		agentKind string
		want      bool
	}{
		{"review committed, asking about review", taskWithOutcome("Review", metav1.ConditionTrue), "review", true},
		{"implement committed, asking about review (the reviewing stage of an implement Task)",
			taskWithOutcome("Implement", metav1.ConditionTrue), "review", false},
		{"documentation committed, asking about review",
			taskWithOutcome("Documentation", metav1.ConditionTrue), "review", false},
		{"incident committed, asking about clarify (the clarifying stage of an incident Task)",
			taskWithOutcome("Incident", metav1.ConditionTrue), "clarify", false},
		{"a BARE CLAIM never matches (B3: the ArgoCD-wedge guard)",
			taskWithOutcome(OutcomeReasonClaimed, metav1.ConditionTrue), "review", false},
		{"a pod-less stage has no agent kind and never matches",
			taskWithOutcome("Clarify", metav1.ConditionTrue), "", false},
		{"no condition", &Task{}, "review", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := OutcomeCommittedFor(tc.task, tc.agentKind); got != tc.want {
				t.Fatalf("OutcomeCommittedFor(%q) = %v, want %v", tc.agentKind, got, tc.want)
			}
		})
	}
}

func TestOutcomeReasonFor(t *testing.T) {
	tests := map[string]string{
		"":              OutcomeReasonClaimed,
		"review":        "Review",
		"implement":     "Implement",
		"documentation": "Documentation",
		"clarify":       "Clarify",
		"brainstorm":    "Brainstorm",
		"incident":      "Incident",
		"refine":        "Refine",
	}
	for in, want := range tests {
		if got := OutcomeReasonFor(in); got != want {
			t.Fatalf("OutcomeReasonFor(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestOutcomeClaimTTLAndHandoffDeadline(t *testing.T) {
	if OutcomeClaimTTL != 60*time.Second {
		t.Fatalf("OutcomeClaimTTL = %v, want 60s", OutcomeClaimTTL)
	}
	if HandoffDeadline != 5*time.Minute {
		t.Fatalf("HandoffDeadline = %v, want 5m", HandoffDeadline)
	}
}
