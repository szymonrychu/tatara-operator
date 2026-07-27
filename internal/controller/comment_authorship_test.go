package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// Authorship is resolved from the operator's OWN write ledger - the agentKind it
// stamped on the mirror comment when it posted it - never from a login string.
// Login matching is a documented weak boundary and there is no forge identity to
// resolve against without new SCM surface.
func TestResolveCommentAgentKind(t *testing.T) {
	mr := &tatarav1alpha1.MergeRequest{}
	mr.Status.Comments = []tatarav1alpha1.Comment{
		{ExternalID: "1", Author: "tatara-bot", IsBot: true, AgentKind: "review", CreatedAt: metav1.Now()},
		{ExternalID: "2", Author: "szymonrychu", CreatedAt: metav1.Now()},
		{ExternalID: "3", Author: "tatara-bot", IsBot: true, CreatedAt: metav1.Now()},
	}

	cases := []struct {
		name       string
		externalID string
		want       string
	}{
		{name: "an operator-authored review comment resolves to review", externalID: "1", want: "review"},
		{name: "a human comment has no agent kind", externalID: "2", want: ""},
		{name: "a bot comment with no ledger entry FAILS CLOSED", externalID: "3", want: ""},
		{name: "an unknown id FAILS CLOSED", externalID: "999", want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveCommentAgentKind(mr, tc.externalID); got != tc.want {
				t.Errorf("ResolveCommentAgentKind(%q) = %q, want %q", tc.externalID, got, tc.want)
			}
		})
	}
}

// Self-loops are impossible BY CONSTRUCTION, not by a filter that can be got
// wrong. The 2026-06 incident posted 40+ duplicate bot comments because
// reactivation was author-blind.
func TestCrossKindTriggers(t *testing.T) {
	cases := []struct {
		name     string
		author   string
		reacting string
		want     bool
	}{
		{name: "review wakes implement", author: "review", reacting: "implement", want: true},
		{name: "review wakes clarify", author: "review", reacting: "clarify", want: true},
		{name: "implement never wakes implement", author: "implement", reacting: "implement", want: false},
		{name: "review never wakes review", author: "review", reacting: "review", want: false},
		{name: "an unresolved author never triggers", author: "", reacting: "clarify", want: false},
		{name: "an unresolved reacting kind never triggers", author: "review", reacting: "", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CrossKindTriggers(tc.author, tc.reacting); got != tc.want {
				t.Errorf("CrossKindTriggers(%q, %q) = %v, want %v", tc.author, tc.reacting, got, tc.want)
			}
		})
	}
}

func TestReactingAgentKind(t *testing.T) {
	cases := []struct {
		name   string
		stage  string
		reason string
		want   string
	}{
		{name: "reviewing", stage: tatarav1alpha1.StageReviewing, want: stage.AgentReview},
		{name: "clarifying", stage: tatarav1alpha1.StageClarifying, want: stage.AgentClarify},
		{name: "conversing", stage: tatarav1alpha1.StageConversing, want: stage.AgentClarify},
		{name: "parked awaiting-human is a live conversational state", stage: tatarav1alpha1.StageParked, reason: stage.ReasonAwaitingHuman, want: stage.AgentClarify},
		{name: "parked identity-unverified is a live conversational state", stage: tatarav1alpha1.StageParked, reason: stage.ReasonIdentityUnverified, want: stage.AgentClarify},
		{name: "parked stage-deadline is NOT", stage: tatarav1alpha1.StageParked, reason: stage.ReasonStageDeadline, want: ""},
		{name: "delivered is settled", stage: tatarav1alpha1.StageDelivered, want: ""},
		{name: "merging runs no agent", stage: tatarav1alpha1.StageMerging, want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &tatarav1alpha1.Task{}
			task.Status.Stage = tc.stage
			task.Status.StageReason = tc.reason
			if got := ReactingAgentKind(task); got != tc.want {
				t.Errorf("ReactingAgentKind(%s/%s) = %q, want %q", tc.stage, tc.reason, got, tc.want)
			}
		})
	}
}
