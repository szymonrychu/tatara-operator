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

// The `conversing` stage is DISSOLVED (#521): entry into a live state is no
// longer its own stage, it is the stage.Live(state) property over
// refined/under-implementation/awaiting-review. ReactingAgentKind's job is
// unchanged - answer with the kind the Task's OWN state (or, parked, its
// ParkedFromState) would run - but the cases now exercise that property
// across more than one live state, plus a park whose ParkedFromState carries
// the same information the deleted stage used to encode directly.
func TestReactingAgentKind(t *testing.T) {
	cases := []struct {
		name            string
		specKind        string
		state           string
		parkReason      string
		parkedFromState string
		want            string
	}{
		{name: "reviewing", specKind: "implement", state: tatarav1alpha1.StateAwaitingReview, want: stage.AgentReview},
		{name: "refined (a live state) resolves off spec.kind", specKind: "implement", state: tatarav1alpha1.StateRefined, want: stage.AgentImplement},
		{name: "under-implementation (a live state) resolves off spec.kind", specKind: "implement", state: tatarav1alpha1.StateUnderImplementation, want: stage.AgentImplement},
		{
			name:     "parked awaiting-human is a live conversational state: resolves off parkedFromState",
			specKind: "implement", state: tatarav1alpha1.StateUnderImplementation,
			parkReason: stage.ReasonAwaitingHuman, parkedFromState: tatarav1alpha1.StateUnderImplementation,
			want: stage.AgentImplement,
		},
		{
			name:     "parked identity-unverified is a live conversational state: resolves off parkedFromState",
			specKind: "implement", state: tatarav1alpha1.StateAwaitingReview,
			parkReason: stage.ReasonIdentityUnverified, parkedFromState: tatarav1alpha1.StateAwaitingReview,
			want: stage.AgentReview,
		},
		{
			name:     "parked stage-deadline is NOT a live conversational state",
			specKind: "implement", state: tatarav1alpha1.StateUnderImplementation,
			parkReason: stage.ReasonStageDeadline, parkedFromState: tatarav1alpha1.StateUnderImplementation,
			want: "",
		},
		{name: "done is settled", specKind: "implement", state: tatarav1alpha1.StateDone, want: ""},
		{name: "merged runs no agent", specKind: "implement", state: tatarav1alpha1.StateMerged, want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &tatarav1alpha1.Task{}
			task.Spec.Kind = tc.specKind
			task.Status.State = tc.state
			task.Status.ParkReason = tc.parkReason
			task.Status.ParkedFromState = tc.parkedFromState
			if got := ReactingAgentKind(task); got != tc.want {
				t.Errorf("ReactingAgentKind(%s/%s) = %q, want %q", tc.state, tc.parkReason, got, tc.want)
			}
		})
	}
}
