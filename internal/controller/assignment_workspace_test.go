package controller

import (
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

func wsReviewTask() *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{}
}

// The review agent must be told its workspace is INHERITED and that touching it
// silently rewrites the merge request it is judging.
func TestAssignment_ReviewIsToldTheWorkspaceIsInherited(t *testing.T) {
	got := assignmentFor(stage.AgentReview, wsReviewTask(), &tatarav1alpha1.Project{}, true)

	for _, want := range []string{
		"NOT a fresh checkout",
		"That agent is GONE",
		"DO NOT EDIT, DELETE, REVERT, REFORMAT OR COMMIT ANYTHING",
		"as if the implementer had written it",
		"Report what you WOULD change in your findings instead.",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("review assignment is missing %q", want)
		}
	}
}

// INHERITED, never "shared": the two stages are sequential on one pod name and
// can never overlap, so telling the agent a concurrent writer exists would be
// false and would make it reason defensively about nothing.
func TestAssignment_ReviewIsNotToldItSharesALiveWorkspace(t *testing.T) {
	got := assignmentFor(stage.AgentReview, wsReviewTask(), &tatarav1alpha1.Project{}, true)
	for _, forbidden := range []string{"shared workspace", "another agent is", "concurrently", "at the same time"} {
		if strings.Contains(strings.ToLower(got), strings.ToLower(forbidden)) {
			t.Errorf("review assignment implies a concurrent writer via %q; the stages are sequential", forbidden)
		}
	}
	if !strings.Contains(got, "never concurrent") {
		t.Error("review assignment must state explicitly that the two stages are never concurrent")
	}
}

// With no persistent workspace the review pod really does get a fresh clone, so
// the paragraph must not be emitted: it would simply be false.
func TestAssignment_NoInheritedWorkspaceParagraphWhenTheFeatureIsOff(t *testing.T) {
	got := assignmentFor(stage.AgentReview, wsReviewTask(), &tatarav1alpha1.Project{}, false)
	if strings.Contains(got, "INHERITED") {
		t.Error("a review pod with no workspace volume must not be told its workspace is inherited")
	}
	if !strings.Contains(got, "submit_outcome(kind=review") {
		t.Error("the rest of the review job must be unchanged")
	}
}

// Only the review agent gets it: every other kind either starts the Task or is
// project-scoped, and none of them is reading someone else's tree.
func TestAssignment_OnlyReviewGetsTheInheritedWorkspaceRule(t *testing.T) {
	for _, kind := range []string{
		stage.AgentImplement, stage.AgentBrainstorm, stage.AgentIncident, stage.AgentDocumentation,
		stage.AgentUpgrade,
	} {
		got := assignmentFor(kind, wsReviewTask(), &tatarav1alpha1.Project{}, true)
		if strings.Contains(got, "The workspace is INHERITED") {
			t.Errorf("agent kind %q must not carry the review-only inherited-workspace rule", kind)
		}
	}
}
