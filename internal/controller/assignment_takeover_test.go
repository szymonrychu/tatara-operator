package controller

import (
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// takeoverAssignmentTask is a kind=takeover Task as MintOrUnparkTakeoverTask
// builds one: bound to an existing PR, carrying the MR's own head branch as the
// branch its pod pushes to.
func takeoverAssignmentTask() *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "proj", RepositoryRef: "repo", Kind: "takeover",
			Goal:         "Take over https://github.com/o/r/pull/9 at @alice's request.",
			InitialState: tatarav1alpha1.StateUnderImplementation,
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", Number: 9, IsPR: true,
				URL: "https://github.com/o/r/pull/9",
			},
		},
	}
}

// THE TAKEOVER SKILL SET WAS DEAD CODE UNTIL #604.
//
// requiredSkillsForKind has had a `case "takeover"` arm since the MR-ownership
// design, but skillsDirective was called with the AGENT kind, and takeover maps
// to AgentImplement (stage.originAgentKinds). So a takeover pod was handed the
// IMPLEMENT list - i.e. it was REQUIRED to invoke tatara-implement-gate, for a
// gate it owns zero Issues for and can never pass.
func TestRequiredSkills_TakeoverGetsItsOwnSkillsAndNotTheGate(t *testing.T) {
	got := requiredSkillsForKind("takeover")
	want := []string{"tatara-implement-workflow", "tatara-implement-takeover", "test-driven-development"}
	if len(got) != len(want) {
		t.Fatalf("requiredSkillsForKind(takeover) = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("requiredSkillsForKind(takeover) = %v, want %v", got, want)
		}
	}
	for _, s := range got {
		if s == "tatara-implement-gate" {
			t.Fatalf("takeover must NOT require the gate skill: it owns zero Issues, so the gate it "+
				"describes can never grant. got %v", got)
		}
	}
}

// The whole point of the threading: the ASSIGNMENT a takeover pod receives must
// carry the takeover skills, not the implement ones. assignmentFor is handed the
// agent kind (implement), so this only holds if the origin kind reaches
// skillsDirective.
func TestAssignmentFor_TakeoverCarriesTheTakeoverSkills(t *testing.T) {
	got := assignmentFor(stage.AgentImplement, takeoverAssignmentTask(), &tatarav1alpha1.Project{}, false)
	if !strings.Contains(got, "tatara-implement-takeover") {
		t.Fatalf("takeover assignment must require tatara-implement-takeover; skills line missing it")
	}
	if strings.Contains(got, "tatara-implement-gate") {
		t.Fatalf("takeover assignment must not require the gate skill")
	}
}

// The takeover job text is its OWN arm, not implement's. implement's text is the
// GATE first and the code second; a takeover has no gate turn at all, and being
// told to run one sends it looking for an approval it can neither obtain nor
// submit (its state has no approved action).
func TestAgentJob_TakeoverHasNoGateAndPushesToTheExistingBranch(t *testing.T) {
	job := agentJob(stage.AgentImplement, takeoverAssignmentTask(), &tatarav1alpha1.Project{}, false)

	for _, want := range []string{
		"already open",         // the MR exists
		"do not open a new",    // and must not be duplicated
		"action=submitted",     // the forward exit
		"action=declined",      // the terminal
		"mr_write",             // comment before declining
		"TERMINAL",             // the decline is terminal, and it is named
		"scm_read(kind=\"ci\"", // it still owns the PR until clean
	} {
		if !strings.Contains(job, want) {
			t.Errorf("takeover job text missing %q", want)
		}
	}

	// NO GATE. None of the gate vocabulary may appear.
	for _, forbidden := range []string{
		"action=approved",
		"approval_citations",
		"approving_maintainer",
		"plan_note_id",
		"### 1. The gate",
	} {
		if strings.Contains(job, forbidden) {
			t.Errorf("takeover job text must not carry the gate: found %q", forbidden)
		}
	}
}

// THE STATE, NOT THE SPEC KIND, DECIDES WHICH PERSONA RUNS. A takeover Task at
// awaiting-review runs the REVIEW agent - stage.AgentKindFor keys that state on
// the STATE, not on spec.kind - and it must get REVIEW job text. Keying the
// prompt on spec.kind alone would hand the reviewer the takeover's push
// instructions, on its own merge request.
func TestAgentJob_TakeoverAtAwaitingReviewIsStillTheReviewer(t *testing.T) {
	job := agentJob(stage.AgentReview, takeoverAssignmentTask(), &tatarav1alpha1.Project{}, false)
	if !strings.Contains(job, "Review the merge request") {
		t.Fatalf("a takeover Task at awaiting-review must get the REVIEW job text, got:\n%s", job)
	}
	if strings.Contains(job, "do not open a new") {
		t.Fatalf("the reviewer must not be handed the takeover's push instructions")
	}
	// And the review pod's skills are the review ones.
	got := assignmentFor(stage.AgentReview, takeoverAssignmentTask(), &tatarav1alpha1.Project{}, false)
	if !strings.Contains(got, "tatara-review-checklist") {
		t.Fatalf("a takeover Task's review pod must require the review checklist")
	}
	if strings.Contains(got, "tatara-implement-takeover") {
		t.Fatalf("the review pod must not require the takeover implementation skills")
	}
}

// An ordinary implement Task is untouched by the threading: it still gets the
// gate, first.
func TestAgentJob_ImplementIsUnchangedByTheTakeoverArm(t *testing.T) {
	plain := &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{Kind: "implement", Goal: "g"}}
	job := agentJob(stage.AgentImplement, plain, &tatarav1alpha1.Project{}, false)
	if !strings.Contains(job, "### 1. The gate") {
		t.Fatalf("the implement arm lost its gate section")
	}
	got := assignmentFor(stage.AgentImplement, plain, &tatarav1alpha1.Project{}, false)
	if !strings.Contains(got, "tatara-implement-gate") {
		t.Fatalf("an implement Task must still require the gate skill")
	}
}
