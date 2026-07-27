package controller

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/promptguidance"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// Every tool an agent goal or assignment names must exist. See
// internal/promptguidance/toolnames.go for the registry and the extraction rule.
//
// issueGoal (sweep.go) and takeoverGoal (takeover_mint.go) are DELIBERATELY not
// covered. Both render third-party text verbatim - an issue's title/URL/body,
// and a human's triggering comment - so backticks inside them are user data,
// not operator prose. Asserting on them would fail the build the first time a
// maintainer wrote `foo_bar` in an issue body.
func TestGoalBuildersNameOnlyRealTools(t *testing.T) {
	// The repo-state block is runtime data (issue titles, MR titles) that can
	// legitimately contain backticks, so the fixture is deliberately plain: this
	// test constrains the OPERATOR'S OWN prose, not what a maintainer types.
	const stateFixture = "ISSUES:\no/a#1 [bug] x\nOPEN MRs:\nnone\nMAIN HEALTH:\no/a main CI: success"

	goals := map[string]string{
		"documentationGoal": documentationGoal("https://github.com/o/a", "deadbeef"),
	}
	for _, kind := range []string{
		stage.AgentBrainstorm,
		stage.AgentClarify,
		stage.AgentIncident,
		stage.AgentRefine,
		stage.AgentImplement,
		stage.AgentReview,
		stage.AgentDocumentation,
	} {
		task := &tatarav1alpha1.Task{}
		task.Name = "task-x"
		task.Spec.ProjectRef = "proj-x"
		proj := &tatarav1alpha1.Project{}
		proj.Name = "proj-x"
		goals["assignmentFor("+kind+")"] = assignmentFor(kind, task, proj)
	}
	_ = stateFixture

	for name, goal := range goals {
		t.Run(name, func(t *testing.T) {
			if bad := promptguidance.UnknownToolNames(goal); len(bad) > 0 {
				t.Fatalf("%s names tools that do not exist: %v\n"+
					"Add the tool to promptguidance.AgentVisibleTools if tatara-cli really "+
					"serves it, otherwise fix the prose to name the tool that does.", name, bad)
			}
		})
	}
}
