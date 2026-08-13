package controller

import (
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestAssignmentImplementCitationRuleFollowsAutoApprove pins the ONE paragraph of
// the implement gate's job text that is only true when the auto-approve carve-out
// is armed. With the flag OFF, `approved` with no citation is refused with
// no-maintainer-comment, so telling the agent it may omit the pair on a
// tatara-proposed issue deterministically drives it into that refusal.
func TestAssignmentImplementCitationRuleFollowsAutoApprove(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "test-task", Namespace: "default"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "test-project", Kind: "implement", Goal: "g"},
	}

	off := &tatarav1alpha1.Project{}
	on := &tatarav1alpha1.Project{
		Spec: tatarav1alpha1.ProjectSpec{AutoApproveTataraProposals: true},
	}

	gotOff := assignmentFor(stage.AgentImplement, task, off, false)
	gotOn := assignmentFor(stage.AgentImplement, task, on, false)

	const omitRule = "Omit the approving_maintainer field AND the approval_citations field TOGETHER"
	if strings.Contains(gotOff, omitRule) {
		t.Error("flag OFF: the agent must NOT be told it may omit the citation pair")
	}
	if !strings.Contains(gotOn, omitRule) {
		t.Error("flag ON: the omit-the-pair rule is the carve-out's instruction and must stay")
	}

	if !strings.Contains(gotOff, "REQUIRES a maintainer comment to cite") {
		t.Error("flag OFF: the agent must be told a tatara-proposed issue still needs a comment")
	}
	if !strings.Contains(gotOff, "`action=discuss`") {
		t.Error("flag OFF: the agent must be told to discuss rather than attempt approved")
	}
	if strings.Contains(gotOn, "REQUIRES a maintainer comment to cite") {
		t.Error("flag ON: the flag-off wording must not appear")
	}

	// The rest of the gate text is unconditional on both sides.
	for _, shared := range []string{
		"YOU judge whether a comment approves",
		"NEVER CITE A COMMENT THAT DECLINES",
		"the plan_note_id field is ALWAYS required",
	} {
		if !strings.Contains(gotOff, shared) || !strings.Contains(gotOn, shared) {
			t.Errorf("both variants must carry %q", shared)
		}
	}
}

// TestAssignmentSkillsDirectiveUsesAgentKind verifies that assignmentFor uses
// the running stage's agentKind for the skills directive, not task.Spec.Kind.
// See issue #397: when a task.Spec.Kind is "incident" but the running stage
// is "clarify", the skills directive must use "clarify", not "incident".
func TestAssignmentSkillsDirectiveUsesAgentKind(t *testing.T) {
	tests := []struct {
		name           string
		agentKind      string
		requiredSkill  string
		forbiddenSkill string
	}{
		{
			// stage.AgentClarify is gone (#521): its job merged into
			// stage.AgentImplement's arm, and tatara-clarify-conversation is
			// superseded by tatara-implement-gate (see requiredSkillsForKind).
			name:           "implement-gate",
			agentKind:      stage.AgentImplement,
			requiredSkill:  "tatara-implement-gate",
			forbiddenSkill: "tatara-incident-investigation",
		},
		{
			name:           "implement-workflow",
			agentKind:      stage.AgentImplement,
			requiredSkill:  "tatara-implement-workflow",
			forbiddenSkill: "tatara-incident-investigation",
		},
		{
			name:           "review",
			agentKind:      stage.AgentReview,
			requiredSkill:  "tatara-review-checklist",
			forbiddenSkill: "tatara-incident-investigation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &tatarav1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-task",
					Namespace: "default",
				},
				Spec: tatarav1alpha1.TaskSpec{
					ProjectRef: "test-project",
					Kind:       "incident",
					Goal:       "test goal",
				},
			}

			assignment := assignmentFor(tt.agentKind, task, &tatarav1alpha1.Project{}, false)

			// Assignment must contain the required skill for this agent kind
			if !strings.Contains(assignment, tt.requiredSkill) {
				t.Errorf("assignmentFor(%s, incident-task) does not contain required skill %q", tt.agentKind, tt.requiredSkill)
			}

			// Assignment must NOT contain the incident skill (which should only appear for incident agent kind)
			if strings.Contains(assignment, tt.forbiddenSkill) {
				t.Errorf("assignmentFor(%s, incident-task) contains forbidden skill %q (should only be in incident)", tt.agentKind, tt.forbiddenSkill)
			}
		})
	}
}

// TestAssignmentFor_ResumedBranchMergeIsFirstAction pins the turn-0 half of the
// "a bootstrap merge conflict is the agent's job, not a boot failure" contract
// (tatara-claude-code-wrapper: bootstrap-conflict-not-fatal).
//
// The operator cannot make this rule conditional: it has no way to know at
// assignment time whether the remote task branch already exists. There is no
// ls-remote anywhere in it, agent.TaskBranch is a pure function of the Task
// spec, and no SCM interface exposes branch existence. So the operator states
// the rule unconditionally and the wrapper supplies the conditional fact in the
// injected CLAUDE.md.
//
// Only the kinds that push code carry it. A brainstorm/incident/refine/review
// agent has no task branch to reconcile (review checks its target out read-only
// and never pushes), so the sentence would be noise there.
func TestAssignmentFor_ResumedBranchMergeIsFirstAction(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-resume"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Kind: "implement", Goal: "g"},
	}

	const rule = "resolving that merge is your FIRST action"

	for _, kind := range []string{stage.AgentImplement, stage.AgentDocumentation, stage.AgentUpgrade} {
		got := assignmentFor(kind, task, &tatarav1alpha1.Project{}, false)
		if !strings.Contains(got, rule) {
			t.Errorf("kind %q pushes code and must carry the resumed-branch merge rule", kind)
		}
		if !strings.Contains(got, "RESUMED") {
			t.Errorf("kind %q: the rule must name the resumed state the wrapper reports", kind)
		}
	}

	for _, kind := range []string{
		stage.AgentBrainstorm, stage.AgentIncident, stage.AgentRefine, stage.AgentReview,
	} {
		got := assignmentFor(kind, task, &tatarav1alpha1.Project{}, false)
		if strings.Contains(got, rule) {
			t.Errorf("kind %q pushes no code and must not carry the resumed-branch merge rule", kind)
		}
	}
}

// --- upgrade --------------------------------------------------------------

func TestRequiredSkills_UpgradeDemandsTheWorkflowAndTDD(t *testing.T) {
	got := requiredSkillsForKind("upgrade")
	want := []string{"tatara-upgrade-workflow", "test-driven-development"}
	if len(got) != len(want) {
		t.Fatalf("requiredSkillsForKind(upgrade) = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("requiredSkillsForKind(upgrade) = %v, want %v", got, want)
		}
	}
	if d := skillsDirective("upgrade"); !strings.HasPrefix(d, "Required skills this turn:") {
		t.Fatalf("upgrade must get MANDATORY wording, not brainstorm's advisory Consult wording: %q", d)
	}
}

func TestAgentJob_UpgradeCarriesTheOneUnitAndReleaseNotesRules(t *testing.T) {
	job := agentJob(stage.AgentUpgrade, &tatarav1alpha1.Project{}, false)
	for _, want := range []string{
		"EXACTLY ONE",
		"task_context(index=true)",
		"release notes",
		"merge_order",
		"submit_outcome(action=submitted",
		"action=declined",
		"nextHopOnly",
	} {
		if !strings.Contains(job, want) {
			t.Errorf("upgrade job text missing %q", want)
		}
	}
	// THE GATE BELONGS TO implement. An upgrade agent has no issue and no
	// maintainer comment to cite, and its outcome schema has no approved action
	// at all - telling it to seek approval sends it into a guaranteed refusal.
	for _, forbidden := range []string{"approving_maintainer", "approval_citations", "action=approved"} {
		if strings.Contains(job, forbidden) {
			t.Errorf("upgrade job text must not carry the gate field %q", forbidden)
		}
	}
}

// The upgrade agent pushes code, so it carries the resumed-branch merge rule
// like every other code-pushing kind.
func TestAgentJob_UpgradeCarriesTheResumedBranchRule(t *testing.T) {
	job := agentJob(stage.AgentUpgrade, &tatarav1alpha1.Project{}, false)
	if !strings.Contains(job, "resolving that merge is your FIRST action") {
		t.Error("upgrade pushes code and must carry the resumed-branch merge rule")
	}
}

func TestAssignmentFor_UpgradeAppliesPromptAppendByKind(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Agent.PromptAppendByKind = map[string]string{"upgrade": "DECK REFRESH RULES HERE"}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "upgrade-abc"},
		Spec:       tatarav1alpha1.TaskSpec{Kind: "upgrade", ProjectRef: "p"},
	}
	if got := assignmentFor(stage.AgentUpgrade, task, proj, false); !strings.Contains(got, "DECK REFRESH RULES HERE") {
		t.Fatal("promptAppendByKind[upgrade] must reach the upgrade assignment")
	}
}
