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

	gotOff := assignmentFor(stage.AgentImplement, task, off)
	gotOn := assignmentFor(stage.AgentImplement, task, on)

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

			assignment := assignmentFor(tt.agentKind, task, &tatarav1alpha1.Project{})

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
