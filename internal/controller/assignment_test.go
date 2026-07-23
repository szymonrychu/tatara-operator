package controller

import (
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestAssignmentSkillsDirectiveUsesAgentKind verifies that assignmentFor uses
// the running stage's agentKind for the skills directive, not task.Spec.Kind.
// See issue #397: when a task.Spec.Kind is "incident" but the running stage
// is "clarify", the skills directive must use "clarify", not "incident".
func TestAssignmentSkillsDirectiveUsesAgentKind(t *testing.T) {
	tests := []struct {
		agentKind      string
		requiredSkill  string
		forbiddenSkill string
	}{
		{
			agentKind:      stage.AgentClarify,
			requiredSkill:  "tatara-clarify-conversation",
			forbiddenSkill: "tatara-incident-investigation",
		},
		{
			agentKind:      stage.AgentImplement,
			requiredSkill:  "tatara-implement-workflow",
			forbiddenSkill: "tatara-incident-investigation",
		},
		{
			agentKind:      stage.AgentReview,
			requiredSkill:  "tatara-review-checklist",
			forbiddenSkill: "tatara-incident-investigation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.agentKind, func(t *testing.T) {
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
