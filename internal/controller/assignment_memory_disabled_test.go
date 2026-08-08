package controller

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/promptguidance"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// A project with memory disabled is NOT degraded - it is configured that way.
// MemoryDegradedGuidance tells the agent to call report_internal_issue, which
// on a disabled project manufactures one bogus platform-problem alert per turn
// (tatara-operator#523). The disabled appendix must say the same operational
// thing (work from the repo, do not stop) WITHOUT the incident report.
func TestAssignmentFor_MemoryDisabledGuidanceIsNotTheDegradedOne(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-memoff"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Kind: "implement", Goal: "g"},
	}
	fa := false
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Memory = &tatarav1alpha1.MemorySpec{Enabled: &fa}
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: tatarav1alpha1.MemoryPhaseDisabled}

	got := assignmentFor(stage.AgentImplement, task, proj)

	if strings.Contains(got, promptguidance.MemoryDegradedGuidance) {
		t.Fatalf("a memory-DISABLED project was handed the DEGRADED appendix:\n%s", got)
	}
	if !strings.Contains(got, promptguidance.MemoryDisabledGuidance) {
		t.Fatalf("MemoryDisabledGuidance missing from the assignment:\n%s", got)
	}
	// The tool is named only to FORBID it. PlatformProblemGuidance above tells the
	// agent to self-report any blocked tool, so an explicit prohibition is needed
	// to override it; what must not appear is an instruction to call it.
	if !strings.Contains(promptguidance.MemoryDisabledGuidance, "Do NOT call `report_internal_issue`") {
		t.Fatal("the disabled appendix must explicitly forbid self-reporting an intentional configuration")
	}
	if strings.Contains(promptguidance.MemoryDisabledGuidance, "Call `report_internal_issue`") {
		t.Fatal("the disabled appendix instructs the agent to file an incident about a configuration")
	}
	// It must still come LAST, so it overrides PlatformProblemGuidance's
	// "report it and stop" rule for the memory tools.
	if strings.Index(got, promptguidance.MemoryDisabledGuidance) <
		strings.Index(got, promptguidance.PlatformProblemGuidance) {
		t.Fatalf("MemoryDisabledGuidance must follow PlatformProblemGuidance:\n%s", got)
	}
}

// The enabled-but-down case is untouched: it IS a platform problem and the
// agent must still self-report it exactly once.
func TestAssignmentFor_EnabledButDownStillGetsDegradedGuidance(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-memdown"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Kind: "implement", Goal: "g"},
	}
	proj := &tatarav1alpha1.Project{}
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Provisioning"}

	got := assignmentFor(stage.AgentImplement, task, proj)
	if !strings.Contains(got, promptguidance.MemoryDegradedGuidance) {
		t.Fatalf("an enabled-but-not-ready project lost its degraded appendix:\n%s", got)
	}
	if strings.Contains(got, promptguidance.MemoryDisabledGuidance) {
		t.Fatalf("an enabled-but-not-ready project was told memory is disabled:\n%s", got)
	}
}
