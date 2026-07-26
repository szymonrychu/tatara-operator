package controller

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/promptguidance"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// The turn-0 directive must TELL a degraded agent that recall is down and that
// this is not a reason to stop. Without it PlatformProblemGuidance's "report it
// and stop" is the last word the agent reads about a failing tool, which is
// exactly the stall the memory subsystem must no longer cause.
func TestAssignmentFor_MemoryDegradedGuidance(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-degraded"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Kind: "implement", Goal: "g"},
	}

	degraded := assignmentFor(stage.AgentImplement, task, &tatarav1alpha1.Project{})
	if !strings.Contains(degraded, promptguidance.MemoryDegradedGuidance) {
		t.Fatalf("degraded project: MemoryDegradedGuidance missing from the assignment:\n%s", degraded)
	}
	if !strings.Contains(degraded, "report_internal_issue") {
		t.Fatalf("degraded guidance must name the self-report tool:\n%s", degraded)
	}
	// It must come LAST so it overrides PlatformProblemGuidance's stop-on-blocked
	// instruction rather than being overridden by it.
	if strings.Index(degraded, promptguidance.MemoryDegradedGuidance) <
		strings.Index(degraded, promptguidance.PlatformProblemGuidance) {
		t.Fatalf("MemoryDegradedGuidance must follow PlatformProblemGuidance:\n%s", degraded)
	}

	healthy := &tatarav1alpha1.Project{}
	readySince := metav1.NewTime(time.Now().Add(-(tatarav1alpha1.MemoryReadyStabilizationWindow + time.Minute)))
	healthy.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Ready", ReadySince: &readySince}
	ok := assignmentFor(stage.AgentImplement, task, healthy)
	if strings.Contains(ok, promptguidance.MemoryDegradedGuidance) {
		t.Fatalf("stably-ready project must NOT carry the degraded appendix:\n%s", ok)
	}
}
