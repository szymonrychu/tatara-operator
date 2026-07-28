package agent

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// The wrapper computes its own 410-Gone deadline from podStart plus the
// AGENT_POD_TTL_SECONDS env the operator stamps. So the operator's TTLDeadline
// and the pod's env MUST come from one resolver, or a conversing pod is refused
// turns at a deadline the operator does not believe in.
func TestPodTTLSecondsIsPerStage(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.AgentPodTTLSeconds = 3600
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{ConversationIdleMinutes: 15}

	cases := []struct {
		name  string
		stage string
		want  int
	}{
		{name: "implementing uses the flat project TTL", stage: tatarav1alpha1.StageImplementing, want: 3600},
		{name: "clarifying uses the flat project TTL", stage: tatarav1alpha1.StageClarifying, want: 3600},
		{name: "conversing uses the conversation idle window", stage: tatarav1alpha1.StageConversing, want: 900},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &tatarav1alpha1.Task{}
			task.Status.Stage = tc.stage
			if got := PodTTLSeconds(proj, task); got != tc.want {
				t.Errorf("PodTTLSeconds(%s) = %d, want %d", tc.stage, got, tc.want)
			}
		})
	}
}

func TestTTLDeadlineUsesPodTTLSeconds(t *testing.T) {
	start := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	proj := &tatarav1alpha1.Project{}
	proj.Spec.AgentPodTTLSeconds = 3600
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{ConversationIdleMinutes: 15}

	task := &tatarav1alpha1.Task{}
	task.Status.Stage = tatarav1alpha1.StageConversing
	task.Status.PodStartedAt = &metav1.Time{Time: start}

	t0, ok := TTLDeadline(proj, task)
	if !ok {
		t.Fatal("TTLDeadline ok = false, want true")
	}
	if want := start.Add(15 * time.Minute); !t0.Equal(want) {
		t.Fatalf("t0 = %v, want %v: the operator and the pod env disagree on the TTL", t0, want)
	}
}

func TestAgentEnvStampsThePerStageTTL(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Name = "infrastructure"
	proj.Spec.AgentPodTTLSeconds = 3600
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{ConversationIdleMinutes: 15}

	task := &tatarav1alpha1.Task{}
	task.Name = "t"
	task.Status.Stage = tatarav1alpha1.StageConversing
	task.Status.AgentKind = "clarify"

	got := ""
	for _, e := range AgentEnv(proj, task) {
		if e.Name == "AGENT_POD_TTL_SECONDS" {
			got = e.Value
		}
	}
	if got != "900" {
		t.Fatalf("AGENT_POD_TTL_SECONDS = %q, want \"900\"", got)
	}
}
