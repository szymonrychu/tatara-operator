package agent

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// An agent kind missing from kindProfiles gets an EMPTY profile, the cli fails
// closed to the always-on tool set, submit_outcome is NOT registered, and the
// Task can never terminate. This is the single highest-consequence kind map in
// the repo, and it is the one that caused the clarify P0 (contract L.5).
func TestUpgrade_ResolvesATerminalProfile(t *testing.T) {
	require.Equal(t, "upgrade", profileForKind("upgrade"),
		"an empty profile means no submit_outcome and a Task that can never terminate")
}

func TestUpgrade_PodNameTokenAndBranchPrefix(t *testing.T) {
	require.Equal(t, "upg", typeAbbrev("upgrade"))
	task := &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{Kind: "upgrade"}}
	require.Equal(t, "chore", branchKind(task))
}

// A cron-minted upgrade Task carries no Source.Number, so TaskBranch falls
// through to the task-name form. This is the branch AdoptPR and taskForBranch
// key on, and it must be stable.
func TestUpgrade_TaskBranchIsTheTaskNameForm(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "upgrade-abc123"},
		Spec:       tatarav1alpha1.TaskSpec{Kind: "upgrade"},
	}
	require.Equal(t, "tatara/task-upgrade-abc123", TaskBranch(task))
}

func TestUpgrade_DefaultsToOpus(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	require.Equal(t, "claude-opus-5", modelForKind(proj, "upgrade", ""),
		"upgrade reads release notes and judges breaking changes; it is a reasoning kind")
}
