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
	require.Equal(t, "chore", branchKind(task), "upgrade takes branchKind's default, which is already chore")
}

// A cron-minted upgrade Task carries no Source.Number, so TaskBranch falls
// through to the task-name form and never consults branchKind at all. This is
// the branch AdoptPR and taskForBranch key on, and it must be stable.
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

// mintedUpgradeTask reproduces the shape queue.BuildTaskFromQueuedEvent hands to
// StampPodName: the pod name is stamped BEFORE the Create, so the Task still
// carries only its GenerateName (the API server fills Name in afterwards).
func mintedUpgradeTask(project, generateName string) *tatarav1alpha1.Task {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tatara", GenerateName: generateName},
		Spec:       tatarav1alpha1.TaskSpec{Kind: "upgrade", ProjectRef: project},
	}
	StampPodName(task, project, "")
	return task
}

// THE COLLISION GUARD. upgrade is the first kind that permits N concurrent
// same-shape REPO-LESS Tasks in one project (maxOpenUpgrades goes to 10), so it
// is the first kind for which a pod name built from (kind, project, repo) alone
// is not unique. Every consequence of a collision is SILENT:
//   - ensureStagePod Gets the pod by name only, so the second Task adopts the
//     first's pod and never spawns one;
//   - the stage-mismatch branch DeleteWrappers the other Task's running pod;
//   - the workspace PVC (PodName+"-ws") is shared by both agents;
//   - the wrapper URL resolves a turn for Task B onto Task A's agent.
func TestUpgrade_TwoTasksInAProjectGetDistinctPodIdentities(t *testing.T) {
	a := mintedUpgradeTask("tatara", "upgrade-qe-tatara-aaaaaaaa-")
	b := mintedUpgradeTask("tatara", "upgrade-qe-tatara-bbbbbbbb-")

	require.NotEqual(t, PodName(a), PodName(b),
		"two live upgrade Tasks sharing a pod name make the second adopt the first's pod")
	require.NotEqual(t, WorkspacePVCName(a), WorkspacePVCName(b),
		"a shared workspace PVC lets two upgrade agents scribble over each other")
	require.NotEqual(t, BaseURL(a, "tatara"), BaseURL(b, "tatara"),
		"a shared wrapper URL dispatches Task B's turn to Task A's agent")

	for _, task := range []*tatarav1alpha1.Task{a, b} {
		name := PodName(task)
		require.Equal(t, name, sanitizeDNS1123(name), "the pod name must stay a DNS-1123 label")
		require.LessOrEqual(t, len(name), 63)
		require.Contains(t, name, "upg-tatara-", "the descriptive prefix must survive the disambiguator")
	}
}

// A named mint (payload.name rather than generateName) is disambiguated off the
// Task name itself, so the same guard holds for any future producer that names
// its upgrade Tasks instead of generating them.
func TestUpgrade_NamedTasksGetDistinctPodNames(t *testing.T) {
	named := func(name string) *tatarav1alpha1.Task {
		task := &tatarav1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Namespace: "tatara", Name: name},
			Spec:       tatarav1alpha1.TaskSpec{Kind: "upgrade", ProjectRef: "tatara"},
		}
		StampPodName(task, "tatara", "")
		return task
	}
	require.NotEqual(t, PodName(named("upgrade-one")), PodName(named("upgrade-two")))
}

// The disambiguator is a pure function of the Task's own identity: re-stamping
// the same Task must never move its pod, PVC or wrapper address.
func TestUpgrade_PodNameIsStablePerTask(t *testing.T) {
	task := mintedUpgradeTask("tatara", "upgrade-qe-tatara-aaaaaaaa-")
	first := PodName(task)
	StampPodName(task, "tatara", "")
	require.Equal(t, first, PodName(task))
}
