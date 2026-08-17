package controller

// openUpgradeLaneCount governs maxOpenUpgrades for the upgrade CRON only
// (design D2). Adopted third-party merge requests are ordinary queue citizens
// bounded by the general pool (QueueCapacity / MaxLivePods), not by this knob,
// so they must never count toward it - on either half of the count: the live
// Task and the not-yet-minted QueuedEvent.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// upgradeTask builds a live upgrade Task with the given labels and source.
func upgradeTask(t *testing.T, name string, labels map[string]string, src *tatarav1alpha1.TaskSource) *tatarav1alpha1.Task {
	t.Helper()
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: labels},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "lanes-proj", Kind: "upgrade", Goal: "g", Source: src,
		},
	}
}

// THE CRON MUST NOT FALL SILENT BEHIND A RENOVATE BACKLOG (design D2). Adopted
// work is bounded by the general pool, not by maxOpenUpgrades - so counting it
// here would read a draining backlog as "lanes full" and stop the agent that
// proactively hunts for bumps, for as long as the backlog lasted, with no error
// and no log anywhere.
func TestOpenUpgradeLaneCount_IgnoresAdoptedTasks(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{}
	proj.Name = "lanes-proj"
	proj.Namespace = testNS

	cronTask := upgradeTask(t, "lanes-cron-1", nil, nil)
	require.NoError(t, k8sClient.Create(ctx, cronTask))

	adoptedLabels := map[string]string{tatarav1alpha1.LabelUpgradeOrigin: tatarav1alpha1.UpgradeOriginAdopted}
	for i, name := range []string{"lanes-adopted-1", "lanes-adopted-2", "lanes-adopted-3"} {
		adopted := upgradeTask(t, name, adoptedLabels, &tatarav1alpha1.TaskSource{Provider: "github", IsPR: true, Number: 100 + i})
		require.NoError(t, k8sClient.Create(ctx, adopted))
	}

	r := newScanReconciler(&fakeReader{})
	n, err := r.openUpgradeLaneCount(ctx, proj)
	require.NoError(t, err)
	require.Equal(t, 1, n, "only the cron-minted Task should count")
}

// ...AND IGNORES ADOPTED EVENTS THAT HAVE NOT MINTED YET, or the same
// suppression happens one step earlier: the count's QueuedEvent half exists
// precisely to catch work already committed to.
func TestOpenUpgradeLaneCount_IgnoresQueuedAdoptions(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{}
	proj.Name = "lanes-proj-qe"
	proj.Namespace = testNS

	mkUpgradeQE(t, proj.Name, "lanes-qe-cron")

	for i, name := range []string{"lanes-qe-adopt-1", "lanes-qe-adopt-2", "lanes-qe-adopt-3"} {
		qe := &tatarav1alpha1.QueuedEvent{}
		qe.Name = name
		qe.Namespace = testNS
		qe.Spec = tatarav1alpha1.QueuedEventSpec{
			Seq: 1, Class: tatarav1alpha1.QueueClassNormal, Kind: "upgrade",
			ProjectRef: proj.Name, RepositoryRef: "lanes-repo", DedupKey: name,
			Payload: tatarav1alpha1.QueuedEventPayload{
				Kind: "upgrade", Goal: "g", RepositoryRef: "lanes-repo",
				AdoptedUpgrade: &tatarav1alpha1.AdoptedUpgradeRef{Number: 200 + i, Author: "renovate"},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, qe))
		qe.Status.State = tatarav1alpha1.QueueStateQueued
		require.NoError(t, k8sClient.Status().Update(ctx, qe))
	}

	r := newScanReconciler(&fakeReader{})
	n, err := r.openUpgradeLaneCount(ctx, proj)
	require.NoError(t, err)
	require.Equal(t, 1, n, "only the cron-minted QueuedEvent should count")
}

// BACKWARD COMPATIBILITY, AND IT IS NOT HYPOTHETICAL. Adopted Tasks minted by
// the PREVIOUS operator carry no upgrade-origin label, and on a live cluster
// several are in flight at upgrade time. Without the structural fallback the
// cron would be suppressed by them for their whole remaining lifetime - which is
// exactly the D2 failure this task exists to prevent, arriving through the door
// nobody watched.
//
// The fallback is exact, not a heuristic: createUpgradeTask sets NO Source at
// all, and MintAdoptedUpgradeTask always sets one with IsPR=true and a number.
func TestOpenUpgradeLaneCount_IgnoresPreUpgradeAdoptedTasks(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{}
	proj.Name = "lanes-proj-legacy"
	proj.Namespace = testNS

	cronTask := upgradeTask(t, "lanes-legacy-cron", nil, nil)
	cronTask.Spec.ProjectRef = proj.Name
	require.NoError(t, k8sClient.Create(ctx, cronTask))

	// Adopted-shaped Task with NO upgrade-origin label - the pre-rollout shape.
	legacyAdopted := upgradeTask(t, "lanes-legacy-adopted", nil, &tatarav1alpha1.TaskSource{Provider: "github", IsPR: true, Number: 41})
	legacyAdopted.Spec.ProjectRef = "lanes-proj-legacy"
	require.NoError(t, k8sClient.Create(ctx, legacyAdopted))

	r := newScanReconciler(&fakeReader{})
	n, err := r.openUpgradeLaneCount(ctx, proj)
	require.NoError(t, err)
	require.Equal(t, 1, n, "an unlabelled pre-rollout adopted Task must still be excluded via the Source fallback")
}

// THE CRON'S OWN WORK STILL COUNTS, both halves, or maxOpenUpgrades stops
// bounding anything at all.
func TestOpenUpgradeLaneCount_StillCountsCronWork(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{}
	proj.Name = "lanes-proj-cron-only"
	proj.Namespace = testNS

	c1 := upgradeTask(t, "lanes-cronwork-1", nil, nil)
	c1.Spec.ProjectRef = proj.Name
	require.NoError(t, k8sClient.Create(ctx, c1))
	c2 := upgradeTask(t, "lanes-cronwork-2", nil, nil)
	c2.Spec.ProjectRef = proj.Name
	require.NoError(t, k8sClient.Create(ctx, c2))

	mkUpgradeQE(t, proj.Name, "lanes-cronwork-qe")

	r := newScanReconciler(&fakeReader{})
	n, err := r.openUpgradeLaneCount(ctx, proj)
	require.NoError(t, err)
	require.Equal(t, 3, n, "two cron Tasks plus one cron QueuedEvent must all count")
}
