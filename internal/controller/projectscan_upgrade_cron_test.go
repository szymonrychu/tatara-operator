package controller

// The dependency-upgrade cron. Each due tick mints AT MOST ONE upgrade Task,
// and only while the live upgrade-Task count is under maxOpenUpgrades.
// Throughput is the cron FREQUENCY, not a fan-out.

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

func listUpgradeQEs(t *testing.T, project string) []tatarav1alpha1.QueuedEvent {
	t.Helper()
	var out []tatarav1alpha1.QueuedEvent
	for _, qe := range listScanQEs(t, project) {
		if qe.Spec.Kind == "upgrade" || qe.Spec.Payload.Kind == "upgrade" {
			out = append(out, qe)
		}
	}
	return out
}

// seedUpgradeCronProject creates a Project whose upgrade cron is due every
// minute, with maxOpenUpgrades under the caller's control.
func seedUpgradeCronProject(t *testing.T, name string, maxOpen int) *tatarav1alpha1.Project {
	t.Helper()
	ctx := context.Background()
	mkSecret(t, name+"-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{}
	proj.Name = name
	proj.Namespace = testNS
	proj.Spec.ScmSecretRef = name + "-scm"
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{
		Provider: "github", Owner: "o", BotLogin: "bot",
		Cron: &tatarav1alpha1.ScmCron{
			Upgrade: tatarav1alpha1.UpgradeActivity{Schedule: "* * * * *", MaxOpenUpgrades: maxOpen},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))
	past := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	proj.Status.LastUpgrade = &past
	require.NoError(t, k8sClient.Status().Update(ctx, proj))
	mkScanRepo(t, name, name+"-repo", "https://github.com/o/r.git")
	return proj
}

func mkUpgradeTask(t *testing.T, project, name, state string) *tatarav1alpha1.Task {
	t.Helper()
	tk := &tatarav1alpha1.Task{}
	tk.Name = name
	tk.Namespace = testNS
	tk.Spec = tatarav1alpha1.TaskSpec{ProjectRef: project, Kind: "upgrade", Goal: "g"}
	require.NoError(t, k8sClient.Create(context.Background(), tk))
	tk.Status.State = state
	require.NoError(t, k8sClient.Status().Update(context.Background(), tk))
	return tk
}

func TestActivityScheduleAndLast_KnowsUpgrade(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{Cron: &tatarav1alpha1.ScmCron{
		Upgrade: tatarav1alpha1.UpgradeActivity{Schedule: "0 */4 * * *"},
	}}
	now := metav1.Now()
	proj.Status.LastUpgrade = &now
	sched, last := activityScheduleAndLast(proj, "upgrade")
	require.Equal(t, "0 */4 * * *", sched)
	require.NotNil(t, last)
}

// A due tick mints ONE upgrade event, minted STRAIGHT into under-implementation.
// That target is not cosmetic: refined's only exit into under-implementation is
// submit_outcome(action=approved), and the upgrade outcome schema has no such
// action, so an upgrade Task triaged to refined could never leave it.
func TestUpgradeCron_DueTickMintsOneTaskIntoUnderImplementation(t *testing.T) {
	proj := seedUpgradeCronProject(t, "upg-cron-1", 1)
	r := newScanReconciler(&fakeReader{})
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	_, _, _, _, err := r.runScans(context.Background(), proj)
	require.NoError(t, err)

	qes := listUpgradeQEs(t, proj.Name)
	require.Len(t, qes, 1)
	require.Equal(t, tatarav1alpha1.StateUnderImplementation, qes[0].Spec.Payload.InitialState,
		"an upgrade Task has no gate to face, so it is minted past `refined`, exactly like the nightly doc batch")
	require.Equal(t, "upgrade", qes[0].Spec.Payload.Labels[labelActivity])
	require.Contains(t, qes[0].Spec.Payload.Goal, "tatara-upgrade-workflow")
	require.Contains(t, qes[0].Spec.Payload.Goal, "o/r", "the goal names the project's repos")
}

// THE CAPACITY GUARD. maxOpenUpgrades=1 with one LIVE upgrade Task mints
// nothing; the same tick with only a DONE sibling mints.
func TestUpgradeCron_MintsOnlyBelowMaxOpenUpgrades(t *testing.T) {
	proj := seedUpgradeCronProject(t, "upg-cron-2", 1)
	mkUpgradeTask(t, proj.Name, "upg-cron-2-live", tatarav1alpha1.StateUnderImplementation)

	r := newScanReconciler(&fakeReader{})
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	_, _, _, _, err := r.runScans(context.Background(), proj)
	require.NoError(t, err)
	require.Empty(t, listUpgradeQEs(t, proj.Name), "a live upgrade Task at the cap must block the mint")

	// The stamp still advanced on the tick, so re-arm the schedule and retire the
	// sibling: the next due tick mints. Re-read first - stampUpgrade wrote
	// through a fresh copy, so the local resourceVersion is stale.
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: proj.Name}, proj))
	past := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	proj.Status.LastUpgrade = &past
	require.NoError(t, k8sClient.Status().Update(context.Background(), proj))
	live := &tatarav1alpha1.Task{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: "upg-cron-2-live"}, live))
	live.Status.State = tatarav1alpha1.StateDone
	require.NoError(t, k8sClient.Status().Update(context.Background(), live))

	_, _, _, _, err = r.runScans(context.Background(), proj)
	require.NoError(t, err)
	require.Len(t, listUpgradeQEs(t, proj.Name), 1, "a finished sibling frees the lane")
}

// A PARKED upgrade Task does NOT hold a lane. It runs no pod and the reaper
// collects it, and `declined` - a correct and common answer for a scheduled
// kind - parks. Counting a park as live would wedge the whole cron behind the
// first cycle that found nothing worth upgrading, for the length of the park
// retention.
func TestUpgradeCron_AParkedSiblingDoesNotHoldALane(t *testing.T) {
	proj := seedUpgradeCronProject(t, "upg-cron-3", 1)
	parked := mkUpgradeTask(t, proj.Name, "upg-cron-3-parked", tatarav1alpha1.StateUnderImplementation)
	parked.Status.ParkReason = "implement-declined"
	require.NoError(t, k8sClient.Status().Update(context.Background(), parked))

	r := newScanReconciler(&fakeReader{})
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	_, _, _, _, err := r.runScans(context.Background(), proj)
	require.NoError(t, err)
	require.Len(t, listUpgradeQEs(t, proj.Name), 1,
		"a declined upgrade parks; if that held a lane the cron would stop for the whole park retention")
}

// ONE Task per fire, never N, and the stamp advances on the TICK. A second
// reconcile inside the same tick collapses on the per-tick dedup key.
func TestUpgradeCron_OneTaskPerTickAndStampsOnTheTick(t *testing.T) {
	proj := seedUpgradeCronProject(t, "upg-cron-4", 3)
	ctx := context.Background()
	r := newScanReconciler(&fakeReader{})
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	_, _, _, _, err := r.runScans(ctx, proj)
	require.NoError(t, err)
	require.NotNil(t, proj.Status.LastUpgrade, "a due tick stamps LastUpgrade")
	require.Less(t, time.Since(proj.Status.LastUpgrade.Time), time.Minute)

	_, _, _, _, err = r.runScans(ctx, proj)
	require.NoError(t, err)
	require.Len(t, listUpgradeQEs(t, proj.Name), 1,
		"a second pass in the same cron period must mint nothing, even with capacity to spare")
}

func TestUpgradeCron_EmptyScheduleDisablesUpgrade(t *testing.T) {
	proj := seedUpgradeCronProject(t, "upg-cron-5", 1)
	ctx := context.Background()
	proj.Spec.Scm.Cron.Upgrade.Schedule = ""
	require.NoError(t, k8sClient.Update(ctx, proj))

	r := newScanReconciler(&fakeReader{})
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	_, _, _, _, err := r.runScans(ctx, proj)
	require.NoError(t, err)
	require.Empty(t, listUpgradeQEs(t, proj.Name), "an empty upgrade schedule is off, matching refine")
}

// The heartbeat gauge is RETRACTED when the cron is off. A GaugeVec child, once
// created, stays exported at its last value for the life of the process, so a
// disabled cron would otherwise leave a frozen timestamp that reads as
// ever-more-overdue and false-pages forever.
func TestUpgradeCron_NextExpectedIsPublishedAndRetracted(t *testing.T) {
	proj := seedUpgradeCronProject(t, "upg-cron-6", 1)
	r := newScanReconciler(&fakeReader{})
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.publishNextExpected(proj, nil, false)
	require.True(t, nextExpectedSeriesExists(t, proj.Name, "upgrade"),
		"an enabled upgrade cron publishes a next-expected fire")

	proj.Spec.Scm.Cron.Upgrade.Schedule = ""
	r.publishNextExpected(proj, nil, false)
	require.False(t, nextExpectedSeriesExists(t, proj.Name, "upgrade"),
		"a disabled upgrade cron RETRACTS the series rather than freezing it")
}
