package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestRecoverOrphans_MaxParkAge_RepingsAndCloses is liveness-hardening finding #6:
// a recoverable-giveup Parked task at the give-up cap on a still-open issue used to
// be spared by the reaper FOREVER (never GC'd, never re-nudged), holding a Task +
// metric with no human signal. Past a max-park-age it must now re-ping the issue
// with a comment AND transition the task to Done so the reaper can reclaim it,
// instead of accumulating silently.
func TestRecoverOrphans_MaxParkAge_RepingsAndCloses(t *testing.T) {
	proj, repo := seedBackstopProject(t, "parkage")
	parked := mkGiveUpTask(t, "parkage", repo.Name, "o/r", 44, maxImplGiveUps, "implement-failed")
	// Age the park well past the max-park-age bound (park anchor = LastActivityAt).
	old := metav1.NewTime(time.Now().Add(-maxRecoverableParkAge - 24*time.Hour))
	cur := getGiveUpTask(t, parked.Name)
	cur.Status.LastActivityAt = &old
	require.NoError(t, k8sClient.Status().Update(context.Background(), cur))

	// Issue #44 is still OPEN.
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/r": {{Repo: "o/r", Number: 44, Labels: []string{"tatara-implementation"}, UpdatedAt: time.Unix(100, 0)}},
		},
	}
	fw := &reapFakeWriter{}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	r.recoverOrphans(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo}, nil)

	got := getGiveUpTask(t, parked.Name)
	require.Equal(t, "Done", got.Status.DeployState, "an aged-out permanently-parked task must be resolved Done for GC")
	require.NotEmpty(t, fw.commentCalls, "the aged-out park must re-ping the issue with a comment (human signal)")
}

// TestRecoverOrphans_MaxParkAge_YoungParkSpared: a recently-parked give-up task
// (within the max-park-age window) is left alone - it is still eligible for the
// normal reroll/recovery paths and must not be prematurely closed.
func TestRecoverOrphans_MaxParkAge_YoungParkSpared(t *testing.T) {
	proj, repo := seedBackstopProject(t, "parkage-young")
	parked := mkGiveUpTask(t, "parkage-young", repo.Name, "o/r", 45, maxImplGiveUps, "implement-failed")
	recent := metav1.NewTime(time.Now().Add(-time.Hour))
	cur := getGiveUpTask(t, parked.Name)
	cur.Status.LastActivityAt = &recent
	require.NoError(t, k8sClient.Status().Update(context.Background(), cur))

	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/r": {{Repo: "o/r", Number: 45, Labels: []string{"tatara-implementation"}, UpdatedAt: time.Unix(100, 0)}},
		},
	}
	fw := &reapFakeWriter{}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	r.recoverOrphans(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo}, nil)

	got := getGiveUpTask(t, parked.Name)
	require.Equal(t, "Parked", got.Status.DeployState, "a young give-up park must not be aged out")
	require.Empty(t, fw.commentCalls, "a young give-up park must not be re-pinged")
}
