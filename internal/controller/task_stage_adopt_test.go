package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// adoptedTriageTask is the shape MintAdoptedUpgradeTask creates: kind=upgrade,
// repo-bound, minted at `new` ONTO a merge request that already existed.
func adoptedTriageTask(name string) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: mdNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    "proj",
			RepositoryRef: "charts",
			Kind:          "upgrade",
			Goal:          "adopt the dependency merge request",
			Source:        &tatarav1alpha1.TaskSource{IsPR: true, Number: 41},
		},
		Status: tatarav1alpha1.TaskStatus{
			State: tatarav1alpha1.StateNew,
			// The mint binds the merge request it was adopted onto, so the
			// mr-binding backstop has nothing to repair.
			MRRefs: []string{tatarav1alpha1.MergeRequestName("charts", 41)},
		},
	}
}

// triageTarget is the `new` row as data, and it is the SECOND enforcement site
// (deviation 13). Widening LegalFor without this leaves the Task parked
// triage-stalled; widening this without LegalFor errors on every pass forever.
func TestTriageTarget_RoutesAnAdoptedUpgradeTaskIntoTheReviewLane(t *testing.T) {
	got, ok := triageTarget(adoptedTriageTask("mt-u-charts-41"))
	require.True(t, ok)
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, got,
		"an adopted upgrade Task is reviewed FIRST: the merge request already exists")

	// The cron shape keeps its NO-ROW behaviour. It never reaches triage (it
	// mints with InitialState = under-implementation), and if it ever did it
	// must park loudly rather than acquire a gate-free lane.
	cron := &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{Kind: "upgrade"}}
	_, ok = triageTarget(cron)
	require.False(t, ok, "a cron-minted upgrade Task must still have no triage row")

	// #604 removed the `takeover` row for the same reason the cron-upgrade row
	// never existed: it owns zero Issue CRs, so a triage that lands it at
	// `refined` lands it in front of a gate that can never grant.
	_, ok = triageTarget(&tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{Kind: "takeover"}})
	require.False(t, ok, "a takeover Task must have no triage row: it is minted into the work")

	// Every pre-existing row is unchanged.
	for kind, want := range map[string]string{
		"implement":  tatarav1alpha1.StateRefined,
		"brainstorm": tatarav1alpha1.StateRefined,
		"incident":   tatarav1alpha1.StateRefined,
		"refine":     tatarav1alpha1.StateRefined,
		"review":     tatarav1alpha1.StateAwaitingReview,
	} {
		got, ok := triageTarget(&tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{Kind: kind}})
		require.True(t, ok, "triageTarget(%s) lost its row", kind)
		require.Equal(t, want, got, "triageTarget(%s)", kind)
	}
}

// End to end through the reconciler: minted at `new`, no pod at `new`, lands at
// awaiting-review on its own.
func TestReconcileTriaging_AdoptedUpgradeTaskLandsAtAwaitingReview(t *testing.T) {
	proj := tsProject(3)
	task := adoptedTriageTask("mt-u-charts-41-deadbeef")

	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(proj, task).
		WithStatusSubresource(&tatarav1alpha1.Task{}).Build()

	r := &TaskReconciler{
		Client: c, APIReader: c, Scheme: scheme,
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		PodConfig: tsPodConfig(),
	}
	_, err := r.reconcileStage(context.Background(), proj, task, time.Unix(1000, 0))
	require.NoError(t, err)
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, task.Status.State,
		"the adopted Task must walk itself into the review lane with no agent turn")
	require.Empty(t, task.Status.ParkReason,
		"a Task parked triage-stalled here means only ONE of the two enforcement sites was widened")

	// mintIssueCRs bails on src.IsPR, so an adopted Task owns ZERO Issue CRs -
	// the same shape that makes the review lane work for a kind=review Task.
	var issues tatarav1alpha1.IssueList
	require.NoError(t, c.List(context.Background(), &issues))
	require.Empty(t, issues.Items, "an adopted merge request Task must mint no Issue CRs")
	require.Empty(t, task.Status.IssueRefs)
}
