package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// gatherDeployState reads tatara_lifecycle_state{state=state} from reg.
func gatherDeployState(t *testing.T, reg *prometheus.Registry, state string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "tatara_lifecycle_state" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "state" && lp.GetValue() == state {
					return m.GetGauge().GetValue()
				}
			}
		}
	}
	return 0
}

// mkDeployStateTask creates an issueLifecycle Task and sets its
// Status.DeployState.
func mkDeployStateTask(t *testing.T, ctx context.Context, name, state string) {
	t.Helper()
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    "lc-recompute-proj",
			RepositoryRef: "lc-recompute-repo",
			Goal:          "recompute test",
			Kind:          "issueLifecycle",
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task %s: %v", name, err)
	}
	task.Status.DeployState = state
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("set lifecycle state %s on %s: %v", state, name, err)
	}
}

// TestUpdateDeployStateCounts_RecomputeCorrectsSkewedGauge asserts that the
// periodic list-and-Set recompute converges tatara_lifecycle_state to the true
// per-state Task count from any skewed starting point: a state inflated above
// truth, a state driven negative (the restart-drift bug), and a drained state
// must all be corrected, zeros included.
func TestUpdateDeployStateCounts_RecomputeCorrectsSkewedGauge(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, reg := newProjectReconcilerWithReg()

	// Known population: two Conversation, one Merge. A non-issueLifecycle Task in
	// a lifecycle state must NOT be counted (the Kind filter).
	mkDeployStateTask(t, ctx, "lc-rc-conv-1", "Conversation")
	mkDeployStateTask(t, ctx, "lc-rc-conv-2", "Conversation")
	mkDeployStateTask(t, ctx, "lc-rc-merge-1", "Merge")

	other := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-rc-other", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    "lc-recompute-proj",
			RepositoryRef: "lc-recompute-repo",
			Goal:          "non-lifecycle",
			Kind:          "implement",
		},
	}
	if err := k8sClient.Create(ctx, other); err != nil {
		t.Fatalf("create non-lifecycle task: %v", err)
	}
	other.Status.DeployState = "Merge" // should be ignored: Kind != issueLifecycle
	if err := k8sClient.Status().Update(ctx, other); err != nil {
		t.Fatalf("set state on non-lifecycle task: %v", err)
	}

	// First recompute establishes authoritative truth for every state. Other
	// suite Tasks may share these states, so capture truth rather than assume 0.
	r.updateDeployStateCounts(ctx)
	truth := make(map[string]float64, len(lifecycleStates))
	for _, s := range lifecycleStates {
		truth[s] = gatherDeployState(t, reg, s)
	}

	// Our three issueLifecycle Tasks must be reflected; the non-lifecycle one not.
	if got := truth["Conversation"]; got < 2 {
		t.Errorf("Conversation truth = %v, want >= 2 (our two tasks)", got)
	}
	if got := truth["Merge"]; got < 1 {
		t.Errorf("Merge truth = %v, want >= 1 (our one task; non-lifecycle excluded)", got)
	}

	// Skew the gauge the way the dropped deltas did: inflate one state, drive
	// another negative (restart drift), inflate a (possibly drained) terminal.
	r.LifecycleMetrics.SetDeployState("Conversation", truth["Conversation"]+17)
	r.LifecycleMetrics.SetDeployState("Merge", -5)
	r.LifecycleMetrics.SetDeployState("Parked", truth["Parked"]+9)

	// Recompute must restore every state to truth, including zeros.
	r.updateDeployStateCounts(ctx)
	for _, s := range lifecycleStates {
		if got := gatherDeployState(t, reg, s); got != truth[s] {
			t.Errorf("after recompute, state %q = %v, want %v", s, got, truth[s])
		}
	}
}
