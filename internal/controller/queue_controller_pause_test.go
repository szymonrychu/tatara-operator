package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/budget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// patchMaxConcurrentAgentsZero explicitly sets spec.maxConcurrentAgents to 0
// via a raw JSON merge patch and returns the refreshed Project. A typed
// k8sClient.Update with MaxConcurrentAgents:0 would marshal the zero int away
// (json:"...,omitempty"), making it indistinguishable from "unset" and letting
// the CRD's +kubebuilder:default=3 stamp it right back to 3 - the same
// defaulted-value trap as the gitlab-mr-rereview-loop incident. A raw merge
// patch sends the literal 0 on the wire, exactly as kubectl/helm would for an
// explicit maxConcurrentAgents: 0 in a manifest.
func patchMaxConcurrentAgentsZero(t *testing.T, ctx context.Context, proj *tatarav1alpha1.Project) *tatarav1alpha1.Project {
	t.Helper()
	patch := client.RawPatch(types.MergePatchType, []byte(`{"spec":{"maxConcurrentAgents":0}}`))
	if err := k8sClient.Patch(ctx, proj, patch); err != nil {
		t.Fatalf("patch maxConcurrentAgents=0: %v", err)
	}
	got := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: proj.Name, Namespace: proj.Namespace}, got); err != nil {
		t.Fatalf("refresh project: %v", err)
	}
	if got.Spec.MaxConcurrentAgents != 0 {
		t.Fatalf("patch did not stick: MaxConcurrentAgents = %d, want 0", got.Spec.MaxConcurrentAgents)
	}
	return got
}

// TestAdmit_MaxConcurrentAgentsZero_PausesBothPools verifies that
// maxConcurrentAgents=0 skips admission entirely for both the normal and
// alert pools (a full project pause, contract B.7 - the pause check is a
// DIRECT proj.Spec.MaxConcurrentAgents == 0 check, never QueueCapacity(),
// which floors at 3 and would silently un-pause the kill switch), while a
// positive value keeps admitting exactly as before (no regression to
// existing concurrency-cap semantics).
func TestAdmit_MaxConcurrentAgentsZero_PausesBothPools(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name                string
		maxConcurrentAgents int
		wantNormal          bool
		wantAlert           bool
	}{
		{"paused", 0, false, false},
		{"active", 5, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			name := "p-pause-" + tc.name
			proj := &tatarav1alpha1.Project{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
				Spec: tatarav1alpha1.ProjectSpec{
					ScmSecretRef:        name + "-scm",
					MaxConcurrentAgents: tc.maxConcurrentAgents,
					Agent: tatarav1alpha1.AgentSpec{
						Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
						MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
					},
					Queue: &tatarav1alpha1.QueueSpec{Capacity: 5, AlertCapacity: 5},
				},
			}
			mustCreate(t, ctx, proj)
			if tc.maxConcurrentAgents == 0 {
				proj = patchMaxConcurrentAgentsZero(t, ctx, proj)
			}
			nQE := mkQueued(t, ctx, name, 1, tatarav1alpha1.QueueClassNormal, "review")
			aQE := mkQueued(t, ctx, name, 2, tatarav1alpha1.QueueClassAlert, "incident")

			metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())
			r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Metrics: metrics}
			qes, tasks := listQEsTasks(t, ctx, proj.Name)
			if _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
				t.Fatal(err)
			}
			assertQEAdmitted(t, ctx, nQE, tc.wantNormal)
			assertQEAdmitted(t, ctx, aQE, tc.wantAlert)

			pausedMetric := testutil.ToFloat64(metrics.AdmissionBlockedCounter(proj.Name, tatarav1alpha1.QueueClassNormal, "", "project_paused"))
			if tc.maxConcurrentAgents == 0 {
				if pausedMetric != 1 {
					t.Fatalf("admission_blocked{normal,project_paused} = %v, want 1", pausedMetric)
				}
			} else if pausedMetric != 0 {
				t.Fatalf("admission_blocked{normal,project_paused} = %v, want 0 (not paused)", pausedMetric)
			}
		})
	}
}

// TestAdmit_MaxConcurrentAgentsZero_NoTaskCreated confirms the paused project
// leaves zero Task objects behind (not just an un-admitted QueuedEvent).
func TestAdmit_MaxConcurrentAgentsZero_NoTaskCreated(t *testing.T) {
	ctx := context.Background()
	name := "p-pause-notask"
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: name + "-scm",
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
				MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
			},
		},
	}
	mustCreate(t, ctx, proj)
	proj = patchMaxConcurrentAgentsZero(t, ctx, proj)
	mkQueued(t, ctx, name, 1, tatarav1alpha1.QueueClassNormal, "review")

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	_, tasks = listQEsTasks(t, ctx, proj.Name)
	if len(tasks) != 0 {
		t.Fatalf("want 0 Tasks for paused project, got %d", len(tasks))
	}
}
