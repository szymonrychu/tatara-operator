package controller

// Round 3 audit tests for project_memory.go findings.

import (
	"context"
	"fmt"
	"testing"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/prometheus/client_golang/prometheus"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// clusterGetErrorClient wraps k8sClient and returns a synthetic non-NotFound
// error on the memoryStackHealth Get for a cnpg Cluster object, so the transient
// health error path is exercised without requiring a broken API server. A
// reconcile reads the Cluster twice: first the shrink-guard read in
// applyMemoryStack (which fails open on error), then the health read. The error
// is injected on the second read so it lands on the health path under test.
type clusterGetErrorClient struct {
	client.Client
	clusterGets int
}

func (c *clusterGetErrorClient) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*cnpgv1.Cluster); ok {
		c.clusterGets++
		if c.clusterGets == 2 {
			return fmt.Errorf("synthetic transient API error")
		}
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// gatherCounterValue reads a simple counter by name from reg.
func gatherCounterValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			for _, m := range mf.GetMetric() {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// R3-Finding 1: memory.PgInstances is the single source of truth for the pg
// instance count. The controller's readiness gate must derive the wanted count
// from memory.PgInstances so provisioned instances and the readiness threshold
// are always in lockstep.
func TestMemoryR3F1_PgInstancesSingleSourceOfTruth(t *testing.T) {
	cases := []struct {
		name        string
		pgInstances int
		want        int
	}{
		{"default (nil spec.memory)", 0, 1},
		{"explicit 1", 1, 1},
		{"ha 3", 3, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &tataradevv1alpha1.Project{}
			p.Name = "r3f1-pginstances-" + tc.name
			if tc.pgInstances > 0 {
				p.Spec.Memory = &tataradevv1alpha1.MemorySpec{PgInstances: tc.pgInstances}
			}

			// memory.PgInstances is the function the builder uses for Cluster.Instances
			// AND the controller now uses for the readiness threshold. They must agree.
			got := memory.PgInstances(p)
			if got != tc.want {
				t.Fatalf("memory.PgInstances = %d, want %d", got, tc.want)
			}

			// The readiness check in memoryPhase derives its threshold from the same
			// value via a serving quorum (memoryQuorum). With all instances ready the
			// phase is Ready (all others healthy).
			phase := memoryPhase(got, got, 1, 1, 1)
			if phase != "Ready" {
				t.Fatalf("memoryPhase with readyInstances==wantInstances=%d and all others ready = %q, want Ready", got, phase)
			}

			// Losing one instance must stay Ready as long as a quorum survives: a
			// 3-node HA cluster serving from primary + 1 replica is available (issue #215).
			if got-1 >= memoryQuorum(got) {
				phase2 := memoryPhase(got-1, got, 1, 1, 1)
				if phase2 != "Ready" {
					t.Fatalf("memoryPhase with readyInstances=%d wantInstances=%d = %q, want Ready (quorum %d)", got-1, got, phase2, memoryQuorum(got))
				}
			}

			// Dropping below quorum flips to Provisioning.
			belowQuorum := memoryQuorum(got) - 1
			phase3 := memoryPhase(belowQuorum, got, 1, 1, 1)
			if phase3 != "Provisioning" {
				t.Fatalf("memoryPhase with readyInstances=%d (below quorum %d) wantInstances=%d = %q, want Provisioning", belowQuorum, memoryQuorum(got), got, phase3)
			}
		})
	}
}

// R3-Finding 2: a transient non-NotFound error from memoryStackHealth must
// increment operator_memory_health_read_errors_total so repeated blips are
// visible in Prometheus rather than masquerading as healthy reconciles.
func TestMemoryR3F2_TransientHealthErrorIncrementsCounter(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)

	// First, apply the stack via a normal reconciler so the objects exist.
	normalR := newMemoryReconciler()
	p := mkMemoryProject(t, "r3f2-health-err")
	if _, err := normalR.Reconcile(
		logf.IntoContext(context.Background(), logf.Log),
		ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: p.Name}},
	); err != nil {
		t.Fatalf("prime reconcile: %v", err)
	}

	// Now build a reconciler with an error-injecting client.
	errClient := &clusterGetErrorClient{Client: k8sClient}
	r := &ProjectReconciler{
		Client:  errClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: metrics,
		MemoryConfig: memory.Config{
			Namespace:        testNS,
			MemoryImage:      "harbor.example/tatara-memory:test",
			LightragImage:    "harbor.example/lightrag:test",
			Neo4jImage:       "neo4j:5-community",
			OpenAISecretName: "openai-shared",
			OIDCIssuer:       "https://keycloak.example/realms/tatara",
			OIDCAudience:     "tatara-memory",
		},
	}

	before := gatherCounterValue(t, reg, "operator_memory_health_read_errors_total")

	// Reconcile: the clusterGetErrorClient will inject a non-NotFound error on
	// Get for the Cluster, triggering the transient health error path.
	result, err := r.Reconcile(
		logf.IntoContext(context.Background(), logf.Log),
		ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: p.Name}},
	)
	if err != nil {
		t.Fatalf("transient health error must not propagate as reconcile error, got: %v", err)
	}
	if result.RequeueAfter != memoryRequeue {
		t.Fatalf("transient health error must requeue at memoryRequeue=%v, got %v", memoryRequeue, result.RequeueAfter)
	}

	after := gatherCounterValue(t, reg, "operator_memory_health_read_errors_total")
	if after-before < 1 {
		t.Fatalf("operator_memory_health_read_errors_total must increment on transient health error: before=%.0f after=%.0f", before, after)
	}
}
