package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestProbeMemoryRoute_Classification verifies the route-presence classifier:
// 404 is "absent" (route missing -> drifted/stale binary), a transport failure
// is "error" (process down), and any other served status -> "present" (the
// route exists; a 401 auth rejection still proves presence).
func TestProbeMemoryRoute_Classification(t *testing.T) {
	cases := []struct {
		name   string
		status int
		want   string
	}{
		{"unauthenticated-401", http.StatusUnauthorized, "present"},
		{"bad-request-400", http.StatusBadRequest, "present"},
		{"ok-200", http.StatusOK, "present"},
		{"server-error-500", http.StatusInternalServerError, "present"},
		{"not-found-404", http.StatusNotFound, "absent"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()
			got := probeMemoryRoute(context.Background(), srv.Client(), http.MethodPost, srv.URL+"/queries")
			if got != tc.want {
				t.Fatalf("status %d classified %q, want %q", tc.status, got, tc.want)
			}
		})
	}
}

func TestProbeMemoryRoute_TransportFailureIsError(t *testing.T) {
	// A closed server yields a connection-refused transport error.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	client := srv.Client()
	srv.Close()
	if got := probeMemoryRoute(context.Background(), client, http.MethodGet, url+"/code-graph/stats"); got != "error" {
		t.Fatalf("transport failure classified %q, want error", got)
	}
}

// setMemoryPhaseReady forces a Project's status.memory.phase to Ready so
// updateMemoryRetrievalProbe will probe it, without standing up a full stack.
func setMemoryPhaseReady(t *testing.T, name string) {
	t.Helper()
	p := getProject(t, name)
	p.Status.Memory = &tataradevv1alpha1.MemoryStatus{
		Phase:    "Ready",
		Endpoint: memory.Endpoint(name, testNS),
	}
	mustStatusUpdate(t, context.Background(), p)
}

// gatherProbeCounter reads operator_memory_retrieval_probe_total{route,result}.
func gatherProbeCounter(t *testing.T, reg *prometheus.Registry, route, result string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "operator_memory_retrieval_probe_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var r, res string
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "route" {
					r = lp.GetValue()
				}
				if lp.GetName() == "result" {
					res = lp.GetValue()
				}
			}
			if r == route && res == result {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// TestUpdateMemoryRetrievalProbe_UnhealthyIncrementsHealthyClears drives the poll
// loop against a toggleable server: consecutive unhealthy (404) cycles increment
// the per-project run, a healthy (401) cycle clears it, the result is metered,
// and a no-longer-Ready project is pruned from the map.
func TestUpdateMemoryRetrievalProbe_UnhealthyIncrementsHealthyClears(t *testing.T) {
	ctx := logfIntoTestCtx()
	r, reg := newMemoryReconcilerWithReg()

	mkMemoryProject(t, "probe-orch")
	setMemoryPhaseReady(t, "probe-orch")

	var unhealthy int32 = 1
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.LoadInt32(&unhealthy) == 1 {
			w.WriteHeader(http.StatusNotFound) // route absent -> drifted binary
			return
		}
		w.WriteHeader(http.StatusUnauthorized) // route present, auth rejected
	}))
	defer srv.Close()
	r.MemoryHTTP = srv.Client()
	r.MemoryBaseURL = func(string) string { return srv.URL }

	// A stale entry for a project that is not Ready must be pruned each pass.
	r.memoryUnhealthyCycles = map[string]int{"ghost-not-ready": 5}

	r.updateMemoryRetrievalProbe(ctx)
	if got := r.memoryUnhealthyCycles["probe-orch"]; got != 1 {
		t.Fatalf("after pass 1 (unhealthy), cycles = %d, want 1", got)
	}
	if _, ok := r.memoryUnhealthyCycles["ghost-not-ready"]; ok {
		t.Fatal("stale non-Ready project was not pruned from the cycle map")
	}

	r.updateMemoryRetrievalProbe(ctx)
	if got := r.memoryUnhealthyCycles["probe-orch"]; got != 2 {
		t.Fatalf("after pass 2 (unhealthy), cycles = %d, want 2", got)
	}
	// Each unhealthy pass meters both routes as absent; two passes -> >= 2.
	if got := gatherProbeCounter(t, reg, "/queries", "absent"); got < 2 {
		t.Fatalf("probe{/queries,absent} = %v, want >= 2", got)
	}

	// Surface recovers: a healthy cycle clears the run.
	atomic.StoreInt32(&unhealthy, 0)
	r.updateMemoryRetrievalProbe(ctx)
	if _, ok := r.memoryUnhealthyCycles["probe-orch"]; ok {
		t.Fatalf("healthy cycle did not clear the run: cycles = %d", r.memoryUnhealthyCycles["probe-orch"])
	}
	if got := gatherProbeCounter(t, reg, "/queries", "present"); got < 1 {
		t.Fatalf("probe{/queries,present} = %v, want >= 1 after healthy pass", got)
	}
}

// TestUpdateMemoryRetrievalProbe_SkipsNonReady verifies a Provisioning stack is
// never probed (the replica gate is the precondition).
func TestUpdateMemoryRetrievalProbe_SkipsNonReady(t *testing.T) {
	ctx := logfIntoTestCtx()
	r := newMemoryReconciler()

	mkMemoryProject(t, "probe-provisioning")
	// Leave status.memory unset (no phase) -> not Ready.

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()
	r.MemoryHTTP = srv.Client()
	r.MemoryBaseURL = func(string) string { return srv.URL }

	r.updateMemoryRetrievalProbe(ctx)
	if _, ok := r.memoryUnhealthyCycles["probe-provisioning"]; ok {
		t.Fatal("a non-Ready project was probed and tracked")
	}
}

// TestReconcileMemory_FoldsSustainedRetrievalFailure verifies the condition
// fold-in: a replica-Ready stack whose retrieval surface has been unhealthy for
// memoryRetrievalUnhealthyThreshold consecutive cycles reads
// MemoryReady=False/RetrievalUnreachable while phase stays Ready; below the
// threshold it stays Ready/True.
func TestReconcileMemory_FoldsSustainedRetrievalFailure(t *testing.T) {
	ctx := logfIntoTestCtx()
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "fold-retrieval")

	// Apply the stack, then make every replica healthy so phase computes Ready.
	if _, err := r.reconcileMemory(ctx, p); err != nil {
		t.Fatalf("apply reconcile: %v", err)
	}
	fakeStackHealthy(t, p.Name)

	// Healthy retrieval surface (no tracked cycles): MemoryReady True/Ready.
	if _, err := r.reconcileMemory(ctx, p); err != nil {
		t.Fatalf("ready reconcile: %v", err)
	}
	if p.Status.Memory.Phase != "Ready" {
		t.Fatalf("phase = %q, want Ready", p.Status.Memory.Phase)
	}
	if c := apimeta.FindStatusCondition(p.Status.Conditions, "MemoryReady"); c == nil ||
		c.Status != metav1.ConditionTrue || c.Reason != "Ready" {
		t.Fatalf("MemoryReady = %+v, want True/Ready", c)
	}

	// One short of the threshold: still green.
	r.memoryUnhealthyCycles = map[string]int{p.Name: memoryRetrievalUnhealthyThreshold - 1}
	if _, err := r.reconcileMemory(ctx, p); err != nil {
		t.Fatalf("sub-threshold reconcile: %v", err)
	}
	if c := apimeta.FindStatusCondition(p.Status.Conditions, "MemoryReady"); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("MemoryReady at %d cycles = %+v, want still True", memoryRetrievalUnhealthyThreshold-1, c)
	}

	// At the threshold: fold the failure into the condition, phase stays Ready.
	r.memoryUnhealthyCycles[p.Name] = memoryRetrievalUnhealthyThreshold
	if _, err := r.reconcileMemory(ctx, p); err != nil {
		t.Fatalf("threshold reconcile: %v", err)
	}
	if p.Status.Memory.Phase != "Ready" {
		t.Fatalf("phase = %q, want Ready (probe must keep running)", p.Status.Memory.Phase)
	}
	c := apimeta.FindStatusCondition(p.Status.Conditions, "MemoryReady")
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != "RetrievalUnreachable" {
		t.Fatalf("MemoryReady at threshold = %+v, want False/RetrievalUnreachable", c)
	}

	// Recovery: run cleared -> condition returns to True/Ready.
	delete(r.memoryUnhealthyCycles, p.Name)
	if _, err := r.reconcileMemory(ctx, p); err != nil {
		t.Fatalf("recovery reconcile: %v", err)
	}
	if c := apimeta.FindStatusCondition(p.Status.Conditions, "MemoryReady"); c == nil ||
		c.Status != metav1.ConditionTrue || c.Reason != "Ready" {
		t.Fatalf("MemoryReady after recovery = %+v, want True/Ready", c)
	}
}
